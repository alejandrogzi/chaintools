// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chaintools::{Block, OwnedChain, StreamItem, StreamingReader, write_chain_dense};
#[cfg(feature = "parallel")]
use rayon::prelude::ParallelSliceMut;

use super::CliError;

pub(super) const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MAX_OPEN_RUNS: usize = 128;

/// Sort criteria for chain ordering.
///
/// # Variants
///
/// * `Score` - Sort by chain score (descending), tie-breaks by ID
/// * `Id` - Sort by chain ID
/// * `Reference` - Sort by reference sequence name and coordinates
/// * `Query` - Sort by query sequence name and coordinates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SortCriterion {
    Score,
    Id,
    Reference,
    Query,
}

/// Container for sorted chain input data.
///
/// # Variants
///
/// * `InMemory` - All chains fit in memory
/// * `Runs` - Chains spilled to temporary files and need merge
pub(super) enum SortedInput {
    InMemory(Vec<OwnedChain>),
    Runs(Vec<TempRun>),
}

/// Represents a temporary run file for external sorting.
///
/// When memory is insufficient, chains are spilled to temporary files
/// that are later merged. Implements automatic cleanup on drop.
///
/// # Fields
///
/// * `path` - Path to the temporary file
pub(super) struct TempRun {
    path: PathBuf,
}

impl TempRun {
    pub(super) fn create(
        dir: &Path,
        prefix: &str,
        next_temp_id: &mut u64,
    ) -> Result<(Self, File), CliError> {
        for _ in 0..1024 {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = dir.join(format!(
                ".chaintools-sort-{prefix}-{}-{nonce}-{}.tmp",
                std::process::id(),
                *next_temp_id
            ));
            *next_temp_id += 1;

            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => return Ok((Self { path }, file)),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(CliError::Io(err)),
            }
        }

        Err(CliError::Message(format!(
            "failed to create temporary {prefix} file in {}",
            dir.display()
        )))
    }
}

impl Drop for TempRun {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Accumulates chains and manages sorting with optional spilling.
///
/// Handles in-memory sorting when data fits, and manages temporary file
/// creation when memory budget is exceeded. Supports metadata preservation.
///
/// # Fields
///
/// * `sort_by` - The sort criterion to use
/// * `max_in_memory_bytes` - Maximum bytes to keep in memory before spilling
/// * `temp_dir` - Directory for temporary run files
/// * `next_generated_id` - Counter for generated chain IDs
/// * `metadata` - Preserved metadata lines (comments)
/// * `records` - Chains currently in memory
/// * `runs` - Temporary run files for external sorting
/// * `chunk_bytes` - Estimated memory usage of current records
/// * `next_temp_id` - Counter for temporary file names
pub(super) struct SortAccumulator<'a> {
    sort_by: SortCriterion,
    max_in_memory_bytes: u64,
    temp_dir: &'a Path,
    next_generated_id: u64,
    metadata: Vec<Vec<u8>>,
    records: Vec<OwnedChain>,
    runs: Vec<TempRun>,
    chunk_bytes: u64,
    next_temp_id: u64,
}

impl<'a> SortAccumulator<'a> {
    pub(super) fn new(
        sort_by: SortCriterion,
        max_in_memory_bytes: u64,
        temp_dir: &'a Path,
    ) -> Self {
        Self {
            sort_by,
            max_in_memory_bytes,
            temp_dir,
            next_generated_id: 1,
            metadata: Vec::new(),
            records: Vec::new(),
            runs: Vec::new(),
            chunk_bytes: 0,
            next_temp_id: 0,
        }
    }

    pub(super) fn push_stream<R: BufRead>(
        &mut self,
        reader: &mut StreamingReader<R>,
    ) -> Result<(), CliError> {
        reader.set_next_generated_id(self.next_generated_id);

        while let Some(item) = reader.next_item()? {
            match item {
                StreamItem::MetaLine(line) => self.metadata.push(line),
                StreamItem::Header(header) => {
                    let blocks = reader.read_blocks(header.offset)?;
                    let chain = header.into_chain(blocks);
                    self.chunk_bytes = self
                        .chunk_bytes
                        .saturating_add(estimate_chain_bytes(&chain));
                    self.records.push(chain);

                    if self.chunk_bytes >= self.max_in_memory_bytes && !self.records.is_empty() {
                        self.runs.push(spill_records_to_run(
                            &mut self.records,
                            self.sort_by,
                            self.temp_dir,
                            &mut self.next_temp_id,
                        )?);
                        self.chunk_bytes = 0;
                    }
                }
            }
        }

        self.next_generated_id = reader.next_generated_id();
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<(Vec<Vec<u8>>, SortedInput), CliError> {
        if self.runs.is_empty() {
            sort_records(&mut self.records, self.sort_by);
            return Ok((self.metadata, SortedInput::InMemory(self.records)));
        }

        if !self.records.is_empty() {
            self.runs.push(spill_records_to_run(
                &mut self.records,
                self.sort_by,
                self.temp_dir,
                &mut self.next_temp_id,
            )?);
        }

        let reduced = reduce_runs(
            self.runs,
            self.sort_by,
            self.temp_dir,
            &mut self.next_temp_id,
        )?;
        Ok((self.metadata, SortedInput::Runs(reduced)))
    }
}

/// Emits sorted chains to a writer.
///
/// Writes all chains in sorted order, handling both in-memory and
/// run-based sorted inputs.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `sorted` - The sorted input (in-memory or runs)
/// * `sort_by` - The sort criterion used
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub(super) fn emit_sorted_chains<W: Write>(
    writer: &mut W,
    sorted: SortedInput,
    sort_by: SortCriterion,
) -> Result<(), CliError> {
    match sorted {
        SortedInput::InMemory(records) => {
            for chain in &records {
                write_chain_dense(writer, chain)?;
            }
        }
        SortedInput::Runs(runs) => {
            with_merged_runs(&runs, sort_by, |chain| {
                write_chain_dense(writer, chain).map_err(CliError::from)
            })?;
        }
    }
    Ok(())
}

/// Merges multiple sorted runs and emits chains in order.
///
/// Uses a binary heap to perform a k-way merge of sorted runs,
/// yielding chains in sorted order without loading all data into memory.
///
/// # Arguments
///
/// * `runs` - Temporary run files to merge
/// * `sort_by` - The sort criterion for merging
/// * `emit` - Callback function to receive each sorted chain
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub(super) fn with_merged_runs<F>(
    runs: &[TempRun],
    sort_by: SortCriterion,
    mut emit: F,
) -> Result<(), CliError>
where
    F: FnMut(&OwnedChain) -> Result<(), CliError>,
{
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::with_capacity(runs.len());

    for (run_index, run) in runs.iter().enumerate() {
        let mut reader = StreamingReader::from_path(&run.path)?;
        if let Some(chain) = reader.next_chain()? {
            heap.push(MergeHead {
                sort_by,
                run_index,
                chain,
            });
        }
        readers.push(reader);
    }

    while let Some(head) = heap.pop() {
        emit(&head.chain)?;
        if let Some(chain) = readers[head.run_index].next_chain()? {
            heap.push(MergeHead {
                sort_by,
                run_index: head.run_index,
                chain,
            });
        }
    }

    Ok(())
}

/// Writes metadata lines to a writer.
///
/// Writes each preserved metadata line (comment lines starting with #)
/// to the output, adding newline characters.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `metadata` - Vector of metadata lines to write
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub(super) fn write_metadata_lines<W: Write>(
    writer: &mut W,
    metadata: &[Vec<u8>],
) -> Result<(), CliError> {
    for line in metadata {
        writer.write_all(line)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

/// Compares two chains according to the sort criterion.
///
/// Provides a comparison function that sorts chains based on the specified
/// criterion, with consistent tie-breaking behavior.
///
/// # Arguments
///
/// * `a` - First chain to compare
/// * `b` - Second chain to compare
/// * `sort_by` - The sort criterion for comparison
///
/// # Output
///
/// Returns `Ordering` indicating the relative order of the chains
pub(super) fn compare_chains(a: &OwnedChain, b: &OwnedChain, sort_by: SortCriterion) -> Ordering {
    match sort_by {
        SortCriterion::Score => compare_score(a, b),
        SortCriterion::Id => compare_id(a, b),
        SortCriterion::Reference => compare_reference(a, b),
        SortCriterion::Query => compare_query(a, b),
    }
}

/// Reduces runs by merging them in batches.
///
/// Continuously merges runs until the number of runs is at or below MAX_OPEN_RUNS.
/// Each merge reads multiple runs and writes a new merged run to disk.
///
/// # Arguments
///
/// * `runs` - Vector of temporary runs to reduce
/// * `sort_by` - Sort criterion for merging
/// * `temp_dir` - Directory for temporary files
/// * `next_temp_id` - Counter for generating unique temp file names
///
/// # Output
///
/// Returns `Ok(Vec<TempRun>)` with reduced runs
fn reduce_runs(
    mut runs: Vec<TempRun>,
    sort_by: SortCriterion,
    temp_dir: &Path,
    next_temp_id: &mut u64,
) -> Result<Vec<TempRun>, CliError> {
    while runs.len() > MAX_OPEN_RUNS {
        let old_runs = std::mem::take(&mut runs);
        let mut next_runs = Vec::new();
        let mut groups = old_runs.into_iter();

        loop {
            let group: Vec<TempRun> = groups.by_ref().take(MAX_OPEN_RUNS).collect();
            if group.is_empty() {
                break;
            }
            let (merged_run, file) = TempRun::create(temp_dir, "merge", next_temp_id)?;
            let writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file);
            write_merged_runs_to_plain(&group, sort_by, writer)?;
            next_runs.push(merged_run);
        }

        runs = next_runs;
    }

    Ok(runs)
}

/// Spills records to a temporary run file.
///
/// Sorts the records, writes them to a temporary file, and returns the TempRun handle.
///
/// # Arguments
///
/// * `records` - Vector of chains to write
/// * `sort_by` - Sort criterion for ordering
/// * `temp_dir` - Directory for temporary files
/// * `next_temp_id` - Counter for generating unique temp file names
///
/// # Output
///
/// Returns `Ok(TempRun)` with the temporary run file handle
fn spill_records_to_run(
    records: &mut Vec<OwnedChain>,
    sort_by: SortCriterion,
    temp_dir: &Path,
    next_temp_id: &mut u64,
) -> Result<TempRun, CliError> {
    let mut chunk = std::mem::take(records);
    sort_records(&mut chunk, sort_by);

    let (run, file) = TempRun::create(temp_dir, "run", next_temp_id)?;
    let mut writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file);
    for chain in &chunk {
        write_chain_dense(&mut writer, chain)?;
    }
    writer.flush()?;
    Ok(run)
}

/// Writes merged runs to a plain text output file.
///
/// Merges multiple runs using heap-based k-way merge and writes sorted chains.
///
/// # Arguments
///
/// * `runs` - Temporary runs to merge
/// * `sort_by` - Sort criterion for merging
/// * `writer` - Output writer
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn write_merged_runs_to_plain<W: Write>(
    runs: &[TempRun],
    sort_by: SortCriterion,
    mut writer: W,
) -> Result<(), CliError> {
    with_merged_runs(runs, sort_by, |chain| {
        write_chain_dense(&mut writer, chain).map_err(CliError::from)
    })?;
    writer.flush()?;
    Ok(())
}

/// Estimates memory usage for a chain in bytes.
///
/// Calculates approximate memory footprint including the chain struct, names, and blocks.
///
/// # Arguments
///
/// * `chain` - The chain to estimate
///
/// # Output
///
/// Returns estimated byte size
fn estimate_chain_bytes(chain: &OwnedChain) -> u64 {
    (std::mem::size_of::<OwnedChain>()
        + chain.reference_name.len()
        + chain.query_name.len()
        + chain.blocks.len() * std::mem::size_of::<Block>()) as u64
}

/// Sorts records using parallel sorting.
///
/// Uses Rayon's parallel sort for high-performance sorting when the parallel feature is enabled.
///
/// # Arguments
///
/// * `records` - Mutable slice of chains to sort
/// * `sort_by` - Sort criterion
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::sort_core::{sort_records, SortCriterion};
///
/// let mut chains = vec![chain1, chain2, chain3];
/// sort_records(&mut chains, SortCriterion::Score);
/// ```
#[cfg(feature = "parallel")]
fn sort_records(records: &mut [OwnedChain], sort_by: SortCriterion) {
    records.par_sort_unstable_by(|a, b| compare_chains(a, b, sort_by));
}

#[cfg(not(feature = "parallel"))]
/// Sorts records using sequential sorting.
///
/// Uses standard sequential sort when the parallel feature is not enabled.
///
/// # Arguments
///
/// * `records` - Mutable slice of chains to sort
/// * `sort_by` - Sort criterion
fn sort_records(records: &mut [OwnedChain], sort_by: SortCriterion) {
    records.sort_unstable_by(|a, b| compare_chains(a, b, sort_by));
}

/// Compares chains by score, with tie-breakers.
fn compare_score(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    b.score
        .cmp(&a.score)
        .then_with(|| compare_tie_breakers(a, b))
}

/// Compares chains by ID, with additional tie-breakers.
fn compare_id(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.id.cmp(&b.id)
        .then_with(|| compare_non_id_tie_breakers(a, b))
}

/// Compares chains by reference name and position.
fn compare_reference(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    compare_reference_primary(a, b).then_with(|| compare_tie_breakers(a, b))
}

/// Compares chains by query name and position.
fn compare_query(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    compare_query_primary(a, b).then_with(|| compare_tie_breakers(a, b))
}

/// Secondary tie-breaker: compares by ID first, then other non-ID fields.
fn compare_tie_breakers(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.id.cmp(&b.id)
        .then_with(|| compare_non_id_tie_breakers(a, b))
}

/// Non-ID tie-breaker: compares reference full, query full, score, then blocks.
fn compare_non_id_tie_breakers(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    compare_reference_full(a, b)
        .then_with(|| compare_query_full(a, b))
        .then_with(|| b.score.cmp(&a.score))
        .then_with(|| compare_blocks(&a.blocks, &b.blocks))
}

/// Primary reference comparison: name then start position.
fn compare_reference_primary(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.reference_name
        .cmp(&b.reference_name)
        .then_with(|| a.reference_start.cmp(&b.reference_start))
}

/// Primary query comparison: name then start position.
fn compare_query_primary(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.query_name
        .cmp(&b.query_name)
        .then_with(|| a.query_start.cmp(&b.query_start))
}

/// Full reference comparison: name, size, strand, start, then end.
fn compare_reference_full(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.reference_name
        .cmp(&b.reference_name)
        .then_with(|| a.reference_size.cmp(&b.reference_size))
        .then_with(|| strand_to_byte(a.reference_strand).cmp(&strand_to_byte(b.reference_strand)))
        .then_with(|| a.reference_start.cmp(&b.reference_start))
        .then_with(|| a.reference_end.cmp(&b.reference_end))
}

/// Full query comparison: name, size, strand, start, then end.
fn compare_query_full(a: &OwnedChain, b: &OwnedChain) -> Ordering {
    a.query_name
        .cmp(&b.query_name)
        .then_with(|| a.query_size.cmp(&b.query_size))
        .then_with(|| strand_to_byte(a.query_strand).cmp(&strand_to_byte(b.query_strand)))
        .then_with(|| a.query_start.cmp(&b.query_start))
        .then_with(|| a.query_end.cmp(&b.query_end))
}

/// Compares block sequences by iterating and comparing size, gaps.
fn compare_blocks(a: &[Block], b: &[Block]) -> Ordering {
    for (lhs, rhs) in a.iter().zip(b.iter()) {
        let cmp = lhs
            .size
            .cmp(&rhs.size)
            .then_with(|| lhs.gap_reference.cmp(&rhs.gap_reference))
            .then_with(|| lhs.gap_query.cmp(&rhs.gap_query));
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    a.len().cmp(&b.len())
}

/// Merge head for k-way merge algorithm.
///
/// Holds a chain from a run along with metadata for heap-based merging.
///
/// # Fields
///
/// * `sort_by` - Sort criterion for comparison
/// * `run_index` - Index of the run this chain came from
/// * `chain` - The chain data
struct MergeHead {
    sort_by: SortCriterion,
    run_index: usize,
    chain: OwnedChain,
}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_chains(&other.chain, &self.chain, self.sort_by)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeHead {
    fn eq(&self, other: &Self) -> bool {
        self.run_index == other.run_index
            && compare_chains(&self.chain, &other.chain, self.sort_by) == Ordering::Equal
    }
}

impl Eq for MergeHead {}

/// Converts a Strand to a byte representation.
fn strand_to_byte(strand: chaintools::Strand) -> u8 {
    match strand {
        chaintools::Strand::Plus => b'+',
        chaintools::Strand::Minus => b'-',
    }
}
