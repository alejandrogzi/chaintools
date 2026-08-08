// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use chaintools::{ChainError, OwnedChain, Strand, StreamingReader};
use clap::{Args, ValueEnum};
use genepred::writer::TargetFormat;
use genepred::{Bed3, Bed6, Bed12, BedFormat, GenePred, Reader, Strand as BedStrand, Writer};
use rust_lapper::{Interval, Lapper};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const DEFAULT_MIN_MATCH: f64 = 0.95;

/// Command-line arguments for the liftover subcommand.
///
/// Maps intervals given on a chain's reference/target assembly onto its query
/// assembly. No genome sequence is needed: a chain carries all the geometry.
/// For the opposite direction, swap the chain first (`chaintools swap`).
///
/// # Examples
///
/// ```bash
/// chaintools liftover --chain map.chain --bed input.bed --type 6 --output lifted.bed
/// ```
#[derive(Debug, Args)]
pub struct LiftoverArgs {
    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to the .chain file mapping reference/target coordinates onto query coordinates."
    )]
    chain: PathBuf,

    #[arg(
        short = 'b',
        long = "bed",
        value_name = "PATH",
        help = "Path to the input BED file, in reference/target coordinates."
    )]
    bed: PathBuf,

    #[arg(
        long = "type",
        value_name = "WIDTH",
        value_enum,
        help = "BED width of the input, also used for the output. Required so no column is silently ignored."
    )]
    bed_type: BedWidth,

    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        help = "Path for the lifted BED output. If not provided, output is written to standard output."
    )]
    output: Option<PathBuf>,

    #[arg(
        short = 'u',
        long = "unmapped",
        value_name = "PATH",
        help = "Path for records that could not be lifted, each preceded by a reason comment."
    )]
    unmapped: Option<PathBuf>,

    #[arg(
        short = 'm',
        long = "min-match",
        value_name = "FRACTION",
        default_value_t = DEFAULT_MIN_MATCH,
        help = "Minimum fraction of input bases that must fall inside aligned chain blocks (0.0-1.0)."
    )]
    min_match: f64,

    #[arg(
        long = "multiple",
        help = "Emit every qualifying mapping instead of rejecting records that several chains can place."
    )]
    multiple: bool,
}

/// BED widths supported by liftover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BedWidth {
    #[value(name = "3")]
    Three,
    #[value(name = "6")]
    Six,
    #[value(name = "12")]
    Twelve,
}

impl fmt::Display for BedWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BedWidth::Three => f.write_str("3"),
            BedWidth::Six => f.write_str("6"),
            BedWidth::Twelve => f.write_str("12"),
        }
    }
}

/// Runs the liftover subcommand.
///
/// # Arguments
///
/// * `args` - Liftover arguments
/// * `_stdin` - Unused: both inputs are files
/// * `stdout` - Output stream (used if no --output provided)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub fn run<R, W, E>(
    args: LiftoverArgs,
    _stdin: &mut R,
    stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_min_match(args.min_match)?;
    super::ensure_inputs_exist(&[("chain", &args.chain), ("BED", &args.bed)], &[])?;
    validate_output_paths(&args)?;

    log::info!(
        "liftover: chain={}, bed={}, type={}, min_match={}, multiple={}",
        args.chain.display(),
        args.bed.display(),
        args.bed_type,
        args.min_match,
        args.multiple
    );

    let index = LiftoverIndex::build(&args.chain)?;
    log::info!(
        "indexed {} chains over {} reference sequence(s)",
        index.chains.len(),
        index.by_reference.len()
    );
    if index.chains.is_empty() {
        log::warn!("chain file contains no chains: every record will be unmapped");
    }

    let mut unmapped = match &args.unmapped {
        Some(path) => Some(BufWriter::new(File::create(path)?)),
        None => None,
    };

    let stats = if let Some(path) = &args.output {
        let mut writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, File::create(path)?);
        let stats = lift_stream(&args, &index, &mut writer, &mut unmapped)?;
        writer.flush()?;
        stats
    } else {
        let stats = lift_stream(&args, &index, stdout, &mut unmapped)?;
        stdout.flush()?;
        stats
    };

    if let Some(writer) = &mut unmapped {
        writer.flush()?;
    }

    super::log_summary(
        "liftover",
        &[
            ("chains", index.chains.len() as u64),
            ("records", stats.read),
            ("mapped", stats.mapped),
            ("unmapped", stats.unmapped),
            ("mappings", stats.mappings),
        ],
    );
    if stats.multiple > 0 {
        log::info!(
            "{} record(s) produced more than one mapping (--multiple)",
            stats.multiple
        );
    }
    if stats.unmapped > 0 && args.unmapped.is_none() {
        log::warn!(
            "{} record(s) were dropped without being written; pass --unmapped PATH to keep them",
            stats.unmapped
        );
    }
    Ok(())
}

/// Rejects a `--min-match` outside `0.0..=1.0`.
fn validate_min_match(min_match: f64) -> Result<(), CliError> {
    if !(0.0..=1.0).contains(&min_match) {
        return Err(CliError::Message(format!(
            "--min-match must be between 0.0 and 1.0, got {min_match}"
        )));
    }
    Ok(())
}

/// Rejects output paths that would clobber an input or each other.
fn validate_output_paths(args: &LiftoverArgs) -> Result<(), CliError> {
    for (label, path) in [
        ("--output", args.output.as_deref()),
        ("--unmapped", args.unmapped.as_deref()),
    ] {
        if let Some(path) = path {
            super::validate_distinct_paths(label, path, Some(&args.chain))?;
            super::validate_distinct_paths(label, path, Some(&args.bed))?;
        }
    }
    if let (Some(output), Some(unmapped)) = (&args.output, &args.unmapped)
        && output == unmapped
    {
        return Err(CliError::Message(
            "--output and --unmapped must not be the same path".to_owned(),
        ));
    }
    Ok(())
}

/// Per-reference interval index over whole chain target spans.
///
/// ponytail: index whole chain spans, then inspect candidate blocks. If
/// profiling shows candidate traversal dominates on pathological high-overlap
/// chain sets, upgrade the index to aligned-block spans.
#[derive(Debug)]
struct LiftoverIndex {
    chains: Vec<OwnedChain>,
    by_reference: HashMap<Vec<u8>, Lapper<u64, usize>>,
}

impl LiftoverIndex {
    /// Parses the chain file once, validating each chain and indexing its span.
    ///
    /// Chains are kept parsed in memory because every BED record may query any
    /// of them; only the target span plus a `Vec` index goes into the interval
    /// index, so chains are never cloned.
    fn build(path: &Path) -> Result<Self, CliError> {
        let mut reader = StreamingReader::from_path(path)?;
        let mut chains: Vec<OwnedChain> = Vec::new();
        let mut intervals: HashMap<Vec<u8>, Vec<Interval<u64, usize>>> = HashMap::new();
        let mut reference_sizes: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut query_sizes: HashMap<Vec<u8>, u32> = HashMap::new();

        while let Some(header) = reader.next_header()? {
            let offset = header.offset;
            let blocks = reader.read_blocks(offset)?;
            let chain = header.into_chain(blocks);
            validate_chain(&chain, offset)?;
            // Reference and query dictionaries stay separate: the same
            // chromosome name legitimately has different sizes in two
            // assemblies.
            record_size(
                &mut reference_sizes,
                &chain.reference_name,
                chain.reference_size,
                offset,
                "reference",
            )?;
            record_size(
                &mut query_sizes,
                &chain.query_name,
                chain.query_size,
                offset,
                "query",
            )?;

            intervals
                .entry(chain.reference_name.clone())
                .or_default()
                .push(Interval {
                    start: u64::from(chain.reference_start),
                    stop: u64::from(chain.reference_end),
                    val: chains.len(),
                });
            chains.push(chain);
        }

        let by_reference = intervals
            .into_iter()
            .map(|(name, spans)| (name, Lapper::new(spans)))
            .collect();
        Ok(LiftoverIndex {
            chains,
            by_reference,
        })
    }

    /// Returns the indices of chains whose target span overlaps `[start, end)`.
    ///
    /// Order follows the interval index, which sorts by span, so it never
    /// depends on `HashMap` iteration order.
    fn candidates(&self, chrom: &[u8], start: u64, end: u64) -> Vec<usize> {
        match self.by_reference.get(chrom) {
            Some(lapper) => lapper.find(start, end).map(|span| span.val).collect(),
            None => Vec::new(),
        }
    }
}

/// Validates the invariants liftover relies on, at index time.
fn validate_chain(chain: &OwnedChain, offset: usize) -> Result<(), CliError> {
    if chain.reference_strand != Strand::Plus {
        return Err(CliError::Chain(format_error(
            offset,
            "liftover requires chains whose target strand is +; run `chaintools swap` to canonicalize",
        )));
    }
    if chain.reference_start > chain.reference_end {
        return Err(CliError::Chain(format_error(
            offset,
            "chain target span is inverted",
        )));
    }
    if chain.query_start > chain.query_end {
        return Err(CliError::Chain(format_error(
            offset,
            "chain query span is inverted",
        )));
    }
    if chain.reference_end > chain.reference_size {
        return Err(CliError::Chain(format_error(
            offset,
            "chain target span exceeds its sequence size",
        )));
    }
    if chain.query_end > chain.query_size {
        return Err(CliError::Chain(format_error(
            offset,
            "chain query span exceeds its sequence size",
        )));
    }
    super::validate_block_spans(chain, offset)?;
    Ok(())
}

/// Records a chromosome size, rejecting a conflicting one for the same name.
fn record_size(
    sizes: &mut HashMap<Vec<u8>, u32>,
    name: &[u8],
    size: u32,
    offset: usize,
    side: &str,
) -> Result<(), CliError> {
    match sizes.get(name) {
        Some(known) if *known != size => Err(CliError::Chain(format_error(
            offset,
            format!(
                "{side} sequence {} is declared as both {known} and {size} bases",
                String::from_utf8_lossy(name)
            ),
        ))),
        _ => {
            if !sizes.contains_key(name) {
                sizes.insert(name.to_vec(), size);
            }
            Ok(())
        }
    }
}

/// One contiguous piece of a source interval that a chain places on the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MappedSegment {
    source_start: u64,
    source_end: u64,
    query_start: u64,
    query_end: u64,
}

/// A source interval's mapping through one candidate chain.
struct CandidateMapping {
    chain_index: usize,
    mapped_bases: u64,
    /// Mapped segments per source span, parallel to the requested spans.
    spans: Vec<Vec<MappedSegment>>,
}

/// Why a record could not be lifted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnmappedReason {
    Deleted,
    Partial,
    Split,
    Duplicated,
}

impl fmt::Display for UnmappedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnmappedReason::Deleted => f.write_str("Deleted in new"),
            UnmappedReason::Partial => f.write_str("Partially deleted in new"),
            UnmappedReason::Split => f.write_str("Split in new"),
            UnmappedReason::Duplicated => f.write_str("Duplicated in new"),
        }
    }
}

/// Maps one source interval through one chain's aligned blocks.
///
/// Returns one segment per aligned block the interval overlaps, in source
/// order. Query coordinates are returned on the forward query sequence, so a `-`
/// chain yields segments whose query coordinates descend as the source ascends.
///
/// # Arguments
///
/// * `chain` - Candidate chain, target side on `+`
/// * `start` - Source interval start, 0-based
/// * `end` - Source interval end, exclusive
///
/// # Output
///
/// Returns `Ok(Vec<MappedSegment>)`, empty when no aligned block overlaps
fn map_interval_on_chain(
    chain: &OwnedChain,
    start: u64,
    end: u64,
) -> Result<Vec<MappedSegment>, CliError> {
    let query_size = u64::from(chain.query_size);
    let mut segments = Vec::new();
    let mut reference = u64::from(chain.reference_start);
    let mut query = u64::from(chain.query_start);

    for block in &chain.blocks {
        let size = u64::from(block.size);
        let reference_end = reference + size;
        if reference >= end {
            break;
        }

        let overlap_start = start.max(reference);
        let overlap_end = end.min(reference_end);
        if overlap_start < overlap_end {
            let oriented_start = query + (overlap_start - reference);
            let oriented_end = query + (overlap_end - reference);
            let (query_start, query_end) = match chain.query_strand {
                Strand::Plus => (oriented_start, oriented_end),
                // The chain's own query coordinates run along the reverse
                // strand, so flipping them around the chromosome size yields
                // forward coordinates.
                Strand::Minus => (
                    checked_flip(query_size, oriented_end)?,
                    checked_flip(query_size, oriented_start)?,
                ),
            };
            segments.push(MappedSegment {
                source_start: overlap_start,
                source_end: overlap_end,
                query_start,
                query_end,
            });
        }

        reference = reference_end + u64::from(block.gap_reference);
        query = query + size + u64::from(block.gap_query);
    }

    Ok(segments)
}

/// Converts an oriented query coordinate to its forward-strand equivalent.
fn checked_flip(query_size: u64, coordinate: u64) -> Result<u64, CliError> {
    query_size.checked_sub(coordinate).ok_or_else(|| {
        CliError::Message("chain query coordinate exceeds its sequence size".to_owned())
    })
}

/// Evaluates every candidate chain for a record's source spans.
///
/// Coverage counts only source bases that fall inside aligned blocks, never
/// bounding-span overlap, and every span must contribute at least one segment
/// (for BED12 that is the "every block must map" rule).
///
/// # Output
///
/// Returns `(qualifying, touching)`, where `touching` counts candidates that
/// mapped at least one base whether or not they qualified.
fn evaluate_candidates(
    index: &LiftoverIndex,
    chrom: &[u8],
    spans: &[(u64, u64)],
    lookup: (u64, u64),
    min_match: f64,
) -> Result<(Vec<CandidateMapping>, usize), CliError> {
    let source_bases: u64 = spans.iter().map(|(start, end)| end - start).sum();
    let mut qualifying = Vec::new();
    let mut touching = 0usize;

    for chain_index in index.candidates(chrom, lookup.0, lookup.1) {
        let chain = &index.chains[chain_index];
        let mut mapped_spans = Vec::with_capacity(spans.len());
        let mut mapped_bases = 0u64;
        for (start, end) in spans {
            let segments = map_interval_on_chain(chain, *start, *end)?;
            mapped_bases += segments
                .iter()
                .map(|segment| segment.source_end - segment.source_start)
                .sum::<u64>();
            mapped_spans.push(segments);
        }

        if mapped_bases == 0 {
            continue;
        }
        touching += 1;
        if mapped_spans.iter().any(Vec::is_empty) {
            continue;
        }
        // Cross-multiplied so no division is needed at the threshold boundary.
        if (mapped_bases as f64) < min_match * source_bases as f64 {
            continue;
        }
        qualifying.push(CandidateMapping {
            chain_index,
            mapped_bases,
            spans: mapped_spans,
        });
    }

    Ok((qualifying, touching))
}

/// Orders qualifying mappings deterministically for `--multiple` output.
fn sort_mappings(mappings: &mut [CandidateMapping], chains: &[OwnedChain]) {
    mappings.sort_by(|a, b| {
        let left = &chains[a.chain_index];
        let right = &chains[b.chain_index];
        b.mapped_bases
            .cmp(&a.mapped_bases)
            .then_with(|| right.score.cmp(&left.score))
            .then_with(|| left.query_name.cmp(&right.query_name))
            .then_with(|| left.query_start.cmp(&right.query_start))
            .then_with(|| left.id.cmp(&right.id))
            // Chain file order is the final tie-break, so duplicate ids cannot
            // make the output nondeterministic.
            .then_with(|| a.chain_index.cmp(&b.chain_index))
    });
}

/// Running counts for the end-of-run summary.
#[derive(Default)]
struct LiftoverStats {
    read: u64,
    mapped: u64,
    unmapped: u64,
    multiple: u64,
    mappings: u64,
}

/// Streams the BED input through the index at the requested width.
fn lift_stream<W: Write>(
    args: &LiftoverArgs,
    index: &LiftoverIndex,
    output: &mut W,
    unmapped: &mut Option<BufWriter<File>>,
) -> Result<LiftoverStats, CliError> {
    match args.bed_type {
        BedWidth::Three => lift_records::<Bed3, W>(args, index, output, unmapped),
        BedWidth::Six => lift_records::<Bed6, W>(args, index, output, unmapped),
        BedWidth::Twelve => lift_records::<Bed12, W>(args, index, output, unmapped),
    }
}

/// Lifts every BED record of one width, preserving input order.
///
/// One record is held at a time: read, lifted, written, dropped.
fn lift_records<F, W>(
    args: &LiftoverArgs,
    index: &LiftoverIndex,
    output: &mut W,
    unmapped: &mut Option<BufWriter<File>>,
) -> Result<LiftoverStats, CliError>
where
    F: BedFormat + Into<GenePred> + TargetFormat,
    W: Write,
{
    let mut reader = Reader::<F>::from_path(&args.bed).map_err(bed_error)?;
    let mut stats = LiftoverStats::default();
    let mut warned_about_extras = false;

    for record in reader.records() {
        let record = record.map_err(bed_error)?;
        stats.read += 1;
        if !warned_about_extras && !record.extras().is_empty() {
            // Trailing columns are copied through verbatim, which is right for
            // BED6+N/BED12+N annotations but wrong if the user understated the
            // width and a coordinate or strand column is sitting in there.
            log::warn!(
                "BED input has columns beyond --type {}; they are copied through unlifted",
                args.bed_type
            );
            warned_about_extras = true;
        }

        match lift_record(index, &record, args)? {
            Ok(lifted) => {
                stats.mapped += 1;
                stats.mappings += lifted.len() as u64;
                if lifted.len() > 1 {
                    stats.multiple += 1;
                }
                for record in &lifted {
                    write_record::<F, _>(record, output)?;
                }
            }
            Err(reason) => {
                stats.unmapped += 1;
                if let Some(writer) = unmapped {
                    writeln!(writer, "# {reason}")?;
                    write_record::<F, _>(&record, writer)?;
                }
            }
        }
    }

    Ok(stats)
}

/// Lifts one BED record, returning either its mappings or why it failed.
fn lift_record(
    index: &LiftoverIndex,
    record: &GenePred,
    args: &LiftoverArgs,
) -> Result<Result<Vec<GenePred>, UnmappedReason>, CliError> {
    let spans = source_spans(record, args.bed_type)?;
    let lookup = (record.start, record.end);
    let (mut qualifying, touching) =
        evaluate_candidates(index, &record.chrom, &spans, lookup, args.min_match)?;

    if qualifying.is_empty() {
        return Ok(Err(match touching {
            0 => UnmappedReason::Deleted,
            1 => UnmappedReason::Partial,
            _ => UnmappedReason::Split,
        }));
    }
    if qualifying.len() > 1 && !args.multiple {
        return Ok(Err(UnmappedReason::Duplicated));
    }

    sort_mappings(&mut qualifying, &index.chains);
    let mut lifted = Vec::with_capacity(qualifying.len());
    for mapping in &qualifying {
        let chain = &index.chains[mapping.chain_index];
        match args.bed_type {
            BedWidth::Three | BedWidth::Six => {
                lifted.push(lift_interval_record(record, chain, &mapping.spans[0])?);
            }
            BedWidth::Twelve => match lift_block_record(record, chain, &mapping.spans)? {
                Some(record) => lifted.push(record),
                // The thick interval could not be placed consistently; the
                // record is rejected rather than clamped.
                None => return Ok(Err(UnmappedReason::Partial)),
            },
        }
    }
    Ok(Ok(lifted))
}

/// Returns the source spans whose coverage decides a record's mapping.
///
/// BED3/BED6 contribute their single interval; BED12 contributes one span per
/// block, so intronic bases never count toward coverage.
fn source_spans(record: &GenePred, bed_type: BedWidth) -> Result<Vec<(u64, u64)>, CliError> {
    if record.start >= record.end {
        return Err(CliError::Message(format!(
            "BED record {}:{}-{} is empty or inverted; liftover requires start < end",
            String::from_utf8_lossy(&record.chrom),
            record.start,
            record.end
        )));
    }

    if bed_type != BedWidth::Twelve {
        return Ok(vec![(record.start, record.end)]);
    }

    let (Some(starts), Some(ends)) = (record.block_starts(), record.block_ends()) else {
        return Err(CliError::Message(format!(
            "BED12 record {}:{}-{} has no blocks",
            String::from_utf8_lossy(&record.chrom),
            record.start,
            record.end
        )));
    };
    if starts.len() != ends.len() || starts.is_empty() {
        return Err(CliError::Message(format!(
            "BED12 record {}:{}-{} has inconsistent block arrays",
            String::from_utf8_lossy(&record.chrom),
            record.start,
            record.end
        )));
    }

    let mut spans = Vec::with_capacity(starts.len());
    for (start, end) in starts.iter().zip(ends) {
        if start >= end {
            return Err(CliError::Message(format!(
                "BED12 record {}:{}-{} has an empty block",
                String::from_utf8_lossy(&record.chrom),
                record.start,
                record.end
            )));
        }
        spans.push((*start, *end));
    }
    Ok(spans)
}

/// Builds a lifted BED3/BED6 record spanning the mapped extent.
///
/// A source interval crossing a chain gap still yields one output interval:
/// chain blocks are not exposed as separate BED records.
fn lift_interval_record(
    record: &GenePred,
    chain: &OwnedChain,
    segments: &[MappedSegment],
) -> Result<GenePred, CliError> {
    let (start, end) = mapped_extent(segments)?;
    let mut lifted = record.clone();
    lifted.chrom = chain.query_name.clone();
    lifted.start = start;
    lifted.end = end;
    lifted.strand = lift_strand(record.strand, chain.query_strand);
    Ok(lifted)
}

/// Builds a lifted BED12 record, projecting every block and the thick interval.
///
/// Returns `Ok(None)` when the thick interval cannot be placed inside the lifted
/// record, which rejects the record instead of clamping it.
fn lift_block_record(
    record: &GenePred,
    chain: &OwnedChain,
    spans: &[Vec<MappedSegment>],
) -> Result<Option<GenePred>, CliError> {
    let mut blocks = Vec::with_capacity(spans.len());
    for segments in spans {
        blocks.push(mapped_extent(segments)?);
    }
    // Transcript order reverses through a `-` chain while BED block arrays stay
    // ascending by genomic coordinate.
    blocks.sort_unstable();

    for pair in blocks.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(CliError::Message(format!(
                "lifted blocks of {} overlap on {}",
                String::from_utf8_lossy(record.name().unwrap_or(b".")),
                String::from_utf8_lossy(&chain.query_name)
            )));
        }
    }

    let start = blocks[0].0;
    let end = blocks[blocks.len() - 1].1;
    let (thick_start, thick_end) = match lift_thick(record, chain, start)? {
        Some(thick) => thick,
        None => return Ok(None),
    };
    if thick_start < start || thick_end > end {
        return Ok(None);
    }

    let mut lifted = record.clone();
    lifted.chrom = chain.query_name.clone();
    lifted.start = start;
    lifted.end = end;
    lifted.strand = lift_strand(record.strand, chain.query_strand);
    lifted.thick_start = Some(thick_start);
    lifted.thick_end = Some(thick_end);
    lifted.block_count = Some(u32::try_from(blocks.len()).map_err(|_| {
        CliError::Message("lifted record has too many blocks for BED12".to_owned())
    })?);
    // The writer derives blockSizes and blockStarts from these absolute
    // coordinates, so source blockStarts are never reused.
    lifted.block_starts = Some(blocks.iter().map(|(start, _)| *start).collect());
    lifted.block_ends = Some(blocks.iter().map(|(_, end)| *end).collect());
    Ok(Some(lifted))
}

/// Maps a BED12 thick interval through the same chain as its blocks.
///
/// Returns `Ok(None)` when a non-empty thick interval has no mapped segment.
fn lift_thick(
    record: &GenePred,
    chain: &OwnedChain,
    lifted_start: u64,
) -> Result<Option<(u64, u64)>, CliError> {
    let thick_start = record.thick_start.unwrap_or(record.start);
    let thick_end = record.thick_end.unwrap_or(record.end);

    if thick_start >= thick_end {
        // ponytail: a zero-width thick interval (the noncoding convention,
        // thickStart == thickEnd == chromStart) collapses to the lifted record
        // start. If interior zero-width thick regions ever need their exact
        // lifted boundary, map the adjacent base instead.
        return Ok(Some((lifted_start, lifted_start)));
    }

    let segments = map_interval_on_chain(chain, thick_start, thick_end)?;
    if segments.is_empty() {
        return Ok(None);
    }
    Ok(Some(mapped_extent(&segments)?))
}

/// Returns the extent covered by mapped segments on the forward query strand.
fn mapped_extent(segments: &[MappedSegment]) -> Result<(u64, u64), CliError> {
    let start = segments
        .iter()
        .map(|segment| segment.query_start)
        .min()
        .ok_or_else(|| CliError::Message("no mapped segments to place".to_owned()))?;
    let end = segments
        .iter()
        .map(|segment| segment.query_end)
        .max()
        .expect("non-empty segments have a maximum");
    Ok((start, end))
}

/// Transforms a BED strand through a chain's query strand.
///
/// A `+` chain preserves the input strand; a `-` chain flips it. An absent or
/// unknown strand is never given a direction.
fn lift_strand(input: Option<BedStrand>, chain_query: Strand) -> Option<BedStrand> {
    match (input, chain_query) {
        (Some(BedStrand::Forward), Strand::Minus) => Some(BedStrand::Reverse),
        (Some(BedStrand::Reverse), Strand::Minus) => Some(BedStrand::Forward),
        (other, _) => other,
    }
}

fn write_record<F, W>(record: &GenePred, writer: &mut W) -> Result<(), CliError>
where
    F: TargetFormat,
    W: Write,
{
    Writer::<F>::from_record(record, writer).map_err(|err| CliError::Message(err.to_string()))
}

fn bed_error(err: genepred::reader::ReaderError) -> CliError {
    CliError::Message(format!("invalid BED input: {err}"))
}

fn format_error(offset: usize, message: impl Into<String>) -> ChainError {
    ChainError::Format {
        offset,
        msg: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Parser)]
    struct LiftoverHarness {
        #[command(flatten)]
        args: LiftoverArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    /// One 100 bp block, `+/+`: chr1:100-200 maps to qry1:500-600.
    const CHAIN_PLUS: &str = "chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n100\n\n";
    /// The same block with a `-` query: oriented 500-600 is forward 1400-1500.
    const CHAIN_MINUS: &str = "chain 200 chr1 1000 + 100 200 qry1 2000 - 500 600 2\n100\n\n";
    /// Two blocks with a gap on both sides: chr1:100-150 and chr1:200-250.
    const CHAIN_GAP: &str =
        "chain 300 chr1 1000 + 100 250 qry1 2000 + 500 640 3\n50\t50\t40\n50\n\n";
    /// A target-only gap: chr1:100-150 and chr1:200-250 stay contiguous on qry1.
    const CHAIN_REF_GAP: &str =
        "chain 310 chr1 1000 + 100 250 qry1 2000 + 500 600 5\n50\t50\t0\n50\n\n";
    /// A query-only gap: chr1:100-200 is contiguous but qry1 jumps 40 bases.
    const CHAIN_QUERY_GAP: &str =
        "chain 320 chr1 1000 + 100 200 qry1 2000 + 500 640 6\n50\t0\t40\n50\n\n";
    /// A chain on another reference sequence.
    const CHAIN_CHR2: &str = "chain 400 chr2 800 + 0 100 qry2 900 + 0 100 4\n100\n\n";

    /// A 1:1 chain over chr1:0-1000, offsetting coordinates by +2000.
    const CHAIN_WIDE_PLUS: &str =
        "chain 1000 chr1 10000 + 0 1000 qry1 10000 + 2000 3000 10\n1000\n\n";
    /// The same region with a `-` query: source [a,b) becomes [8000-b, 8000-a).
    const CHAIN_WIDE_MINUS: &str =
        "chain 1000 chr1 10000 + 0 1000 qry1 10000 - 2000 3000 11\n1000\n\n";

    /// A transcript with two 100 bp exons and a coding region of 150-450.
    const BED12_TWO_EXONS: &str =
        "chr1\t100\t500\ttx1\t742\t+\t150\t450\t255,0,0\t2\t100,100\t0,300\n";

    struct Fixture {
        dir: PathBuf,
        chain: PathBuf,
        bed: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn fixture(chain: &str, bed: &str) -> Fixture {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chaintools-liftover-test-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let chain_path = dir.join("map.chain");
        let bed_path = dir.join("input.bed");
        std::fs::write(&chain_path, chain).expect("write chain");
        std::fs::write(&bed_path, bed).expect("write bed");
        Fixture {
            dir,
            chain: chain_path,
            bed: bed_path,
        }
    }

    fn args_for(
        fixture: &Fixture,
        bed_type: BedWidth,
        min_match: f64,
        multiple: bool,
    ) -> LiftoverArgs {
        LiftoverArgs {
            chain: fixture.chain.clone(),
            bed: fixture.bed.clone(),
            bed_type,
            output: None,
            unmapped: Some(fixture.dir.join("unmapped.bed")),
            min_match,
            multiple,
        }
    }

    /// Lifts the fixture and returns `(output lines, unmapped file lines)`.
    fn lift(
        fixture: &Fixture,
        bed_type: BedWidth,
        min_match: f64,
        multiple: bool,
    ) -> (Vec<String>, Vec<String>) {
        let args = args_for(fixture, bed_type, min_match, multiple);
        let unmapped_path = args.unmapped.clone().expect("unmapped path");
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(args, &mut stdin, &mut stdout, &mut stderr).expect("liftover run");

        let output = String::from_utf8(stdout).expect("utf8 output");
        let unmapped = std::fs::read_to_string(&unmapped_path).unwrap_or_default();
        (lines(&output), lines(&unmapped))
    }

    fn lines(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    fn lift_error(fixture: &Fixture, bed_type: BedWidth, min_match: f64) -> String {
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            args_for(fixture, bed_type, min_match, false),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("liftover should fail")
        .to_string()
    }

    fn parse_chain(text: &str) -> OwnedChain {
        StreamingReader::new(Cursor::new(text.as_bytes()))
            .next_chain()
            .expect("chain parses")
            .expect("one chain")
    }

    fn map(chain: &str, start: u64, end: u64) -> Vec<MappedSegment> {
        map_interval_on_chain(&parse_chain(chain), start, end).expect("mapping succeeds")
    }

    #[test]
    fn parses_minimal_args() {
        let cli = LiftoverHarness::try_parse_from([
            "chaintools",
            "--chain",
            "map.chain",
            "--bed",
            "in.bed",
            "--type",
            "6",
        ])
        .expect("liftover arguments should parse");
        assert_eq!(cli.args.bed_type, BedWidth::Six);
        assert_eq!(cli.args.min_match, DEFAULT_MIN_MATCH);
        assert!(!cli.args.multiple);
        assert!(cli.args.unmapped.is_none());
    }

    #[test]
    fn requires_chain_bed_and_type() {
        for missing in [
            vec!["chaintools", "--bed", "in.bed", "--type", "3"],
            vec!["chaintools", "--chain", "map.chain", "--type", "3"],
            vec!["chaintools", "--chain", "map.chain", "--bed", "in.bed"],
        ] {
            let err = LiftoverHarness::try_parse_from(missing)
                .expect_err("incomplete arguments should be rejected");
            assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        }
    }

    #[test]
    fn rejects_unsupported_bed_width() {
        let err = LiftoverHarness::try_parse_from([
            "chaintools",
            "--chain",
            "map.chain",
            "--bed",
            "in.bed",
            "--type",
            "9",
        ])
        .expect_err("BED9 is not supported");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    // --- Milestone 1: index -------------------------------------------------

    fn index_of(chain: &str) -> LiftoverIndex {
        let fixture = fixture(chain, "");
        LiftoverIndex::build(&fixture.chain).expect("index builds")
    }

    #[test]
    fn index_returns_only_chains_on_the_queried_reference() {
        let index = index_of(&format!("{CHAIN_PLUS}{CHAIN_CHR2}"));
        assert_eq!(index.chains.len(), 2);
        assert_eq!(index.candidates(b"chr1", 100, 200), vec![0]);
        assert_eq!(index.candidates(b"chr2", 0, 100), vec![1]);
        assert!(index.candidates(b"chr3", 0, 100).is_empty());
    }

    #[test]
    fn index_returns_no_candidates_outside_any_chain_span() {
        let index = index_of(CHAIN_PLUS);
        assert!(index.candidates(b"chr1", 900, 950).is_empty());
        // Half-open spans: touching the boundary is not an overlap.
        assert!(index.candidates(b"chr1", 200, 300).is_empty());
        assert!(index.candidates(b"chr1", 50, 100).is_empty());
    }

    #[test]
    fn index_returns_every_overlapping_chain() {
        let index = index_of(&format!("{CHAIN_PLUS}{CHAIN_GAP}"));
        let mut candidates = index.candidates(b"chr1", 120, 130);
        candidates.sort_unstable();
        assert_eq!(candidates, vec![0, 1]);
    }

    #[test]
    fn index_rejects_conflicting_chromosome_sizes() {
        let conflicting =
            format!("{CHAIN_PLUS}chain 100 chr1 999 + 100 200 qry1 2000 + 700 800 9\n100\n\n");
        let fixture = fixture(&conflicting, "");
        let err = LiftoverIndex::build(&fixture.chain)
            .expect_err("conflicting reference sizes should be rejected")
            .to_string();
        assert!(err.contains("declared as both 1000 and 999"), "{err}");
    }

    #[test]
    fn index_allows_one_name_to_differ_between_assemblies() {
        // chr1 is 1000 bases on the reference and 2000 on the query: two
        // assemblies, not a conflict.
        let index = index_of("chain 100 chr1 1000 + 100 200 chr1 2000 + 500 600 1\n100\n\n");
        assert_eq!(index.chains.len(), 1);
    }

    #[test]
    fn index_rejects_a_minus_target_strand() {
        let fixture = fixture(
            "chain 100 chr1 1000 - 100 200 qry1 2000 + 500 600 1\n100\n\n",
            "",
        );
        let err = LiftoverIndex::build(&fixture.chain)
            .expect_err("minus target strand should be rejected")
            .to_string();
        assert!(err.contains("target strand is +"), "{err}");
    }

    #[test]
    fn index_rejects_blocks_that_disagree_with_the_header() {
        let fixture = fixture(
            "chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n50\n\n",
            "",
        );
        let err = LiftoverIndex::build(&fixture.chain)
            .expect_err("short block list should be rejected")
            .to_string();
        assert!(err.contains("do not sum to the target span"), "{err}");
    }

    // --- Milestone 2: interval mapper --------------------------------------

    #[test]
    fn maps_a_full_block_on_the_plus_strand() {
        assert_eq!(
            map(CHAIN_PLUS, 100, 200),
            vec![MappedSegment {
                source_start: 100,
                source_end: 200,
                query_start: 500,
                query_end: 600
            }]
        );
    }

    #[test]
    fn maps_a_partial_block_on_the_plus_strand() {
        assert_eq!(
            map(CHAIN_PLUS, 120, 150),
            vec![MappedSegment {
                source_start: 120,
                source_end: 150,
                query_start: 520,
                query_end: 550
            }]
        );
    }

    #[test]
    fn maps_a_full_block_on_the_minus_strand() {
        assert_eq!(
            map(CHAIN_MINUS, 100, 200),
            vec![MappedSegment {
                source_start: 100,
                source_end: 200,
                query_start: 1400,
                query_end: 1500
            }]
        );
    }

    #[test]
    fn maps_a_partial_block_on_the_minus_strand() {
        // Oriented [520, 550) flips to forward [2000-550, 2000-520).
        assert_eq!(
            map(CHAIN_MINUS, 120, 150),
            vec![MappedSegment {
                source_start: 120,
                source_end: 150,
                query_start: 1450,
                query_end: 1480
            }]
        );
    }

    #[test]
    fn an_interval_inside_one_block_keeps_its_length_on_both_strands() {
        for chain in [CHAIN_PLUS, CHAIN_MINUS] {
            for (start, end) in [(100, 200), (120, 150), (199, 200), (100, 101)] {
                let segments = map(chain, start, end);
                assert_eq!(segments.len(), 1, "{chain}");
                let segment = segments[0];
                assert_eq!(
                    segment.query_end - segment.query_start,
                    end - start,
                    "length changed for {start}..{end}"
                );
            }
        }
    }

    #[test]
    fn negative_strand_mapping_satisfies_the_flip_invariant() {
        let chain = parse_chain(CHAIN_MINUS);
        let query_size = u64::from(chain.query_size);
        for (start, end) in [(100, 200), (120, 150), (100, 101), (150, 200)] {
            let segments = map(CHAIN_MINUS, start, end);
            let segment = segments[0];
            // The oriented interval is the chain's own query coordinate space.
            let oriented_start = u64::from(chain.query_start) + (start - 100);
            let oriented_end = u64::from(chain.query_start) + (end - 100);
            assert_eq!(segment.query_start, query_size - oriented_end);
            assert_eq!(segment.query_end, query_size - oriented_start);
            assert_eq!(segment.query_end - segment.query_start, end - start);
        }
    }

    #[test]
    fn maps_across_a_target_only_gap() {
        let segments = map(CHAIN_REF_GAP, 100, 250);
        assert_eq!(segments.len(), 2);
        assert_eq!((segments[0].query_start, segments[0].query_end), (500, 550));
        assert_eq!((segments[1].query_start, segments[1].query_end), (550, 600));
    }

    #[test]
    fn maps_across_a_query_only_gap() {
        let segments = map(CHAIN_QUERY_GAP, 100, 200);
        assert_eq!(segments.len(), 2);
        assert_eq!((segments[0].query_start, segments[0].query_end), (500, 550));
        assert_eq!((segments[1].query_start, segments[1].query_end), (590, 640));
    }

    #[test]
    fn maps_across_a_dual_sided_gap_without_covering_it() {
        let segments = map(CHAIN_GAP, 100, 250);
        assert_eq!(segments.len(), 2);
        assert_eq!(
            (segments[0].source_start, segments[0].source_end),
            (100, 150)
        );
        assert_eq!(
            (segments[1].source_start, segments[1].source_end),
            (200, 250)
        );
        let mapped: u64 = segments
            .iter()
            .map(|segment| segment.source_end - segment.source_start)
            .sum();
        // The 50 unaligned target bases in the gap are not mapped.
        assert_eq!(mapped, 100);
    }

    #[test]
    fn an_interval_inside_a_gap_maps_nothing() {
        assert!(map(CHAIN_GAP, 150, 200).is_empty());
    }

    #[test]
    fn block_boundaries_are_half_open() {
        assert_eq!(map(CHAIN_GAP, 100, 150).len(), 1);
        assert_eq!(map(CHAIN_GAP, 200, 250).len(), 1);
        // Ends exactly where the first block ends, starts where the second does.
        assert_eq!(map(CHAIN_GAP, 140, 150)[0].query_end, 550);
        assert_eq!(map(CHAIN_GAP, 200, 210)[0].query_start, 590);
    }

    // --- Milestone 2: BED3 -------------------------------------------------

    #[test]
    fn lifts_a_bed3_interval_through_a_plus_chain() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t120\t150\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert_eq!(output, vec!["qry1\t520\t550"]);
        assert!(unmapped.is_empty());
    }

    #[test]
    fn lifts_a_bed3_interval_through_a_minus_chain() {
        let fixture = fixture(CHAIN_MINUS, "chr1\t120\t150\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert_eq!(output, vec!["qry1\t1450\t1480"]);
    }

    #[test]
    fn a_bed3_interval_crossing_a_gap_stays_one_record() {
        let fixture = fixture(CHAIN_GAP, "chr1\t100\t250\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 0.6, false);
        // One output interval spanning the mapped extent, not two records.
        assert_eq!(output, vec!["qry1\t500\t640"]);
    }

    #[test]
    fn reports_an_interval_with_no_chain_as_deleted() {
        let fixture = fixture(CHAIN_PLUS, "chr9\t100\t150\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty());
        assert_eq!(unmapped, vec!["# Deleted in new", "chr9\t100\t150"]);
    }

    #[test]
    fn reports_an_interval_inside_a_gap_as_deleted() {
        let fixture = fixture(CHAIN_GAP, "chr1\t150\t200\n");
        let (_, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert_eq!(unmapped[0], "# Deleted in new");
    }

    #[test]
    fn reports_a_single_insufficient_chain_as_partially_deleted() {
        let fixture = fixture(CHAIN_GAP, "chr1\t100\t250\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty());
        assert_eq!(unmapped[0], "# Partially deleted in new");
    }

    #[test]
    fn reports_several_insufficient_chains_as_split() {
        // Two chains each cover half of chr1:100-300, neither on its own.
        let chains = concat!(
            "chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n100\n\n",
            "chain 100 chr1 1000 + 200 300 qry2 2000 + 700 800 2\n100\n\n"
        );
        let fixture = fixture(chains, "chr1\t100\t300\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty());
        assert_eq!(unmapped[0], "# Split in new");
    }

    #[test]
    fn never_stitches_two_chains_into_one_mapping() {
        // The same two half-covering chains must not combine into 100%.
        let chains = concat!(
            "chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n100\n\n",
            "chain 100 chr1 1000 + 200 300 qry1 2000 + 600 700 2\n100\n\n"
        );
        let fixture = fixture(chains, "chr1\t100\t300\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty(), "{output:?}");
        assert_eq!(unmapped[0], "# Split in new");
        // Each chain alone covers only half, so a lower threshold reports them
        // as competing candidates rather than a single stitched mapping.
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.5, false);
        assert!(output.is_empty(), "{output:?}");
        assert_eq!(unmapped[0], "# Duplicated in new");
    }

    // --- Milestone 3: min-match -------------------------------------------

    /// A chain covering `covered` of the 100 bases at chr1:100-200.
    fn partial_chain(covered: u32) -> String {
        format!(
            "chain 100 chr1 1000 + 100 {} qry1 2000 + 500 {} 1\n{covered}\n\n",
            100 + covered,
            500 + covered
        )
    }

    #[test]
    fn min_match_accepts_full_coverage() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t100\t200\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 1.0, false);
        assert_eq!(output, vec!["qry1\t500\t600"]);
    }

    #[test]
    fn min_match_boundary_is_inclusive() {
        let fixture = fixture(&partial_chain(95), "chr1\t100\t200\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert_eq!(output, vec!["qry1\t500\t595"], "95% must pass at 0.95");
    }

    #[test]
    fn min_match_rejects_one_base_below_the_threshold() {
        let fixture = fixture(&partial_chain(94), "chr1\t100\t200\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty(), "94% must fail at 0.95");
        assert_eq!(unmapped[0], "# Partially deleted in new");
        // The same chain passes once the threshold drops to 0.94.
        let (output, _) = lift(&fixture, BedWidth::Three, 0.94, false);
        assert_eq!(output, vec!["qry1\t500\t594"]);
    }

    #[test]
    fn min_match_counts_aligned_blocks_not_the_chain_span() {
        // CHAIN_GAP spans chr1:100-250 but aligns only 100 of those 150 bases.
        let fixture = fixture(CHAIN_GAP, "chr1\t100\t250\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 0.66, false);
        assert_eq!(output, vec!["qry1\t500\t640"]);
        let (output, _) = lift(&fixture, BedWidth::Three, 0.67, false);
        assert!(output.is_empty(), "bounding-span overlap must not count");
    }

    #[test]
    fn min_match_zero_still_requires_one_mapped_base() {
        let fixture = fixture(CHAIN_GAP, "chr1\t150\t200\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.0, false);
        assert!(output.is_empty());
        assert_eq!(unmapped[0], "# Deleted in new");
    }

    #[test]
    fn rejects_an_out_of_range_min_match() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t100\t200\n");
        for value in [-0.1, 1.1] {
            let err = lift_error(&fixture, BedWidth::Three, value);
            assert!(
                err.contains("--min-match must be between 0.0 and 1.0"),
                "{err}"
            );
        }
    }

    // --- Milestone 3: BED6 ------------------------------------------------

    #[test]
    fn lifts_bed6_preserving_name_and_flipping_strand() {
        let fixture = fixture(CHAIN_MINUS, "chr1\t120\t150\tfeature\t742\t+\n");
        let (output, _) = lift(&fixture, BedWidth::Six, 0.95, false);
        // Score is not preserved: genepred's GenePred does not carry it.
        assert_eq!(output, vec!["qry1\t1450\t1480\tfeature\t0\t-"]);
    }

    #[test]
    fn lifts_every_strand_combination() {
        for (chain, input, expected) in [
            (CHAIN_PLUS, "+", "+"),
            (CHAIN_PLUS, "-", "-"),
            (CHAIN_MINUS, "+", "-"),
            (CHAIN_MINUS, "-", "+"),
            (CHAIN_PLUS, ".", "."),
            (CHAIN_MINUS, ".", "."),
        ] {
            let fixture = fixture(chain, &format!("chr1\t120\t150\tf\t0\t{input}\n"));
            let (output, _) = lift(&fixture, BedWidth::Six, 0.95, false);
            let strand = output[0].split('\t').nth(5).expect("strand column");
            assert_eq!(strand, expected, "{input} through {chain}");
        }
    }

    // --- Milestone 4: multiple mappings -----------------------------------

    /// Two chains that both place chr1:100-200 in full, on different targets.
    const CHAIN_DUPLICATES: &str = concat!(
        "chain 500 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n100\n\n",
        "chain 900 chr1 1000 + 100 200 qry2 2000 + 300 400 2\n100\n\n"
    );

    #[test]
    fn duplicate_mappings_are_rejected_by_default() {
        let fixture = fixture(CHAIN_DUPLICATES, "chr1\t100\t200\n");
        let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert!(output.is_empty());
        assert_eq!(unmapped, vec!["# Duplicated in new", "chr1\t100\t200"]);
    }

    #[test]
    fn multiple_emits_every_mapping_deterministically() {
        let fixture = fixture(CHAIN_DUPLICATES, "chr1\t100\t200\n");
        let expected = vec!["qry2\t300\t400".to_owned(), "qry1\t500\t600".to_owned()];
        // Equal mapped_bases, so the higher chain score wins the tie-break.
        for _ in 0..5 {
            let (output, unmapped) = lift(&fixture, BedWidth::Three, 0.95, true);
            assert_eq!(output, expected, "output order must not vary");
            assert!(unmapped.is_empty());
        }
    }

    #[test]
    fn multiple_orders_by_mapped_bases_first() {
        // The lower-scoring chain maps more bases, so it must come first.
        let chains = concat!(
            "chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1\n100\n\n",
            "chain 900 chr1 1000 + 100 190 qry2 2000 + 300 390 2\n90\n\n"
        );
        let fixture = fixture(chains, "chr1\t100\t200\n");
        let (output, _) = lift(&fixture, BedWidth::Three, 0.9, true);
        assert_eq!(output, vec!["qry1\t500\t600", "qry2\t300\t390"]);
    }

    #[test]
    fn input_order_is_preserved() {
        let fixture = fixture(
            CHAIN_PLUS,
            "chr1\t150\t160\nchr1\t120\t130\nchr1\t100\t110\n",
        );
        let (output, _) = lift(&fixture, BedWidth::Three, 0.95, false);
        assert_eq!(
            output,
            vec!["qry1\t550\t560", "qry1\t520\t530", "qry1\t500\t510"]
        );
    }

    // --- Milestone 5: BED12 ----------------------------------------------

    #[test]
    fn lifts_bed12_through_a_plus_chain() {
        let fixture = fixture(CHAIN_WIDE_PLUS, BED12_TWO_EXONS);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 0.95, false);
        assert_eq!(
            output,
            vec!["qry1\t2100\t2500\ttx1\t0\t+\t2150\t2450\t0,0,0\t2\t100,100,\t0,300,"]
        );
    }

    #[test]
    fn lifts_bed12_through_a_minus_chain_reversing_blocks() {
        let fixture = fixture(CHAIN_WIDE_MINUS, BED12_TWO_EXONS);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 0.95, false);
        // Exons swap genomic order, blockStarts are recomputed, strand flips.
        assert_eq!(
            output,
            vec!["qry1\t7500\t7900\ttx1\t0\t-\t7550\t7850\t0,0,0\t2\t100,100,\t0,300,"]
        );
        assert_bed12_invariants(&output[0]);
    }

    #[test]
    fn a_query_insertion_inside_an_exon_grows_that_block() {
        // 5 query bases are inserted 150 bases into the transcript's first exon.
        let chain = "chain 1 chr1 10000 + 0 1000 qry1 10000 + 2000 3005 12\n150\t0\t5\n850\n\n";
        let fixture = fixture(chain, BED12_TWO_EXONS);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 0.95, false);
        assert_eq!(
            output,
            vec!["qry1\t2100\t2505\ttx1\t0\t+\t2155\t2455\t0,0,0\t2\t105,100,\t0,305,"]
        );
        assert_bed12_invariants(&output[0]);
    }

    #[test]
    fn a_chain_gap_inside_an_intron_does_not_reduce_coverage() {
        // The chain drops chr1:250-350, which is entirely intronic, so both
        // exons still map in full and the record passes at min_match 1.0.
        let chain = "chain 1 chr1 10000 + 0 1000 qry1 10000 + 2000 2900 13\n250\t100\t0\n650\n\n";
        let fixture = fixture(chain, BED12_TWO_EXONS);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 1.0, false);
        assert_eq!(
            output,
            vec!["qry1\t2100\t2400\ttx1\t0\t+\t2150\t2350\t0,0,0\t2\t100,100,\t0,200,"]
        );
    }

    #[test]
    fn a_completely_unmapped_exon_rejects_the_record() {
        // The chain ends before the second exon.
        let chain = "chain 1 chr1 10000 + 0 250 qry1 10000 + 2000 2250 14\n250\n\n";
        let fixture = fixture(chain, BED12_TWO_EXONS);
        let (output, unmapped) = lift(&fixture, BedWidth::Twelve, 0.5, false);
        assert!(output.is_empty(), "every block must contribute: {output:?}");
        assert_eq!(unmapped[0], "# Partially deleted in new");
    }

    #[test]
    fn lifts_a_three_exon_transcript() {
        let bed = "chr1\t100\t700\ttx3\t0\t+\t100\t700\t0,0,0\t3\t100,100,100\t0,300,500\n";
        let fixture = fixture(CHAIN_WIDE_PLUS, bed);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 1.0, false);
        assert_eq!(
            output,
            vec!["qry1\t2100\t2700\ttx3\t0\t+\t2100\t2700\t0,0,0\t3\t100,100,100,\t0,300,500,"]
        );
        assert_bed12_invariants(&output[0]);
    }

    #[test]
    fn preserves_a_zero_width_thick_interval() {
        // The noncoding convention: thickStart == thickEnd == chromStart.
        let bed = "chr1\t100\t500\tnc1\t0\t+\t100\t100\t0,0,0\t2\t100,100\t0,300\n";
        let fixture = fixture(CHAIN_WIDE_PLUS, bed);
        let (output, _) = lift(&fixture, BedWidth::Twelve, 0.95, false);
        let fields: Vec<&str> = output[0].split('\t').collect();
        assert_eq!((fields[6], fields[7]), ("2100", "2100"));
    }

    #[test]
    fn rejects_a_record_whose_thick_interval_cannot_be_placed() {
        // Both exons map, but the coding region falls entirely in a chain gap.
        let chain = "chain 1 chr1 10000 + 0 1000 qry1 10000 + 2000 2800 15\n200\t200\t0\n600\n\n";
        let bed = "chr1\t100\t500\ttx4\t0\t+\t250\t350\t0,0,0\t2\t100,100\t0,300\n";
        let fixture = fixture(chain, bed);
        let (output, unmapped) = lift(&fixture, BedWidth::Twelve, 0.95, false);
        assert!(output.is_empty(), "{output:?}");
        assert_eq!(unmapped[0], "# Partially deleted in new");
    }

    /// Checks the structural BED12 invariants on one output line.
    fn assert_bed12_invariants(line: &str) {
        let fields: Vec<&str> = line.split('\t').collect();
        let start: u64 = fields[1].parse().expect("chromStart");
        let end: u64 = fields[2].parse().expect("chromEnd");
        let count: usize = fields[9].parse().expect("blockCount");
        let sizes: Vec<u64> = parse_list(fields[10]);
        let starts: Vec<u64> = parse_list(fields[11]);

        assert_eq!(count, sizes.len(), "blockCount vs blockSizes: {line}");
        assert_eq!(count, starts.len(), "blockCount vs blockStarts: {line}");
        let mut previous_end = start;
        for (offset, size) in starts.iter().zip(&sizes) {
            let block_start = start + offset;
            let block_end = block_start + size;
            assert!(block_start >= start, "block before chromStart: {line}");
            assert!(block_end <= end, "block past chromEnd: {line}");
            assert!(block_start >= previous_end, "blocks out of order: {line}");
            previous_end = block_end;
        }
    }

    fn parse_list(field: &str) -> Vec<u64> {
        field
            .split(',')
            .filter(|item| !item.is_empty())
            .map(|item| item.parse().expect("numeric list entry"))
            .collect()
    }

    // --- Error handling ---------------------------------------------------

    #[test]
    fn rejects_a_zero_width_bed_interval() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t150\t150\n");
        let err = lift_error(&fixture, BedWidth::Three, 0.95);
        assert!(err.contains("liftover requires start < end"), "{err}");
    }

    #[test]
    fn rejects_output_paths_that_collide_with_inputs() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t100\t200\n");
        for output in [fixture.chain.clone(), fixture.bed.clone()] {
            let mut args = args_for(&fixture, BedWidth::Three, 0.95, false);
            args.output = Some(output);
            let mut stdin = Cursor::new(Vec::<u8>::new());
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let err = run(args, &mut stdin, &mut stdout, &mut stderr)
                .expect_err("clobbering an input should be rejected")
                .to_string();
            assert!(err.contains("must not be the same path"), "{err}");
        }
    }

    #[test]
    fn rejects_the_same_path_for_output_and_unmapped() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t100\t200\n");
        let mut args = args_for(&fixture, BedWidth::Three, 0.95, false);
        let shared = fixture.dir.join("both.bed");
        args.output = Some(shared.clone());
        args.unmapped = Some(shared);
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(args, &mut stdin, &mut stdout, &mut stderr)
            .expect_err("one file cannot be both outputs")
            .to_string();
        assert!(err.contains("--output and --unmapped"), "{err}");
    }

    #[test]
    fn missing_inputs_are_rejected_up_front() {
        let fixture = fixture(CHAIN_PLUS, "chr1\t100\t200\n");
        let mut args = args_for(&fixture, BedWidth::Three, 0.95, false);
        args.bed = fixture.dir.join("absent.bed");
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(args, &mut stdin, &mut stdout, &mut stderr)
            .expect_err("a missing BED should be rejected")
            .to_string();
        assert!(err.contains("BED file does not exist"), "{err}");
    }

    #[test]
    fn unmapped_records_are_dropped_only_when_no_path_is_given() {
        let fixture = fixture(CHAIN_PLUS, "chr9\t100\t150\n");
        let mut args = args_for(&fixture, BedWidth::Three, 0.95, false);
        args.unmapped = None;
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(args, &mut stdin, &mut stdout, &mut stderr).expect("liftover run");
        assert!(stdout.is_empty());
    }
}
