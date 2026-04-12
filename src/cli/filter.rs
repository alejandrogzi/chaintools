// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::str::FromStr;

use chaintools::{Block, ChainError, OwnedChainHeader, Strand, StreamingReader};
use clap::{Args, ValueEnum};
#[cfg(feature = "gzip")]
use flate2::{write::GzEncoder, Compression};

use super::CliError;

/// Command-line arguments for the filter subcommand.
///
/// Provides filtering options for chain files including
/// score thresholds, name filtering, coordinate ranges, and more.
///
/// # Examples
///
/// ```bash
/// chaintools filter --chain input.chain --min-score 500 --query-names chr1,chr2
/// ```
#[derive(Debug, Args)]
pub struct FilterArgs {
    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to .chain file to filter. If not provided, chain data is read from standard input.",
        value_delimiter = ' ',
        num_args = 1..,
    )]
    chains: Vec<PathBuf>,

    #[arg(
        short = 'o',
        long = "out-chain",
        value_name = "PATH",
        help = "Path for the filtered chain output. If not provided, output is written to standard output."
    )]
    out_chain: Option<PathBuf>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress filtered chain output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        short = 'Q',
        long = "query-names",
        value_name = "NAMES",
        help = "Restrict query side to sequences named"
    )]
    query_names: Option<NameSet>,

    #[arg(
        short = 'X',
        long = "exclude-query-names",
        value_name = "NAMES",
        help = "Restrict query side to sequences not named"
    )]
    excluded_query_names: Option<NameSet>,

    #[arg(
        short = 'T',
        long = "target-names",
        value_name = "NAMES",
        help = "Restrict target side to sequences named"
    )]
    target_names: Option<NameSet>,

    #[arg(
        short = 'K',
        long = "exclude-target-names",
        value_name = "NAMES",
        help = "Restrict target side to sequences not named"
    )]
    excluded_target_names: Option<NameSet>,

    #[arg(
        short = 'i',
        long = "chain-id",
        value_name = "ID",
        help = "Only get chains with this ID"
    )]
    id: Option<NonZeroU64>,

    #[arg(
        short = 'm',
        long = "min-score",
        value_name = "SCORE",
        help = "Restrict to chains scoring at least N"
    )]
    min_score: Option<u64>,

    #[arg(
        short = 'M',
        long = "max-score",
        value_name = "SCORE",
        help = "Restrict to chains scoring less than N"
    )]
    max_score: Option<u64>,

    #[arg(
        short = 'R',
        long = "min-query-start",
        value_name = "POSITION",
        help = "Restrict to chains with query start at least N"
    )]
    query_start_min: Option<u64>,

    #[arg(
        short = 'E',
        long = "max-query-start",
        value_name = "POSITION",
        help = "Restrict to chains with query start less than N"
    )]
    query_start_max: Option<u64>,

    #[arg(
        short = 'r',
        long = "min-query-end",
        value_name = "POSITION",
        help = "Restrict to chains with query end at least N"
    )]
    query_end_min: Option<u64>,

    #[arg(
        short = 'e',
        long = "max-query-end",
        value_name = "POSITION",
        help = "Restrict to chains with query end less than N"
    )]
    query_end_max: Option<u64>,

    #[arg(
        short = 'S',
        long = "min-target-start",
        value_name = "POSITION",
        help = "Restrict to chains with target start at least N"
    )]
    target_start_min: Option<u64>,

    #[arg(
        short = 's',
        long = "max-target-start",
        value_name = "POSITION",
        help = "Restrict to chains with target start less than N"
    )]
    target_start_max: Option<u64>,

    #[arg(
        short = 'H',
        long = "min-target-end",
        value_name = "POSITION",
        help = "Restrict to chains with target end at least N"
    )]
    target_end_min: Option<u64>,

    #[arg(
        short = 'j',
        long = "max-target-end",
        value_name = "POSITION",
        help = "Restrict to chains with target end less than N"
    )]
    target_end_max: Option<u64>,

    #[arg(
        short = 'D',
        long = "query-overlap-start",
        value_name = "POSITION",
        help = "Restrict to chains where query overlaps region starting here"
    )]
    query_overlap_start: Option<u64>,

    #[arg(
        short = 'd',
        long = "query-overlap-end",
        value_name = "POSITION",
        help = "Restrict to chains where query overlaps region ending here"
    )]
    query_overlap_end: Option<u64>,

    #[arg(
        short = 'F',
        long = "target-overlap-start",
        value_name = "POSITION",
        help = "Restrict to chains where target overlaps region starting here"
    )]
    target_overlap_start: Option<u64>,

    #[arg(
        short = 'f',
        long = "target-overlap-end",
        value_name = "POSITION",
        help = "Restrict to chains where target overlaps region ending here"
    )]
    target_overlap_end: Option<u64>,

    #[arg(
        short = 'V',
        long = "query-strand",
        value_parser = parse_query_strand,
        value_name = "STRAND",
        help = "Restrict filtering to strand (to + or -)"
    )]
    query_strand: Option<u8>,

    #[arg(long = "output-format", default_value_t = OutputFormat::Dense, value_enum, help = "Output in long format")]
    output_format: OutputFormat,

    #[arg(long = "merge-zero-gaps", help = "Get rid of gaps of length zero")]
    zero_gap: bool,

    #[arg(
        long = "min-gapless-block",
        value_name = "BASES",
        help = "Pass chains with minimum gapless block of at least N"
    )]
    min_gapless: Option<u64>,

    #[arg(
        long = "min-query-gap",
        value_name = "BASES",
        help = "Pass chains with minimum query gap size of at least N"
    )]
    query_min_gap: Option<u64>,

    #[arg(
        long = "min-target-gap",
        value_name = "BASES",
        help = "Pass chains with minimum target gap size of at least N"
    )]
    target_min_gap: Option<u64>,

    #[arg(
        long = "max-query-gap",
        value_name = "BASES",
        help = "Pass chains with maximum query gap size no larger than N"
    )]
    query_max_gap: Option<u64>,

    #[arg(
        long = "max-target-gap",
        value_name = "BASES",
        help = "Pass chains with maximum target gap size no larger than N"
    )]
    target_max_gap: Option<u64>,

    #[arg(
        long = "min-query-span",
        value_name = "BASES",
        help = "Minimum size of spanned query region"
    )]
    query_min_size: Option<u64>,

    #[arg(
        long = "max-query-span",
        value_name = "BASES",
        help = "Maximum size of spanned query region"
    )]
    query_max_size: Option<u64>,

    #[arg(
        long = "min-target-span",
        value_name = "BASES",
        help = "Minimum size of spanned target region"
    )]
    target_min_size: Option<u64>,

    #[arg(
        long = "max-target-span",
        value_name = "BASES",
        help = "Maximum size of spanned target region"
    )]
    target_max_size: Option<u64>,

    #[arg(
        long = "exclude-random",
        help = "Suppress chains involving '_random' chromosomes"
    )]
    no_random: bool,

    #[arg(
        long = "exclude-haplotype",
        help = "Suppress chains involving '_hap|_alt' chromosomes"
    )]
    no_hap: bool,
}

impl FilterArgs {
    /// Returns true if output goes to stdout.
    pub(crate) fn writes_to_stdout(&self) -> bool {
        self.out_chain.is_none()
    }

    /// Returns default log level based on output.
    pub(crate) fn default_log_level(&self) -> log::LevelFilter {
        if self.out_chain.is_some() {
            log::LevelFilter::Info
        } else {
            log::LevelFilter::Off
        }
    }
}

#[derive(Debug, Clone)]
struct NameSet {
    names: HashSet<Vec<u8>>,
}

impl FromStr for NameSet {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_comma_string(value))
    }
}

impl NameSet {
    /// Creates a NameSet from a comma-separated string.
    fn from_comma_string(value: &str) -> Self {
        let mut names = HashSet::new();
        let bytes = value.as_bytes();
        let mut start = 0usize;
        while start < bytes.len() {
            let end = bytes[start..]
                .iter()
                .position(|&b| b == b',')
                .map_or(bytes.len(), |rel| start + rel);
            names.insert(bytes[start..end].to_vec());
            start = end.saturating_add(1);
        }
        Self { names }
    }

    /// Checks if a name is in the set.
    fn contains(&self, name: &[u8]) -> bool {
        self.names.contains(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Dense,
    Long,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Dense => f.write_str("dense"),
            OutputFormat::Long => f.write_str("long"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CBlock {
    target_start: u64,
    target_end: u64,
    query_start: u64,
    query_end: u64,
}

/// Runs the filter subcommand.
///
/// Processes chain files and outputs filtered chains based on score, coordinates,
/// names, and other criteria.
///
/// # Arguments
///
/// * `args` - Filter arguments with filtering criteria
/// * `stdin` - Input stream (used if no --chain provided)
/// * `stdout` - Output stream (used if no --out-chain provided)
/// * `stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
///
/// # Examples
///
/// ```bash
/// # Filter by minimum score
/// chaintools filter --chain input.chain --out-chain filtered.chain --min-score 500
///
/// # Filter by query name and strand
/// chaintools filter --chain input.chain --query-names chr1 --query-strand +
/// ```
pub fn run<R, W, E>(
    args: FilterArgs,
    stdin: &mut R,
    stdout: &mut W,
    stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    log::info!("Started filtering chains");
    validate_output_args(&args)?;

    if let Some(path) = &args.out_chain {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(1024 * 1024, file);
        if args.gzip {
            run_gzip_output(&args, stdin, writer, stderr)?;
        } else {
            let mut writer = writer;
            run_to_writer(&args, stdin, &mut writer, stderr)?;
            writer.flush()?;
        }
    } else if args.gzip {
        run_gzip_output(&args, stdin, stdout, stderr)?;
    } else {
        run_to_writer(&args, stdin, stdout, stderr)?;
    }

    log::info!("Finished filtering chains");
    Ok(())
}

/// Validates output arguments for filter command.
///
/// Checks that gzip output is only used when the gzip feature is enabled.
///
/// # Arguments
///
/// * `args` - Filter command arguments
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` if invalid
#[cfg(feature = "gzip")]
fn validate_output_args(args: &FilterArgs) -> Result<(), CliError> {
    validate_output_path(args)
}

#[cfg(not(feature = "gzip"))]
/// Validates output arguments for filter command.
///
/// Returns an error if gzip is requested but the gzip feature is not enabled.
fn validate_output_args(args: &FilterArgs) -> Result<(), CliError> {
    validate_output_path(args)?;
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

/// Validates that output path differs from input paths.
fn validate_output_path(args: &FilterArgs) -> Result<(), CliError> {
    if let Some(output) = &args.out_chain {
        if args.chains.iter().any(|input| input == output) {
            return Err(CliError::Message(
                "--out-chain must not be the same path as an input --chain".to_owned(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "gzip")]
/// Writes filtered chains with gzip compression.
///
/// Wraps the output writer in a gzip encoder for compressed output.
fn run_gzip_output<R, W, E>(
    args: &FilterArgs,
    stdin: &mut R,
    writer: W,
    stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut writer = GzEncoder::new(writer, Compression::fast());
    run_to_writer(args, stdin, &mut writer, stderr)?;
    writer.try_finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

#[cfg(not(feature = "gzip"))]
/// Writes filtered chains with gzip compression (disabled without gzip feature).
fn run_gzip_output<R, W, E>(
    _args: &FilterArgs,
    _stdin: &mut R,
    _writer: W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    Err(CliError::Message(
        "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
    ))
}

/// Routes to appropriate writer and processes input.
fn run_to_writer<R, W, E>(
    args: &FilterArgs,
    stdin: &mut R,
    stdout: &mut W,
    stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut zero_gap_merge_count = 0usize;
    let mut next_generated_id = 1u64;

    if args.chains.is_empty() {
        let mut reader = StreamingReader::new(stdin);
        process_reader(
            args,
            &mut reader,
            stdout,
            &mut next_generated_id,
            &mut zero_gap_merge_count,
        )?;
    } else {
        for input in &args.chains {
            let mut reader = StreamingReader::from_path(input)?;
            process_reader(
                args,
                &mut reader,
                stdout,
                &mut next_generated_id,
                &mut zero_gap_merge_count,
            )?;
        }
    }

    if args.zero_gap {
        writeln!(stderr, "{zero_gap_merge_count} zero length gaps eliminated")?;
    }
    Ok(())
}

/// Processes chains from a streaming reader.
fn process_reader<R, W>(
    args: &FilterArgs,
    reader: &mut StreamingReader<R>,
    stdout: &mut W,
    next_generated_id: &mut u64,
    zero_gap_merge_count: &mut usize,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    reader.set_next_generated_id(*next_generated_id);
    while let Some(header) = reader.next_header()? {
        reserve_negative_score_warning(&header);
        validate_header(&header)?;

        let passes_header = passes_header_filters(args, &header);
        if !passes_header && !args.zero_gap {
            reader.skip_blocks()?;
            continue;
        }

        let blocks = reader.read_blocks(header.offset)?;
        if args.zero_gap || (passes_header && args.output_format == OutputFormat::Long) {
            let mut absolute = absolute_blocks(&header, &blocks);
            validate_block_span(&header, &absolute)?;
            if args.zero_gap {
                *zero_gap_merge_count += merge_adjacent_blocks_ucsc(&mut absolute);
            }
            if passes_header && passes_block_filters_absolute(args, &absolute) {
                write_chain_absolute(stdout, &header, &absolute, args.output_format)?;
            }
        } else {
            validate_block_span_from_blocks(&header, &blocks)?;
            if passes_block_filters_blocks(args, &blocks) {
                write_chain_dense(stdout, &header, &blocks)?;
            }
        }
    }
    *next_generated_id = reader.next_generated_id();
    Ok(())
}

/// Parses a strand string to get the first byte.
fn parse_query_strand(value: &str) -> Result<u8, String> {
    Ok(value.as_bytes().first().copied().unwrap_or(0))
}

/// Logs a warning for negative scores (placeholder for future logging).
fn reserve_negative_score_warning(header: &OwnedChainHeader) {
    if header.score < 0 {
        log::warn!("chain id {} has negative score {}", header.id, header.score);
    }
}

/// Validates chain header coordinates.
///
/// Checks that start < end, coordinates don't exceed sequence sizes,
/// and chain ID is positive.
///
/// # Arguments
///
/// * `header` - Chain header to validate
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` on validation failure
fn validate_header(header: &OwnedChainHeader) -> Result<(), CliError> {
    if header.query_start >= header.query_end || header.reference_start >= header.reference_end {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain end must be greater than start".into(),
        }));
    }
    if header.query_end > header.query_size || header.reference_end > header.reference_size {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain extends past sequence size".into(),
        }));
    }
    if header.id == 0 {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain id must be positive".into(),
        }));
    }
    Ok(())
}

/// Validates absolute block coordinates against header.
///
/// Checks that the last block ends match the header coordinates.
///
/// # Arguments
///
/// * `header` - Chain header
/// * `blocks` - Absolute blocks
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` on validation failure
fn validate_block_span(header: &OwnedChainHeader, blocks: &[CBlock]) -> Result<(), CliError> {
    let Some(last) = blocks.last() else {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain without any alignment blocks".into(),
        }));
    };
    if last.query_end != u64::from(header.query_end)
        || last.target_end != u64::from(header.reference_end)
    {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain block coordinates do not match header end".into(),
        }));
    }
    Ok(())
}

/// Validates block coordinates from raw blocks.
///
/// Walks through blocks to verify they end at the header coordinates.
///
/// # Arguments
///
/// * `header` - Chain header
/// * `blocks` - Raw blocks
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` on validation failure
fn validate_block_span_from_blocks(
    header: &OwnedChainHeader,
    blocks: &[Block],
) -> Result<(), CliError> {
    if blocks.is_empty() {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain without any alignment blocks".into(),
        }));
    }

    let mut query = u64::from(header.query_start);
    let mut target = u64::from(header.reference_start);
    for (ix, block) in blocks.iter().enumerate() {
        let size = u64::from(block.size);
        query += size;
        target += size;
        if ix + 1 < blocks.len() {
            query += u64::from(block.gap_query);
            target += u64::from(block.gap_reference);
        }
    }

    if query != u64::from(header.query_end) || target != u64::from(header.reference_end) {
        return Err(CliError::Chain(ChainError::Format {
            offset: header.offset,
            msg: "chain block coordinates do not match header end".into(),
        }));
    }
    Ok(())
}

/// Converts raw blocks to absolute coordinates.
///
/// Adds gaps to compute absolute start/end coordinates for each block.
///
/// # Arguments
///
/// * `header` - Chain header
/// * `blocks` - Raw blocks
///
/// # Output
///
/// Returns vector of blocks with absolute coordinates
fn absolute_blocks(header: &OwnedChainHeader, blocks: &[Block]) -> Vec<CBlock> {
    let mut query = u64::from(header.query_start);
    let mut target = u64::from(header.reference_start);
    let mut absolute = Vec::with_capacity(blocks.len());

    for block in blocks {
        let size = u64::from(block.size);
        absolute.push(CBlock {
            target_start: target,
            target_end: target + size,
            query_start: query,
            query_end: query + size,
        });
        target += size + u64::from(block.gap_reference);
        query += size + u64::from(block.gap_query);
    }

    absolute
}

/// Merges adjacent blocks with zero gap (matches UCSC behavior).
///
/// Combines consecutive blocks where the end of one equals the start of the next.
///
/// # Arguments
///
/// * `blocks` - Absolute blocks
///
/// # Output
///
/// Returns number of blocks merged
fn merge_adjacent_blocks_ucsc(blocks: &mut Vec<CBlock>) -> usize {
    let mut kept = Vec::with_capacity(blocks.len());
    let mut last: Option<usize> = None;
    let mut merge_count = 0usize;

    for ix in 0..blocks.len() {
        match last {
            None => kept.push(ix),
            Some(last_ix)
                if blocks[last_ix].query_end != blocks[ix].query_start
                    || blocks[last_ix].target_end != blocks[ix].target_start =>
            {
                kept.push(ix);
            }
            Some(last_ix) => {
                blocks[last_ix].query_end = blocks[ix].query_end;
                blocks[last_ix].target_end = blocks[ix].target_end;
                merge_count += 1;
            }
        }
        last = Some(ix);
    }

    let merged = kept.into_iter().map(|ix| blocks[ix]).collect();
    *blocks = merged;
    merge_count
}

/// Checks if header passes all header-level filters.
///
/// # Arguments
///
/// * `args` - Filter arguments
/// * `header` - Chain header
///
/// # Output
///
/// Returns true if all filters pass
fn passes_header_filters(args: &FilterArgs, header: &OwnedChainHeader) -> bool {
    let query_start = u64::from(header.query_start);
    let query_end = u64::from(header.query_end);
    let target_start = u64::from(header.reference_start);
    let target_end = u64::from(header.reference_end);
    let query_size = query_end - query_start;
    let target_size = target_end - target_start;

    if args
        .query_names
        .as_ref()
        .is_some_and(|names| !names.contains(&header.query_name))
    {
        return false;
    }
    if args
        .excluded_query_names
        .as_ref()
        .is_some_and(|names| names.contains(&header.query_name))
    {
        return false;
    }
    if args
        .target_names
        .as_ref()
        .is_some_and(|names| !names.contains(&header.reference_name))
    {
        return false;
    }
    if args
        .excluded_target_names
        .as_ref()
        .is_some_and(|names| names.contains(&header.reference_name))
    {
        return false;
    }
    if args
        .min_score
        .is_some_and(|min| score_is_below_min(header.score, min))
        || args
            .max_score
            .is_some_and(|max| score_is_at_least_max(header.score, max))
    {
        return false;
    }
    if args.query_start_min.is_some_and(|min| query_start < min)
        || args.query_start_max.is_some_and(|max| query_start >= max)
    {
        return false;
    }
    if args.query_end_min.is_some_and(|min| query_end < min)
        || args.query_end_max.is_some_and(|max| query_end >= max)
    {
        return false;
    }
    if args.target_start_min.is_some_and(|min| target_start < min)
        || args.target_start_max.is_some_and(|max| target_start >= max)
    {
        return false;
    }
    if args.target_end_min.is_some_and(|min| target_end < min)
        || args.target_end_max.is_some_and(|max| target_end >= max)
    {
        return false;
    }
    if args
        .query_overlap_start
        .is_some_and(|start| query_end < start)
        || args.query_overlap_end.is_some_and(|end| query_start >= end)
    {
        return false;
    }
    if args
        .target_overlap_start
        .is_some_and(|start| target_end < start)
        || args
            .target_overlap_end
            .is_some_and(|end| target_start >= end)
    {
        return false;
    }
    if args.query_min_size.is_some_and(|min| query_size < min)
        || args.target_min_size.is_some_and(|min| target_size < min)
    {
        return false;
    }
    if args.query_max_size.is_some_and(|max| query_size > max)
        || args.target_max_size.is_some_and(|max| target_size > max)
    {
        return false;
    }
    if args
        .query_strand
        .is_some_and(|strand| strand != strand_to_byte(header.query_strand))
    {
        return false;
    }
    if args.id.is_some_and(|id| header.id != id.get()) {
        return false;
    }
    if args.no_random
        && (header.reference_name.ends_with(b"_random") || header.query_name.ends_with(b"_random"))
    {
        return false;
    }
    if args.no_hap && (is_haplotype(&header.reference_name) || is_haplotype(&header.query_name)) {
        return false;
    }

    true
}

/// Checks if score is below the minimum threshold.
///
/// Returns true if score is negative or below the minimum threshold.
fn score_is_below_min(score: i64, min: u64) -> bool {
    score < 0 || (score as u64) < min
}

/// Checks if score is at or above the maximum threshold.
fn score_is_at_least_max(score: i64, max: u64) -> bool {
    score >= 0 && (score as u64) >= max
}

/// Checks if raw blocks pass block-level filters.
///
/// # Arguments
///
/// * `args` - Filter arguments
/// * `blocks` - Raw blocks
///
/// # Output
///
/// Returns true if all filters pass
fn passes_block_filters_blocks(args: &FilterArgs, blocks: &[Block]) -> bool {
    if args
        .min_gapless
        .is_some_and(|min| max_gapless_blocks(blocks) < min)
    {
        return false;
    }
    if args
        .query_min_gap
        .is_some_and(|min| query_max_gap_blocks(blocks) < min)
    {
        return false;
    }
    if args
        .target_min_gap
        .is_some_and(|min| target_max_gap_blocks(blocks) < min)
    {
        return false;
    }
    if args
        .query_max_gap
        .is_some_and(|max| query_max_gap_blocks(blocks) > max)
    {
        return false;
    }
    if args
        .target_max_gap
        .is_some_and(|max| target_max_gap_blocks(blocks) > max)
    {
        return false;
    }

    true
}

/// Checks if absolute blocks pass block-level filters.
///
/// # Arguments
///
/// * `args` - Filter arguments
/// * `blocks` - Absolute blocks
///
/// # Output
///
/// Returns true if all filters pass
fn passes_block_filters_absolute(args: &FilterArgs, blocks: &[CBlock]) -> bool {
    if args
        .min_gapless
        .is_some_and(|min| max_gapless(blocks) < min)
    {
        return false;
    }
    if args
        .query_min_gap
        .is_some_and(|min| query_max_gap(blocks) < min)
    {
        return false;
    }
    if args
        .target_min_gap
        .is_some_and(|min| target_max_gap(blocks) < min)
    {
        return false;
    }
    if args
        .query_max_gap
        .is_some_and(|max| query_max_gap(blocks) > max)
    {
        return false;
    }
    if args
        .target_max_gap
        .is_some_and(|max| target_max_gap(blocks) > max)
    {
        return false;
    }

    true
}

/// Returns the maximum gapless block size from raw blocks.
/// Maximum gapless block size from raw blocks.
fn max_gapless_blocks(blocks: &[Block]) -> u64 {
    blocks
        .iter()
        .map(|block| u64::from(block.size))
        .max()
        .unwrap_or(0)
}

/// Returns the maximum query gap size from raw blocks.
/// Maximum query gap from raw blocks.
fn query_max_gap_blocks(blocks: &[Block]) -> u64 {
    blocks
        .iter()
        .take(blocks.len().saturating_sub(1))
        .map(|block| u64::from(block.gap_query))
        .max()
        .unwrap_or(0)
}

/// Returns the maximum target gap size from raw blocks.
/// Maximum target gap from raw blocks.
fn target_max_gap_blocks(blocks: &[Block]) -> u64 {
    blocks
        .iter()
        .take(blocks.len().saturating_sub(1))
        .map(|block| u64::from(block.gap_reference))
        .max()
        .unwrap_or(0)
}

/// Returns the maximum gapless block size from absolute blocks.
fn max_gapless(blocks: &[CBlock]) -> u64 {
    blocks
        .iter()
        .map(|block| block.target_end - block.target_start)
        .max()
        .unwrap_or(0)
}

/// Returns the maximum query gap from absolute blocks.
fn query_max_gap(blocks: &[CBlock]) -> u64 {
    blocks
        .windows(2)
        .map(|pair| pair[1].query_start - pair[0].query_end)
        .max()
        .unwrap_or(0)
}

/// Returns the maximum target gap from absolute blocks.
fn target_max_gap(blocks: &[CBlock]) -> u64 {
    blocks
        .windows(2)
        .map(|pair| pair[1].target_start - pair[0].target_end)
        .max()
        .unwrap_or(0)
}

/// Checks if name ends with "_hap" or "_alt".
fn is_haplotype(name: &[u8]) -> bool {
    name.windows(4)
        .any(|window| window == b"_hap" || window == b"_alt")
}

/// Writes a chain in dense format.
///
/// Dense format is: size [dt dq] on each line.
/// Gap sizes are only written if not the last block.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `header` - Chain header
/// * `blocks` - Chain blocks
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn write_chain_absolute<W: Write>(
    writer: &mut W,
    header: &OwnedChainHeader,
    blocks: &[CBlock],
    output_format: OutputFormat,
) -> Result<(), CliError> {
    write_header(writer, header)?;
    match output_format {
        OutputFormat::Dense => write_absolute_as_dense(writer, blocks)?,
        OutputFormat::Long => write_absolute_as_long(writer, blocks)?,
    }
    writer.write_all(b"\n")?;
    Ok(())
}

/// Writes a chain with absolute coordinates in dense format.
fn write_absolute_as_dense<W: Write>(writer: &mut W, blocks: &[CBlock]) -> Result<(), CliError> {
    for (ix, block) in blocks.iter().enumerate() {
        write!(writer, "{}", block.target_end - block.target_start)?;
        if ix + 1 < blocks.len() {
            let next = &blocks[ix + 1];
            write!(
                writer,
                "\t{}\t{}",
                next.target_start - block.target_end,
                next.query_start - block.query_end
            )?;
        }
        writer.write_all(b"\n")?;
    }
    Ok(())
}

/// Writes a chain with absolute coordinates in long format.
fn write_absolute_as_long<W: Write>(writer: &mut W, blocks: &[CBlock]) -> Result<(), CliError> {
    for (ix, block) in blocks.iter().enumerate() {
        write!(
            writer,
            "{}\t{}\t{}",
            block.target_start,
            block.query_start,
            block.target_end - block.target_start
        )?;
        if ix + 1 < blocks.len() {
            let next = &blocks[ix + 1];
            write!(
                writer,
                "\t{}\t{}",
                next.target_start - block.target_end,
                next.query_start - block.query_end
            )?;
        }
        writer.write_all(b"\n")?;
    }
    Ok(())
}

/// Writes a chain in dense format.
///
/// Dense format is: size [dt dq] on each line.
/// Gap sizes are only written if not the last block.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `header` - Chain header
/// * `blocks` - Chain blocks
///
/// # Example
///
/// ```ignore
/// use chaintools::cli::filter::write_chain_dense;
/// use chaintools::{OwnedChainHeader, Block};
/// use std::io::BufWriter;
///
/// let header = OwnedChainHeader::new(
///     1000,
///     b"chr1".to_vec(),
///     1000,
///     chaintools::Strand::Plus,
///     0,
///     100,
///     b"chr2".to_vec(),
///     1000,
///     chaintools::Strand::Plus,
///     0,
///     100,
///     1,
/// );
/// let blocks = vec![
///     Block {
///         size: 50,
///         gap_reference: 10,
///         gap_query: 5,
///     },
///     Block {
///         size: 30,
///         gap_reference: 0,
///         gap_query: 0,
///     },
/// ];
/// let mut writer = BufWriter::new(Vec::new());
/// let _ = write_chain_dense(&mut writer, &header, &blocks);
/// let _ = writer.into_inner();
/// ```
fn write_chain_dense<W: Write>(
    writer: &mut W,
    header: &OwnedChainHeader,
    blocks: &[Block],
) -> Result<(), CliError> {
    write_header(writer, header)?;
    for (ix, block) in blocks.iter().enumerate() {
        write!(writer, "{}", block.size)?;
        if ix + 1 < blocks.len() {
            write!(writer, "\t{}\t{}", block.gap_reference, block.gap_query)?;
        }
        writer.write_all(b"\n")?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

/// Writes a chain header line in chain format.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `header` - Chain header to write
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn write_header<W: Write>(writer: &mut W, header: &OwnedChainHeader) -> Result<(), CliError> {
    write!(writer, "chain {} ", header.score)?;
    writer.write_all(&header.reference_name)?;
    write!(
        writer,
        " {} + {} {} ",
        header.reference_size, header.reference_start, header.reference_end
    )?;
    writer.write_all(&header.query_name)?;
    write!(writer, " {} ", header.query_size)?;
    writer.write_all(&[strand_to_byte(header.query_strand)])?;
    writeln!(
        writer,
        " {} {} {}",
        header.query_start, header.query_end, header.id
    )?;
    Ok(())
}

/// Converts Strand enum to byte representation.
fn strand_to_byte(strand: Strand) -> u8 {
    match strand {
        Strand::Plus => b'+',
        Strand::Minus => b'-',
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::ffi::OsString;
    use std::fs;
    #[cfg(feature = "gzip")]
    use std::io::Read;
    use std::io::{BufReader, Cursor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Parser)]
    struct FilterHarness {
        #[command(flatten)]
        args: FilterArgs,
    }

    const CHAIN_A: &str = "chain 100 chr1 1000 + 10 60 qry1 500 + 20 70 1\n20\t5\t5\n25\n\n";
    const CHAIN_B: &str = "chain 200 chr2_random 1000 + 100 180 qry2_hap 500 - 200 280 2\n80\n\n";
    const CHAIN_C: &str = "chain 300 chr3 1000 + 50 130 qry3 500 + 90 160 3\n30\t10\t0\n40\n\n";
    const NEGATIVE_SCORE_CHAIN: &str = "chain -5 chr1 100 + 0 10 qry1 100 + 0 10 4\n10\n\n";

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempChain {
        path: PathBuf,
    }

    impl TempChain {
        fn new(contents: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-filter-test-{}-{id}.chain",
                std::process::id()
            ));
            fs::write(&path, contents).expect("write temp chain");
            Self { path }
        }

        fn arg(&self) -> OsString {
            self.path.as_os_str().to_owned()
        }
    }

    impl Drop for TempChain {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    struct TempPath {
        path: PathBuf,
    }

    impl TempPath {
        fn new(suffix: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-filter-output-{}-{id}.{suffix}",
                std::process::id()
            ));
            Self { path }
        }

        fn arg(&self) -> OsString {
            self.path.as_os_str().to_owned()
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn run_ok(args: Vec<OsString>) -> (String, String) {
        run_ok_with_stdin(args, b"")
    }

    fn run_ok_with_stdin(args: Vec<OsString>, stdin_bytes: &[u8]) -> (String, String) {
        let (stdout, stderr) = run_ok_bytes(args, stdin_bytes);
        (
            String::from_utf8(stdout).expect("stdout utf8"),
            String::from_utf8(stderr).expect("stderr utf8"),
        )
    }

    fn run_ok_bytes(args: Vec<OsString>, stdin_bytes: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let cli = FilterHarness::try_parse_from(std::iter::once(arg("filter")).chain(args))
            .expect("filter args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect("filter should run");
        (stdout, stderr)
    }

    fn run_err(args: Vec<OsString>, stdin_bytes: &[u8]) -> CliError {
        let cli = FilterHarness::try_parse_from(std::iter::once(arg("filter")).chain(args))
            .expect("filter args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect_err("filter should fail")
    }

    fn arg(value: &str) -> OsString {
        OsString::from(value)
    }

    #[test]
    fn score_filters_use_inclusive_min_and_exclusive_max() {
        let input = TempChain::new(&format!("{CHAIN_A}{CHAIN_B}{CHAIN_C}"));

        let (stdout, stderr) = run_ok(vec![
            arg("--min-score"),
            arg("200"),
            arg("--max-score"),
            arg("300"),
            arg("--chain"),
            input.arg(),
        ]);

        assert_eq!(stdout, CHAIN_B);
        assert_eq!(stderr, "");
    }

    #[test]
    fn max_score_boundary_excludes_equal_score() {
        let input = TempChain::new(&format!("{CHAIN_A}{CHAIN_B}"));

        let (stdout, stderr) = run_ok(vec![
            arg("--max-score"),
            arg("200"),
            arg("--chain"),
            input.arg(),
        ]);

        assert_eq!(stdout, CHAIN_A);
        assert_eq!(stderr, "");
    }

    #[test]
    fn negative_scores_are_allowed_and_compare_as_signed_values() {
        let input = TempChain::new(NEGATIVE_SCORE_CHAIN);

        let (stdout, stderr) = run_ok(vec![
            arg("--max-score"),
            arg("14000"),
            arg("--chain"),
            input.arg(),
        ]);
        assert_eq!(stdout, NEGATIVE_SCORE_CHAIN);
        assert_eq!(stderr, "");

        let (stdout, stderr) = run_ok(vec![
            arg("--min-score"),
            arg("1"),
            arg("--chain"),
            input.arg(),
        ]);
        assert_eq!(stdout, "");
        assert_eq!(stderr, "");
    }

    #[test]
    fn overlap_start_boundary_matches_upstream_less_than_check() {
        let input = TempChain::new(&format!("{CHAIN_A}{CHAIN_B}{CHAIN_C}"));

        let (stdout, stderr) = run_ok(vec![
            arg("--query-overlap-start"),
            arg("70"),
            arg("--query-overlap-end"),
            arg("71"),
            arg("--chain"),
            input.arg(),
        ]);

        assert_eq!(stdout, CHAIN_A);
        assert_eq!(stderr, "");
    }

    #[test]
    fn name_and_chromosome_suppression_filters_are_exact_byte_matches() {
        let input = TempChain::new(&format!("{CHAIN_A}{CHAIN_B}{CHAIN_C}"));

        let (stdout, stderr) = run_ok(vec![
            arg("--exclude-random"),
            arg("--exclude-haplotype"),
            arg("--chain"),
            input.arg(),
        ]);

        assert_eq!(stdout, format!("{CHAIN_A}{CHAIN_C}"));
        assert_eq!(stderr, "");
    }

    #[test]
    fn gap_filters_use_max_gap_boundaries() {
        let input = TempChain::new(&format!("{CHAIN_A}{CHAIN_B}{CHAIN_C}"));

        let (query_stdout, query_stderr) =
            run_ok(vec![arg("--min-query-gap=5"), arg("--chain"), input.arg()]);
        assert_eq!(query_stdout, CHAIN_A);
        assert_eq!(query_stderr, "");

        let (target_stdout, target_stderr) = run_ok(vec![
            arg("--min-target-gap"),
            arg("10"),
            arg("--chain"),
            input.arg(),
        ]);
        assert_eq!(target_stdout, CHAIN_C);
        assert_eq!(target_stderr, "");
    }

    #[test]
    fn long_output_format_matches_chain_write_long_layout() {
        let input = TempChain::new(CHAIN_A);

        let (stdout, stderr) = run_ok(vec![
            arg("--output-format"),
            arg("long"),
            arg("--chain-id"),
            arg("1"),
            arg("--chain"),
            input.arg(),
        ]);

        assert_eq!(
            stdout,
            "chain 100 chr1 1000 + 10 60 qry1 500 + 20 70 1\n10\t20\t20\t5\t5\n35\t45\t25\n\n"
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn zero_gap_preserves_ucsc_consecutive_merge_behavior() {
        let input = TempChain::new(
            "chain 10 chr1 100 + 0 30 qry1 100 + 0 30 1\n10\t0\t0\n10\t0\t0\n10\n\n",
        );

        let (stdout, stderr) = run_ok(vec![arg("--merge-zero-gaps"), arg("--chain"), input.arg()]);

        assert_eq!(stdout, "chain 10 chr1 100 + 0 30 qry1 100 + 0 30 1\n20\n\n");
        assert_eq!(stderr, "2 zero length gaps eliminated\n");
    }

    #[test]
    fn multiple_inputs_are_streamed_in_argument_order() {
        let first = TempChain::new(CHAIN_A);
        let second = TempChain::new(CHAIN_B);

        let (stdout, stderr) = run_ok(vec![
            arg("--chain"),
            first.arg(),
            arg("--chain"),
            second.arg(),
        ]);

        assert_eq!(stdout, format!("{CHAIN_A}{CHAIN_B}"));
        assert_eq!(stderr, "");
    }

    #[test]
    fn missing_ids_are_generated_like_ucsc_chain_read() {
        let input = TempChain::new(
            "chain 5 chr1 100 + 0 10 qry1 100 + 0 10\n10\n\nchain 6 chr1 100 + 10 20 qry1 100 + 20 30\n10\n\n",
        );

        let (stdout, stderr) = run_ok(vec![arg("--chain"), input.arg()]);

        assert_eq!(
            stdout,
            "chain 5 chr1 100 + 0 10 qry1 100 + 0 10 1\n10\n\nchain 6 chr1 100 + 10 20 qry1 100 + 20 30 2\n10\n\n"
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn generated_ids_continue_across_multiple_inputs() {
        let first = TempChain::new("chain 5 chr1 100 + 0 10 qry1 100 + 0 10\n10\n\n");
        let second = TempChain::new("chain 6 chr1 100 + 10 20 qry1 100 + 20 30\n10\n\n");

        let (stdout, stderr) = run_ok(vec![
            arg("--chain"),
            first.arg(),
            arg("--chain"),
            second.arg(),
        ]);

        assert_eq!(
            stdout,
            "chain 5 chr1 100 + 0 10 qry1 100 + 0 10 1\n10\n\nchain 6 chr1 100 + 10 20 qry1 100 + 20 30 2\n10\n\n"
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn reads_from_stdin_when_chain_argument_is_absent() {
        let (stdout, stderr) = run_ok_with_stdin(Vec::new(), CHAIN_A.as_bytes());

        assert_eq!(stdout, CHAIN_A);
        assert_eq!(stderr, "");
    }

    #[test]
    fn out_chain_writes_output_to_path() {
        let input = TempChain::new(CHAIN_A);
        let output = TempPath::new("chain");

        let (stdout, stderr) = run_ok(vec![
            arg("--chain"),
            input.arg(),
            arg("--out-chain"),
            output.arg(),
        ]);

        assert_eq!(stdout, "");
        assert_eq!(
            fs::read_to_string(&output.path).expect("read output chain"),
            CHAIN_A
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn out_chain_rejects_same_path_as_input_chain() {
        let input = TempChain::new(CHAIN_A);

        let err = run_err(
            vec![arg("--chain"), input.arg(), arg("--out-chain"), input.arg()],
            b"",
        );

        assert!(err
            .to_string()
            .contains("--out-chain must not be the same path as an input --chain"));
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_output_compresses_stdout() {
        let (stdout, stderr) = run_ok_bytes(vec![arg("--gzip")], CHAIN_A.as_bytes());

        let mut decoder = flate2::read::MultiGzDecoder::new(Cursor::new(stdout));
        let mut decoded = String::new();
        decoder
            .read_to_string(&mut decoded)
            .expect("decode gzip stdout");

        assert_eq!(decoded, CHAIN_A);
        assert_eq!(stderr, b"");
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_out_chain_writes_compressed_file() {
        let input = TempChain::new(CHAIN_A);
        let output = TempPath::new("chain.gz");

        let (stdout, stderr) = run_ok(vec![
            arg("--chain"),
            input.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--gzip"),
        ]);

        let mut decoder =
            flate2::read::MultiGzDecoder::new(fs::File::open(&output.path).expect("open gzip"));
        let mut decoded = String::new();
        decoder
            .read_to_string(&mut decoded)
            .expect("decode gzip file");

        assert_eq!(stdout, "");
        assert_eq!(decoded, CHAIN_A);
        assert_eq!(stderr, "");
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn gzip_output_requires_gzip_feature() {
        let err = run_err(vec![arg("--gzip")], CHAIN_A.as_bytes());

        assert!(err
            .to_string()
            .contains("--gzip requires chaintools to be built with the `gzip` feature"));
    }

    #[test]
    fn rejects_non_positive_chain_id_filter() {
        let err =
            FilterHarness::try_parse_from(["filter", "--chain-id", "0", "--chain", "input.chain"])
                .expect_err("zero chain id should be rejected");

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn rejects_negative_score_filters() {
        let err =
            FilterHarness::try_parse_from(["filter", "--min-score=-1", "--chain", "input.chain"])
                .expect_err("negative score should be rejected");

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }
}
