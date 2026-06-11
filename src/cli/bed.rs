// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use chaintools::{Block, ChainError, OwnedChainHeader, Strand, StreamingReader};
use clap::{Args, ValueEnum};
#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};
use genepred::{Bed3, Bed6, Bed12, Extras, GenePred, Writer};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const INPUT_LIST_BUFFER_CAPACITY: usize = 1024 * 1024;

/// Command-line arguments for the bed subcommand.
///
/// Converts chain records to BED intervals. Each chain becomes one BED record:
/// by default aligned chain blocks are represented as BED12 blocks, and with
/// `--spanned` the full chain span is emitted as one block.
#[derive(Debug, Args)]
pub struct BedArgs {
    #[arg(
        short = 'c',
        long = "chains",
        value_name = "PATH",
        num_args = 1..,
        conflicts_with = "file",
        help = "Input chain files. If not provided, chain data is read from standard input."
    )]
    chains: Option<Vec<PathBuf>>,

    #[arg(
        short = 'f',
        long = "file",
        value_name = "PATH",
        conflicts_with = "chains",
        help = "Path to a file listing one input chain path per line"
    )]
    file: Option<PathBuf>,

    #[arg(
        short = 'o',
        long = "out-bed",
        value_name = "PATH",
        help = "Path for BED output. If not provided, output is written to standard output."
    )]
    out_bed: Option<PathBuf>,

    #[arg(
        long = "side",
        value_name = "SIDE",
        value_enum,
        help = "Chain side to project into BED coordinates"
    )]
    side: BedSide,

    #[arg(
        long = "type",
        value_name = "BED_TYPE",
        default_value_t = BedType::Bed12,
        value_enum,
        help = "BED output width"
    )]
    bed_type: BedType,

    #[arg(
        long = "spanned",
        help = "Emit each chain as one spanning BED block instead of preserving aligned blocks"
    )]
    spanned: bool,

    #[arg(
        short = 'L',
        long = "long",
        help = "Use long BED names: {other_chrom}_{chain_id}_{other_span}"
    )]
    long_format: bool,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress BED output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        long = "sort-by",
        value_name = "KEY",
        default_value_t = BedSortBy::None,
        value_enum,
        help = "Sort BED output by the selected key"
    )]
    sort_by: BedSortBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BedSide {
    Reference,
    Query,
}

impl std::fmt::Display for BedSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BedSide::Reference => f.write_str("reference"),
            BedSide::Query => f.write_str("query"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BedType {
    #[value(name = "3")]
    Bed3,
    #[value(name = "6")]
    Bed6,
    #[value(name = "12")]
    Bed12,
}

impl std::fmt::Display for BedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BedType::Bed3 => f.write_str("3"),
            BedType::Bed6 => f.write_str("6"),
            BedType::Bed12 => f.write_str("12"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BedSortBy {
    None,
    Coordinate,
}

impl std::fmt::Display for BedSortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BedSortBy::None => f.write_str("none"),
            BedSortBy::Coordinate => f.write_str("coordinate"),
        }
    }
}

/// Runs the bed subcommand.
pub fn run<R, W, E>(
    args: BedArgs,
    stdin: &mut R,
    stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_output_args(&args)?;
    let inputs = collect_input_paths(&args)?;
    if let Some(out_bed) = &args.out_bed {
        validate_output_path(out_bed, &inputs)?;
    }

    let input_desc = if inputs.is_empty() {
        "<stdin>".to_owned()
    } else {
        format!("{} file(s)", inputs.len())
    };
    let output_desc = args
        .out_bed
        .as_deref()
        .map_or_else(|| "<stdout>".to_owned(), |path| path.display().to_string());
    log::info!(
        "bed: side={}, type={}, spanned={}, long={}, sort_by={}, input={input_desc}, output={output_desc}",
        args.side,
        args.bed_type,
        args.spanned,
        args.long_format,
        args.sort_by
    );

    if let Some(path) = &args.out_bed {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file);
        if args.gzip {
            run_gzip_output(&args, stdin, &inputs, writer)?;
        } else {
            let mut writer = writer;
            run_to_writer(&args, stdin, &inputs, &mut writer)?;
            writer.flush()?;
        }
    } else if args.gzip {
        run_gzip_output(&args, stdin, &inputs, stdout)?;
    } else {
        run_to_writer(&args, stdin, &inputs, stdout)?;
    }

    Ok(())
}

#[cfg(feature = "gzip")]
fn run_gzip_output<R, W>(
    args: &BedArgs,
    stdin: &mut R,
    inputs: &[PathBuf],
    writer: W,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let mut writer = GzEncoder::new(writer, Compression::fast());
    run_to_writer(args, stdin, inputs, &mut writer)?;
    writer.try_finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn run_gzip_output<R, W>(
    _args: &BedArgs,
    _stdin: &mut R,
    _inputs: &[PathBuf],
    _writer: W,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    Err(CliError::Message(
        "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
    ))
}

#[cfg(feature = "gzip")]
fn validate_output_args(_args: &BedArgs) -> Result<(), CliError> {
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn validate_output_args(args: &BedArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

fn run_to_writer<R, W>(
    args: &BedArgs,
    stdin: &mut R,
    inputs: &[PathBuf],
    writer: &mut W,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let mut stats = BedStats::default();
    let mut next_generated_id = 1u64;

    if args.sort_by == BedSortBy::Coordinate {
        let mut records = Vec::new();
        process_inputs(
            args,
            stdin,
            inputs,
            &mut next_generated_id,
            &mut stats,
            |record| {
                records.push(record);
                Ok(())
            },
        )?;
        sort_records(&mut records);
        for record in &records {
            write_bed_record(args.bed_type, record, writer)?;
        }
    } else {
        process_inputs(
            args,
            stdin,
            inputs,
            &mut next_generated_id,
            &mut stats,
            |record| write_bed_record(args.bed_type, &record, writer),
        )?;
    }

    let files = if inputs.is_empty() {
        1
    } else {
        inputs.len() as u64
    };
    super::log_summary(
        "bed",
        &[
            ("files", files),
            ("chains", stats.read),
            ("written", stats.written),
        ],
    );
    Ok(())
}

fn process_inputs<R, F>(
    args: &BedArgs,
    stdin: &mut R,
    inputs: &[PathBuf],
    next_generated_id: &mut u64,
    stats: &mut BedStats,
    mut emit: F,
) -> Result<(), CliError>
where
    R: BufRead,
    F: FnMut(GenePred) -> Result<(), CliError>,
{
    if inputs.is_empty() {
        let mut reader = StreamingReader::new(stdin);
        process_reader(args, &mut reader, next_generated_id, stats, &mut emit)?;
    } else {
        for input in inputs {
            log::debug!("converting {}", input.display());
            let mut reader = StreamingReader::from_path(input)?;
            process_reader(args, &mut reader, next_generated_id, stats, &mut emit)?;
        }
    }
    Ok(())
}

fn process_reader<R, F>(
    args: &BedArgs,
    reader: &mut StreamingReader<R>,
    next_generated_id: &mut u64,
    stats: &mut BedStats,
    emit: &mut F,
) -> Result<(), CliError>
where
    R: BufRead,
    F: FnMut(GenePred) -> Result<(), CliError>,
{
    reader.set_next_generated_id(*next_generated_id);
    while let Some(header) = reader.next_header()? {
        stats.read += 1;
        let blocks = reader.read_blocks(header.offset)?;
        let record = chain_to_gene(args, &header, &blocks)?;
        emit(record)?;
        stats.written += 1;
    }
    *next_generated_id = reader.next_generated_id();
    Ok(())
}

#[derive(Default)]
struct BedStats {
    read: u64,
    written: u64,
}

fn chain_to_gene(
    args: &BedArgs,
    header: &OwnedChainHeader,
    blocks: &[Block],
) -> Result<GenePred, CliError> {
    let (chrom, start, end, strand) = match args.side {
        BedSide::Reference => (
            header.reference_name.clone(),
            u64::from(header.reference_start),
            u64::from(header.reference_end),
            header.reference_strand,
        ),
        BedSide::Query => (
            header.query_name.clone(),
            u64::from(header.query_start),
            u64::from(header.query_end),
            header.query_strand,
        ),
    };

    if end <= start {
        return Err(format_error(
            header.offset,
            "chain side span is empty or inverted",
        ));
    }

    let mut record = GenePred::from_coords(chrom, start, end, Extras::new());
    record.set_name(Some(chain_name(args, header)));
    record.set_strand(Some(to_bed_strand(strand)));
    record.set_thick_start(Some(start));
    record.set_thick_end(Some(end));

    let (block_starts, block_ends) = if args.spanned {
        (vec![start], vec![end])
    } else {
        side_blocks(header, blocks, args.side)?
    };
    let block_count = u32::try_from(block_starts.len()).map_err(|_| {
        format_error(
            header.offset,
            "chain has too many blocks to represent in BED12",
        )
    })?;
    record.set_block_count(Some(block_count));
    record.set_block_starts(Some(block_starts));
    record.set_block_ends(Some(block_ends));

    Ok(record)
}

fn chain_name(args: &BedArgs, header: &OwnedChainHeader) -> Vec<u8> {
    if !args.long_format {
        return format!("chain_{}", header.id).into_bytes();
    }

    let (chrom, start, end) = match args.side {
        BedSide::Reference => (&header.query_name, header.query_start, header.query_end),
        BedSide::Query => (
            &header.reference_name,
            header.reference_start,
            header.reference_end,
        ),
    };
    let span = end.saturating_sub(start);
    let mut name = Vec::with_capacity(chrom.len() + 32);
    name.extend_from_slice(chrom);
    write!(&mut name, "_{}_{}", header.id, span).expect("writing to Vec cannot fail");
    name
}

fn side_blocks(
    header: &OwnedChainHeader,
    blocks: &[Block],
    side: BedSide,
) -> Result<(Vec<u64>, Vec<u64>), CliError> {
    let mut reference = u64::from(header.reference_start);
    let mut query = u64::from(header.query_start);
    let mut starts = Vec::with_capacity(blocks.len());
    let mut ends = Vec::with_capacity(blocks.len());

    for block in blocks {
        let size = u64::from(block.size);
        let reference_end = reference + size;
        let query_end = query + size;

        if size > 0 {
            match side {
                BedSide::Reference => {
                    starts.push(reference);
                    ends.push(reference_end);
                }
                BedSide::Query => {
                    starts.push(query);
                    ends.push(query_end);
                }
            }
        }

        reference = reference_end + u64::from(block.gap_reference);
        query = query_end + u64::from(block.gap_query);
    }

    if reference != u64::from(header.reference_end) || query != u64::from(header.query_end) {
        return Err(format_error(
            header.offset,
            "chain block coordinates do not match header end",
        ));
    }
    if starts.is_empty() {
        return Err(format_error(
            header.offset,
            "chain has no positive-length alignment blocks",
        ));
    }

    Ok((starts, ends))
}

fn write_bed_record<W: Write>(
    bed_type: BedType,
    record: &GenePred,
    writer: &mut W,
) -> Result<(), CliError> {
    let result = match bed_type {
        BedType::Bed3 => Writer::<Bed3>::from_record(record, writer),
        BedType::Bed6 => Writer::<Bed6>::from_record(record, writer),
        BedType::Bed12 => Writer::<Bed12>::from_record(record, writer),
    };
    result.map_err(|err| CliError::Message(err.to_string()))
}

fn sort_records(records: &mut [GenePred]) {
    #[cfg(feature = "parallel")]
    {
        use rayon::prelude::ParallelSliceMut;
        records.par_sort_unstable_by(compare_records);
    }

    #[cfg(not(feature = "parallel"))]
    {
        records.sort_unstable_by(compare_records);
    }
}

fn compare_records(a: &GenePred, b: &GenePred) -> Ordering {
    a.chrom()
        .cmp(b.chrom())
        .then_with(|| a.start().cmp(&b.start()))
        .then_with(|| a.end().cmp(&b.end()))
        .then_with(|| a.name().unwrap_or(b".").cmp(b.name().unwrap_or(b".")))
}

fn collect_input_paths(args: &BedArgs) -> Result<Vec<PathBuf>, CliError> {
    if let Some(paths) = &args.chains {
        return Ok(paths.clone());
    }

    let Some(list_path) = &args.file else {
        return Ok(Vec::new());
    };

    let file = File::open(list_path)?;
    let mut reader = BufReader::with_capacity(INPUT_LIST_BUFFER_CAPACITY, file);
    let mut line = String::new();
    let mut paths = Vec::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.trim().is_empty() {
            continue;
        }
        paths.push(PathBuf::from(trimmed.trim()));
    }

    if paths.is_empty() {
        return Err(CliError::Message(format!(
            "{} does not list any input chain files",
            list_path.display()
        )));
    }

    Ok(paths)
}

fn validate_output_path(out_bed: &Path, inputs: &[PathBuf]) -> Result<(), CliError> {
    if inputs.is_empty() {
        return Ok(());
    }

    let output = normalize_output_path(out_bed)?;
    for input in inputs {
        let normalized_input = normalize_existing_path(input)?;
        if normalized_input == output {
            return Err(CliError::Message(format!(
                "--out-bed must not be the same path as input chain {}",
                input.display()
            )));
        }
    }
    Ok(())
}

fn normalize_existing_path(path: &Path) -> Result<PathBuf, CliError> {
    Ok(std::fs::canonicalize(path)?)
}

fn normalize_output_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.exists() {
        return Ok(std::fs::canonicalize(path)?);
    }

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let parent = if parent.as_os_str().is_empty() {
        std::env::current_dir()?
    } else {
        std::fs::canonicalize(parent)?
    };
    let file_name = path
        .file_name()
        .ok_or_else(|| CliError::Message(format!("invalid --out-bed path {}", path.display())))?;
    Ok(parent.join(file_name))
}

fn to_bed_strand(strand: Strand) -> genepred::Strand {
    match strand {
        Strand::Plus => genepred::Strand::Forward,
        Strand::Minus => genepred::Strand::Reverse,
    }
}

fn format_error(offset: usize, message: impl Into<String>) -> CliError {
    CliError::Chain(ChainError::Format {
        offset,
        msg: message.into().into(),
    })
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug, Parser)]
    struct BedHarness {
        #[command(flatten)]
        args: BedArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    const TWO_BLOCK_CHAIN: &str =
        "chain 100 chr1 1000 + 100 160 qry1 500 - 10 65 7\n20 10 5\n30\n\n";

    struct TempPath {
        path: PathBuf,
    }

    impl TempPath {
        fn new(suffix: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-bed-test-{}-{id}.{suffix}",
                std::process::id()
            ));
            Self { path }
        }

        fn with_contents(suffix: &str, contents: &[u8]) -> Self {
            let temp = Self::new(suffix);
            fs::write(&temp.path, contents).expect("write temp file");
            temp
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

    fn arg(value: &str) -> OsString {
        OsString::from(value)
    }

    fn run_ok(args: Vec<OsString>, stdin_bytes: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let cli = BedHarness::try_parse_from(std::iter::once(arg("bed")).chain(args))
            .expect("bed args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect("bed should run");
        (stdout, stderr)
    }

    fn run_err(args: Vec<OsString>, stdin_bytes: &[u8]) -> CliError {
        let cli = BedHarness::try_parse_from(std::iter::once(arg("bed")).chain(args))
            .expect("bed args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect_err("bed should fail")
    }

    #[test]
    fn reference_bed12_preserves_aligned_blocks() {
        let (stdout, stderr) = run_ok(
            vec![arg("--side"), arg("reference")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chr1\t100\t160\tchain_7\t0\t+\t100\t160\t0,0,0\t2\t20,30,\t0,30,\n"
        );
        assert!(stderr.is_empty());
    }

    #[test]
    fn query_bed12_uses_chain_header_coordinates_and_strand() {
        let (stdout, _) = run_ok(
            vec![arg("--side"), arg("query")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "qry1\t10\t65\tchain_7\t0\t-\t10\t65\t0,0,0\t2\t20,30,\t0,25,\n"
        );
    }

    #[test]
    fn spanned_bed12_emits_one_block() {
        let (stdout, _) = run_ok(
            vec![arg("--side"), arg("reference"), arg("--spanned")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chr1\t100\t160\tchain_7\t0\t+\t100\t160\t0,0,0\t1\t60,\t0,\n"
        );
    }

    #[test]
    fn bed3_and_bed6_select_column_count() {
        let (bed3, _) = run_ok(
            vec![arg("--side"), arg("reference"), arg("--type"), arg("3")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );
        let (bed6, _) = run_ok(
            vec![arg("--side"), arg("reference"), arg("--type"), arg("6")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(String::from_utf8(bed3).unwrap(), "chr1\t100\t160\n");
        assert_eq!(
            String::from_utf8(bed6).unwrap(),
            "chr1\t100\t160\tchain_7\t0\t+\n"
        );
    }

    #[test]
    fn long_format_reference_bed_uses_query_name_and_span() {
        let (stdout, _) = run_ok(
            vec![
                arg("--side"),
                arg("reference"),
                arg("--type"),
                arg("6"),
                arg("-L"),
            ],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chr1\t100\t160\tqry1_7_55\t0\t+\n"
        );
    }

    #[test]
    fn long_format_query_bed_uses_reference_name_and_span() {
        let (stdout, _) = run_ok(
            vec![
                arg("--side"),
                arg("query"),
                arg("--type"),
                arg("6"),
                arg("--long"),
            ],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "qry1\t10\t65\tchr1_7_60\t0\t-\n"
        );
    }

    #[test]
    fn coordinate_sort_orders_bed_records() {
        let input = "chain 1 chr2 100 + 20 30 q 100 + 0 10 2\n10\n\n\
                     chain 1 chr1 100 + 50 60 q 100 + 0 10 3\n10\n\n\
                     chain 1 chr1 100 + 10 20 q 100 + 0 10 1\n10\n\n";
        let (stdout, _) = run_ok(
            vec![
                arg("--side"),
                arg("reference"),
                arg("--type"),
                arg("3"),
                arg("--sort-by"),
                arg("coordinate"),
            ],
            input.as_bytes(),
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chr1\t10\t20\nchr1\t50\t60\nchr2\t20\t30\n"
        );
    }

    #[test]
    fn chains_and_file_conflict_at_parse_time() {
        let result = BedHarness::try_parse_from([
            "bed",
            "--side",
            "reference",
            "--chains",
            "a.chain",
            "--file",
            "paths.txt",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn reads_paths_from_file() {
        let input = TempPath::with_contents("chain", TWO_BLOCK_CHAIN.as_bytes());
        let list = TempPath::with_contents("txt", input.path.to_string_lossy().as_bytes());

        let (stdout, _) = run_ok(
            vec![arg("--side"), arg("reference"), arg("--file"), list.arg()],
            b"",
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chr1\t100\t160\tchain_7\t0\t+\t100\t160\t0,0,0\t2\t20,30,\t0,30,\n"
        );
    }

    #[test]
    fn writes_to_out_bed_path() {
        let output = TempPath::new("bed");

        let (stdout, _) = run_ok(
            vec![
                arg("--side"),
                arg("reference"),
                arg("--out-bed"),
                output.arg(),
            ],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert!(stdout.is_empty());
        assert_eq!(
            fs::read_to_string(&output.path).expect("read BED"),
            "chr1\t100\t160\tchain_7\t0\t+\t100\t160\t0,0,0\t2\t20,30,\t0,30,\n"
        );
    }

    #[test]
    fn rejects_output_matching_input_chain() {
        let input = TempPath::with_contents("chain", TWO_BLOCK_CHAIN.as_bytes());

        let err = run_err(
            vec![
                arg("--side"),
                arg("reference"),
                arg("--chains"),
                input.arg(),
                arg("--out-bed"),
                input.arg(),
            ],
            b"",
        );

        assert!(
            err.to_string()
                .contains("--out-bed must not be the same path")
        );
    }

    #[test]
    fn malformed_block_span_is_rejected() {
        let bad = "chain 1 chr1 100 + 0 10 q 100 + 0 10 1\n5\n\n";
        let err = run_err(vec![arg("--side"), arg("reference")], bad.as_bytes());

        assert!(
            err.to_string()
                .contains("chain block coordinates do not match header end")
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_output_compresses_stdout() {
        let (stdout, _) = run_ok(
            vec![arg("--side"), arg("reference"), arg("--gzip")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        let mut decoded = String::new();
        flate2::read::MultiGzDecoder::new(&stdout[..])
            .read_to_string(&mut decoded)
            .expect("decode gzip");
        assert_eq!(
            decoded,
            "chr1\t100\t160\tchain_7\t0\t+\t100\t160\t0,0,0\t2\t20,30,\t0,30,\n"
        );
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn gzip_requires_feature() {
        let err = run_err(
            vec![arg("--side"), arg("reference"), arg("--gzip")],
            TWO_BLOCK_CHAIN.as_bytes(),
        );

        assert!(
            err.to_string()
                .contains("--gzip requires chaintools to be built with the `gzip` feature")
        );
    }
}
