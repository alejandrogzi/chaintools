// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use chaintools::{OwnedChain, StreamingReader};
use clap::{Args, ValueEnum};
#[cfg(feature = "gzip")]
use flate2::{write::GzEncoder, Compression};

use super::sort_core::{
    emit_sorted_chains, with_merged_runs, write_metadata_lines, SortAccumulator, SortCriterion,
    SortedInput, OUTPUT_BUFFER_CAPACITY,
};
use super::CliError;

const BYTES_PER_GB: f64 = 1_000_000_000.0;
const DEFAULT_MAX_GB: f64 = 16.0;

/// Command-line arguments for the sort subcommand.
///
/// Provides options for sorting chain files by different criteria,
/// with optional output indexing and gzip compression.
///
/// # Examples
///
/// ```bash
/// chaintools sort --chain input.chain --out-chain sorted.chain --sort-by score
/// ```
#[derive(Debug, Args)]
pub struct SortArgs {
    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to .chain file to sort. If not provided, chain data is read from standard input."
    )]
    chain: Option<PathBuf>,

    #[arg(
        short = 'o',
        long = "out-chain",
        value_name = "PATH",
        help = "Path for the sorted chain output. If not provided, output is written to standard output."
    )]
    out_chain: Option<PathBuf>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress sorted chain output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        short = 'S',
        long = "sort-by",
        default_value_t = SortBy::Score,
        value_enum,
        help = "Primary sort key"
    )]
    sort_by: SortBy,

    #[arg(
        short = 'I',
        long = "out-index",
        value_name = "PATH",
        help = "Write an output-offset index for the selected primary sort key"
    )]
    out_index: Option<PathBuf>,

    #[arg(
        short = 'M',
        long = "max-gb",
        value_name = "GB",
        default_value_t = DEFAULT_MAX_GB,
        help = "Maximum in-memory working set in gigabytes before spilling sorted runs to temporary files"
    )]
    max_gb: f64,
}

impl SortArgs {
    pub(crate) fn writes_to_stdout(&self) -> bool {
        self.out_chain.is_none()
    }

    pub(crate) fn default_log_level(&self) -> log::LevelFilter {
        if self.out_chain.is_some() {
            log::LevelFilter::Info
        } else {
            log::LevelFilter::Off
        }
    }

    fn max_in_memory_bytes(&self) -> Result<u64, CliError> {
        if !self.max_gb.is_finite() {
            return Err(CliError::Message(
                "--max-gb must be a finite number".to_owned(),
            ));
        }
        if self.max_gb <= 0.0 {
            return Err(CliError::Message(
                "--max-gb must be greater than zero".to_owned(),
            ));
        }

        let bytes = (self.max_gb * BYTES_PER_GB).ceil();
        if bytes > u64::MAX as f64 {
            return Err(CliError::Message(
                "--max-gb is too large to represent".to_owned(),
            ));
        }

        Ok(bytes as u64)
    }
}

/// Sort criteria for chain files.
///
/// # Variants
///
/// * `Score` - Sort by chain score (descending), tie-breaks by ID
/// * `Target` - Sort by target (reference) sequence name then start position
/// * `Query` - Sort by query sequence name then start position
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SortBy {
    Score,
    Target,
    Query,
}

impl std::fmt::Display for SortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortBy::Score => f.write_str("score"),
            SortBy::Target => f.write_str("target"),
            SortBy::Query => f.write_str("query"),
        }
    }
}

struct CountingWriter<W> {
    inner: W,
    position: u64,
}

/// Writer that tracks the number of bytes written.
///
/// # Fields
///
/// * `inner` - The underlying writer
/// * `position` - Current position in the output
impl<W> CountingWriter<W> {
    /// Creates a new counting writer wrapping the inner writer.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying writer to wrap
    ///
    /// # Output
    ///
    /// Returns a new CountingWriter with position initialized to 0
    fn new(inner: W) -> Self {
        Self { inner, position: 0 }
    }

    /// Returns the current position in the output stream.
    ///
    /// # Output
    ///
    /// Returns the total number of bytes written
    fn position(&self) -> u64 {
        self.position
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.position += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

enum OutputWriter<'a, W: Write> {
    Stdout(&'a mut W),
    File(BufWriter<File>),
}

impl<W: Write> Write for OutputWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            OutputWriter::Stdout(writer) => writer.write(buf),
            OutputWriter::File(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            OutputWriter::Stdout(writer) => writer.flush(),
            OutputWriter::File(writer) => writer.flush(),
        }
    }
}

struct IndexTracker {
    sort_by: SortCriterion,
    last_score: Option<i64>,
    last_id: Option<u64>,
    last_name: Option<Vec<u8>>,
}

impl IndexTracker {
    fn new(sort_by: SortCriterion) -> Self {
        Self {
            sort_by,
            last_score: None,
            last_id: None,
            last_name: None,
        }
    }

    fn before_chain<W: Write>(
        &mut self,
        index: &mut W,
        output_position: u64,
        chain: &OwnedChain,
    ) -> Result<(), CliError> {
        match self.sort_by {
            SortCriterion::Score => {
                if self.last_score != Some(chain.score) {
                    self.last_score = Some(chain.score);
                    writeln!(index, "{output_position:x}\t{}", chain.score)?;
                }
            }
            SortCriterion::Id => {
                if self.last_id != Some(chain.id) {
                    self.last_id = Some(chain.id);
                    writeln!(index, "{output_position:x}\t{}", chain.id)?;
                }
            }
            SortCriterion::Reference => {
                if self.last_name.as_deref() != Some(chain.reference_name.as_slice()) {
                    self.last_name = Some(chain.reference_name.clone());
                    write!(index, "{output_position:x}\t")?;
                    index.write_all(&chain.reference_name)?;
                    index.write_all(b"\n")?;
                }
            }
            SortCriterion::Query => {
                if self.last_name.as_deref() != Some(chain.query_name.as_slice()) {
                    self.last_name = Some(chain.query_name.clone());
                    write!(index, "{output_position:x}\t")?;
                    index.write_all(&chain.query_name)?;
                    index.write_all(b"\n")?;
                }
            }
        }
        Ok(())
    }
}

/// Runs the sort subcommand.
///
/// Sorts chain records by score, reference, or query, with optional external
/// merge for large datasets exceeding the memory budget.
///
/// # Arguments
///
/// * `args` - Sort arguments with input, output, and sort criteria
/// * `stdin` - Input stream (used if no --chain provided)
/// * `stdout` - Output stream (used if no --out-chain provided)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
///
/// # Examples
///
/// ```bash
/// # Sort by score descending
/// chaintools sort --chain input.chain --out-chain sorted.chain --sort-by score
///
/// # Sort by reference sequence
/// chaintools sort --chain input.chain --out-chain by_ref.chain --sort-by target
/// ```
pub fn run<R, W, E>(
    args: SortArgs,
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
    let max_in_memory_bytes = args.max_in_memory_bytes()?;
    let temp_dir = temp_directory(&args);

    let (metadata, sorted) = if let Some(path) = &args.chain {
        let mut reader = StreamingReader::from_path(path)?;
        collect_sorted_input(&args, max_in_memory_bytes, &mut reader, &temp_dir)?
    } else {
        let mut reader = StreamingReader::new(stdin);
        collect_sorted_input(&args, max_in_memory_bytes, &mut reader, &temp_dir)?
    };

    emit_output(&args, &metadata, sorted, stdout)
}

/// Validates output arguments for sort command.
#[cfg(feature = "gzip")]
fn validate_output_args(args: &SortArgs) -> Result<(), CliError> {
    validate_common_args(args)
}

#[cfg(not(feature = "gzip"))]
/// Validates output arguments for sort command.
///
/// Returns an error if gzip is requested but the gzip feature is not enabled.
fn validate_output_args(args: &SortArgs) -> Result<(), CliError> {
    validate_common_args(args)?;
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

/// Validates common arguments for sort command.
///
/// Checks that --out-index and --gzip are not combined, and that --out-index
/// is not the same as --out-chain or --chain.
///
/// # Arguments
///
/// * `args` - Sort command arguments
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` if invalid
fn validate_common_args(args: &SortArgs) -> Result<(), CliError> {
    if args.gzip && args.out_index.is_some() {
        return Err(CliError::Message(
            "--out-index cannot be combined with --gzip because index offsets refer to uncompressed output bytes"
                .to_owned(),
        ));
    }
    if let Some(index_path) = &args.out_index {
        if args.out_chain.as_ref() == Some(index_path) {
            return Err(CliError::Message(
                "--out-index must not be the same path as --out-chain".to_owned(),
            ));
        }
        if args.chain.as_ref() == Some(index_path) {
            return Err(CliError::Message(
                "--out-index must not be the same path as --chain".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Determines the temporary directory for sort operations.
///
/// Uses the output chain directory or input chain directory, falling back to system temp.
///
/// # Arguments
///
/// * `args` - Sort command arguments
///
/// # Output
///
/// Returns the temporary directory path
fn temp_directory(args: &SortArgs) -> PathBuf {
    if let Some(path) = args.out_chain.as_ref().or(args.chain.as_ref()) {
        return path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
    }
    std::env::temp_dir()
}

/// Collects sorted input from a streaming reader.
///
/// Uses a SortAccumulator to sort chains, spilling to disk if memory limit is exceeded.
///
/// # Arguments
///
/// * `args` - Sort command arguments
/// * `max_in_memory_bytes` - Maximum bytes to keep in memory
/// * `reader` - Streaming reader for input chains
/// * `temp_dir` - Directory for temporary files
///
/// # Output
///
/// Returns `Ok((metadata, sorted))` with metadata lines and sorted input
fn collect_sorted_input<R: BufRead>(
    args: &SortArgs,
    max_in_memory_bytes: u64,
    reader: &mut StreamingReader<R>,
    temp_dir: &Path,
) -> Result<(Vec<Vec<u8>>, SortedInput), CliError> {
    let mut accumulator =
        SortAccumulator::new(args.sort_by.criterion(), max_in_memory_bytes, temp_dir);
    accumulator.push_stream(reader)?;
    accumulator.finish()
}

/// Emits sorted output to the appropriate writer.
///
/// Handles both gzip and plain output, optionally writing an index file.
///
/// # Arguments
///
/// * `args` - Sort command arguments
/// * `metadata` - Metadata lines to output
/// * `sorted` - Sorted input (in-memory or runs)
/// * `stdout` - Standard output writer
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn emit_output<W: Write>(
    args: &SortArgs,
    metadata: &[Vec<u8>],
    sorted: SortedInput,
    stdout: &mut W,
) -> Result<(), CliError> {
    #[cfg(feature = "gzip")]
    if args.gzip {
        let writer = open_output_writer(args, stdout)?;
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        write_metadata_lines(&mut encoder, metadata)?;
        emit_sorted_chains(&mut encoder, sorted, args.sort_by.criterion())?;
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        return Ok(());
    }

    let writer = open_output_writer(args, stdout)?;
    if let Some(index_path) = &args.out_index {
        let mut counted = CountingWriter::new(writer);
        let mut index_writer =
            BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, File::create(index_path)?);
        write_metadata_lines(&mut counted, metadata)?;
        emit_sorted_chains_with_index(&mut counted, &mut index_writer, sorted, args.sort_by)?;
        counted.flush()?;
        index_writer.flush()?;
    } else {
        let mut writer = writer;
        write_metadata_lines(&mut writer, metadata)?;
        emit_sorted_chains(&mut writer, sorted, args.sort_by.criterion())?;
        writer.flush()?;
    }

    Ok(())
}

/// Opens the output writer based on sort arguments.
///
/// Returns a writer that writes to either stdout or a file.
///
/// # Arguments
///
/// * `args` - Sort command arguments
/// * `stdout` - Standard output writer
///
/// # Output
///
/// Returns `Ok(OutputWriter)` for writing sorted chains
fn open_output_writer<'a, W: Write>(
    args: &SortArgs,
    stdout: &'a mut W,
) -> Result<OutputWriter<'a, W>, CliError> {
    if let Some(path) = &args.out_chain {
        let file = File::create(path)?;
        Ok(OutputWriter::File(BufWriter::with_capacity(
            OUTPUT_BUFFER_CAPACITY,
            file,
        )))
    } else {
        Ok(OutputWriter::Stdout(stdout))
    }
}

/// Emits sorted chains with an index file.
///
/// Writes chains to the output writer while recording byte offsets in the index file.
///
/// # Arguments
///
/// * `writer` - Output writer for chains
/// * `index_writer` - Index file writer
/// * `sorted` - Sorted input (in-memory or runs)
/// * `sort_by` - Sort criterion
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn emit_sorted_chains_with_index<W: Write>(
    writer: &mut CountingWriter<W>,
    index_writer: &mut BufWriter<File>,
    sorted: SortedInput,
    sort_by: SortBy,
) -> Result<(), CliError> {
    let mut tracker = IndexTracker::new(sort_by.criterion());

    match sorted {
        SortedInput::InMemory(records) => {
            for chain in &records {
                tracker.before_chain(index_writer, writer.position(), chain)?;
                chaintools::write_chain_dense(writer, chain)?;
            }
        }
        SortedInput::Runs(runs) => {
            with_merged_runs(&runs, sort_by.criterion(), |chain| {
                tracker.before_chain(index_writer, writer.position(), chain)?;
                chaintools::write_chain_dense(writer, chain).map_err(CliError::from)
            })?;
        }
    }

    Ok(())
}

impl SortBy {
    fn criterion(self) -> SortCriterion {
        match self {
            SortBy::Score => SortCriterion::Score,
            SortBy::Target => SortCriterion::Reference,
            SortBy::Query => SortCriterion::Query,
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug, Parser)]
    struct SortHarness {
        #[command(flatten)]
        args: SortArgs,
    }

    const SCORE_TIE_A: &str = "chain 100 chr2 1000 + 40 80 qry2 500 + 30 70 5\n40\n\n";
    const SCORE_TIE_B: &str = "chain 100 chr1 1000 + 10 50 qry3 500 + 15 55 2\n40\n\n";
    const SCORE_TIE_C: &str = "chain 300 chr3 1000 + 0 30 qry1 500 + 0 30 9\n30\n\n";
    const TARGET_A: &str = "chain 90 chr1 1000 + 50 90 qryB 500 + 10 50 7\n40\n\n";
    const TARGET_B: &str = "chain 80 chr1 1000 + 10 45 qryA 500 + 20 55 4\n35\n\n";
    const TARGET_C: &str = "chain 70 chr2 1000 + 0 20 qryC 500 + 0 20 3\n20\n\n";

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempChain {
        path: PathBuf,
    }

    impl TempChain {
        fn new(contents: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-sort-test-{}-{id}.chain",
                std::process::id()
            ));
            fs::write(&path, contents).expect("write temp chain");
            Self { path }
        }

        fn arg(&self) -> OsString {
            self.path.as_os_str().to_owned()
        }

        fn read(&self) -> String {
            fs::read_to_string(&self.path).expect("read temp chain")
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
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-sort-output-{}-{id}.{suffix}",
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

    fn arg(value: &str) -> OsString {
        OsString::from(value)
    }

    fn run_ok(args: Vec<OsString>, stdin_bytes: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let cli = SortHarness::try_parse_from(std::iter::once(arg("sort")).chain(args))
            .expect("sort args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect("sort should run");
        (stdout, stderr)
    }

    fn run_err(args: Vec<OsString>, stdin_bytes: &[u8]) -> CliError {
        let cli = SortHarness::try_parse_from(std::iter::once(arg("sort")).chain(args))
            .expect("sort args should parse");
        let mut stdin = BufReader::new(Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect_err("sort should fail")
    }

    #[test]
    fn score_sort_matches_primary_score_order_and_tie_breaks_by_id() {
        let input = TempChain::new(&format!("{SCORE_TIE_A}{SCORE_TIE_B}{SCORE_TIE_C}"));

        let (stdout, stderr) = run_ok(vec![arg("--chain"), input.arg()], b"");

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}")
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn target_sort_matches_name_then_start() {
        let input = TempChain::new(&format!("{TARGET_A}{TARGET_B}{TARGET_C}"));

        let (stdout, stderr) = run_ok(
            vec![arg("--sort-by"), arg("target"), arg("--chain"), input.arg()],
            b"",
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("{TARGET_B}{TARGET_A}{TARGET_C}")
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn query_sort_matches_name_then_start() {
        let input = TempChain::new(
            "chain 10 chr2 100 + 0 10 qry2 100 + 5 15 3\n10\n\n\
             chain 20 chr1 100 + 0 10 qry1 100 + 20 30 2\n10\n\n\
             chain 30 chr3 100 + 0 10 qry1 100 + 10 20 1\n10\n\n",
        );

        let (stdout, stderr) = run_ok(
            vec![arg("--sort-by"), arg("query"), arg("--chain"), input.arg()],
            b"",
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 30 chr3 100 + 0 10 qry1 100 + 10 20 1\n10\n\n\
             chain 20 chr1 100 + 0 10 qry1 100 + 20 30 2\n10\n\n\
             chain 10 chr2 100 + 0 10 qry2 100 + 5 15 3\n10\n\n"
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn metadata_lines_are_emitted_before_sorted_chains() {
        let (stdout, stderr) = run_ok(
            vec![],
            b"#meta-one\n#meta-two\nchain 1 chr2 10 + 0 5 qry2 10 + 0 5 2\n5\n\nchain 2 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n",
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "#meta-one\n#meta-two\nchain 2 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\nchain 1 chr2 10 + 0 5 qry2 10 + 0 5 2\n5\n\n"
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn out_index_uses_hex_offsets_for_score_groups() {
        let input = TempChain::new(&format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}"));
        let index = TempPath::new("tab");

        let (stdout, stderr) = run_ok(
            vec![arg("--chain"), input.arg(), arg("--out-index"), index.arg()],
            b"",
        );

        assert_eq!(
            fs::read_to_string(&index.path).unwrap(),
            "0\t300\n31\t100\n"
        );
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}")
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn out_index_uses_name_groups_for_target_sort() {
        let input = TempChain::new(&format!("{TARGET_A}{TARGET_B}{TARGET_C}"));
        let index = TempPath::new("tab");

        let (stdout, stderr) = run_ok(
            vec![
                arg("--sort-by"),
                arg("target"),
                arg("--chain"),
                input.arg(),
                arg("--out-index"),
                index.arg(),
            ],
            b"",
        );

        assert_eq!(
            fs::read_to_string(&index.path).unwrap(),
            "0\tchr1\n64\tchr2\n"
        );
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("{TARGET_B}{TARGET_A}{TARGET_C}")
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn sort_spills_and_merges_when_memory_budget_is_tight() {
        let input = TempChain::new(&format!("{SCORE_TIE_A}{SCORE_TIE_B}{SCORE_TIE_C}"));

        let (stdout, stderr) = run_ok(
            vec![
                arg("--chain"),
                input.arg(),
                arg("--max-gb"),
                arg("0.000000001"),
            ],
            b"",
        );

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}")
        );
        assert_eq!(stderr, b"");
    }

    #[test]
    fn sort_supports_same_input_and_output_path() {
        let input = TempChain::new(&format!("{SCORE_TIE_A}{SCORE_TIE_B}{SCORE_TIE_C}"));

        let (stdout, stderr) = run_ok(
            vec![arg("--chain"), input.arg(), arg("--out-chain"), input.arg()],
            b"",
        );

        assert_eq!(stdout, b"");
        assert_eq!(stderr, b"");
        assert_eq!(
            input.read(),
            format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}")
        );
    }

    #[test]
    fn sort_writes_to_out_chain_path() {
        let input = TempChain::new(&format!("{SCORE_TIE_A}{SCORE_TIE_B}{SCORE_TIE_C}"));
        let output = TempPath::new("chain");

        let (stdout, stderr) = run_ok(
            vec![
                arg("--chain"),
                input.arg(),
                arg("--out-chain"),
                output.arg(),
            ],
            b"",
        );

        assert_eq!(stdout, b"");
        assert_eq!(stderr, b"");
        assert_eq!(
            fs::read_to_string(&output.path).unwrap(),
            format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}")
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_output_compresses_sorted_stdout() {
        let (stdout, stderr) = run_ok(
            vec![arg("--gzip")],
            format!("{SCORE_TIE_A}{SCORE_TIE_B}{SCORE_TIE_C}").as_bytes(),
        );

        let mut decoder = flate2::read::MultiGzDecoder::new(Cursor::new(stdout));
        let mut decoded = String::new();
        decoder
            .read_to_string(&mut decoded)
            .expect("decode gzip stdout");

        assert_eq!(decoded, format!("{SCORE_TIE_C}{SCORE_TIE_B}{SCORE_TIE_A}"));
        assert_eq!(stderr, b"");
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn gzip_output_requires_gzip_feature() {
        let err = run_err(vec![arg("--gzip")], SCORE_TIE_A.as_bytes());

        assert!(err
            .to_string()
            .contains("--gzip requires chaintools to be built with the `gzip` feature"));
    }

    #[test]
    fn gzip_and_index_are_rejected_together() {
        let err = run_err(
            vec![arg("--gzip"), arg("--out-index"), arg("out.tab")],
            SCORE_TIE_A.as_bytes(),
        );

        assert!(err
            .to_string()
            .contains("--out-index cannot be combined with --gzip"));
    }

    #[test]
    fn zero_memory_budget_is_rejected() {
        let err = run_err(vec![arg("--max-gb"), arg("0")], SCORE_TIE_A.as_bytes());

        assert!(err
            .to_string()
            .contains("--max-gb must be greater than zero"));
    }

    #[test]
    fn hierarchical_merge_handles_many_runs() {
        let mut input = String::new();
        let mut expected = String::new();
        for id in (1..=130).rev() {
            let chain = format!("chain {id} chr1 100 + 0 10 qry1 100 + 0 10 {id}\n10\n\n");
            input.push_str(&chain);
        }
        for id in 130..=130 {
            let chain = format!("chain {id} chr1 100 + 0 10 qry1 100 + 0 10 {id}\n10\n\n");
            expected.push_str(&chain);
        }
        for id in (1..130).rev() {
            let chain = format!("chain {id} chr1 100 + 0 10 qry1 100 + 0 10 {id}\n10\n\n");
            expected.push_str(&chain);
        }

        let (stdout, stderr) = run_ok(vec![arg("--max-gb"), arg("0.000000001")], input.as_bytes());

        assert_eq!(String::from_utf8(stdout).unwrap(), expected);
        assert_eq!(stderr, b"");
    }
}
