// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use chaintools::{storage::is_gz_path, StreamingReader};
use clap::{Args, ValueEnum};
#[cfg(feature = "gzip")]
use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};

use super::sort_core::{
    emit_sorted_chains, write_metadata_lines, SortAccumulator, SortCriterion,
    OUTPUT_BUFFER_CAPACITY,
};
use super::CliError;

const BYTES_PER_GB: f64 = 1_000_000_000.0;
const DEFAULT_MAX_GB: f64 = 16.0;
const COPY_BUFFER_CAPACITY: usize = 1024 * 1024;

/// Command-line arguments for the merge subcommand.
///
/// Provides options for merging multiple chain files together, with optional
/// sorting and gzip compression support.
///
/// # Examples
///
/// ```bash
/// chaintools merge --chains a.chain b.chain --out-chain merged.chain --sort-by score
/// ```
#[derive(Debug, Args)]
pub struct MergeArgs {
    #[arg(
        short = 'c',
        long = "chains",
        value_name = "PATH",
        num_args = 1..,
        conflicts_with = "file",
        required_unless_present = "file",
        help = "Input chain files to merge"
    )]
    chains: Option<Vec<PathBuf>>,

    #[arg(
        short = 'f',
        long = "file",
        value_name = "PATH",
        conflicts_with = "chains",
        required_unless_present = "chains",
        help = "Path to a file listing one input chain path per line"
    )]
    file: Option<PathBuf>,

    #[arg(
        short = 'o',
        long = "out-chain",
        value_name = "PATH",
        required = true,
        help = "Path for the merged chain output"
    )]
    out_chain: PathBuf,

    #[arg(
        short = 'S',
        long = "sort-by",
        value_name = "KEY",
        value_enum,
        help = "Sort merged output by the selected primary key"
    )]
    sort_by: Option<MergeSortBy>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress merged output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        short = 'M',
        long = "max-gb",
        value_name = "GB",
        default_value_t = DEFAULT_MAX_GB,
        help = "Maximum in-memory working set in gigabytes before spilling sorted runs to temporary files"
    )]
    max_gb: f64,
}

impl MergeArgs {
    pub(crate) fn writes_to_stdout(&self) -> bool {
        false
    }

    pub(crate) fn default_log_level(&self) -> log::LevelFilter {
        log::LevelFilter::Info
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

/// Sort criteria for merged output.
///
/// # Variants
///
/// * `Score` - Sort by chain score (descending)
/// * `Id` - Sort by chain ID
/// * `Reference` - Sort by reference sequence name and position
/// * `Query` - Sort by query sequence name and position
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MergeSortBy {
    Score,
    Id,
    Reference,
    Query,
}

impl MergeSortBy {
    fn criterion(self) -> SortCriterion {
        match self {
            MergeSortBy::Score => SortCriterion::Score,
            MergeSortBy::Id => SortCriterion::Id,
            MergeSortBy::Reference => SortCriterion::Reference,
            MergeSortBy::Query => SortCriterion::Query,
        }
    }
}

/// Runs the merge subcommand.
///
/// Merges multiple chain files into a single output file, with optional
/// sorting and gzip compression.
///
/// # Arguments
///
/// * `args` - Merge arguments with input paths and output
/// * `_stdin` - Unused (input via --chains or --file)
/// * `_stdout` - Unused (output via --out-chain)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
///
/// # Examples
///
/// ```bash
/// # Merge multiple files
/// chaintools merge --chains a.chain b.chain c.chain --out-chain merged.chain
///
/// # Merge and sort by score
/// chaintools merge --chains a.chain b.chain --out-chain sorted.chain --sort-by score
/// ```
pub fn run<R, W, E>(
    args: MergeArgs,
    _stdin: &mut R,
    _stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_output_args(&args)?;
    let inputs = collect_input_paths(&args)?;
    validate_input_paths(&args.out_chain, &inputs)?;

    if let Some(sort_by) = args.sort_by {
        let max_in_memory_bytes = args.max_in_memory_bytes()?;
        let temp_dir = output_directory(&args.out_chain);
        let mut accumulator =
            SortAccumulator::new(sort_by.criterion(), max_in_memory_bytes, &temp_dir);

        for path in &inputs {
            let mut reader = StreamingReader::from_path(path)?;
            accumulator.push_stream(&mut reader)?;
        }

        let (metadata, sorted) = accumulator.finish()?;
        emit_sorted_output(&args, &metadata, sorted, sort_by.criterion())?;
    } else {
        emit_unsorted_output(&args, &inputs)?;
    }

    Ok(())
}

/// Validates output arguments for merge command.
#[cfg(feature = "gzip")]
fn validate_output_args(_args: &MergeArgs) -> Result<(), CliError> {
    Ok(())
}

#[cfg(not(feature = "gzip"))]
/// Validates output arguments for merge command.
///
/// Returns an error if gzip is requested but the gzip feature is not enabled.
fn validate_output_args(args: &MergeArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

/// Collects input chain file paths from arguments.
///
/// Reads paths from --chains directly or from a file listing paths.
///
/// # Arguments
///
/// * `args` - Merge command arguments
///
/// # Output
///
/// Returns `Ok(Vec<PathBuf>)` with input paths or `Err(CliError)` on failure
fn collect_input_paths(args: &MergeArgs) -> Result<Vec<PathBuf>, CliError> {
    if let Some(paths) = &args.chains {
        return Ok(paths.clone());
    }

    let list_path = args
        .file
        .as_ref()
        .expect("clap enforces either --chains or --file");
    let file = File::open(list_path)?;
    let mut reader = BufReader::with_capacity(COPY_BUFFER_CAPACITY, file);
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

/// Validates that output path differs from all input paths.
///
/// Ensures the output chain file is not the same as any input file.
///
/// # Arguments
///
/// * `out_chain` - Output chain path
/// * `inputs` - Input chain paths
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` if output matches an input
fn validate_input_paths(out_chain: &Path, inputs: &[PathBuf]) -> Result<(), CliError> {
    let output = normalize_output_path(out_chain)?;

    for input in inputs {
        let normalized_input = normalize_existing_path(input)?;
        if normalized_input == output {
            return Err(CliError::Message(format!(
                "--out-chain must not be the same path as input chain {}",
                input.display()
            )));
        }
    }

    Ok(())
}

/// Normalizes an existing input path to its canonical form.
///
/// # Arguments
///
/// * `path` - Input path to normalize
///
/// # Output
///
/// Returns `Ok(PathBuf)` with the canonical path or `Err(CliError)` on failure
fn normalize_existing_path(path: &Path) -> Result<PathBuf, CliError> {
    std::fs::canonicalize(path).map_err(CliError::from)
}

/// Normalizes an output path (which may not exist yet).
///
/// If the path exists, canonicalizes it. Otherwise, resolves relative to current directory.
///
/// # Arguments
///
/// * `path` - Output path to normalize
///
/// # Output
///
/// Returns `Ok(PathBuf)` with the normalized path or `Err(CliError)` on failure
fn normalize_output_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.exists() {
        return normalize_existing_path(path);
    }

    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    Ok(std::env::current_dir()?.join(path))
}

/// Returns the directory containing the output chain file.
///
/// # Arguments
///
/// * `out_chain` - Output chain path
///
/// # Output
///
/// Returns the parent directory or "." if no parent
fn output_directory(out_chain: &Path) -> PathBuf {
    out_chain
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

/// Emits sorted output by merging all runs.
///
/// Opens all run files, performs k-way merge, and writes sorted chains.
///
/// # Arguments
///
/// * `args` - Merge command arguments
/// * `metadata` - Metadata lines to output
/// * `sorted` - Sorted runs to merge
/// * `sort_by` - Sort criterion
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn emit_sorted_output(
    args: &MergeArgs,
    metadata: &[Vec<u8>],
    sorted: super::sort_core::SortedInput,
    sort_by: SortCriterion,
) -> Result<(), CliError> {
    #[cfg(feature = "gzip")]
    if args.gzip {
        let writer = open_output_writer(&args.out_chain)?;
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        write_metadata_lines(&mut encoder, metadata)?;
        emit_sorted_chains(&mut encoder, sorted, sort_by)?;
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        return Ok(());
    }

    let mut writer = open_output_writer(&args.out_chain)?;
    write_metadata_lines(&mut writer, metadata)?;
    emit_sorted_chains(&mut writer, sorted, sort_by)?;
    writer.flush()?;
    Ok(())
}

/// Emits unsorted output by appending all input files.
///
/// Concatenates input chain files with proper record separation.
///
/// # Arguments
///
/// * `args` - Merge command arguments
/// * `inputs` - Input chain file paths
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn emit_unsorted_output(args: &MergeArgs, inputs: &[PathBuf]) -> Result<(), CliError> {
    #[cfg(feature = "gzip")]
    if args.gzip {
        let writer = open_output_writer(&args.out_chain)?;
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        append_unsorted_inputs(inputs, &mut encoder)?;
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        return Ok(());
    }

    let mut writer = open_output_writer(&args.out_chain)?;
    append_unsorted_inputs(inputs, &mut writer)?;
    writer.flush()?;
    Ok(())
}

/// Opens a buffered writer for the output chain file.
///
/// # Arguments
///
/// * `path` - Output file path
///
/// # Output
///
/// Returns `Ok(BufWriter<File>)` or `Err(CliError)` on failure
fn open_output_writer(path: &Path) -> Result<BufWriter<File>, CliError> {
    let file = File::create(path)?;
    Ok(BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file))
}

/// Appends all unsorted input files to the writer.
///
/// # Arguments
///
/// * `inputs` - Input file paths
/// * `writer` - Output writer
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn append_unsorted_inputs<W: Write>(inputs: &[PathBuf], writer: &mut W) -> Result<(), CliError> {
    let mut boundary = BoundaryState::default();

    for path in inputs {
        append_unsorted_input(path, writer, &mut boundary)?;
    }

    Ok(())
}

/// Appends a single unsorted input file to the writer.
///
/// Handles gzip detection and decompression, ensuring proper record separation.
///
/// # Arguments
///
/// * `path` - Input file path
/// * `writer` - Output writer
/// * `boundary` - State for tracking record boundaries
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn append_unsorted_input<W: Write>(
    path: &Path,
    writer: &mut W,
    boundary: &mut BoundaryState,
) -> Result<(), CliError> {
    #[cfg(feature = "gzip")]
    if is_gz_path(path) {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(COPY_BUFFER_CAPACITY, file);
        let decoder = MultiGzDecoder::new(reader);
        let reader = BufReader::with_capacity(COPY_BUFFER_CAPACITY, decoder);
        return copy_reader(reader, writer, boundary);
    }

    #[cfg(not(feature = "gzip"))]
    if is_gz_path(path) {
        return Err(CliError::Chain(chaintools::storage::gzip_feature_error()));
    }

    let file = File::open(path)?;
    let reader = BufReader::with_capacity(COPY_BUFFER_CAPACITY, file);
    copy_reader(reader, writer, boundary)
}

/// Copies data from a reader to a writer, tracking boundaries.
///
/// Reads the entire reader and writes to the writer while observing
/// boundary state for record separation.
///
/// # Arguments
///
/// * `reader` - Input reader
/// * `writer` - Output writer
/// * `boundary` - State for tracking record boundaries
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn copy_reader<R: Read, W: Write>(
    mut reader: R,
    writer: &mut W,
    boundary: &mut BoundaryState,
) -> Result<(), CliError> {
    let mut buffer = [0u8; COPY_BUFFER_CAPACITY];
    let read = reader.read(&mut buffer)?;
    if read == 0 {
        return Ok(());
    }

    ensure_record_separator(writer, boundary)?;
    writer.write_all(&buffer[..read])?;
    boundary.observe(&buffer[..read]);

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        boundary.observe(&buffer[..read]);
    }

    Ok(())
}

/// Ensures proper record separation between input files.
///
/// Adds blank lines if needed based on the trailing newlines of the previous file.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `boundary` - State for tracking record boundaries
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn ensure_record_separator<W: Write>(
    writer: &mut W,
    boundary: &mut BoundaryState,
) -> Result<(), CliError> {
    if !boundary.wrote_any {
        return Ok(());
    }

    match boundary.trailing_newlines() {
        0 => {
            writer.write_all(b"\n\n")?;
            boundary.observe(b"\n\n");
        }
        1 => {
            writer.write_all(b"\n")?;
            boundary.observe(b"\n");
        }
        _ => {}
    }

    Ok(())
}

#[derive(Default)]
struct BoundaryState {
    wrote_any: bool,
    penultimate: Option<u8>,
    last: Option<u8>,
}

impl BoundaryState {
    fn trailing_newlines(&self) -> u8 {
        match (self.penultimate, self.last) {
            (_, Some(b'\n')) if self.penultimate == Some(b'\n') => 2,
            (_, Some(b'\n')) => 1,
            _ => 0,
        }
    }

    fn observe(&mut self, buf: &[u8]) {
        if buf.is_empty() {
            return;
        }

        self.wrote_any = true;
        match buf.len() {
            1 => {
                self.penultimate = self.last;
                self.last = Some(buf[0]);
            }
            len => {
                self.penultimate = Some(buf[len - 2]);
                self.last = Some(buf[len - 1]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    #[cfg(feature = "gzip")]
    use std::{fs, io::Cursor};

    #[derive(Debug, Parser)]
    struct MergeHarness {
        #[command(flatten)]
        args: MergeArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempPath {
        path: PathBuf,
    }

    impl TempPath {
        fn new(suffix: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "chaintools-merge-test-{}-{id}.{suffix}",
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
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn arg(value: &str) -> OsString {
        OsString::from(value)
    }

    fn write_file(path: &Path, contents: &[u8]) {
        std::fs::write(path, contents).expect("write temp file");
    }

    #[cfg(feature = "gzip")]
    fn write_gzip(path: &Path, contents: &[u8]) {
        let file = File::create(path).expect("create gzip");
        let mut encoder = GzEncoder::new(file, Compression::fast());
        encoder.write_all(contents).expect("write gzip");
        encoder.finish().expect("finish gzip");
    }

    fn run_ok(args: Vec<OsString>) -> Vec<u8> {
        let cli = MergeHarness::try_parse_from(std::iter::once(arg("merge")).chain(args))
            .expect("merge args should parse");
        let mut stdin = std::io::Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect("merge should run");
        stdout
    }

    fn run_err(args: Vec<OsString>) -> CliError {
        let cli = MergeHarness::try_parse_from(std::iter::once(arg("merge")).chain(args))
            .expect("merge args should parse");
        let mut stdin = std::io::Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect_err("merge should fail")
    }

    #[test]
    fn merge_concatenates_inputs_in_order() {
        let input_a = TempPath::new("chain");
        let input_b = TempPath::new("chain");
        let output = TempPath::new("chain");
        write_file(
            &input_a.path,
            b"chain 10 chr1 100 + 0 10 qry1 100 + 0 10 1\n10\n",
        );
        write_file(
            &input_b.path,
            b"chain 20 chr2 100 + 0 20 qry2 100 + 0 20 2\n20\n\n",
        );

        let stdout = run_ok(vec![
            arg("--chains"),
            input_a.arg(),
            input_b.arg(),
            arg("--out-chain"),
            output.arg(),
        ]);

        assert_eq!(stdout, b"");
        assert_eq!(
            std::fs::read_to_string(&output.path).unwrap(),
            "chain 10 chr1 100 + 0 10 qry1 100 + 0 10 1\n10\n\nchain 20 chr2 100 + 0 20 qry2 100 + 0 20 2\n20\n\n"
        );
    }

    #[test]
    fn merge_reads_inputs_from_file_list() {
        let input_a = TempPath::new("chain");
        let input_b = TempPath::new("chain");
        let list = TempPath::new("txt");
        let output = TempPath::new("chain");
        write_file(
            &input_a.path,
            b"chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n",
        );
        write_file(
            &input_b.path,
            b"chain 6 chr2 10 + 0 6 qry2 10 + 0 6 2\n6\n\n",
        );
        write_file(
            &list.path,
            format!("{}\n\n{}\n", input_a.path.display(), input_b.path.display()).as_bytes(),
        );

        run_ok(vec![
            arg("--file"),
            list.arg(),
            arg("--out-chain"),
            output.arg(),
        ]);

        assert_eq!(
            std::fs::read_to_string(&output.path).unwrap(),
            "chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\nchain 6 chr2 10 + 0 6 qry2 10 + 0 6 2\n6\n\n"
        );
    }

    #[test]
    fn merge_rejects_same_input_and_output_path() {
        let input = TempPath::new("chain");
        write_file(&input.path, b"chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n");

        let err = run_err(vec![
            arg("--chains"),
            input.arg(),
            arg("--out-chain"),
            input.arg(),
        ]);

        assert!(err
            .to_string()
            .contains("--out-chain must not be the same path as input chain"));
    }

    #[test]
    fn merge_sorts_by_reference() {
        let input_a = TempPath::new("chain");
        let input_b = TempPath::new("chain");
        let output = TempPath::new("chain");
        write_file(
            &input_a.path,
            b"#a\nchain 5 chr2 100 + 0 5 qry2 100 + 0 5 2\n5\n\n",
        );
        write_file(
            &input_b.path,
            b"#b\nchain 5 chr1 100 + 0 5 qry1 100 + 0 5 1\n5\n\n",
        );

        run_ok(vec![
            arg("--chains"),
            input_a.arg(),
            input_b.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--sort-by"),
            arg("reference"),
        ]);

        assert_eq!(
            std::fs::read_to_string(&output.path).unwrap(),
            "#a\n#b\nchain 5 chr1 100 + 0 5 qry1 100 + 0 5 1\n5\n\nchain 5 chr2 100 + 0 5 qry2 100 + 0 5 2\n5\n\n"
        );
    }

    #[test]
    fn merge_sorts_by_id() {
        let input_a = TempPath::new("chain");
        let input_b = TempPath::new("chain");
        let output = TempPath::new("chain");
        write_file(
            &input_a.path,
            b"chain 5 chr2 100 + 0 5 qry2 100 + 0 5 20\n5\n\n",
        );
        write_file(
            &input_b.path,
            b"chain 9 chr1 100 + 0 5 qry1 100 + 0 5 10\n5\n\n",
        );

        run_ok(vec![
            arg("--chains"),
            input_a.arg(),
            input_b.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--sort-by"),
            arg("id"),
        ]);

        assert_eq!(
            std::fs::read_to_string(&output.path).unwrap(),
            "chain 9 chr1 100 + 0 5 qry1 100 + 0 5 10\n5\n\nchain 5 chr2 100 + 0 5 qry2 100 + 0 5 20\n5\n\n"
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn merge_reads_gzip_input_and_writes_gzip_output() {
        let input_a = TempPath::new("chain.gz");
        let input_b = TempPath::new("chain");
        let output = TempPath::new("chain.gz");
        write_gzip(&input_a.path, b"chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n");
        write_file(
            &input_b.path,
            b"chain 6 chr2 10 + 0 6 qry2 10 + 0 6 2\n6\n\n",
        );

        run_ok(vec![
            arg("--chains"),
            input_a.arg(),
            input_b.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--gzip"),
        ]);

        let mut decoder = MultiGzDecoder::new(Cursor::new(fs::read(&output.path).unwrap()));
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).expect("decode gzip");

        assert_eq!(
            decoded,
            "chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\nchain 6 chr2 10 + 0 6 qry2 10 + 0 6 2\n6\n\n"
        );
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn merge_rejects_gzip_without_feature() {
        let input = TempPath::new("chain");
        let output = TempPath::new("chain.gz");
        write_file(&input.path, b"chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n");

        let err = run_err(vec![
            arg("--chains"),
            input.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--gzip"),
        ]);

        assert!(err
            .to_string()
            .contains("--gzip requires chaintools to be built with the `gzip` feature"));
    }

    #[test]
    fn zero_memory_budget_is_rejected() {
        let input = TempPath::new("chain");
        let output = TempPath::new("chain");
        write_file(&input.path, b"chain 5 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n");

        let err = run_err(vec![
            arg("--chains"),
            input.arg(),
            arg("--out-chain"),
            output.arg(),
            arg("--sort-by"),
            arg("score"),
            arg("--max-gb"),
            arg("0"),
        ]);

        assert!(err
            .to_string()
            .contains("--max-gb must be greater than zero"));
    }
}
