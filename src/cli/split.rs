// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
#[cfg(feature = "gzip")]
use std::io::BufReader;
use std::io::{BufRead, BufWriter, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
#[cfg(feature = "mmap")]
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;
use memchr::memchr;

use super::CliError;

const IO_BUFFER_CAPACITY: usize = 1024 * 1024;
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

#[derive(Debug, Args)]
pub struct SplitArgs {
    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to the input chain file. If not provided, chain data is read from standard input."
    )]
    chain: Option<PathBuf>,

    #[arg(
        short = 'o',
        long = "outdir",
        value_name = "PATH",
        required = true,
        help = "Base output directory. Split files are written under <PATH>/chains/"
    )]
    outdir: PathBuf,

    #[arg(
        long = "files",
        value_name = "N",
        conflicts_with = "chunks",
        required_unless_present = "chunks",
        help = "Produce exactly N output files"
    )]
    files: Option<NonZeroUsize>,

    #[arg(
        long = "chunks",
        value_name = "N",
        conflicts_with = "files",
        required_unless_present = "files",
        help = "Maximum number of chains per output file"
    )]
    chunks: Option<NonZeroUsize>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress every split output file with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,
}

impl SplitArgs {
    pub(crate) fn writes_to_stdout(&self) -> bool {
        false
    }

    pub(crate) fn default_log_level(&self) -> log::LevelFilter {
        log::LevelFilter::Info
    }

    fn mode(&self) -> SplitMode {
        if let Some(files) = self.files {
            SplitMode::Files(files)
        } else {
            SplitMode::Chunks(
                self.chunks
                    .expect("clap enforces either --files or --chunks"),
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SplitMode {
    Files(NonZeroUsize),
    Chunks(NonZeroUsize),
}

impl SplitMode {
    fn describe(self) -> String {
        match self {
            SplitMode::Files(count) => format!("{} output files", count.get()),
            SplitMode::Chunks(count) => format!("{} chains per file", count.get()),
        }
    }
}

/// Runs the split subcommand.
///
/// Splits a chain file into multiple output files, either by exact file count
/// or by maximum chains per file.
///
/// # Arguments
///
/// * `args` - Split command arguments with input, output directory, and split mode
/// * `stdin` - Input stream (used if no --chain path provided)
/// * `_stdout` - Unused (output goes to --outdir)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
///
/// # Examples
///
/// ```bash
/// # Split into exactly 10 files
/// chaintools split --chain input.chain --out-dir out/ --files 10
///
/// # Split with max 1000 chains per file
/// chaintools split --chain input.chain --out-dir out/ --chunks 1000
/// ```
pub fn run<R, W, E>(
    args: SplitArgs,
    stdin: &mut R,
    _stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_output_args(&args)?;

    let input = if let Some(path) = &args.chain {
        log::info!("Loading chain text from {}", path.display());
        LoadedInput::from_path(path)?
    } else {
        log::info!("Reading chain text from standard input");
        LoadedInput::from_stdin(stdin)?
    };

    if input.bytes().is_empty() {
        log::error!("Input chain stream is empty");
        return Err(CliError::Message("input chain stream is empty".to_owned()));
    }

    let chain_starts = scan_chain_starts(input.bytes())?;
    if chain_starts.is_empty() {
        log::error!("Input contains no chain records");
        return Err(CliError::Message(
            "input contains no chain records".to_owned(),
        ));
    }

    log::info!(
        "Loaded {} chains from {} bytes of chain text",
        chain_starts.len(),
        input.bytes().len()
    );

    let output_dir = args.outdir.join("chains");
    fs::create_dir_all(&output_dir)?;
    log::debug!("Writing split files under {}", output_dir.display());

    let mode = args.mode();
    let collapsed = should_collapse(mode, chain_starts.len());

    if collapsed {
        log::warn!(
            "Requested {} for {} chains; producing a single output",
            mode.describe(),
            chain_starts.len()
        );
        let output = single_output_path(&output_dir, &input.basename, args.gzip);
        ensure_output_paths_absent(std::slice::from_ref(&output))?;

        if let Some(source_path) = input.symlink_source(args.gzip) {
            log::info!(
                "Symlinking original input {} to {}",
                source_path.display(),
                output.display()
            );
            create_symlink(&source_path, &output)?;
            return Ok(());
        }

        if input.source_path().is_none() {
            log::warn!("Cannot symlink standard input; writing a single output file instead");
        } else {
            log::warn!(
                "Cannot symlink input because requested output compression differs from the source"
            );
        }

        write_output_slice(
            output,
            input.shared_bytes(),
            0..input.bytes().len(),
            args.gzip,
        )?;
        log::info!("Finished writing 1 split file");
        return Ok(());
    }

    let plans = plan_outputs(
        &output_dir,
        &input.basename,
        input.bytes().len(),
        &chain_starts,
        mode,
        args.gzip,
    );
    ensure_output_paths_absent(
        &plans
            .iter()
            .map(|plan| plan.path.clone())
            .collect::<Vec<_>>(),
    )?;

    log::info!("Writing {} split files", plans.len());
    log::debug!("Split mode: {}", mode.describe());

    write_output_plans(&plans, input.shared_bytes(), args.gzip)?;

    log::info!(
        "Finished writing {} split files into {}",
        plans.len(),
        output_dir.display()
    );
    Ok(())
}

#[cfg(feature = "gzip")]
fn validate_output_args(_args: &SplitArgs) -> Result<(), CliError> {
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn validate_output_args(args: &SplitArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

fn should_collapse(mode: SplitMode, total_chains: usize) -> bool {
    match mode {
        SplitMode::Files(files) => files.get() > total_chains,
        SplitMode::Chunks(chunks) => chunks.get() > total_chains,
    }
}

fn plan_outputs(
    output_dir: &Path,
    basename: &str,
    total_bytes: usize,
    chain_starts: &[usize],
    mode: SplitMode,
    gzip: bool,
) -> Vec<OutputPlan> {
    let total_chains = chain_starts.len();
    let chain_ranges = match mode {
        SplitMode::Files(files) => (0..files.get())
            .map(|index| {
                let start = index * total_chains / files.get();
                let end = (index + 1) * total_chains / files.get();
                start..end
            })
            .collect::<Vec<_>>(),
        SplitMode::Chunks(chunks) => (0..total_chains)
            .step_by(chunks.get())
            .map(|start| start..std::cmp::min(start + chunks.get(), total_chains))
            .collect::<Vec<_>>(),
    };

    let width = std::cmp::max(5, decimal_width(chain_ranges.len()));
    chain_ranges
        .into_iter()
        .enumerate()
        .map(|(index, chain_range)| {
            let byte_start = if chain_range.start == 0 {
                0
            } else {
                chain_starts[chain_range.start]
            };
            let byte_end = if chain_range.end >= total_chains {
                total_bytes
            } else {
                chain_starts[chain_range.end]
            };
            let path = output_dir.join(output_filename(index + 1, width, basename, gzip));
            OutputPlan {
                path,
                byte_range: byte_start..byte_end,
            }
        })
        .collect()
}

fn single_output_path(output_dir: &Path, basename: &str, gzip: bool) -> PathBuf {
    output_dir.join(output_filename(1, 5, basename, gzip))
}

fn output_filename(index: usize, width: usize, basename: &str, gzip: bool) -> String {
    if gzip {
        format!("part.{index:0width$}.{basename}.chain.gz")
    } else {
        format!("part.{index:0width$}.{basename}.chain")
    }
}

fn decimal_width(mut value: usize) -> usize {
    let mut width = 1;
    while value >= 10 {
        value /= 10;
        width += 1;
    }
    width
}

fn ensure_output_paths_absent(paths: &[PathBuf]) -> Result<(), CliError> {
    for path in paths {
        if path.exists() {
            return Err(CliError::Message(format!(
                "refusing to overwrite existing split output {}",
                path.display()
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "parallel")]
fn write_output_plans(
    plans: &[OutputPlan],
    source: SharedInputBytes,
    gzip: bool,
) -> Result<(), CliError> {
    use rayon::prelude::*;

    plans.par_iter().try_for_each(|plan| {
        write_output_slice(
            plan.path.clone(),
            source.clone(),
            plan.byte_range.clone(),
            gzip,
        )
    })
}

#[cfg(not(feature = "parallel"))]
fn write_output_plans(
    plans: &[OutputPlan],
    source: SharedInputBytes,
    gzip: bool,
) -> Result<(), CliError> {
    for plan in plans {
        write_output_slice(
            plan.path.clone(),
            source.clone(),
            plan.byte_range.clone(),
            gzip,
        )?;
    }
    Ok(())
}

fn write_output_slice(
    path: PathBuf,
    source: SharedInputBytes,
    byte_range: std::ops::Range<usize>,
    gzip: bool,
) -> Result<(), CliError> {
    let slice = &source.as_slice()[byte_range];
    log::debug!("Writing {} bytes to {}", slice.len(), path.display());

    let file = File::create(&path)?;
    let writer = BufWriter::with_capacity(IO_BUFFER_CAPACITY, file);

    #[cfg(feature = "gzip")]
    if gzip {
        use flate2::{write::GzEncoder, Compression};

        let mut encoder = GzEncoder::new(writer, Compression::fast());
        encoder.write_all(slice)?;
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        return Ok(());
    }

    #[cfg(not(feature = "gzip"))]
    let _ = gzip;

    let mut writer = writer;
    writer.write_all(slice)?;
    writer.flush()?;
    Ok(())
}

fn scan_chain_starts(bytes: &[u8]) -> Result<Vec<usize>, CliError> {
    let mut starts = Vec::new();
    let mut pos = 0usize;
    let mut in_chain = false;

    while pos < bytes.len() {
        let line_start = pos;
        let (next_pos, line) = read_trimmed_line(bytes, pos);
        pos = next_pos;

        if is_blank(line) {
            in_chain = false;
            continue;
        }

        if !in_chain {
            if line.starts_with(b"#") {
                continue;
            }
            if line.starts_with(b"chain ") {
                starts.push(line_start);
                in_chain = true;
                continue;
            }
            return Err(CliError::Message(format!(
                "unexpected content at byte {line_start}: expected a chain header or metadata line"
            )));
        }

        if line.starts_with(b"#") {
            return Err(CliError::Message(format!(
                "metadata line inside chain record at byte {line_start}"
            )));
        }
        if line.starts_with(b"chain ") {
            return Err(CliError::Message(format!(
                "missing blank line between chain records at byte {line_start}"
            )));
        }
    }

    Ok(starts)
}

fn read_trimmed_line(bytes: &[u8], pos: usize) -> (usize, &[u8]) {
    if pos >= bytes.len() {
        return (pos, &[]);
    }
    let line_end = match memchr(b'\n', &bytes[pos..]) {
        Some(relative) => pos + relative,
        None => bytes.len(),
    };
    let next = if line_end < bytes.len() {
        line_end + 1
    } else {
        bytes.len()
    };
    let mut line = &bytes[pos..line_end];
    if let Some(b'\r') = line.last().copied() {
        line = &line[..line.len() - 1];
    }
    (next, line)
}

fn is_blank(line: &[u8]) -> bool {
    line.iter().all(|byte| byte.is_ascii_whitespace())
}

#[cfg_attr(all(feature = "mmap", not(feature = "gzip")), allow(dead_code))]
#[derive(Clone)]
enum SharedInputBytes {
    #[cfg(feature = "mmap")]
    Mmap(Arc<memmap2::Mmap>),
    Owned(std::sync::Arc<Vec<u8>>),
}

impl SharedInputBytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            SharedInputBytes::Mmap(bytes) => &bytes[..],
            SharedInputBytes::Owned(bytes) => bytes.as_slice(),
        }
    }
}

struct LoadedInput {
    bytes: SharedInputBytes,
    basename: String,
    source_path: Option<PathBuf>,
    source_encoding: SourceEncoding,
    _temp: Option<TempInputFile>,
}

impl LoadedInput {
    fn from_path(path: &Path) -> Result<Self, CliError> {
        if path.extension().is_some_and(|ext| ext == OsStr::new("gz")) {
            #[cfg(feature = "gzip")]
            {
                let file = File::open(path)?;
                let mut decoder = flate2::read::MultiGzDecoder::new(BufReader::with_capacity(
                    IO_BUFFER_CAPACITY,
                    file,
                ));
                let mut buffer = Vec::new();
                decoder.read_to_end(&mut buffer)?;
                log::debug!("Loaded gzip input through decompression");
                return Ok(Self {
                    bytes: SharedInputBytes::Owned(std::sync::Arc::new(buffer)),
                    basename: derive_basename(path),
                    source_path: Some(fs::canonicalize(path)?),
                    source_encoding: SourceEncoding::Gzip,
                    _temp: None,
                });
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(CliError::Chain(chaintools::storage::gzip_feature_error()));
            }
        }

        #[cfg(feature = "mmap")]
        {
            let file = File::open(path)?;
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
            log::debug!("Loaded plain input through memory mapping");
            return Ok(Self {
                bytes: SharedInputBytes::Mmap(Arc::new(mmap)),
                basename: derive_basename(path),
                source_path: Some(fs::canonicalize(path)?),
                source_encoding: SourceEncoding::Plain,
                _temp: None,
            });
        }

        #[cfg(not(feature = "mmap"))]
        {
            let buffer = fs::read(path)?;
            log::debug!("Loaded plain input into owned memory");
            Ok(Self {
                bytes: SharedInputBytes::Owned(std::sync::Arc::new(buffer)),
                basename: derive_basename(path),
                source_path: Some(fs::canonicalize(path)?),
                source_encoding: SourceEncoding::Plain,
                _temp: None,
            })
        }
    }

    fn from_stdin<R: Read>(stdin: &mut R) -> Result<Self, CliError> {
        let (temp, size) = TempInputFile::write_from_reader(stdin)?;
        if size == 0 {
            log::error!("Standard input is empty");
            return Err(CliError::Message("standard input is empty".to_owned()));
        }

        let gzip = temp.starts_with_gzip_magic()?;
        if gzip {
            #[cfg(feature = "gzip")]
            {
                let file = File::open(&temp.path)?;
                let mut decoder = flate2::read::MultiGzDecoder::new(BufReader::with_capacity(
                    IO_BUFFER_CAPACITY,
                    file,
                ));
                let mut buffer = Vec::new();
                decoder.read_to_end(&mut buffer)?;
                log::debug!("Decoded gzip-compressed standard input into memory");
                return Ok(Self {
                    bytes: SharedInputBytes::Owned(std::sync::Arc::new(buffer)),
                    basename: "stdin".to_owned(),
                    source_path: None,
                    source_encoding: SourceEncoding::Gzip,
                    _temp: Some(temp),
                });
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(CliError::Chain(chaintools::storage::gzip_feature_error()));
            }
        }

        #[cfg(feature = "mmap")]
        {
            let file = File::open(&temp.path)?;
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
            log::debug!("Staged standard input to a temporary mmap-backed file");
            return Ok(Self {
                bytes: SharedInputBytes::Mmap(Arc::new(mmap)),
                basename: "stdin".to_owned(),
                source_path: None,
                source_encoding: SourceEncoding::Plain,
                _temp: Some(temp),
            });
        }

        #[cfg(not(feature = "mmap"))]
        {
            let buffer = fs::read(&temp.path)?;
            log::debug!("Staged standard input into owned memory");
            Ok(Self {
                bytes: SharedInputBytes::Owned(std::sync::Arc::new(buffer)),
                basename: "stdin".to_owned(),
                source_path: None,
                source_encoding: SourceEncoding::Plain,
                _temp: Some(temp),
            })
        }
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    fn shared_bytes(&self) -> SharedInputBytes {
        self.bytes.clone()
    }

    fn source_path(&self) -> Option<&Path> {
        self.source_path.as_deref()
    }

    fn symlink_source(&self, gzip_output: bool) -> Option<PathBuf> {
        let path = self.source_path.as_ref()?;
        match (self.source_encoding, gzip_output) {
            (SourceEncoding::Plain, false) | (SourceEncoding::Gzip, true) => Some(path.clone()),
            _ => None,
        }
    }
}

#[cfg_attr(not(feature = "gzip"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceEncoding {
    Plain,
    Gzip,
}

#[derive(Debug, Clone)]
struct OutputPlan {
    path: PathBuf,
    byte_range: std::ops::Range<usize>,
}

struct TempInputFile {
    path: PathBuf,
}

impl TempInputFile {
    fn write_from_reader<R: Read>(reader: &mut R) -> Result<(Self, u64), CliError> {
        let path = create_temp_path("stdin")?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let size = copy_with_buffer(reader, &mut file)?;
        file.flush()?;
        Ok((Self { path }, size))
    }

    fn starts_with_gzip_magic(&self) -> Result<bool, CliError> {
        let mut file = File::open(&self.path)?;
        let mut magic = [0u8; 2];
        let read = file.read(&mut magic)?;
        Ok(read == 2 && magic == GZIP_MAGIC)
    }
}

impl Drop for TempInputFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn copy_with_buffer<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u64, CliError> {
    let mut buffer = [0u8; IO_BUFFER_CAPACITY];
    let mut total = 0u64;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        total += read as u64;
    }

    Ok(total)
}

fn create_temp_path(prefix: &str) -> Result<PathBuf, CliError> {
    let dir = std::env::temp_dir();
    for attempt in 0..1024u64 {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = dir.join(format!(
            ".chaintools-split-{prefix}-{}-{nonce}-{attempt}.tmp",
            std::process::id()
        ));
        if !path.exists() {
            return Ok(path);
        }
    }

    Err(CliError::Message(format!(
        "failed to create temporary {prefix} file in {}",
        dir.display()
    )))
}

fn derive_basename(path: &Path) -> String {
    let file_name = path
        .file_name()
        .unwrap_or_else(|| path.as_os_str())
        .to_string_lossy();
    let without_gz = file_name.strip_suffix(".gz").unwrap_or(&file_name);
    let without_chain = without_gz.strip_suffix(".chain").unwrap_or(without_gz);
    if without_chain.is_empty() {
        "input".to_owned()
    } else {
        without_chain.to_owned()
    }
}

#[cfg(unix)]
fn create_symlink(source: &Path, destination: &Path) -> Result<(), CliError> {
    std::os::unix::fs::symlink(source, destination)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(source: &Path, destination: &Path) -> Result<(), CliError> {
    std::os::windows::fs::symlink_file(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::ffi::OsString;
    use std::io::BufReader;
    #[cfg(feature = "gzip")]
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug, Parser)]
    struct SplitHarness {
        #[command(flatten)]
        args: SplitArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("chaintools-split-test-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn arg(value: &str) -> OsString {
        OsString::from(value)
    }

    fn run_ok(args: Vec<OsString>, stdin_bytes: &[u8]) {
        let cli = SplitHarness::try_parse_from(std::iter::once(arg("split")).chain(args))
            .expect("split args should parse");
        let mut stdin = BufReader::new(std::io::Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect("split should run");
    }

    fn run_err(args: Vec<OsString>, stdin_bytes: &[u8]) -> CliError {
        let cli = SplitHarness::try_parse_from(std::iter::once(arg("split")).chain(args))
            .expect("split args should parse");
        let mut stdin = BufReader::new(std::io::Cursor::new(stdin_bytes));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(cli.args, &mut stdin, &mut stdout, &mut stderr).expect_err("split should fail")
    }

    fn read_split_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut entries = fs::read_dir(dir)
            .expect("read split dir")
            .map(|entry| {
                let entry = entry.expect("dir entry");
                let name = entry.file_name().to_string_lossy().into_owned();
                let bytes = fs::read(entry.path()).expect("read split file");
                (name, bytes)
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    #[test]
    fn split_by_chunks_preserves_original_bytes_when_recombined() {
        let temp = TempDir::new();
        let input = temp.path.join("sample.chain");
        let output = temp.path.join("out");
        let input_bytes = b"#meta\nchain 10 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n#between\nchain 20 chr2 10 + 0 6 qry2 10 + 0 6 2\n6\n\n#tail\n";
        fs::write(&input, input_bytes).expect("write input");

        run_ok(
            vec![
                arg("--chain"),
                input.as_os_str().to_owned(),
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--chunks"),
                arg("1"),
            ],
            b"",
        );

        let files = read_split_files(&output.join("chains"));
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, "part.00001.sample.chain");
        assert_eq!(files[1].0, "part.00002.sample.chain");

        let rebuilt = files
            .into_iter()
            .flat_map(|(_, bytes)| bytes)
            .collect::<Vec<_>>();
        assert_eq!(rebuilt, input_bytes);
    }

    #[test]
    fn split_by_files_produces_exact_file_count() {
        let temp = TempDir::new();
        let input = temp.path.join("sample.chain");
        let output = temp.path.join("out");
        fs::write(
            &input,
            b"chain 1 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\nchain 2 chr1 10 + 0 5 qry1 10 + 0 5 2\n5\n\nchain 3 chr1 10 + 0 5 qry1 10 + 0 5 3\n5\n\n",
        )
        .expect("write input");

        run_ok(
            vec![
                arg("--chain"),
                input.as_os_str().to_owned(),
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--files"),
                arg("2"),
            ],
            b"",
        );

        let files = read_split_files(&output.join("chains"));
        assert_eq!(files.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn split_symlinks_when_requested_count_exceeds_chain_count() {
        let temp = TempDir::new();
        let input = temp.path.join("sample.chain");
        let output = temp.path.join("out");
        fs::write(&input, b"chain 1 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n").expect("write input");

        run_ok(
            vec![
                arg("--chain"),
                input.as_os_str().to_owned(),
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--files"),
                arg("3"),
            ],
            b"",
        );

        let split = output.join("chains").join("part.00001.sample.chain");
        assert!(fs::symlink_metadata(&split)
            .expect("metadata")
            .file_type()
            .is_symlink());
    }

    #[test]
    fn split_stdin_falls_back_to_single_written_file() {
        let temp = TempDir::new();
        let output = temp.path.join("out");
        let input = b"chain 1 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\n";

        run_ok(
            vec![
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--files"),
                arg("3"),
            ],
            input,
        );

        let files = read_split_files(&output.join("chains"));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "part.00001.stdin.chain");
        assert_eq!(files[0].1, input);
    }

    #[test]
    fn split_rejects_empty_input() {
        let temp = TempDir::new();
        let output = temp.path.join("out");

        let err = run_err(
            vec![
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--chunks"),
                arg("1"),
            ],
            b"",
        );

        assert!(err.to_string().contains("standard input is empty"));
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn split_can_gzip_outputs() {
        use flate2::read::MultiGzDecoder;

        let temp = TempDir::new();
        let input = temp.path.join("sample.chain");
        let output = temp.path.join("out");
        fs::write(
            &input,
            b"chain 1 chr1 10 + 0 5 qry1 10 + 0 5 1\n5\n\nchain 2 chr1 10 + 0 5 qry1 10 + 0 5 2\n5\n\n",
        )
        .expect("write input");

        run_ok(
            vec![
                arg("--chain"),
                input.as_os_str().to_owned(),
                arg("--outdir"),
                output.as_os_str().to_owned(),
                arg("--chunks"),
                arg("1"),
                arg("--gzip"),
            ],
            b"",
        );

        let files = read_split_files(&output.join("chains"));
        assert_eq!(files.len(), 2);
        assert!(files[0].0.ends_with(".chain.gz"));

        let mut decoder = MultiGzDecoder::new(Cursor::new(files[0].1.clone()));
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).expect("decode gzip");
        assert!(decoded.starts_with("chain 1 "));
    }
}
