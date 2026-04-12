// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::PathBuf;

use chaintools::antirepeat::{AntiRepeatConfig, AntiRepeatEngine};
use chaintools::sequence::SequenceCache;
use chaintools::writer::write_chain_dense;
use chaintools::{OwnedChain, StreamItem, StreamingReader};
use clap::Args;
#[cfg(feature = "gzip")]
use flate2::{write::GzEncoder, Compression};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MAX_BATCH_CHAINS: usize = 256;
const MAX_BATCH_BLOCKS: usize = 65_536;

/// Command-line arguments for the anti-repeat subcommand.
///
/// Filters chain files by removing alignments dominated by repeats or
/// degenerate DNA. Requires reference and query sequence files for analysis.
///
/// # Examples
///
/// ```bash
/// chaintools anti-repeat --reference target.2bit --query query.2bit --chain input.chain --out-chain filtered.chain
/// ```
#[derive(Debug, Args)]
pub struct AntiRepeatArgs {
    #[arg(
        short = 'r',
        long = "reference",
        value_name = "PATH",
        help = "Path to the reference sequence file (.2bit, .fa, .fasta, .fna, and gzip variants)."
    )]
    reference: PathBuf,

    #[arg(
        short = 'q',
        long = "query",
        value_name = "PATH",
        help = "Path to the query sequence file (.2bit, .fa, .fasta, .fna, and gzip variants)."
    )]
    query: PathBuf,

    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to the input .chain file. If not provided, chain data is read from standard input."
    )]
    chain: Option<PathBuf>,

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
        help = "Compress anti-repeat output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        short = 'm',
        long = "min-score",
        default_value_t = 5_000,
        value_name = "SCORE",
        help = "Minimum adjusted chain score required to keep a chain"
    )]
    min_score: i64,

    #[arg(
        short = 'M',
        long = "no-check-score",
        default_value_t = 200_000,
        value_name = "SCORE",
        help = "Chains scoring at least this value bypass sequence-based checks"
    )]
    no_check_score: i64,
}

impl AntiRepeatArgs {
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
}

/// Runs the anti-repeat subcommand.
///
/// Filters chain alignments dominated by repeats or degenerate DNA
/// using reference and query sequence data.
///
/// # Arguments
///
/// * `args` - Anti-repeat arguments with reference/query sequences
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
/// # Filter repeats using 2bit files
/// chaintools anti-repeat --reference target.2bit --query query.2bit --chain input.chain --out-chain filtered.chain
///
/// # Filter from stdin with gzip output
/// chaintools anti-repeat --reference ref.2bit --query qry.2bit --gzip > output.chain.gz
/// ```
pub fn run<R, W, E>(
    args: AntiRepeatArgs,
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

    let engine = AntiRepeatEngine::new(
        &args.reference,
        &args.query,
        AntiRepeatConfig {
            min_score: args.min_score,
            no_check_score: args.no_check_score,
        },
    )?;

    #[cfg(feature = "gzip")]
    if args.gzip {
        let writer = open_output_writer(&args, stdout)?;
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        if let Some(path) = &args.chain {
            let mut reader = StreamingReader::from_path(path)?;
            process_stream(&mut reader, &engine, &mut encoder)?;
        } else {
            let mut reader = StreamingReader::new(stdin);
            process_stream(&mut reader, &engine, &mut encoder)?;
        }
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        return Ok(());
    }

    let mut writer = open_output_writer(&args, stdout)?;
    if let Some(path) = &args.chain {
        let mut reader = StreamingReader::from_path(path)?;
        process_stream(&mut reader, &engine, &mut writer)?;
    } else {
        let mut reader = StreamingReader::new(stdin);
        process_stream(&mut reader, &engine, &mut writer)?;
    }
    writer.flush()?;
    Ok(())
}

/// Validates output arguments for anti-repeat command.
///
/// Checks that gzip output is only used when the gzip feature is enabled.
///
/// # Arguments
///
/// * `_args` - Anti-repeat command arguments
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` if invalid
#[cfg(feature = "gzip")]
fn validate_output_args(_args: &AntiRepeatArgs) -> Result<(), CliError> {
    Ok(())
}

/// Validates output arguments for anti-repeat command.
///
/// Returns an error if gzip is requested but the gzip feature is not enabled.
///
/// # Arguments
///
/// * `args` - Anti-repeat command arguments
///
/// # Output
///
/// Returns `Ok(())` if valid or `Err(CliError)` if gzip requested without feature
///
/// # Examples
///
/// ```
/// use chaintools::cli::anti_repeat::{validate_output_args, AntiRepeatArgs};
/// use chaintools::CliError;
///
/// let args = AntiRepeatArgs::test_validate_args(vec!["chaintools", "--in", "file.chain"]);
/// assert!(validate_output_args(&args).is_ok());
///
/// let args = AntiRepeatArgs::test_validate_args(vec!["chaintools", "--in", "file.chain", "--gzip"]);
/// assert!(matches!(validate_output_args(&args), Err(CliError::Message(_))));
/// ```
#[cfg(not(feature = "gzip"))]
fn validate_output_args(args: &AntiRepeatArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

/// Processes chains from a streaming reader through the anti-repeat engine.
///
/// Reads chains from the reader, batches them, and writes passing chains to the writer.
/// Batches are flushed when reaching size or block limits.
///
/// # Arguments
///
/// * `reader` - Streaming reader for input chains
/// * `engine` - Anti-repeat filter engine
/// * `writer` - Output writer for filtered chains
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
fn process_stream<R: BufRead, W: Write>(
    reader: &mut StreamingReader<R>,
    engine: &AntiRepeatEngine,
    writer: &mut W,
) -> Result<(), CliError> {
    let mut pending = Vec::new();
    let mut pending_blocks = 0usize;

    while let Some(item) = reader.next_item()? {
        match item {
            StreamItem::MetaLine(line) => {
                flush_pending(&mut pending, engine, writer)?;
                pending_blocks = 0;
                writer.write_all(&line)?;
                writer.write_all(b"\n")?;
            }
            StreamItem::Header(header) => {
                let blocks = reader.read_blocks(header.offset)?;
                pending_blocks = pending_blocks.saturating_add(blocks.len());
                pending.push(header.into_chain(blocks));
                if pending.len() >= MAX_BATCH_CHAINS || pending_blocks >= MAX_BATCH_BLOCKS {
                    flush_pending(&mut pending, engine, writer)?;
                    pending_blocks = 0;
                }
            }
        }
    }

    flush_pending(&mut pending, engine, writer)?;
    Ok(())
}

/// Flushes pending chains through the anti-repeat engine.
///
/// Takes all pending chains, filters them using the anti-repeat engine,
/// and writes the resulting chains to the output writer.
///
/// # Arguments
///
/// * `pending` - Vector of pending chains to process
/// * `engine` - Anti-repeat filter engine
/// * `writer` - Output writer
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::anti_repeat::flush_pending;
/// use chaintools::cli::anti_repeat::AntiRepeatEngine;
/// use chaintools::OwnedChain;
///
/// let mut pending = vec![];
/// let engine = AntiRepeatEngine::new(100);
/// let mut writer = Vec::new();
/// let _ = flush_pending(&mut pending, &engine, &mut writer);
/// ```
fn flush_pending<W: Write>(
    pending: &mut Vec<OwnedChain>,
    engine: &AntiRepeatEngine,
    writer: &mut W,
) -> Result<(), CliError> {
    if pending.is_empty() {
        return Ok(());
    }

    let batch = std::mem::take(pending);
    for chain in filter_batch(batch, engine)? {
        write_chain_dense(writer, &chain)?;
    }
    Ok(())
}

/// Filters a batch of chains using parallel processing.
///
/// Uses Rayon to process chains in parallel, checking each against the anti-repeat engine.
///
/// # Arguments
///
/// * `batch` - Vector of chains to filter
/// * `engine` - Anti-repeat filter engine
///
/// # Output
///
/// Returns `Ok(Vec<OwnedChain>)` containing chains that pass the filter
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::anti_repeat::{filter_batch, AntiRepeatEngine};
///
/// let engine = AntiRepeatEngine::default();
/// let batch = vec![chain1, chain2, chain3];
/// let filtered = filter_batch(batch, &engine)?;
/// ```
#[cfg(feature = "parallel")]
fn filter_batch(
    batch: Vec<OwnedChain>,
    engine: &AntiRepeatEngine,
) -> Result<Vec<OwnedChain>, CliError> {
    use rayon::prelude::*;

    let results: Vec<Result<Option<OwnedChain>, chaintools::ChainError>> = batch
        .into_par_iter()
        .map_init(SequenceCache::default, |cache, chain| {
            if engine.chain_passes(cache, &chain)? {
                Ok(Some(chain))
            } else {
                Ok(None)
            }
        })
        .collect();

    let mut kept = Vec::new();
    for result in results {
        if let Some(chain) = result? {
            kept.push(chain);
        }
    }
    Ok(kept)
}

/// Filters a batch of chains using sequential processing.
///
/// Processes chains sequentially when the parallel feature is not enabled.
///
/// # Arguments
///
/// * `batch` - Vector of chains to filter
/// * `engine` - Anti-repeat filter engine
///
/// # Output
///
/// Returns `Ok(Vec<OwnedChain>)` containing chains that pass the filter
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::anti_repeat::{filter_batch, AntiRepeatEngine};
///
/// let engine = AntiRepeatEngine::default();
/// let batch = vec![chain1, chain2, chain3];
/// let filtered = filter_batch(batch, &engine)?;
/// ```
#[cfg(not(feature = "parallel"))]
fn filter_batch(
    batch: Vec<OwnedChain>,
    engine: &AntiRepeatEngine,
) -> Result<Vec<OwnedChain>, CliError> {
    let mut cache = SequenceCache::default();
    let mut kept = Vec::new();
    for chain in batch {
        if engine.chain_passes(&mut cache, &chain)? {
            kept.push(chain);
        }
    }
    Ok(kept)
}

enum OutputWriter<'a, W: Write> {
    Stdout(&'a mut W),
    File(BufWriter<File>),
}

impl<W: Write> Write for OutputWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            OutputWriter::Stdout(writer) => writer.write(buf),
            OutputWriter::File(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OutputWriter::Stdout(writer) => writer.flush(),
            OutputWriter::File(writer) => writer.flush(),
        }
    }
}

/// Opens the output writer based on arguments.
///
/// Returns a writer that writes to either stdout or a file.
///
/// # Arguments
///
/// * `args` - Anti-repeat command arguments
/// * `stdout` - Standard output writer
///
/// # Output
///
/// Returns `Ok(OutputWriter)` for writing filtered chains
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::anti_repeat::open_output_writer;
/// use chaintools::AntiRepeatArgs;
/// use std::io::BufWriter;
///
/// let args = AntiRepeatArgs::test_validate_args(vec!["chaintools"]);
/// let mut stdout = BufWriter::new(Vec::new());
/// let writer = open_output_writer(&args, &mut stdout).unwrap();
/// ```
fn open_output_writer<'a, W: Write>(
    args: &AntiRepeatArgs,
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[cfg(feature = "gzip")]
    use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};
    use std::fs;
    #[cfg(feature = "gzip")]
    use std::io::Read;
    use std::io::{BufWriter, Cursor};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use twobit::convert::fasta::FastaReader;
    use twobit::convert::to_2bit;

    #[derive(Debug, Parser)]
    struct Harness {
        #[command(flatten)]
        args: AntiRepeatArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "chaintools-antirepeat-test-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_twobit(path: &Path, fasta: &str) {
        let reader = FastaReader::mem_open(fasta.as_bytes().to_vec()).expect("open FASTA");
        let mut writer = BufWriter::new(File::create(path).expect("create 2bit"));
        to_2bit(&mut writer, &reader).expect("write 2bit");
        writer.flush().expect("flush 2bit");
    }

    fn write_text(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write text file");
    }

    #[cfg(feature = "gzip")]
    fn write_gzip_text(path: &Path, contents: &str) {
        let file = File::create(path).expect("create gzip file");
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(contents.as_bytes())
            .expect("write gzip contents");
        encoder.finish().expect("finish gzip file");
    }

    fn run_command(args: AntiRepeatArgs, stdin_data: &str) -> (Vec<u8>, Vec<u8>) {
        let mut stdin = Cursor::new(stdin_data.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(args, &mut stdin, &mut stdout, &mut stderr).expect("anti-repeat run");
        (stdout, stderr)
    }

    #[test]
    fn parses_minimal_args() {
        let cli = Harness::try_parse_from([
            "chaintools",
            "--reference",
            "target.2bit",
            "--query",
            "query.2bit",
        ])
        .expect("anti-repeat arguments should parse");

        assert_eq!(cli.args.reference, PathBuf::from("target.2bit"));
        assert_eq!(cli.args.query, PathBuf::from("query.2bit"));
        assert!(cli.args.chain.is_none());
        assert!(cli.args.out_chain.is_none());
        assert_eq!(cli.args.min_score, 5_000);
        assert_eq!(cli.args.no_check_score, 200_000);
    }

    #[test]
    fn rejects_unsupported_reference_or_query_paths() {
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let err = run(
            AntiRepeatArgs {
                reference: PathBuf::from("reference.txt"),
                query: PathBuf::from("query.2bit"),
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("unsupported reference should be rejected");

        assert!(err.to_string().contains("unsupported sequence format"));
    }

    #[test]
    fn preserves_metadata_and_kept_chain_format_on_stdin() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.2bit");
        let query = temp.path().join("query.2bit");
        write_twobit(&reference, ">chr1\nAAAA\n");
        write_twobit(&query, ">chr1\naaaa\n");

        let input = concat!(
            "#meta-one\n",
            "chain 250000 chr1 4 + 0 4 chr1 4 + 0 4 7\n",
            "4\n\n",
            "#meta-two\n",
            "chain 10000 chr1 4 + 0 4 chr1 4 + 0 4 8\n",
            "4\n\n"
        );

        let (stdout, _) = run_command(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            input,
        );

        let expected = concat!(
            "#meta-one\n",
            "chain 250000 chr1 4 + 0 4 chr1 4 + 0 4 7\n",
            "4\n\n",
            "#meta-two\n"
        );
        assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    }

    #[test]
    fn minus_strand_query_fetch_is_reverse_complemented() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.2bit");
        let query = temp.path().join("query.2bit");
        write_twobit(&reference, ">chr1\nAGTC\n");
        write_twobit(&query, ">chr1\nTTGACTAA\n");

        let chain = "chain 10000 chr1 4 + 0 4 chr1 8 - 2 6 1\n4\n\n";
        let (stdout, _) = run_command(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            chain,
        );

        assert_eq!(String::from_utf8(stdout).unwrap(), chain);
    }

    #[test]
    fn all_n_matches_are_rejected_like_ucsc() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.2bit");
        let query = temp.path().join("query.2bit");
        write_twobit(&reference, ">chr1\nNNNN\n");
        write_twobit(&query, ">chr1\nNNNN\n");

        let chain = "chain 10000 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let (stdout, _) = run_command(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            chain,
        );

        assert!(stdout.is_empty(), "all-N chain should be discarded");
    }

    #[test]
    fn plain_fasta_inputs_are_supported() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.fasta");
        let query = temp.path().join("query.fa");
        write_text(&reference, ">chr1 reference\nACGT\n");
        write_text(&query, ">chr1 query\nACGT\n");

        let chain = "chain 10000 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let (stdout, _) = run_command(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            chain,
        );

        assert_eq!(String::from_utf8(stdout).unwrap(), chain);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzipped_fasta_inputs_are_supported() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.fasta.gz");
        let query = temp.path().join("query.fa.gz");
        write_gzip_text(&reference, ">chr1 reference\nACGT\n");
        write_gzip_text(&query, ">chr1 query\nACGT\n");

        let chain = "chain 10000 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let (stdout, _) = run_command(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            chain,
        );

        assert_eq!(String::from_utf8(stdout).unwrap(), chain);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_output_writes_compressed_kept_chain() {
        let temp = TempDir::new();
        let reference = temp.path().join("reference.2bit");
        let query = temp.path().join("query.2bit");
        let output = temp.path().join("output.chain.gz");
        write_twobit(&reference, ">chr1\nACGT\n");
        write_twobit(&query, ">chr1\nACGT\n");

        let chain = "chain 10000 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let mut stdin = Cursor::new(chain.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            AntiRepeatArgs {
                reference,
                query,
                chain: None,
                out_chain: Some(output.clone()),
                gzip: true,
                min_score: 5_000,
                no_check_score: 200_000,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect("anti-repeat run");

        let mut decoded = String::new();
        MultiGzDecoder::new(Cursor::new(fs::read(output).expect("read gzip output")))
            .read_to_string(&mut decoded)
            .expect("decode gzip output");
        assert_eq!(decoded, chain);
    }
}
