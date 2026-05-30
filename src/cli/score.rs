// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::PathBuf;

use chaintools::io::writer::{write_chain_dense, write_chain_header};
use chaintools::seq::score::chainscore::ChainScorer;
use chaintools::seq::score::gapcalc::GapCalc;
use chaintools::seq::score::scoring::ScoreMatrix;
use chaintools::seq::sequence::SequenceCache;
use chaintools::{ChainError, OwnedChain, StreamItem, StreamingReader};
use clap::Args;
#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const MAX_BATCH_CHAINS: usize = 256;
const MAX_BATCH_BLOCKS: usize = 65_536;

/// Command-line arguments for the score subcommand.
///
/// Recomputes each chain's score from sequence, equivalent to UCSC
/// `chainScore`: the input header score is ignored, every chain is rescored
/// from the target/query sequences, and chains scoring below `--min-score`
/// are dropped. Output preserves input order by default (a streaming, parallel
/// path); `--sort-by-score` opts into kent's score-descending order.
///
/// # Examples
///
/// ```bash
/// chaintools score --reference target.2bit --query query.2bit --chain input.chain --out-chain scored.chain
/// ```
#[derive(Debug, Args)]
pub struct ScoreArgs {
    #[arg(
        short = 'r',
        long = "reference",
        value_name = "PATH",
        help = "Path to the reference (target) sequence file (.2bit, .fa, .fasta, .fna, and gzip variants)."
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
        help = "Path for the rescored chain output. If not provided, output is written to standard output."
    )]
    out_chain: Option<PathBuf>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress score output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,

    #[arg(
        short = 'm',
        long = "min-score",
        default_value_t = 1_000,
        value_name = "SCORE",
        help = "Minimum recomputed chain score required to keep a chain"
    )]
    min_score: i64,

    #[arg(
        short = 's',
        long = "score-scheme",
        value_name = "PATH",
        help = "Path to a blastz/lastz score-matrix file. Defaults to the UCSC default DNA matrix."
    )]
    score_scheme: Option<PathBuf>,

    #[arg(
        short = 'g',
        long = "linear-gap",
        value_name = "SPEC",
        help = "Gap cost model: a file path, or one of `loose` (default), `medium`/`original`, `cheap`, `rnaDna`."
    )]
    linear_gap: Option<String>,

    #[arg(
        long = "sort-by-score",
        help = "Buffer all kept chains and emit them in score-descending order (mimics UCSC ordering). Disables streaming."
    )]
    sort_by_score: bool,

    #[arg(
        short = 'M',
        long = "skip-missing-chains",
        help = "Skip (instead of erroring on) chains whose reference/query sequence is absent from the input. Dropped chains are reported as warnings on stderr."
    )]
    skip_missing_chains: bool,
}

impl ScoreArgs {
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

/// Runs the score subcommand.
///
/// Recomputes chain scores from reference/query sequence and filters by
/// `--min-score`, in a manner output-equivalent to UCSC `chainScore`.
///
/// # Arguments
///
/// * `args` - Score arguments with reference/query sequences and options
/// * `stdin` - Input stream (used if no --chain provided)
/// * `stdout` - Output stream (used if no --out-chain provided)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub fn run<R, W, E>(
    args: ScoreArgs,
    stdin: &mut R,
    stdout: &mut W,
    stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_output_args(&args)?;

    let matrix = match &args.score_scheme {
        Some(path) => ScoreMatrix::from_blastz_file(path)?,
        None => ScoreMatrix::default_dna(),
    };
    let gap = match &args.linear_gap {
        Some(spec) => GapCalc::from_linear_gap(spec)?,
        None => GapCalc::default_costs(),
    };
    let scorer = ChainScorer::new(&args.reference, &args.query, matrix, gap)?;

    // Chains skipped because of a missing sequence (only when
    // --skip-missing-chains is set); reported as warnings after processing.
    let mut skipped: Vec<SkippedChain> = Vec::new();

    #[cfg(feature = "gzip")]
    if args.gzip {
        let writer = open_output_writer(&args, stdout)?;
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        if let Some(path) = &args.chain {
            let mut reader = StreamingReader::from_path(path)?;
            process_stream(&mut reader, &scorer, &mut encoder, &args, &mut skipped)?;
        } else {
            let mut reader = StreamingReader::new(stdin);
            process_stream(&mut reader, &scorer, &mut encoder, &args, &mut skipped)?;
        }
        encoder.try_finish()?;
        encoder.get_mut().flush()?;
        warn_skipped(stderr, &skipped)?;
        return Ok(());
    }

    let mut writer = open_output_writer(&args, stdout)?;
    if let Some(path) = &args.chain {
        let mut reader = StreamingReader::from_path(path)?;
        process_stream(&mut reader, &scorer, &mut writer, &args, &mut skipped)?;
    } else {
        let mut reader = StreamingReader::new(stdin);
        process_stream(&mut reader, &scorer, &mut writer, &args, &mut skipped)?;
    }
    writer.flush()?;
    warn_skipped(stderr, &skipped)?;
    Ok(())
}

/// Validates output arguments for the score command.
#[cfg(feature = "gzip")]
fn validate_output_args(_args: &ScoreArgs) -> Result<(), CliError> {
    Ok(())
}

/// Validates output arguments for the score command.
///
/// Returns an error if gzip is requested but the gzip feature is not enabled.
#[cfg(not(feature = "gzip"))]
fn validate_output_args(args: &ScoreArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

/// Processes chains from a streaming reader through the scorer.
///
/// Dispatches to the streaming (input-order) path or, when `--sort-by-score`
/// is set, the buffered score-descending path. Chains skipped because of a
/// missing sequence are appended to `skipped`.
fn process_stream<R: BufRead, W: Write>(
    reader: &mut StreamingReader<R>,
    scorer: &ChainScorer,
    writer: &mut W,
    args: &ScoreArgs,
    skipped: &mut Vec<SkippedChain>,
) -> Result<(), CliError> {
    if args.sort_by_score {
        process_sorted(
            reader,
            scorer,
            writer,
            args.min_score,
            args.skip_missing_chains,
            skipped,
        )
    } else {
        process_streaming(
            reader,
            scorer,
            writer,
            args.min_score,
            args.skip_missing_chains,
            skipped,
        )
    }
}

/// Streaming, input-order processing (the default, fastest path).
///
/// Accumulates chains into a bounded batch, flushing on metadata lines (which
/// are echoed inline, in input order) and at EOF. Each flush rescores the
/// batch in parallel and writes kept chains in batch (input) order.
fn process_streaming<R: BufRead, W: Write>(
    reader: &mut StreamingReader<R>,
    scorer: &ChainScorer,
    writer: &mut W,
    min_score: i64,
    skip_missing: bool,
    skipped: &mut Vec<SkippedChain>,
) -> Result<(), CliError> {
    let mut pending = Vec::new();
    let mut pending_blocks = 0usize;

    while let Some(item) = reader.next_item()? {
        match item {
            StreamItem::MetaLine(line) => {
                flush_pending(
                    &mut pending,
                    scorer,
                    writer,
                    min_score,
                    skip_missing,
                    skipped,
                )?;
                pending_blocks = 0;
                writer.write_all(&line)?;
                writer.write_all(b"\n")?;
            }
            StreamItem::Header(header) => {
                let blocks = reader.read_blocks(header.offset)?;
                pending_blocks = pending_blocks.saturating_add(blocks.len());
                pending.push(header.into_chain(blocks));
                if pending.len() >= MAX_BATCH_CHAINS || pending_blocks >= MAX_BATCH_BLOCKS {
                    flush_pending(
                        &mut pending,
                        scorer,
                        writer,
                        min_score,
                        skip_missing,
                        skipped,
                    )?;
                    pending_blocks = 0;
                }
            }
        }
    }

    flush_pending(
        &mut pending,
        scorer,
        writer,
        min_score,
        skip_missing,
        skipped,
    )?;
    Ok(())
}

/// Buffered, score-descending processing (`--sort-by-score`).
///
/// Comments are collected and emitted first (in input order), then all kept
/// chains are emitted sorted by score descending (stable). This is not
/// streaming: the full kept set is held in memory.
fn process_sorted<R: BufRead, W: Write>(
    reader: &mut StreamingReader<R>,
    scorer: &ChainScorer,
    writer: &mut W,
    min_score: i64,
    skip_missing: bool,
    skipped: &mut Vec<SkippedChain>,
) -> Result<(), CliError> {
    let mut comments: Vec<Vec<u8>> = Vec::new();
    let mut kept: Vec<OwnedChain> = Vec::new();
    let mut pending = Vec::new();
    let mut pending_blocks = 0usize;

    while let Some(item) = reader.next_item()? {
        match item {
            StreamItem::MetaLine(line) => {
                collect_pending(
                    &mut pending,
                    scorer,
                    min_score,
                    &mut kept,
                    skip_missing,
                    skipped,
                )?;
                pending_blocks = 0;
                comments.push(line);
            }
            StreamItem::Header(header) => {
                let blocks = reader.read_blocks(header.offset)?;
                pending_blocks = pending_blocks.saturating_add(blocks.len());
                pending.push(header.into_chain(blocks));
                if pending.len() >= MAX_BATCH_CHAINS || pending_blocks >= MAX_BATCH_BLOCKS {
                    collect_pending(
                        &mut pending,
                        scorer,
                        min_score,
                        &mut kept,
                        skip_missing,
                        skipped,
                    )?;
                    pending_blocks = 0;
                }
            }
        }
    }
    collect_pending(
        &mut pending,
        scorer,
        min_score,
        &mut kept,
        skip_missing,
        skipped,
    )?;

    for line in &comments {
        writer.write_all(line)?;
        writer.write_all(b"\n")?;
    }
    kept.sort_by(|a, b| b.score.cmp(&a.score));
    for chain in &kept {
        write_chain_dense(writer, chain)?;
    }
    Ok(())
}

/// A chain dropped because its reference/query sequence is absent.
struct SkippedChain {
    /// Reconstructed chain header line (the "specific line" that was dropped).
    header: String,
    /// Name of the sequence that was missing.
    missing: String,
}

/// Outcome of rescoring a single chain.
enum ChainScore {
    /// Successfully rescored (header score already overwritten).
    Scored(OwnedChain),
    /// Skipped because a referenced sequence was missing (skip_missing only).
    MissingSkipped { header: String, missing: String },
}

/// Rescores the pending batch and writes kept chains in batch order.
fn flush_pending<W: Write>(
    pending: &mut Vec<OwnedChain>,
    scorer: &ChainScorer,
    writer: &mut W,
    min_score: i64,
    skip_missing: bool,
    skipped: &mut Vec<SkippedChain>,
) -> Result<(), CliError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending);
    for outcome in score_batch(batch, scorer, skip_missing)? {
        match outcome {
            ChainScore::Scored(chain) => {
                if chain.score >= min_score {
                    write_chain_dense(writer, &chain)?;
                }
            }
            ChainScore::MissingSkipped { header, missing } => {
                skipped.push(SkippedChain { header, missing });
            }
        }
    }
    Ok(())
}

/// Rescores the pending batch and appends kept chains to `kept` in batch order.
fn collect_pending(
    pending: &mut Vec<OwnedChain>,
    scorer: &ChainScorer,
    min_score: i64,
    kept: &mut Vec<OwnedChain>,
    skip_missing: bool,
    skipped: &mut Vec<SkippedChain>,
) -> Result<(), CliError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending);
    for outcome in score_batch(batch, scorer, skip_missing)? {
        match outcome {
            ChainScore::Scored(chain) => {
                if chain.score >= min_score {
                    kept.push(chain);
                }
            }
            ChainScore::MissingSkipped { header, missing } => {
                skipped.push(SkippedChain { header, missing });
            }
        }
    }
    Ok(())
}

/// Rescores a batch of chains in parallel, overwriting each header score.
///
/// Returns one [`ChainScore`] per input chain, in input order; filtering by
/// `min_score` is left to the caller. When `skip_missing` is set, a chain whose
/// sequence is absent yields [`ChainScore::MissingSkipped`] instead of erroring.
#[cfg(feature = "parallel")]
fn score_batch(
    batch: Vec<OwnedChain>,
    scorer: &ChainScorer,
    skip_missing: bool,
) -> Result<Vec<ChainScore>, CliError> {
    use rayon::prelude::*;

    let results: Vec<Result<ChainScore, ChainError>> = batch
        .into_par_iter()
        .map_init(SequenceCache::default, |cache, chain| {
            score_one(scorer, cache, chain, skip_missing)
        })
        .collect();

    let mut scored = Vec::with_capacity(results.len());
    for result in results {
        scored.push(result?);
    }
    Ok(scored)
}

/// Rescores a batch of chains sequentially, overwriting each header score.
#[cfg(not(feature = "parallel"))]
fn score_batch(
    batch: Vec<OwnedChain>,
    scorer: &ChainScorer,
    skip_missing: bool,
) -> Result<Vec<ChainScore>, CliError> {
    let mut cache = SequenceCache::default();
    let mut scored = Vec::with_capacity(batch.len());
    for chain in batch {
        scored.push(score_one(scorer, &mut cache, chain, skip_missing)?);
    }
    Ok(scored)
}

/// Rescores a single chain, classifying a missing-sequence error as a skip.
///
/// On success the chain's header score is overwritten and the chain is returned
/// as [`ChainScore::Scored`]. A [`ChainError::MissingSequence`] is converted to
/// [`ChainScore::MissingSkipped`] when `skip_missing` is set; otherwise (and for
/// every other error) the error propagates.
fn score_one(
    scorer: &ChainScorer,
    cache: &mut SequenceCache,
    mut chain: OwnedChain,
    skip_missing: bool,
) -> Result<ChainScore, ChainError> {
    match scorer.score_chain(cache, &chain) {
        Ok(score) => {
            chain.score = score;
            Ok(ChainScore::Scored(chain))
        }
        Err(err) => {
            if skip_missing && matches!(err, ChainError::MissingSequence { .. }) {
                Ok(ChainScore::MissingSkipped {
                    header: format_header_line(&chain),
                    missing: missing_sequence_name(err),
                })
            } else {
                Err(err)
            }
        }
    }
}

/// Extracts the name from a [`ChainError::MissingSequence`] (empty otherwise).
fn missing_sequence_name(err: ChainError) -> String {
    match err {
        ChainError::MissingSequence { name } => name.into_owned(),
        _ => String::new(),
    }
}

/// Reconstructs a chain's header line (without trailing newline) for reporting.
fn format_header_line(chain: &OwnedChain) -> String {
    let mut buf = Vec::new();
    // write_chain_header only fails on I/O; writing to a Vec cannot fail.
    let _ = write_chain_header(&mut buf, chain);
    String::from_utf8_lossy(&buf).trim_end().to_string()
}

/// Emits a consolidated warning to stderr listing every dropped chain.
///
/// Prints one summary line with the count, then one `WARN:` line per dropped
/// chain naming the missing sequence and echoing the chain header. Independent
/// of the logging subsystem, so it appears even when chains are written to
/// stdout (where logging is disabled).
fn warn_skipped<E: Write>(stderr: &mut E, skipped: &[SkippedChain]) -> Result<(), CliError> {
    if skipped.is_empty() {
        return Ok(());
    }
    writeln!(
        stderr,
        "WARN: skipped {} chain(s) referencing sequences missing from the reference/query",
        skipped.len()
    )?;
    for entry in skipped {
        writeln!(
            stderr,
            "WARN:   dropped (missing sequence '{}'): {}",
            entry.missing, entry.header
        )?;
    }
    Ok(())
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

/// Opens the output writer based on arguments (stdout or a file).
fn open_output_writer<'a, W: Write>(
    args: &ScoreArgs,
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
    use flate2::read::MultiGzDecoder;
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
        args: ScoreArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("chaintools-score-test-{}-{id}", std::process::id()));
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

    fn base_args(reference: PathBuf, query: PathBuf) -> ScoreArgs {
        ScoreArgs {
            reference,
            query,
            chain: None,
            out_chain: None,
            gzip: false,
            min_score: 1_000,
            score_scheme: None,
            linear_gap: None,
            sort_by_score: false,
            skip_missing_chains: false,
        }
    }

    fn run_command(args: ScoreArgs, stdin_data: &str) -> Vec<u8> {
        run_command_io(args, stdin_data).0
    }

    fn run_command_io(args: ScoreArgs, stdin_data: &str) -> (Vec<u8>, Vec<u8>) {
        let mut stdin = Cursor::new(stdin_data.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(args, &mut stdin, &mut stdout, &mut stderr).expect("score run");
        (stdout, stderr)
    }

    #[test]
    fn parses_minimal_args_with_defaults() {
        let cli =
            Harness::try_parse_from(["chaintools", "--reference", "t.2bit", "--query", "q.2bit"])
                .expect("score arguments should parse");

        assert_eq!(cli.args.reference, PathBuf::from("t.2bit"));
        assert_eq!(cli.args.query, PathBuf::from("q.2bit"));
        assert!(cli.args.chain.is_none());
        assert!(cli.args.out_chain.is_none());
        assert_eq!(cli.args.min_score, 1_000);
        assert!(cli.args.score_scheme.is_none());
        assert!(cli.args.linear_gap.is_none());
        assert!(!cli.args.sort_by_score);
    }

    #[test]
    fn parses_score_scheme_and_linear_gap() {
        let cli = Harness::try_parse_from([
            "chaintools",
            "--reference",
            "t.2bit",
            "--query",
            "q.2bit",
            "--score-scheme",
            "matrix.txt",
            "--linear-gap",
            "medium",
            "--min-score",
            "2000",
            "--sort-by-score",
        ])
        .expect("score arguments should parse");

        assert_eq!(cli.args.score_scheme, Some(PathBuf::from("matrix.txt")));
        assert_eq!(cli.args.linear_gap.as_deref(), Some("medium"));
        assert_eq!(cli.args.min_score, 2_000);
        assert!(cli.args.sort_by_score);
    }

    #[test]
    fn rescores_overwrites_header_score() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        // Input score is a lie (5); recomputed score is 764. Lower the
        // threshold so the rescored chain is kept (764 < default min 1000).
        let input = "chain 5 chr1 8 + 0 8 chr1 8 + 0 8 1\n8\n\n";
        let mut args = base_args(reference, query);
        args.min_score = 100;
        let stdout = run_command(args, input);
        let expected = "chain 764 chr1 8 + 0 8 chr1 8 + 0 8 1\n8\n\n";
        assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    }

    #[test]
    fn drops_chains_below_min_score() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGT\n");
        write_twobit(&query, ">chr1\nACGT\n");

        // Recomputed score is 382 < default min 1000 -> dropped.
        let input = "chain 999999 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let mut args = base_args(reference.clone(), query.clone());
        let stdout = run_command(args, input);
        assert!(stdout.is_empty(), "below-min chain should be dropped");

        // Lower the threshold and it is kept with the recomputed score.
        args = base_args(reference, query);
        args.min_score = 100;
        let stdout = run_command(args, input);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 382 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n"
        );
    }

    #[test]
    fn preserves_comments_inline_in_input_order() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        let input = concat!(
            "#meta-one\n",
            "chain 0 chr1 8 + 0 8 chr1 8 + 0 8 7\n",
            "8\n\n",
            "#meta-two\n",
            "chain 0 chr1 4 + 0 4 chr1 4 + 0 4 8\n",
            "4\n\n"
        );
        let mut args = base_args(reference, query);
        args.min_score = 100;
        let stdout = run_command(args, input);
        let expected = concat!(
            "#meta-one\n",
            "chain 764 chr1 8 + 0 8 chr1 8 + 0 8 7\n",
            "8\n\n",
            "#meta-two\n",
            "chain 382 chr1 4 + 0 4 chr1 4 + 0 4 8\n",
            "4\n\n"
        );
        assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    }

    #[test]
    fn sort_by_score_emits_comments_then_descending() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        // First chain rescores to 382, second to 764. Sorted desc => 764 first.
        let input = concat!(
            "#header\n",
            "chain 0 chr1 4 + 0 4 chr1 4 + 0 4 1\n",
            "4\n\n",
            "chain 0 chr1 8 + 0 8 chr1 8 + 0 8 2\n",
            "8\n\n"
        );
        let mut args = base_args(reference, query);
        args.min_score = 100;
        args.sort_by_score = true;
        let stdout = run_command(args, input);
        let expected = concat!(
            "#header\n",
            "chain 764 chr1 8 + 0 8 chr1 8 + 0 8 2\n",
            "8\n\n",
            "chain 382 chr1 4 + 0 4 chr1 4 + 0 4 1\n",
            "4\n\n"
        );
        assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    }

    #[test]
    fn rejects_unsupported_reference_path() {
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let err = run(
            ScoreArgs {
                reference: PathBuf::from("reference.txt"),
                query: PathBuf::from("query.2bit"),
                chain: None,
                out_chain: None,
                gzip: false,
                min_score: 1_000,
                score_scheme: None,
                linear_gap: None,
                sort_by_score: false,
                skip_missing_chains: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("unsupported reference should be rejected");

        assert!(err.to_string().contains("unsupported sequence format"));
    }

    #[test]
    fn minus_strand_query_is_reverse_complemented() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nAGTC\n");
        write_twobit(&query, ">chr1\nTTGACTAA\n");

        // RC of query[2..6] = "GACT" -> "AGTC" == reference. Score 382.
        let input = "chain 0 chr1 4 + 0 4 chr1 8 - 2 6 1\n4\n\n";
        let mut args = base_args(reference, query);
        args.min_score = 100;
        let stdout = run_command(args, input);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 382 chr1 4 + 0 4 chr1 8 - 2 6 1\n4\n\n"
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_output_writes_compressed_kept_chain() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        let output = temp.path().join("output.chain.gz");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        let input = "chain 0 chr1 8 + 0 8 chr1 8 + 0 8 1\n8\n\n";
        let mut stdin = Cursor::new(input.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut args = base_args(reference, query);
        args.out_chain = Some(output.clone());
        args.gzip = true;
        args.min_score = 100;
        run(args, &mut stdin, &mut stdout, &mut stderr).expect("score run");

        let mut decoded = String::new();
        MultiGzDecoder::new(Cursor::new(fs::read(output).expect("read gzip output")))
            .read_to_string(&mut decoded)
            .expect("decode gzip output");
        assert_eq!(decoded, "chain 764 chr1 8 + 0 8 chr1 8 + 0 8 1\n8\n\n");
    }

    #[test]
    fn score_scheme_file_is_applied() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        let scheme = temp.path().join("matrix.txt");
        write_twobit(&reference, ">chr1\nACGT\n");
        write_twobit(&query, ">chr1\nACGT\n");
        // All-tens matrix: every ACGT pair scores 10. ACGT vs ACGT = 40.
        fs::write(
            &scheme,
            "A C G T\n10 10 10 10\n10 10 10 10\n10 10 10 10\n10 10 10 10\n",
        )
        .expect("write scheme");

        let input = "chain 0 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n";
        let mut args = base_args(reference, query);
        args.score_scheme = Some(scheme);
        args.min_score = 1;
        let stdout = run_command(args, input);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 40 chr1 4 + 0 4 chr1 4 + 0 4 1\n4\n\n"
        );
    }

    #[test]
    fn linear_gap_alias_changes_gap_cost() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTNNACGT\n");

        // Two ACGT blocks (382 each) with a query gap of 2.
        let input = "chain 0 chr1 8 + 0 8 chr1 10 + 0 10 1\n4\t0\t2\n4\n\n";

        // Default (loose): cost(2,0) = 360 -> 382 + 382 - 360 = 404.
        let mut args = base_args(reference.clone(), query.clone());
        args.min_score = 1;
        let stdout = run_command(args, input);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 404 chr1 8 + 0 8 chr1 10 + 0 10 1\n4\t0\t2\n4\n\n"
        );

        // Medium: cost(2,0) = 425 -> 382 + 382 - 425 = 339.
        let mut args = base_args(reference, query);
        args.min_score = 1;
        args.linear_gap = Some("medium".to_owned());
        let stdout = run_command(args, input);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 339 chr1 8 + 0 8 chr1 10 + 0 10 1\n4\t0\t2\n4\n\n"
        );
    }

    #[test]
    fn missing_sequence_errors_without_flag() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        // References chrZ, which is absent from the 2bit.
        let input = "chain 0 chrZ 8 + 0 8 chrZ 8 + 0 8 1\n8\n\n";
        let mut stdin = Cursor::new(input.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut args = base_args(reference, query);
        args.min_score = 1;
        let err = run(args, &mut stdin, &mut stdout, &mut stderr)
            .expect_err("missing sequence should error without --skip-missing-chains");
        assert!(err.to_string().contains("missing sequence"));
    }

    #[test]
    fn skip_missing_chains_drops_and_warns() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        // First chain references absent chrZ; second is valid on chr1 (-> 764).
        let input = concat!(
            "chain 0 chrZ 8 + 0 8 chrZ 8 + 0 8 1\n8\n\n",
            "chain 0 chr1 8 + 0 8 chr1 8 + 0 8 2\n8\n\n"
        );
        let mut args = base_args(reference, query);
        args.min_score = 1;
        args.skip_missing_chains = true;
        let (stdout, stderr) = run_command_io(args, input);

        // Only the valid chain survives, with its recomputed score.
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "chain 764 chr1 8 + 0 8 chr1 8 + 0 8 2\n8\n\n"
        );

        // The warning reports the count and the specific dropped chain line.
        let warn = String::from_utf8(stderr).unwrap();
        assert!(
            warn.contains("WARN: skipped 1 chain(s) referencing sequences missing"),
            "missing count warning: {warn}"
        );
        assert!(
            warn.contains("missing sequence 'chrZ'"),
            "missing name in warning: {warn}"
        );
        assert!(
            warn.contains("chain 0 chrZ 8 + 0 8 chrZ 8 + 0 8 1"),
            "dropped chain header in warning: {warn}"
        );
    }
}
