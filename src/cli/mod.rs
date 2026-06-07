// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

pub mod anti_repeat;
pub mod filter;
pub mod merge;
pub mod score;
pub mod sort;
mod sort_core;
pub mod split;

use std::fmt;
use std::io::{self, BufRead, Write};
use std::path::Path;

use chaintools::ChainError;
use clap::{Parser, Subcommand};
use log::LevelFilter;

/// Command-line interface for chaintools.
///
/// Provides the main CLI entry point with global options for
/// thread count and logging level, plus subcommands.
#[derive(Debug, Parser)]
#[command(name = "chaintools")]
#[command(about = "work with .chain files in rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
pub struct Cli {
    #[arg(
        short = 't',
        long,
        global = true,
        value_name = "N",
        help_heading = "Global Options",
        default_value_t = num_cpus::get()
    )]
    threads: usize,

    #[arg(
        short = 'L',
        long,
        global = true,
        value_name = "LEVEL",
        help = "Set CLI logging level: off, error, warn, info, debug, trace",
        help_heading = "Global Options"
    )]
    level: Option<LevelFilter>,

    #[command(subcommand)]
    command: Command,
}

/// Subcommands available in the chaintools CLI.
#[derive(Debug, Subcommand)]
enum Command {
    #[command(
        name = "antirepeat",
        alias = "anti-repeat",
        about = "Filter chains dominated by repeats or degenerate DNA"
    )]
    AntiRepeat(anti_repeat::AntiRepeatArgs),
    #[command(about = "Filter chain files")]
    Filter(filter::FilterArgs),
    #[command(about = "Merge chain files")]
    Merge(merge::MergeArgs),
    #[command(about = "Recompute chain scores from sequence (UCSC chainScore-compatible)")]
    Score(score::ScoreArgs),
    #[command(about = "Split chain files")]
    Split(split::SplitArgs),
    #[command(about = "Sort chain files")]
    Sort(sort::SortArgs),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::AntiRepeat(_) => f.write_str("antirepeat"),
            Command::Filter(_) => f.write_str("filter"),
            Command::Merge(_) => f.write_str("merge"),
            Command::Score(_) => f.write_str("score"),
            Command::Split(_) => f.write_str("split"),
            Command::Sort(_) => f.write_str("sort"),
        }
    }
}

/// Errors that can occur during CLI execution.
///
/// # Variants
///
/// * `Message` - String message error
/// * `Io` - I/O error
/// * `Chain` - Chain parsing error
///
/// # Examples
///
/// ```ignore
/// use chaintools::cli::CliError;
///
/// // Create a message error
/// let msg_err = CliError::Message("invalid configuration".to_string());
///
/// // Handle different error types
/// match msg_err {
///     CliError::Message(msg) => eprintln!("Error: {}", msg),
///     CliError::Io(err) => eprintln!("I/O error: {}", err),
///     CliError::Chain(err) => eprintln!("Chain error: {}", err),
/// }
/// ```
#[derive(Debug)]
pub enum CliError {
    Message(String),
    Io(io::Error),
    Chain(ChainError),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Message(message) => f.write_str(message),
            CliError::Io(err) => write!(f, "I/O error: {err}"),
            CliError::Chain(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CliError::Io(err) => Some(err),
            CliError::Chain(err) => Some(err),
            CliError::Message(_) => None,
        }
    }
}

impl From<io::Error> for CliError {
    fn from(value: io::Error) -> Self {
        CliError::Io(value)
    }
}

impl From<ChainError> for CliError {
    fn from(value: ChainError) -> Self {
        CliError::Chain(value)
    }
}

/// Main CLI entry point.
///
/// Executes the appropriate subcommand with given I/O streams.
///
/// # Arguments
///
/// * `cli` - Parsed CLI arguments
/// * `stdin` - Input stream
/// * `stdout` - Output stream
/// * `stderr` - Error output stream
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub fn run<R, W, E>(cli: Cli, stdin: &mut R, stdout: &mut W, stderr: &mut E) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    configure_threads(cli.threads)?;
    let level = resolve_log_level(cli.level);
    configure_logging(level)?;

    log::info!(
        "chaintools [{}] v{}",
        &cli.command,
        env!("CARGO_PKG_VERSION")
    );
    let start = std::time::Instant::now();

    let result = match cli.command {
        Command::AntiRepeat(args) => anti_repeat::run(args, stdin, stdout, stderr),
        Command::Filter(args) => filter::run(args, stdin, stdout, stderr),
        Command::Merge(args) => merge::run(args, stdin, stdout, stderr),
        Command::Score(args) => score::run(args, stdin, stdout, stderr),
        Command::Split(args) => split::run(args, stdin, stdout, stderr),
        Command::Sort(args) => sort::run(args, stdin, stdout, stderr),
    };

    log::info!("Execution time: {:?}", start.elapsed());
    result
}

/// Resolves the effective log level for a run.
///
/// Logging is verbose by default: every tool defaults to `Info`, regardless of
/// whether chains are written to a file or to standard output. Logs are emitted
/// on stderr (via `simple_logger`), so they never corrupt the stdout chain
/// stream. An explicit `--level` is always honored; `--level off` silences all
/// logging.
///
/// # Arguments
///
/// * `requested` - The `--level` value, if the user supplied one
///
/// # Output
///
/// Returns the `LevelFilter` to install
fn resolve_log_level(requested: Option<LevelFilter>) -> LevelFilter {
    requested.unwrap_or(LevelFilter::Info)
}

/// Emits a uniform end-of-run summary line at the `Info` level.
///
/// Produces `"{tool} complete: k1=v1, k2=v2, ..."` so every subcommand reports
/// its final counts in a consistent shape.
///
/// # Arguments
///
/// * `tool` - Subcommand name (e.g. `"score"`)
/// * `fields` - Ordered `(label, value)` pairs to render
///
/// # Examples
///
/// ```ignore
/// log_summary("score", &[("read", 1200), ("kept", 980), ("dropped", 220)]);
/// // INFO  score complete: read=1200, kept=980, dropped=220
/// ```
pub(crate) fn log_summary(tool: &str, fields: &[(&str, u64)]) {
    if !log::log_enabled!(log::Level::Info) {
        return;
    }
    let rendered = fields
        .iter()
        .map(|(label, value)| format!("{label}={value}"))
        .collect::<Vec<_>>()
        .join(", ");
    log::info!("{tool} complete: {rendered}");
}

/// Verifies that required and optionally-provided input files exist.
///
/// Runs as an upfront pre-flight check so a missing input fails immediately
/// with a clear message, before any engine is built, any standard input is
/// read, or any output is written. Uses [`Path::try_exists`] so a permission
/// or I/O error while checking is reported distinctly from a genuine absence.
///
/// # Arguments
///
/// * `required` - `(label, path)` pairs that must exist
/// * `optional` - `(label, maybe_path)` pairs; `None` means "use a default such
///   as standard input" and is skipped
///
/// # Output
///
/// Returns `Ok(())` if every present path exists, or `Err(CliError::Message)`
/// naming the first offending file.
///
/// # Examples
///
/// ```ignore
/// ensure_inputs_exist(
///     &[("reference", &args.reference), ("query", &args.query)],
///     &[("input chain", args.chain.as_deref())],
/// )?;
/// ```
pub(crate) fn ensure_inputs_exist(
    required: &[(&str, &Path)],
    optional: &[(&str, Option<&Path>)],
) -> Result<(), CliError> {
    for (label, path) in required {
        check_input_exists(label, path)?;
    }
    for (label, path) in optional {
        if let Some(path) = path {
            check_input_exists(label, path)?;
        }
    }
    Ok(())
}

/// Checks that a single input file exists, mapping the outcome to a `CliError`.
fn check_input_exists(label: &str, path: &Path) -> Result<(), CliError> {
    match path.try_exists() {
        Ok(true) => Ok(()),
        Ok(false) => Err(CliError::Message(format!(
            "{label} file does not exist: {}",
            path.display()
        ))),
        Err(err) => Err(CliError::Message(format!(
            "cannot access {label} file {}: {err}",
            path.display()
        ))),
    }
}

/// Configures the logging subsystem with the given level.
fn configure_logging(level: LevelFilter) -> Result<(), CliError> {
    log::set_max_level(level);
    if level == LevelFilter::Off {
        return Ok(());
    }

    simple_logger::SimpleLogger::new()
        .with_level(level)
        .init()
        .map_err(|err| CliError::Message(format!("failed to initialize logger: {err}")))?;
    log::set_max_level(level);
    Ok(())
}

#[cfg(feature = "parallel")]
/// Configures the global rayon thread pool.
fn configure_threads(threads: usize) -> Result<(), CliError> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|err| {
            CliError::Message(format!("failed to configure global thread pool: {err}"))
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_reserved_globals_before_subcommand() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "--threads",
            "4",
            "--level",
            "debug",
            "filter",
            "--chain",
            "input.chain",
        ])
        .expect("global options should parse");

        assert_eq!(cli.threads, 4);
        assert_eq!(cli.level, Some(LevelFilter::Debug));
        assert!(matches!(cli.command, Command::Filter(_)));
    }

    #[test]
    fn parses_reserved_globals_after_subcommand() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "filter",
            "--threads",
            "2",
            "--level",
            "trace",
            "--chain",
            "input.chain",
        ])
        .expect("global options should parse after subcommand");

        assert_eq!(cli.threads, 2);
        assert_eq!(cli.level, Some(LevelFilter::Trace));
        assert!(matches!(cli.command, Command::Filter(_)));
    }

    #[test]
    fn rejects_unknown_log_level() {
        let err = Cli::try_parse_from(["chaintools", "--level", "chatty", "filter"])
            .expect_err("unknown log level should be rejected");

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn defaults_to_info_when_unspecified() {
        let cli = Cli::try_parse_from(["chaintools", "filter"]).expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Info);
    }

    #[test]
    fn honors_requested_level() {
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "debug", "filter"]).expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Debug);
    }

    #[test]
    fn explicit_off_silences_logging() {
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "off", "filter"]).expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Off);
    }

    #[test]
    fn stdout_output_still_defaults_to_info() {
        // Verbose-by-default: a tool writing chains to stdout (no --out-chain)
        // still logs at Info on stderr; previously this was forced Off.
        let cli = Cli::try_parse_from([
            "chaintools",
            "anti-repeat",
            "--reference",
            "target.2bit",
            "--query",
            "query.2bit",
        ])
        .expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Info);
    }

    #[test]
    fn stdout_output_honors_requested_level() {
        // This combination (stdout output + explicit --level) used to be
        // rejected; verbose-by-default now allows it.
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "info", "sort"]).expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Info);
    }

    #[test]
    fn out_chain_output_defaults_to_info() {
        let cli = Cli::try_parse_from(["chaintools", "filter", "--out-chain", "out.chain"])
            .expect("args parse");
        assert_eq!(resolve_log_level(cli.level), LevelFilter::Info);
    }
}

#[cfg(not(feature = "parallel"))]
/// Returns an error if threads is requested without parallel feature.
fn configure_threads(threads: usize) -> Result<(), CliError> {
    if threads == 1 {
        return Ok(());
    } else {
        return Err(CliError::Message(
            "--threads requires chaintools to be built with the `parallel` feature".to_owned(),
        ));
    }
}
