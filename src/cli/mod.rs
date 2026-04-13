// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

pub mod anti_repeat;
pub mod filter;
pub mod merge;
pub mod sort;
mod sort_core;
pub mod split;

use std::fmt;
use std::io::{self, BufRead, Write};

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
    let level = resolve_log_level(&cli.command, cli.level)?;
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
        Command::Split(args) => split::run(args, stdin, stdout, stderr),
        Command::Sort(args) => sort::run(args, stdin, stdout, stderr),
    };

    log::info!("Execution time: {:?}", start.elapsed());
    result
}

/// Resolves the log level based on command and requested level.
fn resolve_log_level(
    command: &Command,
    requested: Option<LevelFilter>,
) -> Result<LevelFilter, CliError> {
    match command {
        Command::AntiRepeat(args) if args.writes_to_stdout() => {
            if requested.is_some_and(|level| level != LevelFilter::Off) {
                return Err(CliError::Message(
                    "--level requires --out-chain when anti-repeat writes chain output to stdout"
                        .to_owned(),
                ));
            }
            Ok(LevelFilter::Off)
        }
        Command::AntiRepeat(args) => Ok(requested.unwrap_or_else(|| args.default_log_level())),
        Command::Filter(args) if args.writes_to_stdout() => {
            if requested.is_some_and(|level| level != LevelFilter::Off) {
                return Err(CliError::Message(
                    "--level requires --out-chain when filter writes chain output to stdout"
                        .to_owned(),
                ));
            }
            Ok(LevelFilter::Off)
        }
        Command::Filter(args) => Ok(requested.unwrap_or_else(|| args.default_log_level())),
        Command::Merge(args) if args.writes_to_stdout() => {
            if requested.is_some_and(|level| level != LevelFilter::Off) {
                return Err(CliError::Message(
                    "--level requires --out-chain when merge writes chain output to stdout"
                        .to_owned(),
                ));
            }
            Ok(LevelFilter::Off)
        }
        Command::Merge(args) => Ok(requested.unwrap_or_else(|| args.default_log_level())),
        Command::Split(args) if args.writes_to_stdout() => {
            if requested.is_some_and(|level| level != LevelFilter::Off) {
                return Err(CliError::Message(
                    "--level requires --outdir when split writes chain output to stdout".to_owned(),
                ));
            }
            Ok(LevelFilter::Off)
        }
        Command::Split(args) => Ok(requested.unwrap_or_else(|| args.default_log_level())),
        Command::Sort(args) if args.writes_to_stdout() => {
            if requested.is_some_and(|level| level != LevelFilter::Off) {
                return Err(CliError::Message(
                    "--level requires --out-chain when sort writes chain output to stdout"
                        .to_owned(),
                ));
            }
            Ok(LevelFilter::Off)
        }
        Command::Sort(args) => Ok(requested.unwrap_or_else(|| args.default_log_level())),
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
    fn stdout_filter_defaults_logging_off() {
        let cli = Cli::try_parse_from(["chaintools", "filter"]).expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Off
        );
    }

    #[test]
    fn stdout_filter_rejects_requested_logging() {
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "info", "filter"]).expect("args parse");

        let err = resolve_log_level(&cli.command, cli.level)
            .expect_err("stdout logging should be rejected");
        assert!(err
            .to_string()
            .contains("--level requires --out-chain when filter writes chain output to stdout"));
    }

    #[test]
    fn stdout_filter_allows_explicit_off() {
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "off", "filter"]).expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Off
        );
    }

    #[test]
    fn out_chain_filter_defaults_logging_to_info() {
        let cli = Cli::try_parse_from(["chaintools", "filter", "--out-chain", "out.chain"])
            .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Info
        );
    }

    #[test]
    fn out_chain_filter_uses_requested_logging_level() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "--level",
            "debug",
            "filter",
            "--out-chain",
            "out.chain",
        ])
        .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Debug
        );
    }

    #[test]
    fn stdout_sort_defaults_logging_off() {
        let cli = Cli::try_parse_from(["chaintools", "sort"]).expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Off
        );
    }

    #[test]
    fn stdout_sort_rejects_requested_logging() {
        let cli =
            Cli::try_parse_from(["chaintools", "--level", "info", "sort"]).expect("args parse");

        let err = resolve_log_level(&cli.command, cli.level)
            .expect_err("stdout logging should be rejected");
        assert!(err
            .to_string()
            .contains("--level requires --out-chain when sort writes chain output to stdout"));
    }

    #[test]
    fn out_chain_sort_defaults_logging_to_info() {
        let cli = Cli::try_parse_from(["chaintools", "sort", "--out-chain", "out.chain"])
            .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Info
        );
    }

    #[test]
    fn out_chain_merge_defaults_logging_to_info() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "merge",
            "--chains",
            "a.chain",
            "--out-chain",
            "out.chain",
        ])
        .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Info
        );
    }

    #[test]
    fn outdir_split_defaults_logging_to_info() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "split",
            "--chain",
            "input.chain",
            "--outdir",
            "out",
            "--chunks",
            "10",
        ])
        .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Info
        );
    }

    #[test]
    fn stdout_antirepeat_defaults_logging_off() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "anti-repeat",
            "--reference",
            "target.2bit",
            "--query",
            "query.2bit",
        ])
        .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Off
        );
    }

    #[test]
    fn out_chain_antirepeat_defaults_logging_to_info() {
        let cli = Cli::try_parse_from([
            "chaintools",
            "anti-repeat",
            "--reference",
            "target.2bit",
            "--query",
            "query.2bit",
            "--out-chain",
            "out.chain",
        ])
        .expect("args parse");

        assert_eq!(
            resolve_log_level(&cli.command, cli.level).unwrap(),
            LevelFilter::Info
        );
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
