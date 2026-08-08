// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::PathBuf;

use chaintools::io::writer::write_chain_dense;
use chaintools::{ChainError, OwnedChain, Strand, StreamItem, StreamingReader};
use clap::Args;
#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;

/// Command-line arguments for the swap subcommand.
///
/// Swaps the target and query sides of every chain, equivalent to UCSC
/// `chainSwap`. The swapped chain describes the same alignment in the opposite
/// direction; scores and chain IDs are preserved and output order matches input
/// order (pipe into `chaintools sort` if sorted output is needed).
///
/// # Examples
///
/// ```bash
/// chaintools swap --chain input.chain --out-chain swapped.chain
/// cat input.chain | chaintools swap > swapped.chain
/// ```
#[derive(Debug, Args)]
pub struct SwapArgs {
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
        help = "Path for the swapped chain output. If not provided, output is written to standard output."
    )]
    out_chain: Option<PathBuf>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress swap output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,
}

/// Runs the swap subcommand.
///
/// Streams chains, swaps each chain's target and query sides, and writes the
/// result. Metadata/comment lines are passed through unchanged.
///
/// # Arguments
///
/// * `args` - Swap arguments
/// * `stdin` - Input stream (used if no --chain provided)
/// * `stdout` - Output stream (used if no --out-chain provided)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub fn run<R, W, E>(
    args: SwapArgs,
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
    super::ensure_inputs_exist(&[], &[("input chain", args.chain.as_deref())])?;
    if let Some(out_chain) = &args.out_chain {
        super::validate_distinct_paths("--out-chain", out_chain, args.chain.as_deref())?;
    }

    let input_desc = args
        .chain
        .as_deref()
        .map_or_else(|| "<stdin>".to_owned(), |path| path.display().to_string());
    let output_desc = args
        .out_chain
        .as_deref()
        .map_or_else(|| "<stdout>".to_owned(), |path| path.display().to_string());
    log::info!("swap: reading chains from {input_desc}, writing to {output_desc}");

    if let Some(path) = &args.out_chain {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file);
        if args.gzip {
            run_gzip_output(&args, stdin, writer)?;
        } else {
            let mut writer = writer;
            run_to_writer(&args, stdin, &mut writer)?;
            writer.flush()?;
        }
    } else if args.gzip {
        run_gzip_output(&args, stdin, stdout)?;
    } else {
        run_to_writer(&args, stdin, stdout)?;
        stdout.flush()?;
    }

    Ok(())
}

#[cfg(feature = "gzip")]
fn run_gzip_output<R, W>(args: &SwapArgs, stdin: &mut R, writer: W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let mut writer = GzEncoder::new(writer, Compression::fast());
    run_to_writer(args, stdin, &mut writer)?;
    writer.try_finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn run_gzip_output<R, W>(_args: &SwapArgs, _stdin: &mut R, _writer: W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    Err(CliError::Message(
        "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
    ))
}

#[cfg(feature = "gzip")]
fn validate_output_args(_args: &SwapArgs) -> Result<(), CliError> {
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn validate_output_args(args: &SwapArgs) -> Result<(), CliError> {
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

fn run_to_writer<R, W>(args: &SwapArgs, stdin: &mut R, writer: &mut W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let written = if let Some(path) = &args.chain {
        let mut reader = StreamingReader::from_path(path)?;
        process_stream(&mut reader, writer)?
    } else {
        let mut reader = StreamingReader::new(stdin);
        process_stream(&mut reader, writer)?
    };

    super::log_summary("swap", &[("chains", written), ("written", written)]);
    Ok(())
}

/// Streams chains through the swap transform, returning the number written.
fn process_stream<R: BufRead, W: Write>(
    reader: &mut StreamingReader<R>,
    writer: &mut W,
) -> Result<u64, CliError> {
    let mut written = 0u64;
    while let Some(item) = reader.next_item()? {
        match item {
            StreamItem::MetaLine(line) => {
                writer.write_all(&line)?;
                writer.write_all(b"\n")?;
            }
            StreamItem::Header(header) => {
                let offset = header.offset;
                let blocks = reader.read_blocks(offset)?;
                let mut chain = header.into_chain(blocks);
                swap_chain(&mut chain, offset)?;
                write_chain_dense(writer, &chain)?;
                written += 1;
            }
        }
    }
    Ok(written)
}

/// Swaps a chain's target and query sides in place (UCSC `chainSwap`).
///
/// The swapped chain describes the same alignment in the opposite direction:
/// the old query becomes the new target (always on `+`) and the old target
/// becomes the new query, keeping the original query strand. Score and chain ID
/// are untouched.
///
/// For a `+` query both sides walk forward, so block order is preserved and each
/// block's `dt`/`dq` are exchanged. For a `-` query the new coordinates run
/// opposite to the original traversal, so block order reverses; because gaps sit
/// *between* blocks, the gap following a reversed block is the one recorded on
/// its successor in the reversed list, not the one it carried itself.
///
/// # Arguments
///
/// * `chain` - Chain to swap, mutated in place
/// * `offset` - Byte offset of the chain header, for error reporting
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` for a malformed chain
fn swap_chain(chain: &mut OwnedChain, offset: usize) -> Result<(), ChainError> {
    if chain.reference_strand != Strand::Plus {
        return Err(format_error(
            offset,
            "cannot swap a chain whose target strand is not +",
        ));
    }
    super::validate_block_spans(chain, offset)?;

    // Derive both new header intervals from the chromosome sizes before any
    // field is moved, so no step depends on a half-swapped chain.
    let (reference_start, reference_end, query_start, query_end) = match chain.query_strand {
        Strand::Plus => (
            chain.query_start,
            chain.query_end,
            chain.reference_start,
            chain.reference_end,
        ),
        Strand::Minus => (
            flip(chain.query_size, chain.query_end, offset)?,
            flip(chain.query_size, chain.query_start, offset)?,
            flip(chain.reference_size, chain.reference_end, offset)?,
            flip(chain.reference_size, chain.reference_start, offset)?,
        ),
    };

    std::mem::swap(&mut chain.reference_name, &mut chain.query_name);
    std::mem::swap(&mut chain.reference_size, &mut chain.query_size);
    chain.reference_start = reference_start;
    chain.reference_end = reference_end;
    chain.query_start = query_start;
    chain.query_end = query_end;
    // The new target is the old query read on `+`, and the new query is the old
    // target read in the old query's direction: the strand letter carries over.

    match chain.query_strand {
        Strand::Plus => {
            for block in &mut chain.blocks {
                std::mem::swap(&mut block.gap_reference, &mut block.gap_query);
            }
        }
        Strand::Minus => {
            chain.blocks.reverse();
            for index in 0..chain.blocks.len().saturating_sub(1) {
                // `index + 1` still holds its original gaps here: this loop only
                // ever reads ahead of what it has already written.
                let successor = chain.blocks[index + 1];
                chain.blocks[index].gap_reference = successor.gap_query;
                chain.blocks[index].gap_query = successor.gap_reference;
            }
            if let Some(last) = chain.blocks.last_mut() {
                last.gap_reference = 0;
                last.gap_query = 0;
            }
        }
    }

    Ok(())
}

/// Converts a coordinate to the opposite strand of a sequence of `size` bases.
fn flip(size: u32, coord: u32, offset: usize) -> Result<u32, ChainError> {
    size.checked_sub(coord)
        .ok_or_else(|| format_error(offset, "chain coordinate exceeds its sequence size"))
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

    #[derive(Debug, Parser)]
    struct SwapHarness {
        #[command(flatten)]
        args: SwapArgs,
    }

    /// One `+/+` block.
    const PLUS_ONE_BLOCK: &str = "chain 100 chr1 1000 + 10 30 qry1 500 + 40 60 7\n20\n\n";
    /// Several `+/+` blocks with unequal target/query gaps.
    const PLUS_MANY_BLOCKS: &str =
        "chain 4200 chr1 1000 + 10 63 qry1 500 + 40 93 9\n20\t3\t1\n10\t5\t7\n15\n\n";
    /// One `+/-` block.
    const MINUS_ONE_BLOCK: &str = "chain 250 chr2 100 + 0 4 qry2 20 - 3 7 11\n4\n\n";
    /// Several `+/-` blocks with unequal gaps on the `-` query.
    const MINUS_MANY_BLOCKS: &str =
        "chain 777 chr2 100 + 0 15 qry2 60 - 5 19 13\n4\t1\t2\n3\t2\t0\n5\n\n";

    fn swap_text(input: &str) -> String {
        let mut stdin = Cursor::new(input.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            SwapArgs {
                chain: None,
                out_chain: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect("swap run");
        String::from_utf8(stdout).expect("utf8 output")
    }

    #[test]
    fn parses_minimal_args() {
        let cli = SwapHarness::try_parse_from(["chaintools", "--chain", "in.chain"])
            .expect("swap arguments should parse");
        assert_eq!(cli.args.chain, Some(PathBuf::from("in.chain")));
        assert!(cli.args.out_chain.is_none());
        assert!(!cli.args.gzip);
    }

    #[test]
    fn swaps_single_plus_block() {
        assert_eq!(
            swap_text(PLUS_ONE_BLOCK),
            "chain 100 qry1 500 + 40 60 chr1 1000 + 10 30 7\n20\n\n"
        );
    }

    #[test]
    fn swaps_many_plus_blocks_exchanging_gaps() {
        // Block order is preserved; each block's dt/dq are exchanged.
        assert_eq!(
            swap_text(PLUS_MANY_BLOCKS),
            "chain 4200 qry1 500 + 40 93 chr1 1000 + 10 63 9\n20\t1\t3\n10\t7\t5\n15\n\n"
        );
    }

    #[test]
    fn swaps_single_minus_block_from_sizes() {
        // New target = query on +: 20 - 7 = 13 .. 20 - 3 = 17.
        // New query = target on -: 100 - 4 = 96 .. 100 - 0 = 100.
        assert_eq!(
            swap_text(MINUS_ONE_BLOCK),
            "chain 250 qry2 20 + 13 17 chr2 100 - 96 100 11\n4\n\n"
        );
    }

    #[test]
    fn swaps_many_minus_blocks_reversing_block_order_and_gaps() {
        // Blocks reverse to 5, 3, 4; the gap after a reversed block is the one
        // recorded on its successor in the reversed list, with dt/dq exchanged.
        assert_eq!(
            swap_text(MINUS_MANY_BLOCKS),
            "chain 777 qry2 60 + 41 55 chr2 100 - 85 100 13\n5\t0\t2\n3\t2\t1\n4\n\n"
        );
    }

    #[test]
    fn double_swap_is_the_identity() {
        for fixture in [
            PLUS_ONE_BLOCK,
            PLUS_MANY_BLOCKS,
            MINUS_ONE_BLOCK,
            MINUS_MANY_BLOCKS,
        ] {
            assert_eq!(swap_text(&swap_text(fixture)), fixture);
        }
    }

    #[test]
    fn preserves_metadata_lines_and_chain_order() {
        let input = format!("#meta\n{PLUS_ONE_BLOCK}{MINUS_ONE_BLOCK}");
        let swapped = swap_text(&input);
        assert!(swapped.starts_with("#meta\n"));
        assert_eq!(swap_text(&swapped), input);
    }

    #[test]
    fn rejects_blocks_that_do_not_sum_to_the_span() {
        let mut stdin = Cursor::new(b"chain 1 chr1 100 + 0 10 qry1 100 + 0 10 1\n4\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            SwapArgs {
                chain: None,
                out_chain: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("short block list should be rejected");
        assert!(err.to_string().contains("do not sum to the target span"));
    }

    #[test]
    fn rejects_minus_target_strand() {
        let mut stdin = Cursor::new(b"chain 1 chr1 100 - 0 4 qry1 100 + 0 4 1\n4\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            SwapArgs {
                chain: None,
                out_chain: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("minus target strand should be rejected");
        assert!(err.to_string().contains("target strand is not +"));
    }
}
