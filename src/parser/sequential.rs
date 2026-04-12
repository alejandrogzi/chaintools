// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::borrow::Cow;

#[cfg(any(feature = "parallel", feature = "index"))]
use std::ops::Range;

use crate::{Block, ChainError};

use super::common::{is_blank, parse_block, parse_header_with_default_id, read_line, ChainMeta};

/// Parses all chains from a byte buffer using sequential parsing.
///
/// Processes the entire buffer line by line, parsing chain headers and
/// their associated alignment blocks. Returns metadata for all chains
/// and a flat vector of all blocks with index ranges.
///
/// # Arguments
///
/// * `bytes` - Complete file buffer to parse
///
/// # Output
///
/// Returns `Ok((Vec<ChainMeta>, Vec<Block>))` with chain metadata and
/// all alignment blocks, or `Err(ChainError)` if parsing fails
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::sequential::parse_chains_sequential;
///
/// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\n";
/// let (chains, blocks) = parse_chains_sequential(data)?;
///
/// assert_eq!(chains.len(), 1);
/// assert_eq!(blocks.len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub(crate) fn parse_chains_sequential(
    bytes: &[u8],
) -> Result<(Vec<ChainMeta>, Vec<Block>), ChainError> {
    let mut chains = Vec::new();
    let mut blocks = Vec::new();
    let mut pos = 0usize;
    let len = bytes.len();
    let mut next_id = 1u64;

    while pos < len {
        let line_start = pos;
        let (next_pos, line) = read_line(bytes, pos);
        pos = next_pos;
        if is_blank(line) {
            continue;
        }
        let (mut meta, has_explicit_id) = parse_header_with_default_id(line, line_start, next_id)?;
        if !has_explicit_id {
            next_id = next_id.checked_add(1).ok_or_else(|| ChainError::Format {
                offset: line_start,
                msg: Cow::Borrowed("generated chain id overflows u64"),
            })?;
        }
        let block_start = blocks.len();

        loop {
            if pos >= len {
                break;
            }
            let block_line_start = pos;
            let (next, line) = read_line(bytes, pos);
            pos = next;
            if is_blank(line) {
                break;
            }
            let block = parse_block(line, block_line_start)?;
            blocks.push(block);
        }

        let block_end = blocks.len();
        if block_end == block_start {
            return Err(ChainError::Format {
                offset: line_start,
                msg: Cow::Borrowed("chain without any alignment blocks"),
            });
        }
        meta.blocks = block_start..block_end;
        chains.push(meta);
    }

    Ok((chains, blocks))
}

/// Locates the byte ranges of all chains in the buffer.
///
/// Scans the buffer and identifies the start and end positions of each
/// chain record. Used by parallel parsing and indexing to divide work.
///
/// # Arguments
///
/// * `bytes` - Complete file buffer to scan
///
/// # Output
///
/// Returns `Ok(Vec<Range<usize>>)` with byte ranges for each chain,
/// or `Err(ChainError)` if the format is invalid
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::sequential::locate_chain_ranges;
///
/// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\nchain 2 chr1 1000 + 0 100 chr2 1000 + 0 100 2\n20\n20\n\n";
/// let ranges = locate_chain_ranges(data)?;
///
/// assert_eq!(ranges.len(), 2);
/// assert!(ranges[0].end < ranges[1].start);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg(any(feature = "parallel", feature = "index"))]
pub(crate) fn locate_chain_ranges(bytes: &[u8]) -> Result<Vec<Range<usize>>, ChainError> {
    let mut ranges = Vec::new();
    let mut pos = 0usize;
    let len = bytes.len();
    while pos < len {
        let chain_start = pos;
        let (next_pos, line) = read_line(bytes, pos);
        pos = next_pos;
        if is_blank(line) {
            continue;
        }
        if !line.starts_with(b"chain ") {
            return Err(ChainError::Format {
                offset: chain_start,
                msg: Cow::Borrowed("expected chain header starting with 'chain '"),
            });
        }
        while pos < len {
            let (next, line) = read_line(bytes, pos);
            pos = next;
            if is_blank(line) {
                break;
            }
        }
        ranges.push(chain_start..pos);
    }
    Ok(ranges)
}
