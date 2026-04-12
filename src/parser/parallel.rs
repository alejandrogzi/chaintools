// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use rayon::prelude::*;

use crate::{Block, ChainError};

use super::common::{parse_chain_in_range, ChainMeta};
use super::locate_chain_ranges;

/// Parses all chains from a byte buffer using parallel processing.
///
/// Uses Rayon to parse multiple chains simultaneously on different threads.
/// First locates all chain ranges, then parses them in parallel while preserving
/// input order, and finally merges the per-chain block buffers.
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
/// use chaintools::parser::parallel::parse_chains_parallel;
///
/// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\nchain 2 chr1 1000 + 0 100 chr2 1000 + 0 100 2\n20\n20\n\n";
/// let (chains, blocks) = parse_chains_parallel(data)?;
///
/// assert_eq!(chains.len(), 2);
/// assert_eq!(blocks.len(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub(crate) fn parse_chains_parallel(
    bytes: &[u8],
) -> Result<(Vec<ChainMeta>, Vec<Block>), ChainError> {
    let ranges = locate_chain_ranges(bytes)?;
    let parsed: Result<Vec<(ChainMeta, Vec<Block>)>, ChainError> = ranges
        .into_par_iter()
        .map(|range| parse_chain_in_range(bytes, range))
        .collect();

    let parsed = parsed?;
    let mut metas = Vec::with_capacity(parsed.len());
    let total_blocks = parsed.iter().map(|(_, blocks)| blocks.len()).sum();
    let mut all_blocks = Vec::with_capacity(total_blocks);
    for (mut meta, mut blocks) in parsed {
        let start = all_blocks.len();
        all_blocks.append(&mut blocks);
        let end = all_blocks.len();
        meta.blocks = start..end;
        metas.push(meta);
    }

    Ok((metas, all_blocks))
}
