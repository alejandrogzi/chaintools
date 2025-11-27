use std::ops::Range;

use rayon::prelude::*;

use crate::{Block, ChainError};

use super::common::{parse_chain_in_range, ChainMeta};
use super::locate_chain_ranges;

/// Parses all chains from a byte buffer using parallel processing.
///
/// Uses Rayon to parse multiple chains simultaneously on different threads.
/// First locates all chain ranges, then parses them in parallel, and finally
/// reassembles the results in the correct order.
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
    let parsed: Result<Vec<(usize, ChainMeta, Vec<Block>)>, ChainError> = ranges
        .into_par_iter()
        .enumerate()
        .map(|(idx, range)| parse_one(bytes, range, idx))
        .collect();

    let mut parsed = parsed?;
    parsed.sort_by(|a, b| a.0.cmp(&b.0));

    let mut metas = Vec::with_capacity(parsed.len());
    let mut all_blocks = Vec::new();
    for (_idx, mut meta, mut blocks) in parsed.into_iter() {
        let start = all_blocks.len();
        all_blocks.append(&mut blocks);
        let end = all_blocks.len();
        meta.blocks = start..end;
        metas.push(meta);
    }

    Ok((metas, all_blocks))
}

/// Parses a single chain within the given range.
///
/// Helper function used by the parallel parser to parse individual chains.
/// Wraps the common parse_chain_in_range function with index tracking.
///
/// # Arguments
///
/// * `bytes` - Complete file buffer
/// * `range` - Byte range containing exactly one chain
/// * `idx` - Index of this chain for ordering purposes
///
/// # Output
///
/// Returns `Ok((idx, ChainMeta, Vec<Block>))` with the parsed data,
/// or `Err(ChainError)` if parsing fails
fn parse_one(
    bytes: &[u8],
    range: Range<usize>,
    idx: usize,
) -> Result<(usize, ChainMeta, Vec<Block>), ChainError> {
    let (meta, blocks) = parse_chain_in_range(bytes, range)?;
    Ok((idx, meta, blocks))
}
