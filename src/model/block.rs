// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::ops::Range;
use std::sync::Arc;

use crate::model::error::ChainError;

/// One alignment block.
///
/// Represents a contiguous alignment region between reference and query sequences.
/// Each block defines the size of the aligned region and gaps to the next block.
///
/// # Fields
///
/// * `size` - Length of the aligned region in bases
/// * `gap_reference` - Gap size on the reference sequence before the next block
/// * `gap_query` - Gap size on the query sequence before the next block
///
/// # Examples
///
/// ```
/// use chaintools::Block;
///
/// let block = Block {
///     size: 100,          // 100 bases aligned
///     gap_reference: 50,  // 50 bases gap on the reference
///     gap_query: 30,      // 30 bases gap on the query
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Block {
    pub size: u32,
    pub gap_reference: u32,
    pub gap_query: u32,
}

/// Alignment block represented with absolute reference and query coordinates.
///
/// This form is useful while constructing chains: callers can accumulate
/// already-aligned intervals first, then convert them to UCSC dense chain
/// blocks once neighboring gaps are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsoluteBlock {
    pub reference_start: u32,
    pub reference_end: u32,
    pub query_start: u32,
    pub query_end: u32,
}

impl AbsoluteBlock {
    /// Returns the reference interval length.
    #[inline]
    pub fn reference_len(&self) -> u32 {
        self.reference_end.saturating_sub(self.reference_start)
    }

    /// Returns the query interval length.
    #[inline]
    pub fn query_len(&self) -> u32 {
        self.query_end.saturating_sub(self.query_start)
    }

    /// Returns the aligned length when reference and query lengths match.
    #[inline]
    pub fn aligned_len(&self) -> Option<u32> {
        let reference_len = self.reference_len();
        if reference_len == self.query_len() {
            Some(reference_len)
        } else {
            None
        }
    }

    /// Returns true for a positive-length ungapped alignment interval.
    #[inline]
    pub fn is_gapless_match_block(&self) -> bool {
        self.reference_end > self.reference_start
            && self.query_end > self.query_start
            && self.aligned_len().is_some()
    }

    /// Validates that both absolute intervals have positive length.
    pub fn validate(&self) -> Result<(), ChainError> {
        if self.reference_end <= self.reference_start {
            return Err(block_error(
                "absolute block reference interval is empty or inverted",
            ));
        }
        if self.query_end <= self.query_start {
            return Err(block_error(
                "absolute block query interval is empty or inverted",
            ));
        }
        Ok(())
    }
}

/// Converts absolute alignment blocks to UCSC dense chain blocks.
///
/// The input must be non-empty, sorted, non-overlapping, positive length, and
/// each block must have the same reference and query length. Gaps are computed
/// from neighboring absolute coordinates; the final dense block always carries
/// zero gaps.
pub fn absolute_to_dense_blocks(blocks: &[AbsoluteBlock]) -> Result<Vec<Block>, ChainError> {
    if blocks.is_empty() {
        return Err(block_error("absolute block list is empty"));
    }

    let mut dense = Vec::with_capacity(blocks.len());
    for (index, block) in blocks.iter().copied().enumerate() {
        block.validate()?;
        let size = block
            .aligned_len()
            .ok_or_else(|| block_error("absolute block reference and query lengths differ"))?;

        if let Some(next) = blocks.get(index + 1).copied() {
            if next.reference_start <= block.reference_start {
                return Err(block_error(
                    "absolute blocks are not sorted by reference start",
                ));
            }
            if next.query_start <= block.query_start {
                return Err(block_error("absolute blocks are not sorted by query start"));
            }
            let gap_reference = next
                .reference_start
                .checked_sub(block.reference_end)
                .ok_or_else(|| block_error("absolute block reference coordinates overlap"))?;
            let gap_query = next
                .query_start
                .checked_sub(block.query_end)
                .ok_or_else(|| block_error("absolute block query coordinates overlap"))?;
            dense.push(Block {
                size,
                gap_reference,
                gap_query,
            });
        } else {
            dense.push(Block {
                size,
                gap_reference: 0,
                gap_query: 0,
            });
        }
    }

    Ok(dense)
}

/// Borrowed slice of blocks stored contiguously.
///
/// Provides zero-copy access to a contiguous range of blocks stored in shared
/// memory. Uses Arc to allow multiple references to the same underlying storage.
///
/// # Fields
///
/// * `storage` - Shared storage containing all blocks
/// * `range` - Range of indices this slice refers to
///
/// # Examples
///
/// ```ignore
/// use chaintools::{Block, BlockSlice};
/// use std::sync::Arc;
///
/// let blocks = vec![Block {
///     size: 100,
///     gap_reference: 0,
///     gap_query: 0,
/// }];
/// let storage = Arc::new(blocks);
/// let slice = BlockSlice::new(storage.clone(), 0..1);
///
/// assert_eq!(slice.as_slice().len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct BlockSlice {
    storage: Arc<Vec<Block>>,
    range: Range<usize>,
}

fn block_error(message: impl Into<String>) -> ChainError {
    ChainError::Format {
        offset: 0,
        msg: message.into().into(),
    }
}

impl BlockSlice {
    /// Creates a new block slice from shared storage and a range.
    ///
    /// # Arguments
    ///
    /// * `storage` - Shared storage containing all blocks
    /// * `range` - Range of indices this slice should reference
    ///
    /// # Output
    ///
    /// Returns a new `BlockSlice` instance
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::{Block, BlockSlice};
    /// use std::sync::Arc;
    ///
    /// let blocks = vec![Block {
    ///     size: 100,
    ///     gap_reference: 0,
    ///     gap_query: 0,
    /// }];
    /// let storage = Arc::new(blocks);
    /// let slice = BlockSlice::new(storage, 0..1);
    /// ```
    pub fn new(storage: Arc<Vec<Block>>, range: Range<usize>) -> Self {
        BlockSlice { storage, range }
    }

    /// Returns a slice reference to the blocks in this range.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to this BlockSlice
    ///
    /// # Output
    ///
    /// Returns a `&[Block]` containing the blocks in the specified range
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::{Block, BlockSlice};
    /// use std::sync::Arc;
    ///
    /// let blocks = vec![
    ///     Block {
    ///         size: 100,
    ///         gap_reference: 0,
    ///         gap_query: 0,
    ///     },
    ///     Block {
    ///         size: 50,
    ///         gap_reference: 10,
    ///         gap_query: 5,
    ///     },
    /// ];
    /// let storage = Arc::new(blocks);
    /// // Note: BlockSlice::new is private, this is just for demonstration
    /// // In practice, BlockSlice is created internally by the parser
    /// let slice = BlockSlice::new(storage, 0..2);
    ///
    /// let block_slice = slice.as_slice();
    /// assert_eq!(block_slice.len(), 2);
    /// assert_eq!(block_slice[0].size, 100);
    /// ```
    pub fn as_slice(&self) -> &[Block] {
        &self.storage[self.range.clone()]
    }
}
