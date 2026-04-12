// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::ops::Range;
use std::sync::Arc;

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
