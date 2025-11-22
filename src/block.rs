use std::ops::Range;
use std::sync::Arc;

/// One alignment block.
///
/// Represents a contiguous alignment region between target and query sequences.
/// Each block defines the size of the aligned region and gaps to the next block.
///
/// # Fields
///
/// * `size` - Length of the aligned region in bases
/// * `dt` - Gap size on target sequence before next block
/// * `dq` - Gap size on query sequence before next block
///
/// # Examples
///
/// ```
/// use chaintools::Block;
///
/// let block = Block {
///     size: 100,  // 100 bases aligned
///     dt: 50,     // 50 bases gap on target
///     dq: 30,     // 30 bases gap on query
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Block {
    pub size: u32,
    pub dt: u32,
    pub dq: u32,
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
/// let blocks = vec![Block { size: 100, dt: 0, dq: 0 }];
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
    /// let blocks = vec![Block { size: 100, dt: 0, dq: 0 }];
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
    ///     Block { size: 100, dt: 0, dq: 0 },
    ///     Block { size: 50, dt: 10, dq: 5 },
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
