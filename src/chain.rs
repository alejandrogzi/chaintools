use crate::{BlockSlice, ByteSlice};

/// A parsed chain record with zero-copy references into shared storage.
///
/// Represents a chain alignment between two sequences (target and query) with
/// metadata and alignment blocks. Uses zero-copy references to avoid allocations
/// when parsing large files.
///
/// # Fields
///
/// * `score` - Alignment score
/// * `t_name` - Target sequence name (zero-copy reference)
/// * `t_size` - Target sequence length
/// * `t_strand` - Target strand orientation
/// * `t_start` - Target start coordinate
/// * `t_end` - Target end coordinate
/// * `q_name` - Query sequence name (zero-copy reference)
/// * `q_size` - Query sequence length
/// * `q_strand` - Query strand orientation
/// * `q_start` - Query start coordinate
/// * `q_end` - Query end coordinate
/// * `id` - Chain identifier
/// * `blocks` - Alignment blocks (zero-copy slice)
///
/// # Examples
///
/// ```ignore
/// use chaintools::{Chain, Strand, ByteSlice};
///
/// // Create a chain with zero-copy references
/// let chain = Chain {
///     score: 100,
///     t_name: ByteSlice::from(b"chr1"),
///     t_size: 1000,
///     t_strand: Strand::Plus,
///     t_start: 0,
///     t_end: 100,
///     q_name: ByteSlice::from(b"chr2"),
///     q_size: 1000,
///     q_strand: Strand::Plus,
///     q_start: 0,
///     q_end: 100,
///     id: 1,
///     blocks: BlockSlice::empty(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Chain {
    pub score: i64,
    pub t_name: ByteSlice,
    pub t_size: u32,
    pub t_strand: Strand,
    pub t_start: u32,
    pub t_end: u32,
    pub q_name: ByteSlice,
    pub q_size: u32,
    pub q_strand: Strand,
    pub q_start: u32,
    pub q_end: u32,
    pub id: u64,
    pub blocks: BlockSlice,
}

/// Strand of an alignment target/query.
///
/// Represents the orientation of a sequence in the alignment.
/// Plus indicates forward orientation, Minus indicates reverse complement.
///
/// # Examples
///
/// ```
/// use chaintools::Strand;
///
/// let forward = Strand::Plus;
/// let reverse = Strand::Minus;
///
/// assert!(forward != reverse);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strand {
    Plus,
    Minus,
}
