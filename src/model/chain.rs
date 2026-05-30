// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use crate::{BlockSlice, ByteSlice};

/// A parsed chain record with zero-copy references into shared storage.
///
/// Represents a chain alignment between two sequences (reference and query) with
/// metadata and alignment blocks. Uses zero-copy references to avoid allocations
/// when parsing large files.
///
/// # Fields
///
/// * `score` - Alignment score
/// * `reference_name` - Reference sequence name (zero-copy reference)
/// * `reference_size` - Reference sequence length
/// * `reference_strand` - Reference strand orientation
/// * `reference_start` - Reference start coordinate
/// * `reference_end` - Reference end coordinate
/// * `query_name` - Query sequence name (zero-copy reference)
/// * `query_size` - Query sequence length
/// * `query_strand` - Query strand orientation
/// * `query_start` - Query start coordinate
/// * `query_end` - Query end coordinate
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
///     reference_name: ByteSlice::from(b"chr1"),
///     reference_size: 1000,
///     reference_strand: Strand::Plus,
///     reference_start: 0,
///     reference_end: 100,
///     query_name: ByteSlice::from(b"chr2"),
///     query_size: 1000,
///     query_strand: Strand::Plus,
///     query_start: 0,
///     query_end: 100,
///     id: 1,
///     blocks: BlockSlice::empty(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Chain {
    pub score: i64,
    pub reference_name: ByteSlice,
    pub reference_size: u32,
    pub reference_strand: Strand,
    pub reference_start: u32,
    pub reference_end: u32,
    pub query_name: ByteSlice,
    pub query_size: u32,
    pub query_strand: Strand,
    pub query_start: u32,
    pub query_end: u32,
    pub id: u64,
    pub blocks: BlockSlice,
}

/// Strand of an alignment reference/query.
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
