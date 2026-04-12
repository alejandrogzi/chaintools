// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::path::Path;

use crate::error::ChainError;
use crate::sequence::{SequenceCache, SequenceResolver};
use crate::{OwnedChain, Strand};

const OK_BEST2: f64 = 0.80;
const MAX_OVER_OK: f64 = 1.0 - OK_BEST2;
const NT_VAL: [i8; 256] = build_nt_val();
const IS_LOWER_DNA: [bool; 256] = build_lower_dna();
const COMPLEMENT: [u8; 256] = build_complement_table();

/// Configuration for the anti-repeat filter.
///
/// # Fields
///
/// * `min_score` - Minimum adjusted score required to keep a chain
/// * `no_check_score` - Chains scoring at or above this bypass sequence checks
///
/// # Examples
///
/// ```ignore
/// use chaintools::antirepeat::AntiRepeatConfig;
///
/// let config = AntiRepeatConfig {
///     min_score: 5000,
///     no_check_score: 200000,
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct AntiRepeatConfig {
    pub min_score: i64,
    pub no_check_score: i64,
}

/// Engine for filtering chains dominated by repeats or degenerate DNA.
///
/// Uses sequence-based filters to identify and remove low-quality chain
/// alignments that may represent repetitive or degenerate sequence matches.
/// Requires reference and query sequence data for analysis.
///
/// # Fields
///
/// * `reference` - Resolver for reference sequences
/// * `query` - Resolver for query sequences
/// * `config` - Filter configuration parameters
///
/// # Examples
///
/// ```ignore
/// use chaintools::antirepeat::{AntiRepeatEngine, AntiRepeatConfig};
///
/// let engine = AntiRepeatEngine::new(
///     "reference.2bit",
///     "query.2bit",
///     AntiRepeatConfig {
///         min_score: 5000,
///         no_check_score: 200000,
///     },
/// )?;
/// ```
#[derive(Debug, Clone)]
pub struct AntiRepeatEngine {
    reference: SequenceResolver,
    query: SequenceResolver,
    config: AntiRepeatConfig,
}

impl AntiRepeatEngine {
    /// Creates a new anti-repeat engine from sequence files.
    ///
    /// Initializes the engine with reference and query sequence sources.
    /// Both files must be in a supported format (2bit, FASTA, or gzipped FASTA).
    ///
    /// # Arguments
    ///
    /// * `reference` - Path to reference sequence file
    /// * `query` - Path to query sequence file
    /// * `config` - Filter configuration
    ///
    /// # Output
    ///
    /// Returns `Ok(AntiRepeatEngine)` or `Err(ChainError)` on failure
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::antirepeat::{AntiRepeatEngine, AntiRepeatConfig};
    ///
    /// let engine = AntiRepeatEngine::new(
    ///     "reference.2bit",
    ///     "query.2bit",
    ///     AntiRepeatConfig { min_score: 5000, no_check_score: 200000 },
    /// )?;
    /// ```
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(
        reference: P,
        query: Q,
        config: AntiRepeatConfig,
    ) -> Result<Self, ChainError> {
        Ok(Self {
            reference: SequenceResolver::new(reference)?,
            query: SequenceResolver::new(query)?,
            config,
        })
    }

    /// Returns the configuration for this engine.
    ///
    /// # Output
    ///
    /// Returns a copy of the AntiRepeatConfig
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::antirepeat::AntiRepeatEngine;
    ///
    /// let engine = AntiRepeatEngine::new("ref.2bit", "qry.2bit", config)?;
    /// let cfg = engine.config();
    /// ```
    pub fn config(&self) -> AntiRepeatConfig {
        self.config
    }

    /// Determines if a chain passes the anti-repeat filters.
    ///
    /// Applies both degeneracy and repeat filters to the chain using the
    /// configured sequence data. Chains scoring above `no_check_score` are
    /// automatically accepted without sequence analysis.
    ///
    /// # Arguments
    ///
    /// * `cache` - Per-worker sequence cache
    /// * `chain` - The chain to evaluate
    ///
    /// # Output
    ///
    /// Returns `Ok(true)` if the chain passes filters, `Ok(false)` if rejected
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::antirepeat::AntiRepeatEngine;
    /// use chaintools::sequence::SequenceCache;
    ///
    /// let engine = AntiRepeatEngine::new("ref.2bit", "qry.2bit", config)?;
    /// let mut cache = SequenceCache::default();
    ///
    /// if engine.chain_passes(&mut cache, &chain)? {
    ///     // Keep this chain
    /// }
    /// ```
    pub fn chain_passes(
        &self,
        cache: &mut SequenceCache,
        chain: &OwnedChain,
    ) -> Result<bool, ChainError> {
        if chain.score >= self.config.no_check_score {
            return Ok(true);
        }

        let t_length = span_len(chain.reference_start, chain.reference_end, "target")?;
        let q_length = span_len(chain.query_start, chain.query_end, "query")?;

        let t_seq = self.reference.fetch(
            cache,
            &chain.reference_name,
            chain.reference_start,
            t_length,
        )?;
        let q_seq = match chain.query_strand {
            Strand::Plus => {
                self.query
                    .fetch(cache, &chain.query_name, chain.query_start, q_length)?
            }
            Strand::Minus => {
                let fetch_start = chain
                    .query_size
                    .checked_sub(chain.query_end)
                    .ok_or_else(|| sequence_error("query minus-strand fetch underflows"))?;
                let mut seq = self
                    .query
                    .fetch(cache, &chain.query_name, fetch_start, q_length)?;
                reverse_complement_in_place(&mut seq);
                seq
            }
        };

        if !degeneracy_filter(&t_seq, &q_seq, chain, self.config.min_score) {
            return Ok(false);
        }
        Ok(repeat_filter(&t_seq, &q_seq, chain, self.config.min_score))
    }
}

/// Filters chains based on nucleotide degeneracy.
///
/// Checks if the chain has sufficient specific (non-degenerate) matches
/// between target and query sequences. Chains with too many degenerate
/// matches (N nucleotides) are rejected unless their adjusted score
/// meets the minimum threshold.
///
/// # Arguments
///
/// * `target` - Target/reference sequence bytes
/// * `query` - Query sequence bytes
/// * `chain` - The chain to evaluate
/// * `min_score` - Minimum adjusted score threshold
///
/// # Output
///
/// Returns `true` if the chain passes the degeneracy filter
///
/// # Examples
///
/// ```ignore
/// use chaintools::antirepeat::degeneracy_filter;
/// use chaintools::{OwnedChain, Strand, Block};
///
/// let target = b"ACGT";
/// let query = b"ACGT";
/// let chain = OwnedChain { /* ... */ };
///
/// if degeneracy_filter(target, query, &chain, 5000) {
///     // Chain passes degeneracy check
/// }
/// ```
pub fn degeneracy_filter(target: &[u8], query: &[u8], chain: &OwnedChain, min_score: i64) -> bool {
    let mut counts = [0i32; 5];
    let mut t_cursor = chain.reference_start;
    let mut q_cursor = chain.query_start;

    for block in &chain.blocks {
        let size = block.size as usize;
        let t_offset = (t_cursor - chain.reference_start) as usize;
        let q_offset = (q_cursor - chain.query_start) as usize;
        let t_block = &target[t_offset..t_offset + size];
        let q_block = &query[q_offset..q_offset + size];

        for i in 0..size {
            let q_base = NT_VAL[q_block[i] as usize];
            let t_base = NT_VAL[t_block[i] as usize];
            if q_base == t_base {
                counts[(q_base + 1) as usize] += 1;
            }
        }

        t_cursor += block.size + block.gap_reference;
        q_cursor += block.size + block.gap_query;
    }

    let total_matches = counts[1] + counts[2] + counts[3] + counts[4];
    let mut best2 = counts[1] + counts[2];
    for &(i, j) in &[(1usize, 3usize), (1, 4), (2, 3), (2, 4), (3, 4)] {
        let sum2 = counts[i] + counts[j];
        if best2 < sum2 {
            best2 = sum2;
        }
    }

    let observed_best2 = best2 as f64 / total_matches as f64;
    let over_ok = observed_best2 - OK_BEST2;
    if over_ok <= 0.0 {
        true
    } else {
        let adjust_factor = 1.01 - over_ok / MAX_OVER_OK;
        let adjusted_score = chain.score as f64 * adjust_factor;
        adjusted_score >= min_score as f64
    }
}

/// Filters chains based on repeat (softmask) content.
///
/// Checks if the chain has too many repeat-masked bases (lowercase in 2bit
/// or soft-masked sequences). Chains with excessive repeat content are
/// rejected unless their adjusted score meets the minimum threshold.
///
/// # Arguments
///
/// * `target` - Target/reference sequence bytes
/// * `query` - Query sequence bytes
/// * `chain` - The chain to evaluate
/// * `min_score` - Minimum adjusted score threshold
///
/// # Output
///
/// Returns `true` if the chain passes the repeat filter
///
/// # Examples
///
/// ```ignore
/// use chaintools::antirepeat::repeat_filter;
/// use chaintools::{OwnedChain, Strand, Block};
///
/// let target = b"ACGT";
/// let query = b"aaaa";
/// let chain = OwnedChain { /* ... */ };
///
/// if !repeat_filter(target, query, &chain, 5000) {
///     // Chain rejected due to excessive repeats
/// }
/// ```
pub fn repeat_filter(target: &[u8], query: &[u8], chain: &OwnedChain, min_score: i64) -> bool {
    let mut rep_count = 0u64;
    let mut total = 0u64;
    let mut t_cursor = chain.reference_start;
    let mut q_cursor = chain.query_start;

    for block in &chain.blocks {
        let size = block.size as usize;
        let t_offset = (t_cursor - chain.reference_start) as usize;
        let q_offset = (q_cursor - chain.query_start) as usize;
        let t_block = &target[t_offset..t_offset + size];
        let q_block = &query[q_offset..q_offset + size];

        for i in 0..size {
            if IS_LOWER_DNA[q_block[i] as usize] || IS_LOWER_DNA[t_block[i] as usize] {
                rep_count += 1;
            }
        }
        total += block.size as u64;
        t_cursor += block.size + block.gap_reference;
        q_cursor += block.size + block.gap_query;
    }

    let adjusted_score = chain.score as f64 * 2.0 * (total - rep_count) as f64 / total as f64;
    adjusted_score >= min_score as f64
}

/// Performs in-place reverse complement of a DNA sequence.
///
/// Converts a DNA sequence to its reverse complement, replacing each
/// base with its complement (A<->T, C<->G) and reversing the order.
/// Preserves case: uppercase stays uppercase, lowercase stays lowercase.
///
/// # Arguments
///
/// * `sequence` - Mutable reference to the sequence to transform
///
/// # Examples
///
/// ```ignore
/// use chaintools::antirepeat::reverse_complement_in_place;
///
/// let mut seq = b"ACGT".to_vec();
/// reverse_complement_in_place(&mut seq);
/// assert_eq!(&seq, b"ACGT"); // Reversed: "GTCA" -> complement "CATG" -> reverse
///
/// let mut seq2 = b"AcgT".to_vec();
/// reverse_complement_in_place(&mut seq2);
/// assert_eq!(&seq2, b"aCgT");
/// ```
pub fn reverse_complement_in_place(sequence: &mut [u8]) {
    let len = sequence.len();
    for i in 0..(len / 2) {
        let j = len - 1 - i;
        let left = COMPLEMENT[sequence[i] as usize];
        let right = COMPLEMENT[sequence[j] as usize];
        sequence[i] = right;
        sequence[j] = left;
    }
    if len % 2 == 1 {
        let mid = len / 2;
        sequence[mid] = COMPLEMENT[sequence[mid] as usize];
    }
}

/// Calculates span length (end - start) with error handling.
///
/// Computes the difference between end and start coordinates,
/// returning an error if the result would underflow.
///
/// # Arguments
///
/// * `start` - Start coordinate
/// * `end` - End coordinate
/// * `label` - Label for error messages
///
/// # Output
///
/// Returns `Ok(u32)` with the span length or `Err(ChainError)` on underflow
fn span_len(start: u32, end: u32, label: &str) -> Result<u32, ChainError> {
    end.checked_sub(start)
        .ok_or_else(|| sequence_error(format!("{label} span underflows")))
}

/// Creates a sequence error with a custom message.
///
/// Helper function to create an unsupported ChainError with a custom message.
///
/// # Arguments
///
/// * `message` - Error message
///
/// # Output
///
/// Returns a `ChainError::Unsupported` with the message
fn sequence_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}

const fn build_nt_val() -> [i8; 256] {
    let mut table = [-1i8; 256];
    table[b'A' as usize] = 0;
    table[b'a' as usize] = 0;
    table[b'C' as usize] = 1;
    table[b'c' as usize] = 1;
    table[b'G' as usize] = 2;
    table[b'g' as usize] = 2;
    table[b'T' as usize] = 3;
    table[b't' as usize] = 3;
    table[b'N' as usize] = -1;
    table[b'n' as usize] = -1;
    table
}

const fn build_lower_dna() -> [bool; 256] {
    let mut table = [false; 256];
    table[b'a' as usize] = true;
    table[b'c' as usize] = true;
    table[b'g' as usize] = true;
    table[b't' as usize] = true;
    table[b'n' as usize] = true;
    table
}

const fn build_complement_table() -> [u8; 256] {
    let mut table = [0; 256];
    let mut idx = 0;
    while idx < 256 {
        table[idx] = idx as u8;
        idx += 1;
    }

    table[b'A' as usize] = b'T';
    table[b'a' as usize] = b't';
    table[b'C' as usize] = b'G';
    table[b'c' as usize] = b'g';
    table[b'G' as usize] = b'C';
    table[b'g' as usize] = b'c';
    table[b'T' as usize] = b'A';
    table[b't' as usize] = b'a';
    table[b'N' as usize] = b'N';
    table[b'n' as usize] = b'n';
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Block;

    fn chain_with_blocks(score: i64, blocks: &[(u32, u32, u32)]) -> OwnedChain {
        let blocks_vec: Vec<Block> = blocks
            .iter()
            .map(|&(size, gap_reference, gap_query)| Block {
                size,
                gap_reference,
                gap_query,
            })
            .collect();
        let reference_end = blocks_vec
            .iter()
            .map(|block| block.size + block.gap_reference)
            .sum::<u32>()
            .saturating_sub(blocks_vec.last().map_or(0, |block| block.gap_reference));
        let query_end = blocks_vec
            .iter()
            .map(|block| block.size + block.gap_query)
            .sum::<u32>()
            .saturating_sub(blocks_vec.last().map_or(0, |block| block.gap_query));

        OwnedChain {
            score,
            reference_name: b"chr1".to_vec(),
            reference_size: 100,
            reference_strand: Strand::Plus,
            reference_start: 0,
            reference_end,
            query_name: b"chr1".to_vec(),
            query_size: 100,
            query_strand: Strand::Plus,
            query_start: 0,
            query_end,
            id: 1,
            blocks: blocks_vec,
        }
    }

    #[test]
    fn reverse_complement_preserves_case() {
        let mut seq = b"AcgTNn".to_vec();
        reverse_complement_in_place(&mut seq);
        assert_eq!(&seq, b"nNAcgT");
    }

    #[test]
    fn degeneracy_filter_rejects_all_n_matches_like_ucsc() {
        let chain = chain_with_blocks(10_000, &[(4, 0, 0)]);
        assert!(!degeneracy_filter(b"NNNN", b"NNNN", &chain, 5_000));
    }

    #[test]
    fn repeat_filter_rejects_zero_length_chain_like_ucsc() {
        let chain = OwnedChain {
            score: 10_000,
            reference_name: b"chr1".to_vec(),
            reference_size: 100,
            reference_strand: Strand::Plus,
            reference_start: 0,
            reference_end: 0,
            query_name: b"chr1".to_vec(),
            query_size: 100,
            query_strand: Strand::Plus,
            query_start: 0,
            query_end: 0,
            id: 1,
            blocks: Vec::new(),
        };
        assert!(!repeat_filter(b"", b"", &chain, 5_000));
    }

    #[test]
    fn degeneracy_filter_respects_threshold_boundary() {
        let chain = chain_with_blocks(5_000, &[(10, 0, 0)]);
        assert!(degeneracy_filter(
            b"AAAACCCCGG",
            b"AAAACCCCGG",
            &chain,
            5_000
        ));
    }

    #[test]
    fn repeat_filter_counts_softmask_on_either_side() {
        let chain = chain_with_blocks(10_000, &[(4, 0, 0)]);
        assert!(!repeat_filter(b"aaaa", b"AAAA", &chain, 9_000));
        assert!(repeat_filter(b"AAAA", b"AAAA", &chain, 9_000));
    }
}
