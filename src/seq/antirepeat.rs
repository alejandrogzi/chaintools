// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::HashSet;
use std::path::Path;

use crate::model::error::ChainError;
use crate::seq::sequence::SequenceResolver;
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
/// use chaintools::seq::antirepeat::AntiRepeatConfig;
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
/// use chaintools::seq::antirepeat::{AntiRepeatEngine, AntiRepeatConfig};
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
    /// use chaintools::seq::antirepeat::{AntiRepeatEngine, AntiRepeatConfig};
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
        Self::new_filtered(reference, query, config, None, None)
    }

    /// Creates an engine that preloads only the named sequences.
    ///
    /// When `reference_names`/`query_names` are `Some`, only the listed
    /// sequences are loaded into memory from the corresponding file (others are
    /// skipped). `None` loads every sequence. Restricting the load to the names
    /// a chain file actually references bounds peak memory on assemblies with
    /// many unused scaffolds; sequences referenced but absent from the file
    /// still surface as [`ChainError::MissingSequence`] at fetch time, exactly
    /// as when everything is loaded.
    ///
    /// # Arguments
    ///
    /// * `reference` - Path to reference sequence file
    /// * `query` - Path to query sequence file
    /// * `config` - Filter configuration
    /// * `reference_names` - Reference sequence names to load, or `None` for all
    /// * `query_names` - Query sequence names to load, or `None` for all
    ///
    /// # Output
    ///
    /// Returns `Ok(AntiRepeatEngine)` or `Err(ChainError)` on failure
    pub fn new_filtered<P: AsRef<Path>, Q: AsRef<Path>>(
        reference: P,
        query: Q,
        config: AntiRepeatConfig,
        reference_names: Option<&HashSet<Vec<u8>>>,
        query_names: Option<&HashSet<Vec<u8>>>,
    ) -> Result<Self, ChainError> {
        Ok(Self {
            reference: SequenceResolver::new_filtered(reference, reference_names)?,
            query: SequenceResolver::new_filtered(query, query_names)?,
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
    /// use chaintools::seq::antirepeat::AntiRepeatEngine;
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
    /// * `chain` - The chain to evaluate
    ///
    /// # Output
    ///
    /// Returns `Ok(true)` if the chain passes filters, `Ok(false)` if rejected
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::seq::antirepeat::AntiRepeatEngine;
    ///
    /// let engine = AntiRepeatEngine::new("ref.2bit", "qry.2bit", config)?;
    ///
    /// if engine.chain_passes(&chain)? {
    ///     // Keep this chain
    /// }
    /// ```
    pub fn chain_passes(&self, chain: &OwnedChain) -> Result<bool, ChainError> {
        if chain.score >= self.config.no_check_score {
            return Ok(true);
        }

        // Borrow the aligned region directly from the preloaded chromosomes
        // instead of copying a per-chain span. The plus path feeds borrowed
        // sub-slices to the same span-relative kernel; the minus path reverse-
        // complements the query on the fly while walking (count_blocks_minus),
        // avoiding both the span copy and the separate reverse pass.
        let t_chr = self.reference.chromosome(&chain.reference_name)?;
        let q_chr = self.query.chromosome(&chain.query_name)?;

        let t_span = window(t_chr, chain.reference_start, chain.reference_end, "target")?;
        let (counts, rep_count, total) = match chain.query_strand {
            Strand::Plus => {
                let q_span = window(q_chr, chain.query_start, chain.query_end, "query")?;
                count_blocks(t_span, q_span, chain)
            }
            Strand::Minus => {
                validate_minus_window(q_chr, chain)?;
                count_blocks_minus(t_span, q_chr, chain)
            }
        };

        // Evaluating degeneracy first and short-circuiting with `&&` reproduces
        // the original "reject on degeneracy, otherwise check repeats" decision
        // bit-for-bit; both are pure functions of the counts, so computing the
        // repeat counts unconditionally cannot change the result.
        Ok(evaluate_degeneracy(&counts, chain.score, self.config.min_score)
            && evaluate_repeat(rep_count, total, chain.score, self.config.min_score))
    }
}

/// Accumulates one aligned position into the degeneracy and repeat counters.
///
/// Shared by every walk so the per-base logic cannot drift between the plus and
/// minus paths. A match is counted only when both bases map to the same
/// nucleotide (`NT_VAL`; N maps to -1 and is ignored); a position counts toward
/// repeats when either base is soft-masked or `n` (`IS_LOWER_DNA`).
#[inline(always)]
fn accumulate(t_byte: u8, q_byte: u8, counts: &mut [i32; 5], rep_count: &mut u64) {
    let q_base = NT_VAL[q_byte as usize];
    let t_base = NT_VAL[t_byte as usize];
    if q_base == t_base {
        counts[(q_base + 1) as usize] += 1;
    }
    if IS_LOWER_DNA[q_byte as usize] || IS_LOWER_DNA[t_byte as usize] {
        *rep_count += 1;
    }
}

/// Walks the chain's aligned blocks once, accumulating the raw counts that both
/// the degeneracy and repeat filters need.
///
/// Returns `(counts, rep_count, total)` where `counts` holds, by nucleotide,
/// the number of positions where target and query agree (`counts[0]` is the
/// degenerate N==N bucket, `counts[1..=4]` are A/C/G/T), `rep_count` is the
/// number of positions soft-masked on either side, and `total` is the number of
/// aligned bases. This is the shared core of [`degeneracy_filter`] and
/// [`repeat_filter`]; both, and [`AntiRepeatEngine::chain_passes`], derive their
/// decisions from these counts so the logic cannot drift between paths.
///
/// # Arguments
///
/// * `target` - Target/reference sequence bytes
/// * `query` - Query sequence bytes
/// * `chain` - The chain to evaluate
fn count_blocks(target: &[u8], query: &[u8], chain: &OwnedChain) -> ([i32; 5], u64, u64) {
    let mut counts = [0i32; 5];
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
            accumulate(t_block[i], q_block[i], &mut counts, &mut rep_count);
        }

        total += block.size as u64;
        t_cursor += block.size + block.gap_reference;
        q_cursor += block.size + block.gap_query;
    }

    (counts, rep_count, total)
}

/// Walks a minus-strand chain, reverse-complementing the query on the fly.
///
/// `t_span` is the borrowed target region `[reference_start, reference_end)`
/// (same span-relative indexing as [`count_blocks`]); `q_chr` is the full query
/// chromosome. For a minus-strand query the old path fetched
/// `q_chr[fetch_start .. fetch_start + q_length]` (with
/// `fetch_start = query_size - query_end`), reverse-complemented it, then read it
/// forward. That is equivalent, position for position, to reading `q_chr`
/// downward from `query_size - 1 - q_cursor` and complementing each base — so
/// this produces byte-identical counts with no allocation and no reverse pass.
/// Callers must validate the query window with [`validate_minus_window`] first.
fn count_blocks_minus(t_span: &[u8], q_chr: &[u8], chain: &OwnedChain) -> ([i32; 5], u64, u64) {
    let mut counts = [0i32; 5];
    let mut rep_count = 0u64;
    let mut total = 0u64;
    let mut t_cursor = chain.reference_start;
    let mut q_cursor = chain.query_start;

    for block in &chain.blocks {
        let size = block.size as usize;
        let t_offset = (t_cursor - chain.reference_start) as usize;
        let t_block = &t_span[t_offset..t_offset + size];
        let q_top = (chain.query_size - 1 - q_cursor) as usize;

        for i in 0..size {
            let q_byte = COMPLEMENT[q_chr[q_top - i] as usize];
            accumulate(t_block[i], q_byte, &mut counts, &mut rep_count);
        }

        total += block.size as u64;
        t_cursor += block.size + block.gap_reference;
        q_cursor += block.size + block.gap_query;
    }

    (counts, rep_count, total)
}

/// Applies the degeneracy decision to per-nucleotide match counts.
///
/// Chains whose best two nucleotides comprise at most `OK_BEST2` of all matches
/// pass unconditionally; beyond that the score is penalized and compared against
/// `min_score`. The all-N case (`total_matches == 0`) yields a `NaN` comparison
/// that evaluates to `false`, rejecting the chain as UCSC does.
fn evaluate_degeneracy(counts: &[i32; 5], score: i64, min_score: i64) -> bool {
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
        let adjusted_score = score as f64 * adjust_factor;
        adjusted_score >= min_score as f64
    }
}

/// Applies the repeat decision to the soft-masked/total base counts.
///
/// A zero-length chain (`total == 0`) yields a `NaN` comparison that evaluates
/// to `false`, rejecting the chain as UCSC does.
fn evaluate_repeat(rep_count: u64, total: u64, score: i64, min_score: i64) -> bool {
    let adjusted_score = score as f64 * 2.0 * (total - rep_count) as f64 / total as f64;
    adjusted_score >= min_score as f64
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
/// use chaintools::seq::antirepeat::degeneracy_filter;
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
    let (counts, _, _) = count_blocks(target, query, chain);
    evaluate_degeneracy(&counts, chain.score, min_score)
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
/// use chaintools::seq::antirepeat::repeat_filter;
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
    let (_, rep_count, total) = count_blocks(target, query, chain);
    evaluate_repeat(rep_count, total, chain.score, min_score)
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
/// use chaintools::seq::antirepeat::reverse_complement_in_place;
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

/// Borrows the aligned window `[start, end)` from a chromosome.
///
/// Validates `start <= end` and `end <= chr.len()` before slicing, returning the
/// same error classes the old fetch path produced for an underflowing span or an
/// out-of-range request, so malformed or oversized chains error instead of
/// panicking on slice indexing.
///
/// # Arguments
///
/// * `chr` - Full chromosome bytes
/// * `start` - Window start coordinate
/// * `end` - Window end coordinate
/// * `label` - Label for error messages ("target"/"query")
///
/// # Output
///
/// Returns `Ok(&[u8])` with `chr[start..end]` or `Err(ChainError)`
fn window<'a>(chr: &'a [u8], start: u32, end: u32, label: &str) -> Result<&'a [u8], ChainError> {
    if end < start {
        return Err(sequence_error(format!("{label} span underflows")));
    }
    let end = end as usize;
    if end > chr.len() {
        return Err(sequence_error(format!(
            "{label} range {start}-{end} exceeds sequence length {}",
            chr.len()
        )));
    }
    Ok(&chr[start as usize..end])
}

/// Validates that a minus-strand query window fits within the query chromosome.
///
/// The minus-strand walk reads `q_chr` downward from `query_size - 1 - q_cursor`;
/// this confirms that index range — `[query_size - query_end, query_size - query_start)`
/// — lies within `[0, q_chr.len())`, reproducing the underflow/"exceeds" errors
/// the old fetch + reverse-complement path raised, so [`count_blocks_minus`] can
/// index without underflow or out-of-bounds panics on well-formed input.
///
/// # Arguments
///
/// * `q_chr` - Full query chromosome bytes
/// * `chain` - The chain being evaluated
///
/// # Output
///
/// Returns `Ok(())` if the window is valid, or `Err(ChainError)`
fn validate_minus_window(q_chr: &[u8], chain: &OwnedChain) -> Result<(), ChainError> {
    if chain.query_end < chain.query_start {
        return Err(sequence_error("query span underflows"));
    }
    let fetch_start = chain
        .query_size
        .checked_sub(chain.query_end)
        .ok_or_else(|| sequence_error("query minus-strand fetch underflows"))?;
    let q_length = (chain.query_end - chain.query_start) as usize;
    let end = (fetch_start as usize)
        .checked_add(q_length)
        .ok_or_else(|| sequence_error("query minus-strand range overflows"))?;
    if end > q_chr.len() {
        return Err(sequence_error(format!(
            "query minus-strand range {fetch_start}-{end} exceeds sequence length {}",
            q_chr.len()
        )));
    }
    Ok(())
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

    // Deterministic xorshift so the differential fuzz test is reproducible.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn random_sequence(rng: &mut Rng, len: usize) -> Vec<u8> {
        const UPPER: [u8; 5] = [b'A', b'C', b'G', b'T', b'N'];
        const LOWER: [u8; 5] = [b'a', b'c', b'g', b't', b'n'];
        (0..len)
            .map(|_| {
                let base = (rng.below(5)) as usize;
                if rng.below(2) == 0 {
                    UPPER[base]
                } else {
                    LOWER[base]
                }
            })
            .collect()
    }

    // Reproduces exactly what chain_passes did before borrowing: copy the target
    // and query spans out of the chromosomes, reverse-complement the query for
    // minus strand, then run the span-relative kernel.
    fn old_counts(
        reference: &[u8],
        query: &[u8],
        chain: &OwnedChain,
    ) -> ([i32; 5], u64, u64) {
        let t_seq =
            reference[chain.reference_start as usize..chain.reference_end as usize].to_vec();
        let q_seq = match chain.query_strand {
            Strand::Plus => {
                query[chain.query_start as usize..chain.query_end as usize].to_vec()
            }
            Strand::Minus => {
                let fetch_start = (chain.query_size - chain.query_end) as usize;
                let q_length = (chain.query_end - chain.query_start) as usize;
                let mut seq = query[fetch_start..fetch_start + q_length].to_vec();
                reverse_complement_in_place(&mut seq);
                seq
            }
        };
        count_blocks(&t_seq, &q_seq, chain)
    }

    // Mirrors the new chain_passes data flow: borrow spans, complement on the fly.
    fn new_counts(
        reference: &[u8],
        query: &[u8],
        chain: &OwnedChain,
    ) -> ([i32; 5], u64, u64) {
        let t_span = &reference[chain.reference_start as usize..chain.reference_end as usize];
        match chain.query_strand {
            Strand::Plus => count_blocks(
                t_span,
                &query[chain.query_start as usize..chain.query_end as usize],
                chain,
            ),
            Strand::Minus => count_blocks_minus(t_span, query, chain),
        }
    }

    fn random_chain(rng: &mut Rng, ref_len: usize, qry_len: usize) -> Option<OwnedChain> {
        let n_blocks = 1 + rng.below(5);
        let mut blocks = Vec::new();
        let mut span_t = 0u64;
        let mut span_q = 0u64;
        for b in 0..n_blocks {
            let size = (1 + rng.below(60)) as u32;
            let (gap_reference, gap_query) = if b + 1 < n_blocks {
                (rng.below(20) as u32, rng.below(20) as u32)
            } else {
                (0, 0)
            };
            span_t += (size + gap_reference) as u64;
            span_q += (size + gap_query) as u64;
            blocks.push(Block {
                size,
                gap_reference,
                gap_query,
            });
        }
        if span_t as usize >= ref_len || span_q as usize >= qry_len {
            return None;
        }
        let reference_start = rng.below(ref_len as u64 - span_t) as u32;
        let query_start = rng.below(qry_len as u64 - span_q) as u32;
        let strand = if rng.below(2) == 0 {
            Strand::Plus
        } else {
            Strand::Minus
        };
        Some(OwnedChain {
            score: 10_000,
            reference_name: b"chr1".to_vec(),
            reference_size: ref_len as u32,
            reference_strand: Strand::Plus,
            reference_start,
            reference_end: reference_start + span_t as u32,
            query_name: b"chr1".to_vec(),
            query_size: qry_len as u32,
            query_strand: strand,
            query_start,
            query_end: query_start + span_q as u32,
            id: 1,
            blocks,
        })
    }

    #[test]
    fn borrowed_walk_matches_old_span_copy_path() {
        // The borrow + on-the-fly reverse-complement path must produce exactly
        // the same counts as copying the span and reverse-complementing it.
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        let reference = random_sequence(&mut rng, 4_000);
        let query = random_sequence(&mut rng, 4_000);

        let mut checked_plus = 0;
        let mut checked_minus = 0;
        for _ in 0..20_000 {
            let Some(chain) = random_chain(&mut rng, reference.len(), query.len()) else {
                continue;
            };
            match chain.query_strand {
                Strand::Plus => checked_plus += 1,
                Strand::Minus => checked_minus += 1,
            }
            assert_eq!(
                new_counts(&reference, &query, &chain),
                old_counts(&reference, &query, &chain),
                "mismatch for chain {chain:?}"
            );
        }
        // Make sure both strands were actually exercised.
        assert!(checked_plus > 100 && checked_minus > 100);
    }
}
