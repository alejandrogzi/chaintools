// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Chain rescoring engine, equivalent to UCSC `chainScore` /
//! `chainCalcScore` (pinned kent commit `f1f04f7`).
//!
//! For each chain, the score is the sum over every block's bases of
//! `matrix[query][target]`, minus the gap cost between consecutive blocks.
//! Sequence is fetched per-chain via 2bit/FASTA random access (target always
//! `+`; query reverse-complemented for `-` strand), mirroring
//! [`crate::seq::antirepeat`]. Scores are accumulated in `i64`, which is an exact
//! integer and reproduces kent's `double` + `%1.0f` output for any realistic
//! genome.

use std::path::Path;

use crate::model::block::AbsoluteBlock;
use crate::model::error::ChainError;
use crate::seq::revcomp::reverse_complement_in_place;
use crate::seq::score::gapcalc::GapCalc;
use crate::seq::score::scoring::{CompactMatrix, ScoreMatrix};
use crate::seq::sequence::{SequenceCache, SequenceResolver};
use crate::{OwnedChain, Strand};

/// Configuration for chain rescoring.
///
/// # Fields
///
/// * `min_score` - Minimum recomputed score required to keep a chain
#[derive(Debug, Clone, Copy)]
pub struct ScoreConfig {
    pub min_score: i64,
}

impl Default for ScoreConfig {
    fn default() -> Self {
        // kent chainScore default -minScore is 1000.
        ScoreConfig { min_score: 1000 }
    }
}

/// Engine that recomputes chain scores from sequence.
///
/// Holds the target (reference) and query sequence resolvers, the score matrix
/// (kept as the canonical source of truth, with a compact form derived for the
/// hot loop), and the gap cost model.
///
/// # Examples
///
/// ```ignore
/// use chaintools::seq::score::chainscore::ChainScorer;
/// use chaintools::seq::score::scoring::ScoreMatrix;
/// use chaintools::seq::score::gapcalc::GapCalc;
/// use chaintools::seq::sequence::SequenceCache;
///
/// let scorer = ChainScorer::new(
///     "target.2bit",
///     "query.2bit",
///     ScoreMatrix::default_dna(),
///     GapCalc::default_costs(),
/// )?;
/// let mut cache = SequenceCache::default();
/// let score = scorer.score_chain(&mut cache, &chain)?;
/// ```
#[derive(Debug, Clone)]
pub struct ChainScorer {
    reference: SequenceResolver,
    query: SequenceResolver,
    matrix: ScoreMatrix,
    compact: CompactMatrix,
    gap: GapCalc,
}

impl ChainScorer {
    /// Creates a chain scorer from sequence files, a matrix, and gap costs.
    ///
    /// `reference` is kent's target (`t`); `query` is kent's query (`q`). Both
    /// may be `.2bit` or FASTA (auto-detected, covering kent's `-faQ`).
    ///
    /// # Arguments
    ///
    /// * `reference` - Path to the target/reference sequence file
    /// * `query` - Path to the query sequence file
    /// * `matrix` - Score matrix (default or `-scoreScheme`)
    /// * `gap` - Gap cost model (default or `-linearGap`)
    ///
    /// # Output
    ///
    /// Returns `Ok(ChainScorer)` or `Err(ChainError)` if a sequence file
    /// cannot be opened.
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(
        reference: P,
        query: Q,
        matrix: ScoreMatrix,
        gap: GapCalc,
    ) -> Result<Self, ChainError> {
        let compact = matrix.compact();
        Ok(Self {
            reference: SequenceResolver::new(reference)?,
            query: SequenceResolver::new(query)?,
            matrix,
            compact,
            gap,
        })
    }

    /// Returns the canonical score matrix backing this scorer.
    ///
    /// The hot loop uses a compact form derived from this matrix; the full
    /// matrix is retained as the source of truth (e.g. for inspection or tests).
    pub fn matrix(&self) -> &ScoreMatrix {
        &self.matrix
    }

    /// Recomputes a chain's score from sequence (`chainCalcScore`).
    ///
    /// Fetches the chain's own target span and strand-correct query span, then
    /// walks the blocks: each block contributes the sum of `matrix[q][t]` over
    /// its bases, and each gap between consecutive blocks subtracts
    /// `GapCalc::cost(gap_query, gap_reference)`. Returns the `i64` score; the
    /// input header score is ignored.
    ///
    /// # Arguments
    ///
    /// * `cache` - Per-worker sequence cache (open 2bit handles)
    /// * `chain` - The chain to score
    ///
    /// # Output
    ///
    /// Returns `Ok(i64)` with the recomputed score or `Err(ChainError)` if the
    /// chain's coordinates fall outside the sequences.
    pub fn score_chain(
        &self,
        cache: &mut SequenceCache,
        chain: &OwnedChain,
    ) -> Result<i64, ChainError> {
        let t_len = span_len(chain.reference_start, chain.reference_end, "target")?;
        let q_len = span_len(chain.query_start, chain.query_end, "query")?;

        let t_seq =
            self.reference
                .fetch(cache, &chain.reference_name, chain.reference_start, t_len)?;
        let q_seq = match chain.query_strand {
            Strand::Plus => self
                .query
                .fetch(cache, &chain.query_name, chain.query_start, q_len)?,
            Strand::Minus => {
                let fetch_start = chain
                    .query_size
                    .checked_sub(chain.query_end)
                    .ok_or_else(|| score_error("query minus-strand fetch underflows"))?;
                let mut seq = self
                    .query
                    .fetch(cache, &chain.query_name, fetch_start, q_len)?;
                reverse_complement_in_place(&mut seq);
                seq
            }
        };

        let mut score: i64 = 0;
        let mut t_pos = chain.reference_start;
        let mut q_pos = chain.query_start;
        let block_count = chain.blocks.len();

        for (index, block) in chain.blocks.iter().enumerate() {
            let size = block.size as usize;
            let t_off = (t_pos - chain.reference_start) as usize;
            let q_off = (q_pos - chain.query_start) as usize;

            let t_block = t_seq
                .get(t_off..t_off + size)
                .ok_or_else(|| score_error("target block exceeds fetched sequence"))?;
            let q_block = q_seq
                .get(q_off..q_off + size)
                .ok_or_else(|| score_error("query block exceeds fetched sequence"))?;

            score += score_ungapped_slices(q_block, t_block, &self.compact);

            if index + 1 < block_count {
                let dq = gap_to_i32(block.gap_query, "query")?;
                let dt = gap_to_i32(block.gap_reference, "reference")?;
                let cost = self.gap.cost(dq, dt);
                score -= i64::from(cost);
            }

            t_pos += block.size + block.gap_reference;
            q_pos += block.size + block.gap_query;
        }

        Ok(score)
    }
}

/// Scores two equal-length ungapped sequence slices.
///
/// The matrix is indexed as `matrix[query][reference]`, matching UCSC chain
/// scoring. Panics if the slices have different lengths.
#[inline]
pub fn score_ungapped_slices(query: &[u8], reference: &[u8], matrix: &CompactMatrix) -> i64 {
    assert_eq!(
        query.len(),
        reference.len(),
        "ungapped scoring requires equal-length slices"
    );
    let mut score = 0i64;
    for i in 0..query.len() {
        score += i64::from(matrix.pair(query[i], reference[i]));
    }
    score
}

/// Scores one absolute, ungapped alignment block against full sequences.
pub fn score_absolute_block(
    block: AbsoluteBlock,
    query_seq: &[u8],
    reference_seq: &[u8],
    matrix: &CompactMatrix,
) -> Result<i64, ChainError> {
    validate_score_block(block)?;
    let reference = absolute_slice(
        reference_seq,
        block.reference_start,
        block.reference_end,
        "reference",
    )?;
    let query = absolute_slice(query_seq, block.query_start, block.query_end, "query")?;
    Ok(score_ungapped_slices(query, reference, matrix))
}

/// Scores absolute alignment blocks as a candidate chain.
///
/// Each block contributes its ungapped slice score and each neighboring gap
/// subtracts `gap.cost(dq, dt)`, where `dq` is the query gap and `dt` is the
/// reference/target gap.
pub fn score_absolute_blocks(
    blocks: &[AbsoluteBlock],
    query_seq: &[u8],
    reference_seq: &[u8],
    matrix: &CompactMatrix,
    gap: &GapCalc,
) -> Result<i64, ChainError> {
    if blocks.is_empty() {
        return Err(score_error("absolute block list is empty"));
    }

    let mut score = 0i64;
    for (index, block) in blocks.iter().copied().enumerate() {
        score += score_absolute_block(block, query_seq, reference_seq, matrix)?;

        if let Some(next) = blocks.get(index + 1).copied() {
            if next.reference_start <= block.reference_start {
                return Err(score_error(
                    "absolute blocks are not sorted by reference start",
                ));
            }
            if next.query_start <= block.query_start {
                return Err(score_error("absolute blocks are not sorted by query start"));
            }

            let dt = next
                .reference_start
                .checked_sub(block.reference_end)
                .ok_or_else(|| score_error("absolute block reference coordinates overlap"))?;
            let dq = next
                .query_start
                .checked_sub(block.query_end)
                .ok_or_else(|| score_error("absolute block query coordinates overlap"))?;
            score -= i64::from(gap.cost(gap_to_i32(dq, "query")?, gap_to_i32(dt, "reference")?));
        }
    }

    Ok(score)
}

/// Computes `end - start`, erroring on underflow.
fn span_len(start: u32, end: u32, label: &str) -> Result<u32, ChainError> {
    end.checked_sub(start)
        .ok_or_else(|| score_error(format!("{label} span underflows")))
}

fn validate_score_block(block: AbsoluteBlock) -> Result<(), ChainError> {
    block.validate()?;
    if block.aligned_len().is_none() {
        return Err(score_error(
            "absolute block reference and query lengths differ",
        ));
    }
    Ok(())
}

fn absolute_slice<'a>(
    seq: &'a [u8],
    start: u32,
    end: u32,
    label: &str,
) -> Result<&'a [u8], ChainError> {
    if end < start {
        return Err(score_error(format!("{label} span underflows")));
    }
    seq.get(start as usize..end as usize)
        .ok_or_else(|| score_error(format!("{label} absolute block exceeds sequence")))
}

fn gap_to_i32(gap: u32, label: &str) -> Result<i32, ChainError> {
    i32::try_from(gap).map_err(|_| score_error(format!("{label} gap exceeds i32 range")))
}

/// Creates an unsupported `ChainError` with a custom message.
fn score_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbsoluteBlock, Block};
    use std::fs::File;
    use std::io::BufWriter;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use twobit::convert::fasta::FastaReader;
    use twobit::convert::to_2bit;

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "chaintools-chainscore-test-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn write_twobit(path: &Path, fasta: &str) {
        let reader = FastaReader::mem_open(fasta.as_bytes().to_vec()).expect("open FASTA");
        let mut writer = BufWriter::new(File::create(path).expect("create 2bit"));
        to_2bit(&mut writer, &reader).expect("write 2bit");
        writer.flush().expect("flush 2bit");
    }

    #[allow(clippy::too_many_arguments)]
    fn single_block_chain(
        reference_name: &str,
        reference_size: u32,
        query_name: &str,
        query_size: u32,
        query_strand: Strand,
        query_start: u32,
        query_end: u32,
        size: u32,
    ) -> OwnedChain {
        OwnedChain {
            score: 0,
            reference_name: reference_name.as_bytes().to_vec(),
            reference_size,
            reference_strand: Strand::Plus,
            reference_start: 0,
            reference_end: size,
            query_name: query_name.as_bytes().to_vec(),
            query_size,
            query_strand,
            query_start,
            query_end,
            id: 1,
            blocks: vec![Block {
                size,
                gap_reference: 0,
                gap_query: 0,
            }],
        }
    }

    fn scorer(reference: &Path, query: &Path) -> ChainScorer {
        ChainScorer::new(
            reference,
            query,
            ScoreMatrix::default_dna(),
            GapCalc::default_costs(),
        )
        .expect("build scorer")
    }

    #[test]
    fn score_ungapped_slices_scores_query_against_reference() {
        let compact = ScoreMatrix::default_dna().compact();
        assert_eq!(score_ungapped_slices(b"CN", b"AC", &compact), -114);
    }

    #[test]
    fn score_absolute_block_scores_full_sequence_slices() {
        let compact = ScoreMatrix::default_dna().compact();
        let block = AbsoluteBlock {
            reference_start: 2,
            reference_end: 6,
            query_start: 2,
            query_end: 6,
        };

        let score =
            score_absolute_block(block, b"GGACGTCC", b"TTACGTAA", &compact).expect("score block");
        assert_eq!(score, 382);
    }

    #[test]
    fn score_absolute_blocks_subtracts_query_gap_cost() {
        let compact = ScoreMatrix::default_dna().compact();
        let gap = GapCalc::default_costs();
        let blocks = [
            AbsoluteBlock {
                reference_start: 0,
                reference_end: 4,
                query_start: 0,
                query_end: 4,
            },
            AbsoluteBlock {
                reference_start: 4,
                reference_end: 8,
                query_start: 6,
                query_end: 10,
            },
        ];

        let score = score_absolute_blocks(&blocks, b"ACGTNNACGT", b"ACGTACGT", &compact, &gap)
            .expect("score absolute blocks");
        assert_eq!(score, 404);
    }

    #[test]
    fn score_absolute_blocks_rejects_overlap() {
        let compact = ScoreMatrix::default_dna().compact();
        let gap = GapCalc::default_costs();
        let blocks = [
            AbsoluteBlock {
                reference_start: 0,
                reference_end: 4,
                query_start: 0,
                query_end: 4,
            },
            AbsoluteBlock {
                reference_start: 3,
                reference_end: 7,
                query_start: 4,
                query_end: 8,
            },
        ];

        let err =
            score_absolute_blocks(&blocks, b"ACGTACGT", b"ACGTACGT", &compact, &gap).unwrap_err();
        assert!(err.to_string().contains("reference coordinates overlap"));
    }

    #[test]
    fn single_block_acgt_plus_plus() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGT\n");
        write_twobit(&query, ">chr1\nACGT\n");

        let scorer = scorer(&reference, &query);
        let mut cache = SequenceCache::default();
        let chain = single_block_chain("chr1", 4, "chr1", 4, Strand::Plus, 0, 4, 4);
        // 91 (A/A) + 100 (C/C) + 100 (G/G) + 91 (T/T) = 382.
        assert_eq!(scorer.score_chain(&mut cache, &chain).unwrap(), 382);
    }

    #[test]
    fn single_block_eight_bases() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTACGT\n");

        let scorer = scorer(&reference, &query);
        let mut cache = SequenceCache::default();
        let chain = single_block_chain("chr1", 8, "chr1", 8, Strand::Plus, 0, 8, 8);
        // 2*(91 + 100 + 100 + 91) = 764.
        assert_eq!(scorer.score_chain(&mut cache, &chain).unwrap(), 764);
    }

    #[test]
    fn mismatch_and_degenerate_contributions() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        // target A C, query C N.
        write_twobit(&reference, ">chr1\nAC\n");
        write_twobit(&query, ">chr1\nCN\n");

        let scorer = scorer(&reference, &query);
        let mut cache = SequenceCache::default();
        let chain = single_block_chain("chr1", 2, "chr1", 2, Strand::Plus, 0, 2, 2);
        // query C vs target A = -114; query N vs target C = 0.
        assert_eq!(scorer.score_chain(&mut cache, &chain).unwrap(), -114);
    }

    #[test]
    fn two_blocks_with_query_gap() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        // Target: ACGT ACGT (8 bases, no gap). Query: ACGT NN ACGT (query gap 2).
        write_twobit(&reference, ">chr1\nACGTACGT\n");
        write_twobit(&query, ">chr1\nACGTNNACGT\n");

        let scorer = scorer(&reference, &query);
        let mut cache = SequenceCache::default();
        let chain = OwnedChain {
            score: 0,
            reference_name: b"chr1".to_vec(),
            reference_size: 8,
            reference_strand: Strand::Plus,
            reference_start: 0,
            reference_end: 8,
            query_name: b"chr1".to_vec(),
            query_size: 10,
            query_strand: Strand::Plus,
            query_start: 0,
            query_end: 10,
            id: 1,
            blocks: vec![
                Block {
                    size: 4,
                    gap_reference: 0,
                    gap_query: 2,
                },
                Block {
                    size: 4,
                    gap_reference: 0,
                    gap_query: 0,
                },
            ],
        };
        // 382 + 382 - gapCalcCost(dq=2, dt=0) = 382 + 382 - 360 = 404.
        assert_eq!(scorer.score_chain(&mut cache, &chain).unwrap(), 404);
    }

    #[test]
    fn minus_strand_query_matches_plus_equivalent() {
        let temp = TempDir::new();
        let reference = temp.path().join("t.2bit");
        let query = temp.path().join("q.2bit");
        // Reference AGTC. Query forward TTGACTAA; reverse-complement of [2,6)
        // ("GACT" -> RC "AGTC") aligns to the reference. (Mirrors the
        // antirepeat minus-strand fixture.)
        write_twobit(&reference, ">chr1\nAGTC\n");
        write_twobit(&query, ">chr1\nTTGACTAA\n");

        let scorer = scorer(&reference, &query);
        let mut cache = SequenceCache::default();
        let chain = single_block_chain("chr1", 4, "chr1", 8, Strand::Minus, 2, 6, 4);
        // RC of query[2..6] = "GACT" -> "AGTC" == reference -> all matches.
        // A/A=91 + G/G=100 + T/T=91 + C/C=100 = 382.
        assert_eq!(scorer.score_chain(&mut cache, &chain).unwrap(), 382);
    }
}
