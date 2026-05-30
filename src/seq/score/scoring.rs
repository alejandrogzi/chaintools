// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! DNA score matrix, a faithful re-implementation of the relevant parts of
//! UCSC `axt.c` (pinned kent commit `f1f04f7`).
//!
//! The canonical representation is a `256 x 256` integer matrix indexed by raw
//! sequence bytes, exactly like kent's `axtScoreScheme.matrix`. Only A/C/G/T
//! (in every case combination) are nonzero; every byte pair involving `N` or
//! any other character scores `0`. The matrix is indexed `matrix[query][target]`
//! (query base selects the row, target base selects the column).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::model::error::ChainError;

/// The four lowercase canonical bases, in kent's `trans` order.
const TRANS: [u8; 4] = [b'a', b'c', b'g', b't'];

/// `256 x 256` integer score matrix indexed by raw sequence byte.
///
/// Built either from kent's default blastz matrix ([`ScoreMatrix::default_dna`])
/// or parsed from a blastz/lastz score-scheme file
/// ([`ScoreMatrix::from_blastz_file`]). Use [`ScoreMatrix::pair`] for a single
/// lookup, or [`ScoreMatrix::compact`] to derive a cache-friendly form for hot
/// loops.
#[derive(Clone)]
pub struct ScoreMatrix {
    m: Box<[[i32; 256]; 256]>,
}

impl ScoreMatrix {
    /// Returns kent's default DNA scoring matrix (`axtScoreSchemeDefault`).
    ///
    /// Sets the 16 lowercase A/C/G/T entries and propagates them to all four
    /// case combinations, leaving everything else (including `N`) at `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::seq::score::scoring::ScoreMatrix;
    ///
    /// let m = ScoreMatrix::default_dna();
    /// assert_eq!(m.pair(b'A', b'A'), 91);
    /// assert_eq!(m.pair(b'a', b'c'), -114);
    /// assert_eq!(m.pair(b'N', b'A'), 0);
    /// ```
    pub fn default_dna() -> ScoreMatrix {
        let mut m = Box::new([[0i32; 256]; 256]);

        // Lower-case core matrix, query (row) x target (column).
        let core: [[i32; 4]; 4] = [
            [91, -114, -31, -123],
            [-114, 100, -125, -31],
            [-31, -125, 100, -114],
            [-123, -31, -114, 91],
        ];
        for (i, row) in core.iter().enumerate() {
            for (j, &value) in row.iter().enumerate() {
                m[TRANS[i] as usize][TRANS[j] as usize] = value;
            }
        }

        propagate_case(&mut m);
        ScoreMatrix { m }
    }

    /// Reads a blastz/lastz score-scheme file (`-scoreScheme`).
    ///
    /// Opens `path` and delegates to [`ScoreMatrix::from_lf`].
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the score-scheme file
    ///
    /// # Output
    ///
    /// Returns `Ok(ScoreMatrix)` or `Err(ChainError)` on I/O or parse failure.
    pub fn from_blastz_file<P: AsRef<Path>>(path: P) -> Result<ScoreMatrix, ChainError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|err| {
            scoring_error(format!(
                "cannot open score scheme {}: {err}",
                path.display()
            ))
        })?;
        Self::from_lf(BufReader::new(file))
    }

    /// Parses a blastz/lastz score-scheme from a reader (`axtScoreSchemeReadLf`).
    ///
    /// The grammar is:
    ///
    /// ```text
    ///   A    C    G    T
    ///   91 -114  -31 -123
    /// -114  100 -125  -31
    ///  -31 -125  100 -114
    /// -123  -31 -114   91
    /// O = 400, E = 30
    /// ```
    ///
    /// Leading `tag = value` settings lines are skipped, the header row
    /// (first characters `A C G T`) is located, the following four rows are
    /// read (each 4 values, or 5 with a leading row label that is skipped),
    /// and an optional trailing `O =`/`E =` line is consumed. Values are keyed
    /// to lowercase A/C/G/T in kent's fixed `trans` order — the header labels'
    /// text beyond the first character is not used — then case is propagated.
    ///
    /// # Arguments
    ///
    /// * `reader` - Buffered reader over the score-scheme text
    ///
    /// # Output
    ///
    /// Returns `Ok(ScoreMatrix)` or `Err(ChainError)` on a malformed scheme.
    pub fn from_lf<R: BufRead>(reader: R) -> Result<ScoreMatrix, ChainError> {
        let mut lines = reader.lines();
        let mut m = Box::new([[0i32; 256]; 256]);

        loop {
            let row = match next_words(&mut lines)? {
                Some(words) => words,
                None => return Err(scoring_error("score scheme is empty or has no matrix")),
            };

            // A `tag = value` settings line has '=' in the first or second word.
            let is_setting = row[0].contains('=') || (row.len() > 1 && row[1].contains('='));
            if is_setting {
                continue;
            }

            // Otherwise this must be the header row of base letters.
            if row.len() < 4
                || !row[0].starts_with('A')
                || !row[1].starts_with('C')
                || !row[2].starts_with('G')
                || !row[3].starts_with('T')
            {
                return Err(scoring_error(
                    "score scheme does not look like a matrix (expected A C G T header)",
                ));
            }

            for i in 0..4 {
                let data = next_words(&mut lines)?
                    .ok_or_else(|| scoring_error("score scheme matrix has fewer than four rows"))?;
                let start = if data.len() == 5 { 1 } else { 0 };
                if data.len() < start + 4 {
                    return Err(scoring_error(format!(
                        "score scheme matrix row {} has too few values",
                        i + 1
                    )));
                }
                for j in 0..4 {
                    let word = &data[start + j];
                    let value = word.parse::<i32>().map_err(|_| {
                        scoring_error(format!("score scheme value '{word}' is not an integer"))
                    })?;
                    m[TRANS[i] as usize][TRANS[j] as usize] = value;
                }
            }
            // A trailing O=/E= line may follow; it is parsed only for parity
            // and does not affect scoring, so we read past it if present.
            let _ = lines.next();
            break;
        }

        propagate_case(&mut m);
        Ok(ScoreMatrix { m })
    }

    /// Returns the score for a (query, target) byte pair.
    ///
    /// # Arguments
    ///
    /// * `q` - Query base byte (row index)
    /// * `t` - Target base byte (column index)
    ///
    /// # Output
    ///
    /// Returns the matrix entry (`0` for any non-ACGT byte on either axis).
    #[inline]
    pub fn pair(&self, q: u8, t: u8) -> i32 {
        self.m[q as usize][t as usize]
    }

    /// Derives a compact, cache-friendly representation for hot loops.
    ///
    /// Produces a 256-entry byte→code LUT (A/a→0, C/c→1, G/g→2, T/t→3,
    /// everything else→4) plus a `5 x 5` value table. By construction this is
    /// exactly equivalent to the full matrix: every ACGT pair carries the same
    /// value, and every non-ACGT pair is `0` in both forms.
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::seq::score::scoring::ScoreMatrix;
    ///
    /// let m = ScoreMatrix::default_dna();
    /// let c = m.compact();
    /// assert_eq!(c.pair(b'A', b'A'), m.pair(b'A', b'A'));
    /// ```
    pub fn compact(&self) -> CompactMatrix {
        let mut code = [4u8; 256];
        for (value, &(lower, upper)) in [(b'a', b'A'), (b'c', b'C'), (b'g', b'G'), (b't', b'T')]
            .iter()
            .enumerate()
        {
            code[lower as usize] = value as u8;
            code[upper as usize] = value as u8;
        }

        let mut small = [[0i32; 5]; 5];
        for i in 0..4 {
            for j in 0..4 {
                small[i][j] = self.m[TRANS[i] as usize][TRANS[j] as usize];
            }
        }
        CompactMatrix { code, small }
    }
}

impl std::fmt::Debug for ScoreMatrix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The full 256x256 array is huge; summarize with the ACGT core.
        let core: Vec<[i32; 4]> = TRANS
            .iter()
            .map(|&q| {
                let mut row = [0i32; 4];
                for (j, &t) in TRANS.iter().enumerate() {
                    row[j] = self.m[q as usize][t as usize];
                }
                row
            })
            .collect();
        f.debug_struct("ScoreMatrix").field("acgt", &core).finish()
    }
}

/// Compact score lookup: byte→code LUT plus a `5 x 5` value table.
///
/// Row/column index 4 (any non-ACGT byte) is always `0`, so non-ACGT pairs
/// score `0` without a branch. Derived from a [`ScoreMatrix`] via
/// [`ScoreMatrix::compact`].
#[derive(Debug, Clone)]
pub struct CompactMatrix {
    code: [u8; 256],
    small: [[i32; 5]; 5],
}

impl CompactMatrix {
    /// Returns the score for a (query, target) byte pair.
    #[inline]
    pub fn pair(&self, q: u8, t: u8) -> i32 {
        self.small[self.code[q as usize] as usize][self.code[t as usize] as usize]
    }
}

/// Propagates the lowercase ACGT block to every case combination.
///
/// Mirrors kent's `propagateCase`: for each pair of bases, the lowercase value
/// is copied to the upper/upper, upper/lower, and lower/upper combinations.
fn propagate_case(m: &mut [[i32; 256]; 256]) {
    let two_case: [[u8; 4]; 2] = [[b'a', b'c', b'g', b't'], [b'A', b'C', b'G', b'T']];
    for i1 in 0..2 {
        for i2 in 0..2 {
            if i1 == 0 && i2 == 0 {
                continue;
            }
            for j1 in 0..4 {
                for j2 in 0..4 {
                    let value = m[two_case[0][j1] as usize][two_case[0][j2] as usize];
                    m[two_case[i1][j1] as usize][two_case[i2][j2] as usize] = value;
                }
            }
        }
    }
}

/// Returns the next non-blank line split into whitespace-separated words.
///
/// Returns `Ok(None)` at EOF. Mirrors kent's `lineFileChopNext`, which skips
/// blank lines.
fn next_words<R: BufRead>(
    lines: &mut std::io::Lines<R>,
) -> Result<Option<Vec<String>>, ChainError> {
    for line in lines.by_ref() {
        let line = line?;
        let words: Vec<String> = line.split_whitespace().map(str::to_owned).collect();
        if words.is_empty() {
            continue;
        }
        return Ok(Some(words));
    }
    Ok(None)
}

/// Creates an unsupported `ChainError` with a custom message.
fn scoring_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn default_matrix_values() {
        let m = ScoreMatrix::default_dna();
        assert_eq!(m.pair(b'a', b'a'), 91);
        assert_eq!(m.pair(b'c', b'c'), 100);
        assert_eq!(m.pair(b'g', b'g'), 100);
        assert_eq!(m.pair(b't', b't'), 91);
        assert_eq!(m.pair(b'a', b'c'), -114);
        assert_eq!(m.pair(b'c', b'g'), -125);
        assert_eq!(m.pair(b'a', b'g'), -31);
        assert_eq!(m.pair(b'a', b't'), -123);
    }

    #[test]
    fn default_matrix_is_case_insensitive() {
        let m = ScoreMatrix::default_dna();
        assert_eq!(m.pair(b'A', b'A'), 91);
        assert_eq!(m.pair(b'A', b'a'), 91);
        assert_eq!(m.pair(b'a', b'A'), 91);
        assert_eq!(m.pair(b'A', b'C'), -114);
        assert_eq!(m.pair(b'a', b'C'), -114);
        assert_eq!(m.pair(b'A', b'c'), -114);
    }

    #[test]
    fn non_acgt_scores_zero() {
        let m = ScoreMatrix::default_dna();
        assert_eq!(m.pair(b'N', b'A'), 0);
        assert_eq!(m.pair(b'A', b'N'), 0);
        assert_eq!(m.pair(b'n', b'a'), 0);
        assert_eq!(m.pair(b'-', b'A'), 0);
        assert_eq!(m.pair(0, 0), 0);
        assert_eq!(m.pair(255, 255), 0);
    }

    #[test]
    fn default_matrix_is_symmetric() {
        let m = ScoreMatrix::default_dna();
        for &q in &TRANS {
            for &t in &TRANS {
                assert_eq!(m.pair(q, t), m.pair(t, q), "asymmetry at {q} {t}");
            }
        }
    }

    #[test]
    fn compact_matches_full_for_all_byte_pairs() {
        let m = ScoreMatrix::default_dna();
        let c = m.compact();
        for q in 0u16..=255 {
            for t in 0u16..=255 {
                assert_eq!(
                    c.pair(q as u8, t as u8),
                    m.pair(q as u8, t as u8),
                    "mismatch at ({q}, {t})"
                );
            }
        }
    }

    #[test]
    fn reads_blastz_with_header_only() {
        let scheme =
            "A C G T\n91 -114 -31 -123\n-114 100 -125 -31\n-31 -125 100 -114\n-123 -31 -114 91\n";
        let m = ScoreMatrix::from_lf(Cursor::new(scheme)).expect("parse scheme");
        assert_eq!(m.pair(b'A', b'A'), 91);
        assert_eq!(m.pair(b'C', b'G'), -125);
        assert_eq!(m.pair(b'N', b'A'), 0);
    }

    #[test]
    fn reads_blastz_with_row_labels_and_gap_line() {
        let scheme = "   A    C    G    T\nA   91 -114  -31 -123\nC -114  100 -125  -31\nG  -31 -125  100 -114\nT -123  -31 -114   91\nO = 400, E = 30\n";
        let m = ScoreMatrix::from_lf(Cursor::new(scheme)).expect("parse scheme");
        assert_eq!(m.pair(b'a', b'a'), 91);
        assert_eq!(m.pair(b't', b't'), 91);
        assert_eq!(m.pair(b'a', b't'), -123);
    }

    #[test]
    fn asymmetric_custom_matrix_honors_orientation() {
        // Deliberately asymmetric: query=A,target=C is 7 but query=C,target=A
        // is 9. This catches a transpose bug that the symmetric default hides.
        let scheme = "A C G T\n1 7 0 0\n9 2 0 0\n0 0 3 0\n0 0 0 4\n";
        let m = ScoreMatrix::from_lf(Cursor::new(scheme)).expect("parse scheme");
        assert_eq!(m.pair(b'A', b'C'), 7); // query A, target C
        assert_eq!(m.pair(b'C', b'A'), 9); // query C, target A
        assert_eq!(m.pair(b'A', b'A'), 1);
        assert_eq!(m.pair(b'T', b'T'), 4);
        // Case propagation keeps the asymmetry.
        assert_eq!(m.pair(b'a', b'c'), 7);
        assert_eq!(m.pair(b'c', b'a'), 9);
        // And the compact form must agree.
        let c = m.compact();
        assert_eq!(c.pair(b'A', b'C'), 7);
        assert_eq!(c.pair(b'C', b'A'), 9);
    }

    #[test]
    fn rejects_non_matrix_input() {
        let err = ScoreMatrix::from_lf(Cursor::new("hello world\n")).unwrap_err();
        assert!(err.to_string().contains("does not look like a matrix"));
    }
}
