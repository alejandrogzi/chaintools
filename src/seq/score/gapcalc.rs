// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Piecewise-linear gap cost model, a faithful re-implementation of UCSC
//! `gapCalc.c` (pinned kent commit `f1f04f7`).
//!
//! The model is read from a textual specification (`tableSize`, `smallSize`,
//! `position`, `qGap`, `tGap`, `bothGap`) or one of the built-in named tables
//! (`loose`, `medium`/`original`, `cheap`, `rnaDna`). Costs are integer-valued
//! and computed with the exact truncation/clamping order kent uses so that
//! [`GapCalc::cost`] reproduces `gapCalcCost` byte-for-byte.

use crate::model::error::ChainError;

/// Clamp value returned by kent when an extrapolated cost goes negative.
const BIGNUM: i32 = 0x3fff_ffff; // 1_073_741_823

/// Default ("loose") gap costs — chicken/human distance. Matches kent
/// `defaultGapCosts`. This is the table used when no `-linearGap` is given.
const DEFAULT_GAP_COSTS: &str = "tablesize 11
smallSize 111
position 1 2 3 11 111 2111 12111 32111 72111 152111 252111
qGap 325 360 400 450 600 1100 3600 7600 15600 31600 56600
tGap 325 360 400 450 600 1100 3600 7600 15600 31600 56600
bothGap 625 660 700 750 900 1400 4000 8000 16000 32000 57000
";

/// "medium"/"original" gap costs — mouse/human distance. Matches kent
/// `originalGapCosts`.
const ORIGINAL_GAP_COSTS: &str = "tableSize 11
smallSize 111
position 1 2 3 11 111 2111 12111 32111 72111 152111 252111
qGap 350 425 450 600 900 2900 22900 57900 117900 217900 317900
tGap 350 425 450 600 900 2900 22900 57900 117900 217900 317900
bothGap 750 825 850 1000 1300 3300 23300 58300 118300 218300 318300
";

/// "cheap" gap costs. Matches kent `cheapGapCosts`.
const CHEAP_GAP_COSTS: &str = "tableSize 3
smallSize 100
position 1 100 1000
qGap 0 30 300
tGap 0 30 300
bothGap 0 30 300
";

/// "rnaDna" gap costs (asymmetric q/t). Matches kent `rnaDnaGapCosts`.
const RNA_DNA_GAP_COSTS: &str = "tablesize 12
smallSize 111
position 1 2 3 11 31 111 2111 12111 32111 72111 152111 252111
qGap 325 360 400 450 600 800 1100 3600 7600 15600 31600 56600
tGap 200 210 220 250 300 400 500 600 800 1200 2000 4000
bothGap 625 660 700 750 900 1100 1400 4000 8000 16000 32000 57000
";

/// Piecewise-linear gap cost table, equivalent to kent's `struct gapCalc`.
///
/// Small gaps (`< small_size`) are answered from a dense precomputed table;
/// larger gaps interpolate over the "long" knots and extrapolate past the last
/// knot with a fixed slope. Query-only, target-only, and both-sides gaps each
/// have their own value sets, so asymmetric tables (e.g. `rnaDna`) are honored.
#[derive(Debug, Clone)]
pub struct GapCalc {
    small_size: i32,
    q_small: Vec<i32>,
    t_small: Vec<i32>,
    b_small: Vec<i32>,
    long_pos: Vec<i32>,
    q_long: Vec<f64>,
    t_long: Vec<f64>,
    b_long: Vec<f64>,
    q_last_pos: i32,
    t_last_pos: i32,
    b_last_pos: i32,
    q_last_val: f64,
    t_last_val: f64,
    b_last_val: f64,
    q_last_slope: f64,
    t_last_slope: f64,
    b_last_slope: f64,
}

impl GapCalc {
    /// Returns the default ("loose") gap cost table.
    ///
    /// This is the table kent uses for `chainScore` when `-linearGap` is not
    /// supplied. The underlying specification is a compile-time constant known
    /// to be valid, so this never fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::seq::score::gapcalc::GapCalc;
    ///
    /// let gap = GapCalc::default_costs();
    /// assert_eq!(gap.cost(1, 0), 325);
    /// ```
    pub fn default_costs() -> GapCalc {
        Self::from_spec_str(DEFAULT_GAP_COSTS).expect("built-in default gap costs are valid")
    }

    /// Resolves a `-linearGap` argument into a [`GapCalc`].
    ///
    /// Recognizes the built-in aliases `loose`, `medium`/`original`, `cheap`,
    /// and `rnaDna`; anything else is treated as a path to a gap-spec file.
    /// kent's CLI alias path only reaches `loose` and `medium`; the remaining
    /// named tables are provided for completeness.
    ///
    /// # Arguments
    ///
    /// * `arg` - Alias name or path to a gap specification file
    ///
    /// # Output
    ///
    /// Returns `Ok(GapCalc)` or `Err(ChainError)` if a file cannot be read or
    /// parsed.
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::seq::score::gapcalc::GapCalc;
    ///
    /// let loose = GapCalc::from_linear_gap("loose").unwrap();
    /// assert_eq!(loose.cost(1, 1), 660);
    /// ```
    pub fn from_linear_gap(arg: &str) -> Result<GapCalc, ChainError> {
        match arg {
            "loose" => Self::from_spec_str(DEFAULT_GAP_COSTS),
            "medium" | "original" => Self::from_spec_str(ORIGINAL_GAP_COSTS),
            "cheap" => Self::from_spec_str(CHEAP_GAP_COSTS),
            "rnaDna" => Self::from_spec_str(RNA_DNA_GAP_COSTS),
            path => {
                let contents = std::fs::read_to_string(path).map_err(|err| {
                    gap_error(format!("cannot read linearGap file {path}: {err}"))
                })?;
                Self::from_spec_str(&contents)
            }
        }
    }

    /// Parses a gap cost specification string.
    ///
    /// Accepts the kent gap-spec grammar: lines tagged (case-insensitively)
    /// `tableSize`, `smallSize`, `position`, `qGap`, `tGap`, `bothGap`, with
    /// whitespace-separated values. Blank lines and `#` comment lines are
    /// ignored. The small and long tables are then built exactly as
    /// `gapCalcRead` does.
    ///
    /// # Arguments
    ///
    /// * `spec` - The gap specification text
    ///
    /// # Output
    ///
    /// Returns `Ok(GapCalc)` or `Err(ChainError)` on a malformed specification.
    pub fn from_spec_str(spec: &str) -> Result<GapCalc, ChainError> {
        let mut table_size: Option<usize> = None;
        let mut small_size: Option<i32> = None;
        let mut position: Option<Vec<i32>> = None;
        let mut q_gap: Option<Vec<f64>> = None;
        let mut t_gap: Option<Vec<f64>> = None;
        let mut both_gap: Option<Vec<f64>> = None;

        for raw in spec.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut words = line.split_whitespace();
            let tag = words.next().unwrap_or("");
            let rest: Vec<&str> = words.collect();
            match tag.to_ascii_lowercase().as_str() {
                "tablesize" => table_size = Some(parse_one_usize(&rest, "tableSize")?),
                "smallsize" => small_size = Some(parse_one_i32(&rest, "smallSize")?),
                "position" => position = Some(parse_i32_list(&rest, "position")?),
                "qgap" => q_gap = Some(parse_f64_list(&rest, "qGap")?),
                "tgap" => t_gap = Some(parse_f64_list(&rest, "tGap")?),
                "bothgap" => both_gap = Some(parse_f64_list(&rest, "bothGap")?),
                other => {
                    return Err(gap_error(format!(
                        "unknown gap specification tag '{other}'"
                    )));
                }
            }
        }

        let small_size =
            small_size.ok_or_else(|| gap_error("gap specification missing smallSize"))?;
        let position = position.ok_or_else(|| gap_error("gap specification missing position"))?;
        let q_gap = q_gap.ok_or_else(|| gap_error("gap specification missing qGap"))?;
        let t_gap = t_gap.ok_or_else(|| gap_error("gap specification missing tGap"))?;
        let both_gap = both_gap.ok_or_else(|| gap_error("gap specification missing bothGap"))?;

        let n = position.len();
        if let Some(declared) = table_size
            && declared != n
        {
            return Err(gap_error(format!(
                "tableSize {declared} does not match {n} position values"
            )));
        }
        if q_gap.len() != n || t_gap.len() != n || both_gap.len() != n {
            return Err(gap_error(
                "position/qGap/tGap/bothGap must all have the same length",
            ));
        }
        if n < 2 {
            return Err(gap_error("gap specification needs at least two knots"));
        }
        if small_size < 1 {
            return Err(gap_error("smallSize must be positive"));
        }

        Self::build(small_size, position, q_gap, t_gap, both_gap)
    }

    /// Builds the dense small table and the long extrapolation parameters.
    fn build(
        small_size: i32,
        position: Vec<i32>,
        q_gap: Vec<f64>,
        t_gap: Vec<f64>,
        both_gap: Vec<f64>,
    ) -> Result<GapCalc, ChainError> {
        let small_len = small_size as usize;
        let mut q_small = vec![0i32; small_len];
        let mut t_small = vec![0i32; small_len];
        let mut b_small = vec![0i32; small_len];
        for i in 1..small_size {
            let idx = i as usize;
            q_small[idx] = interpolate(i, &position, &q_gap);
            t_small[idx] = interpolate(i, &position, &t_gap);
            b_small[idx] = interpolate(i, &position, &both_gap);
        }

        // Long tables begin at the knot whose position equals smallSize.
        let start = position
            .iter()
            .position(|&p| p == small_size)
            .ok_or_else(|| gap_error("smallSize must appear in the position list"))?;
        let long_pos: Vec<i32> = position[start..].to_vec();
        let q_long: Vec<f64> = q_gap[start..].to_vec();
        let t_long: Vec<f64> = t_gap[start..].to_vec();
        let b_long: Vec<f64> = both_gap[start..].to_vec();
        let m = long_pos.len();
        if m < 2 {
            return Err(gap_error(
                "gap specification needs at least two long knots past smallSize",
            ));
        }

        let last = m - 1;
        let span = (long_pos[last] - long_pos[last - 1]) as f64;
        let q_last_slope = (q_long[last] - q_long[last - 1]) / span;
        let t_last_slope = (t_long[last] - t_long[last - 1]) / span;
        let b_last_slope = (b_long[last] - b_long[last - 1]) / span;

        Ok(GapCalc {
            small_size,
            q_small,
            t_small,
            b_small,
            q_last_pos: long_pos[last],
            t_last_pos: long_pos[last],
            b_last_pos: long_pos[last],
            q_last_val: q_long[last],
            t_last_val: t_long[last],
            b_last_val: b_long[last],
            q_last_slope,
            t_last_slope,
            b_last_slope,
            long_pos,
            q_long,
            t_long,
            b_long,
        })
    }

    /// Computes the gap cost for a query gap `dq` and target gap `dt`.
    ///
    /// This mirrors kent `gapCalcCost` exactly: negative gaps are clamped to
    /// zero, query-only / target-only / both-sided gaps select different value
    /// sets, small gaps use the dense table, and gaps past the last knot
    /// extrapolate with a fixed slope (clamped to `BIGNUM` on negative
    /// overflow). The result is integer-valued.
    ///
    /// # Arguments
    ///
    /// * `dq` - Gap on the query side (`block.gap_query`)
    /// * `dt` - Gap on the target/reference side (`block.gap_reference`)
    ///
    /// # Output
    ///
    /// Returns the gap cost as an `i32`.
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::seq::score::gapcalc::GapCalc;
    ///
    /// let gap = GapCalc::default_costs();
    /// assert_eq!(gap.cost(2, 0), 360);
    /// assert_eq!(gap.cost(1000, 1000), 1372);
    /// ```
    #[inline]
    pub fn cost(&self, dq: i32, dt: i32) -> i32 {
        let dt = dt.max(0);
        let dq = dq.max(0);
        if dt == 0 {
            if dq < self.small_size {
                self.q_small[dq as usize]
            } else if dq >= self.q_last_pos {
                let cost =
                    (self.q_last_val + self.q_last_slope * f64::from(dq - self.q_last_pos)) as i32;
                if cost < 0 { BIGNUM } else { cost }
            } else {
                interpolate(dq, &self.long_pos, &self.q_long)
            }
        } else if dq == 0 {
            if dt < self.small_size {
                self.t_small[dt as usize]
            } else if dt >= self.t_last_pos {
                let cost =
                    (self.t_last_val + self.t_last_slope * f64::from(dt - self.t_last_pos)) as i32;
                if cost < 0 { BIGNUM } else { cost }
            } else {
                interpolate(dt, &self.long_pos, &self.t_long)
            }
        } else {
            let both = dq.wrapping_add(dt);
            if both < self.small_size {
                self.b_small[both as usize]
            } else if both >= self.b_last_pos {
                let cost = (self.b_last_val + self.b_last_slope * f64::from(both - self.b_last_pos))
                    as i32;
                if cost < 0 { BIGNUM } else { cost }
            } else {
                interpolate(both, &self.long_pos, &self.b_long)
            }
        }
    }
}

/// Linear interpolation over known points, truncated to `i32`.
///
/// Mirrors kent's `interpolate`: returns `v[i]` on an exact hit, linearly
/// interpolates between the bracketing knots, and extrapolates from the last
/// two knots when `x` is past the end. The double result is truncated toward
/// zero (C `int` cast), matching kent's truncation order.
fn interpolate(x: i32, s: &[i32], v: &[f64]) -> i32 {
    let n = s.len();
    for i in 0..n {
        let ss = s[i];
        if x == ss {
            return v[i] as i32;
        } else if x < ss {
            if i == 0 {
                // Unreachable for valid kent tables (x >= position[0]); kent
                // would read s[-1] here, so fall back to the first value.
                return v[0] as i32;
            }
            let ds = f64::from(ss - s[i - 1]);
            let dv = v[i] - v[i - 1];
            return (v[i - 1] + dv * f64::from(x - s[i - 1]) / ds) as i32;
        }
    }
    let i = n - 1;
    let ds = f64::from(s[i] - s[i - 1]);
    let dv = v[i] - v[i - 1];
    (v[i - 1] + dv * f64::from(x - s[i - 1]) / ds) as i32
}

/// Parses a single non-negative `usize` from a token list.
fn parse_one_usize(rest: &[&str], tag: &str) -> Result<usize, ChainError> {
    let first = rest
        .first()
        .ok_or_else(|| gap_error(format!("{tag} is missing a value")))?;
    first.parse::<usize>().map_err(|_| {
        gap_error(format!(
            "{tag} value '{first}' is not a non-negative integer"
        ))
    })
}

/// Parses a single `i32` from a token list.
fn parse_one_i32(rest: &[&str], tag: &str) -> Result<i32, ChainError> {
    let first = rest
        .first()
        .ok_or_else(|| gap_error(format!("{tag} is missing a value")))?;
    first
        .parse::<i32>()
        .map_err(|_| gap_error(format!("{tag} value '{first}' is not an integer")))
}

/// Parses a whitespace-separated list of `i32` values.
fn parse_i32_list(rest: &[&str], tag: &str) -> Result<Vec<i32>, ChainError> {
    if rest.is_empty() {
        return Err(gap_error(format!("{tag} has no values")));
    }
    rest.iter()
        .map(|word| {
            word.parse::<i32>()
                .map_err(|_| gap_error(format!("{tag} value '{word}' is not an integer")))
        })
        .collect()
}

/// Parses a whitespace-separated list of `f64` values.
fn parse_f64_list(rest: &[&str], tag: &str) -> Result<Vec<f64>, ChainError> {
    if rest.is_empty() {
        return Err(gap_error(format!("{tag} has no values")));
    }
    rest.iter()
        .map(|word| {
            word.parse::<f64>()
                .map_err(|_| gap_error(format!("{tag} value '{word}' is not a number")))
        })
        .collect()
}

/// Creates an unsupported `ChainError` with a custom message.
fn gap_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_acceptance_vectors() {
        let g = GapCalc::default_costs();
        // (dq, dt, expected) — verbatim from the spec's §1.3 acceptance table.
        let cases = [
            (0, 0, 0),
            (1, 0, 325),
            (0, 1, 325),
            (1, 1, 660),
            (5, 0, 412),
            (2, 3, 712),
            (50, 60, 898),
            (110, 0, 598),
            (111, 0, 600),
            (0, 111, 600),
            (200, 0, 622),
            (2111, 0, 1100),
            (1000, 1000, 1372),
            (252111, 0, 56600),
            (300000, 0, 68572),
            (150000, 150000, 68972),
        ];
        for (dq, dt, expected) in cases {
            assert_eq!(g.cost(dq, dt), expected, "cost({dq}, {dt})");
        }
    }

    #[test]
    fn small_table_spot_values() {
        let g = GapCalc::default_costs();
        let q_expected = [
            (1, 325),
            (2, 360),
            (3, 400),
            (5, 412),
            (10, 443),
            (50, 508),
            (110, 598),
        ];
        for (dq, expected) in q_expected {
            assert_eq!(g.cost(dq, 0), expected, "qSmall[{dq}]");
        }
        let b_expected = [(2, 660), (5, 712), (110, 898)];
        for (both, expected) in b_expected {
            // both-sided gap with dq + dt == both, both sides nonzero.
            assert_eq!(g.cost(1, both - 1), expected, "bSmall[{both}]");
        }
    }

    #[test]
    fn last_slopes_are_quarter() {
        let g = GapCalc::default_costs();
        assert!((g.q_last_slope - 0.25).abs() < 1e-12);
        assert!((g.t_last_slope - 0.25).abs() < 1e-12);
        assert!((g.b_last_slope - 0.25).abs() < 1e-12);
    }

    #[test]
    fn negative_gaps_are_clamped_to_zero() {
        let g = GapCalc::default_costs();
        assert_eq!(g.cost(-5, -5), 0);
        assert_eq!(g.cost(-5, 0), 0);
        assert_eq!(g.cost(0, -5), 0);
        // dq negative -> 0, dt = 1 -> target-only branch.
        assert_eq!(g.cost(-5, 1), 325);
    }

    #[test]
    fn medium_alias_parses() {
        let g = GapCalc::from_linear_gap("medium").expect("medium parses");
        // qGap[0] at position 1 is 350 in the original/medium table.
        assert_eq!(g.cost(1, 0), 350);
        assert_eq!(g.cost(0, 1), 350);
        assert_eq!(g.cost(1, 1), 825); // bothGap[1] at position 2
    }

    #[test]
    fn rna_dna_is_asymmetric() {
        let g = GapCalc::from_linear_gap("rnaDna").expect("rnaDna parses");
        // position 1: qGap 325, tGap 200 -> query-only vs target-only differ.
        assert_eq!(g.cost(1, 0), 325);
        assert_eq!(g.cost(0, 1), 200);
    }

    #[test]
    fn cheap_table_parses() {
        let g = GapCalc::from_linear_gap("cheap").expect("cheap parses");
        assert_eq!(g.cost(1, 0), 0);
        assert_eq!(g.cost(100, 0), 30);
        assert_eq!(g.cost(1000, 0), 300);
    }

    #[test]
    fn unknown_tag_is_rejected() {
        let err = GapCalc::from_spec_str("bogus 1\n").unwrap_err();
        assert!(err.to_string().contains("unknown gap specification tag"));
    }

    #[test]
    fn whitespace_and_case_tolerant() {
        let spec = "  TABLESIZE   3\n\tsmallsize 100\nPOSITION 1 100 1000\nQGAP 0 30 300\nTGAP 0 30 300\nBOTHGAP 0 30 300\n";
        let g = GapCalc::from_spec_str(spec).expect("tolerant parse");
        assert_eq!(g.cost(100, 0), 30);
    }
}
