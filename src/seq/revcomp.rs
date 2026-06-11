// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Reverse-complement utilities for DNA sequence bytes.

const COMPLEMENT: [u8; 256] = build_complement_table();

/// Returns the DNA complement for a single byte.
///
/// A/C/G/T, N, and common IUPAC ambiguity codes are complemented while
/// preserving case. Unknown bytes are left unchanged.
#[inline]
pub fn complement_base(base: u8) -> u8 {
    COMPLEMENT[base as usize]
}

/// Reverse-complements a sequence in place.
pub fn reverse_complement_in_place(sequence: &mut [u8]) {
    let len = sequence.len();
    for i in 0..(len / 2) {
        let j = len - 1 - i;
        let left = complement_base(sequence[i]);
        let right = complement_base(sequence[j]);
        sequence[i] = right;
        sequence[j] = left;
    }
    if len % 2 == 1 {
        let mid = len / 2;
        sequence[mid] = complement_base(sequence[mid]);
    }
}

/// Returns a reverse-complemented copy of a sequence.
pub fn reverse_complement(sequence: &[u8]) -> Vec<u8> {
    let mut reversed = sequence.to_vec();
    reverse_complement_in_place(&mut reversed);
    reversed
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

    table[b'R' as usize] = b'Y';
    table[b'r' as usize] = b'y';
    table[b'Y' as usize] = b'R';
    table[b'y' as usize] = b'r';
    table[b'S' as usize] = b'S';
    table[b's' as usize] = b's';
    table[b'W' as usize] = b'W';
    table[b'w' as usize] = b'w';
    table[b'K' as usize] = b'M';
    table[b'k' as usize] = b'm';
    table[b'M' as usize] = b'K';
    table[b'm' as usize] = b'k';
    table[b'B' as usize] = b'V';
    table[b'b' as usize] = b'v';
    table[b'V' as usize] = b'B';
    table[b'v' as usize] = b'b';
    table[b'D' as usize] = b'H';
    table[b'd' as usize] = b'h';
    table[b'H' as usize] = b'D';
    table[b'h' as usize] = b'd';

    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_complement_preserves_soft_masking() {
        let mut seq = b"AcgTNn".to_vec();
        reverse_complement_in_place(&mut seq);
        assert_eq!(&seq, b"nNAcgT");
    }

    #[test]
    fn reverse_complement_returns_copy() {
        let seq = b"ACGT";
        assert_eq!(reverse_complement(seq), b"ACGT");
        assert_eq!(seq, b"ACGT");
    }

    #[test]
    fn complements_iupac_ambiguity_codes() {
        assert_eq!(
            reverse_complement(b"RYSWKMBDHVryswkmbdhv"),
            b"bdhvkmwsryBDHVKMWSRY"
        );
    }

    #[test]
    fn unknown_bytes_are_preserved() {
        assert_eq!(reverse_complement(b"A.-Z"), b"Z-.T");
    }
}
