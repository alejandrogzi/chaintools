<p align="center">
  <p align="center">
    <img width=200 align="center" src="../logo.png" >
  </p>

  <span>
    <h1 align="center">
        chaintools
    </h1>
  </span>

  <p align="center">
    <a href="https://img.shields.io/badge/version-0.0.10-green" target="_blank">
      <img alt="Version Badge" src="https://img.shields.io/badge/version-0.0.10-green">
    </a>
    <a href="https://crates.io/crates/chaintools" target="_blank">
      <img alt="Crates.io Version" src="https://img.shields.io/crates/v/chaintools">
    </a>
    <a href="https://github.com/alejandrogzi/chaintools" target="_blank">
      <img alt="GitHub License" src="https://img.shields.io/github/license/alejandrogzi/chaintools?color=blue">
    </a>
    <a href="https://crates.io/crates/chaintools" target="_blank">
      <img alt="Crates.io Total Downloads" src="https://img.shields.io/crates/d/chaintools">
    </a>
  </p>
</p>


# chaintools compare

Compare two chain files by the exact base-pair relations they encode, separate
from how those relations are packaged into chains.

## Usage

```bash
chaintools compare --chain-a A.chain --chain-b B.chain
chaintools compare -a A.chain -b B.chain --by-sequence --top 3
```

Both inputs must be paths; there is no stdin mode.

## Options

- `--chain-a <PATH>` / `-a`: first chain file.
- `--chain-b <PATH>` / `-b`: second chain file.
- `--by-sequence`: add the per-target agreement table and top-chain lists.
- `--top <N>`: with `--by-sequence`, show the top `N` scored chains per target.
  Default `3`.
- `--memory-ceiling <GIB>` / `-M`: fail when the canonical-mapping estimate exceeds this many GiB. Default `16`.

## What is compared

Every aligned block is canonicalized into the base-to-base relation it
represents: a `(target, query, strand, diagonal)` key plus a target interval.
Within a key, overlapping and adjacent intervals are merged and duplicates are
removed. The result is independent of chain IDs, chain order, chain splitting,
and block splitting along the same mapping diagonal.

This means a mapping split across two chains compares equal to the same mapping
in one chain — but the two files' *continuity* differs, and `compare` reports
both facts independently. Chain counts, header overlap, and approximate
coordinate matching are never used as equivalence tests.

## Output

- **MAPPING AGREEMENT** — unique pair bp in each file, shared bp, A-only and
  B-only bp, `A_retained_by_B = shared / A`, `B_retained_by_A = shared / B`,
  pair Dice and pair Jaccard. The directional values are not called
  precision/recall: neither file is ground truth.
- **COVERAGE / AMBIGUITY** — unique target and query bp covered by the
  mappings, and bp mapped more than once on each side. This distinguishes
  genuinely broader coverage from redundant multi-mapping.
- **CONTINUITY** — chains, blocks, chain aligned-bp N50/L50, largest chain,
  median blocks per chain, and chromosome fragmentation (median target top-1
  and top-3 share, median/max chains→90%), reusing the `stats` metrics.
- **GAPS** — target/query gap bp, gap-event counts, and largest gaps as A/B
  rows with absolute and percent delta. Fewer gaps is not automatically
  reported as better.
- **INTERPRETATION** — one or two lines drawn only from the measured
  dimensions: mappings identical (possibly with a fragmentation difference),
  or differing (Dice quoted, no winner declared); broader coverage with no
  additional multi-mapping if that holds. There is no combined quality score.
- **BY TARGET** (with `--by-sequence`) — per-target continuity (chains,
  top-1/top-3 share, N50, chains→90%) alongside per-target A-only/B-only/
  shared bp and Dice, so divergences can be localized. Followed by the top
  `N` scored chains of each file per target.

## Example: identical mappings, different fragmentation

A holds one 24-bp chain; B holds the same two 12-bp mappings as two chains:

```bash
$ chaintools compare -a frag_a.chain -b frag_b.chain
MAPPING AGREEMENT
A_unique_pair_bp	24
B_unique_pair_bp	24
shared_pair_bp	24
A_only_pair_bp	0
B_only_pair_bp	0
A_retained_by_B	100.00%
B_retained_by_A	100.00%
pair_Dice	100.00%
pair_Jaccard	100.00%

COVERAGE / AMBIGUITY
metric	A	B
unique_target_covered_bp	24	24
unique_query_covered_bp	24	24
target_bp_mapped_more_than_once	0	0
query_bp_mapped_more_than_once	0	0

CONTINUITY
metric	A	B
chains	1	2
blocks	2	2
chain_aligned_bp_n50	24	12
chain_aligned_bp_l50	1	1
largest_chain_aligned_bp	24	12
median_blocks_per_chain	2.0	1.0
median_target_top1_share	100.00%	50.00%
median_target_top3_share	100.00%	100.00%
median_target_chains_to_90pct	1.0	2.0
max_target_chains_to_90pct	1	2

GAPS
metric	A	B	absolute_delta	percent_delta
target_gap_bp	0	0	+0	0.00%
query_gap_bp	0	0	+0	0.00%
target_only_gap_events	0	0	+0	0.00%
query_only_gap_events	0	0	+0	0.00%
dual_gap_events	0	0	+0	0.00%
largest_target_gap	0	0	+0	0.00%
largest_query_gap	0	0	+0	0.00%

INTERPRETATION
Canonical mappings are identical, but B is more fragmented.
```

100% mapping agreement and worse continuity in the same report: that is the
point of the tool.

## Compatibility

Before comparing, both files are fully validated (same checks as `stats`) and
sequence sizes are cross-checked. The same target or query name assigned
conflicting sizes in the two files is fatal. A sequence present in only one
file is not an error — that may be a real coverage difference.

## Memory

Canonical mappings are held in memory (one ~32-byte segment per block, both
files). The estimate is checked against a configurable ceiling (default 16 GiB,
set via `--memory-ceiling <GIB>`) before canonicalization; past it the command
fails with a clear error instead of OOMing. Per-key external sorting is the
planned upgrade path for larger inputs and is not implemented.
