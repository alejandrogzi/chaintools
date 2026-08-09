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


# chaintools stats

Summarize alignment amount, gaps, strands, and chain fragmentation in one
streaming pass over a chain file.

## Usage

```bash
chaintools stats --chain file.chain
chaintools stats --chain file.chain --by-sequence --top 3
chaintools stats < file.chain
```

If `--chain` is omitted, chain data is read from standard input. `.gz` input
paths are supported when the `gzip` feature is enabled.

## Options

- `--chain <PATH>`: input `.chain` file. Omit to read standard input.
- `--by-sequence`: add the per-target fragmentation table and top-chain lists.
- `--top <N>`: with `--by-sequence`, show the top `N` scored chains per target.
  Default `3`.

## Output

Output is tab-separated, grouped into sections:

- **GENERAL** — chain and block counts, distinct target/query sequence counts.
- **ALIGNMENT** — `aligned_pair_bp` (sum of block sizes), `target_span_bp`
  and `query_span_bp` (sum of header spans). `aligned_pair_bp` is never called
  genome coverage.
- **CONTINUITY** — largest chain, chain aligned-bp N50/L50, median chain
  aligned bp, median blocks per chain, and global chromosome fragmentation
  (median/max target chains→90%).
- **GAPS** — within-chain gap bp on each side, gap-event types
  (target-only `dt>0,dq==0`, query-only `dt==0,dq>0`, dual `dt>0,dq>0`), and
  largest gaps.
- **STRAND** — plus/minus chain and aligned-bp counts (query strand).
- **SCORE** — sum of header scores and mean score per chain.
- **BY TARGET** (with `--by-sequence`) — one row per target: chain count,
  total aligned bp, top-1 and top-3 aligned-bp share, chains required for
  50/90/95% of the target's aligned bp, and per-target chain N50.
- **TOP CHAINS** (with `--by-sequence`) — for each target, the top `N` chains
  by score with aligned bp, spans, block count, alignment density
  (`aligned_bp / target_span`), gaps, and largest gap.

### Definitions

- **top-1 share** = largest chain aligned bp / total chain aligned bp on the
  target. **top-3 share** is the same with the largest 3 chains. These are
  chain-concentration metrics, not unique genome coverage: one 100-Mb chain
  scores 100%, two 50-Mb chains score 50%, even with identical base-pair
  mappings.
- **chains→N%** = smallest `k` such that the `k` largest chains cover ≥ N% of
  the target's aligned bp.
- **N50** uses chain aligned bp, not header span.
- Score alone is never used as a continuity metric; the top-chain table reports
  density and spans alongside it.

## Example

```bash
$ chaintools stats --chain a.chain
GENERAL
chains	4
blocks	7
target_sequences	2
query_sequences	3

ALIGNMENT
aligned_pair_bp	70
target_span_bp	76
query_span_bp	78

CONTINUITY
largest_chain_aligned_bp	40
chain_aligned_bp_n50	40
chain_aligned_bp_l50	1
median_chain_aligned_bp	10.0
median_blocks_per_chain	1.0
median_target_top1_share	65.00%
median_target_top3_share	100.00%
median_target_chains_to_90pct	2.0
max_target_chains_to_90pct	2

GAPS
target_gap_bp	6
query_gap_bp	8
target_only_gap_events	1
query_only_gap_events	1
dual_gap_events	1
largest_target_gap	4
largest_query_gap	5

STRAND
plus_chains	2
minus_chains	2
plus_aligned_bp	50
minus_aligned_bp	20

SCORE
score_sum	200
mean_score_per_chain	50.00
```

## Validation

Every chain is validated before it is counted: the target strand must be `+`,
spans must be non-empty and fit inside their sequence sizes, no block may be
empty, and block sizes plus gaps must account for both header spans exactly.
Conflicting sizes for the same sequence name anywhere in the input are fatal.
A malformed chain aborts with a `format error` naming the byte offset.
