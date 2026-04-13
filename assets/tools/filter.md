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
    <a href="https://img.shields.io/badge/version-0.0.2-green" target="_blank">
      <img alt="Version Badge" src="https://img.shields.io/badge/version-0.0.2-green">
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


# chaintools filter

Filter chain records from one or more chain files, or from standard input when
no `--chain` argument is provided.

## Input

- `--chain <PATH>`: read a chain file. May be repeated.
- No `--chain`: read chain data from standard input.
- `.gz` input paths are supported when the `gzip` feature is enabled.

## Output

Matching chains are written to standard output in UCSC chain format.

- `--out-chain <PATH>`: write matching chains to this path instead of standard
  output.
- `--gzip`: gzip-compress the output. Output is uncompressed unless this option
  is present. Requires the `gzip` feature.
- `--output-format dense`: default chain format.
- `--output-format long`: UCSC long chain format.
- `--merge-zero-gaps`: merge zero-length gaps before filtering and report the
  merge count to standard error.

## Filters

- `--query-names <NAMES>`
- `--exclude-query-names <NAMES>`
- `--target-names <NAMES>`
- `--exclude-target-names <NAMES>`
- `--chain-id <ID>`
- `--min-score <SCORE>`
- `--max-score <SCORE>`
- `--min-query-start <POSITION>`
- `--max-query-start <POSITION>`
- `--min-query-end <POSITION>`
- `--max-query-end <POSITION>`
- `--min-target-start <POSITION>`
- `--max-target-start <POSITION>`
- `--min-target-end <POSITION>`
- `--max-target-end <POSITION>`
- `--query-overlap-start <POSITION>`
- `--query-overlap-end <POSITION>`
- `--target-overlap-start <POSITION>`
- `--target-overlap-end <POSITION>`
- `--query-strand <STRAND>`
- `--min-gapless-block <BASES>`
- `--min-query-gap <BASES>`
- `--min-target-gap <BASES>`
- `--max-query-gap <BASES>`
- `--max-target-gap <BASES>`
- `--min-query-span <BASES>`
- `--max-query-span <BASES>`
- `--min-target-span <BASES>`
- `--max-target-span <BASES>`
- `--exclude-random`
- `--exclude-haplotype`

## Differences from UCSC chainFilter

- Input is provided with repeated `--chain <PATH>` arguments. If no `--chain`
  is provided, input is read from standard input.
- Output can be redirected with `--out-chain <PATH>` instead of shell
  redirection.
- `--out-chain` is rejected when it exactly matches an input `--chain` path.
- Output gzip compression is explicit with `--gzip`; a `.gz` output path alone
  does not enable compression.
- Arguments use descriptive chaintools names rather than UCSC flag names.
- Disabled filters are represented by omitted options, not sentinel defaults
  such as `-2147483647`.
- `--chain-id` accepts only positive identifiers.
- Score filter thresholds are non-negative, but parsed chain scores may be
  negative. Negative scores pass `--max-score` thresholds and fail positive
  `--min-score` thresholds.
- When writing chain output to standard output, logging is disabled and non-off
  `--level` values require `--out-chain`.
- When `--out-chain` is used, logging defaults to `info`. Use `--level` to set
  `off`, `error`, `warn`, `info`, `debug`, or `trace`.
- Negative chain scores are accepted. A warning is emitted when the effective
  logging level includes warnings.
- `--max-score` has no visible CLI default; when omitted it behaves as
  `u64::MAX`.
- Global `--threads/-t` and `--level/-L` are reserved at the top-level CLI.
