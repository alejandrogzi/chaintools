<p align="center">
  <p align="center">
    <img width=200 align="center" src="./assets/logo.png" >
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


# chaintools anti-repeat

Filters chains that are dominated by soft-masked sequence or degenerate nucleotide matches.

## Usage

```bash
chaintools anti-repeat \
  --reference target.2bit-or-fasta \
  --query query.2bit-or-fasta \
  [--chain input.chain] \
  [--out-chain output.chain] \
  [--gzip] \
  [--min-score 5000] \
  [--no-check-score 200000]
```

If `--chain` is omitted, input is read from standard input. If `--out-chain` is omitted, output is written to standard output.

## Behavior

- Chains with score `>= --no-check-score` are written without sequence checks.
- Remaining chains must pass both:
  - the degeneracy filter
  - the repeat-mask filter
- Output order matches input order.
- `#` metadata lines are preserved in stream order.
- Output chain text is written in canonical dense chain format.

## Accepted sequence inputs

`--reference` and `--query` each accept:

- `.2bit`
- `.fa`
- `.fasta`
- `.fna`
- gzipped FASTA variants such as `.fa.gz` and `.fasta.gz`

Soft-masked lowercase bases from `.2bit` are preserved and used by the repeat filter.

## Differences from UCSC `chainAntiRepeat`

- Uses descriptive named arguments instead of positional UCSC arguments.
- Supports stdin/stdout and optional gzip-compressed output.
- Processes chains in bounded batches and parallelizes filtering when built with the `parallel` feature, while preserving output order.
- Preserves the observed UCSC edge behavior for zero-match and zero-length filter cases instead of normalizing them.
