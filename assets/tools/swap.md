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
    <a href="https://img.shields.io/badge/version-0.0.9-green" target="_blank">
      <img alt="Version Badge" src="https://img.shields.io/badge/version-0.0.9-green">
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


# chaintools swap

Swaps the target and query sides of every chain, so the same alignment is described in the opposite direction.

## Usage

```bash
chaintools swap \
  [--chain input.chain] \
  [--out-chain output.chain] \
  [--gzip]
```

If `--chain` is omitted, input is read from standard input. If `--out-chain` is omitted, output is written to standard output.

```bash
cat input.chain | chaintools swap > swapped.chain
```

## Behavior

- The old query becomes the new target, always on `+`; the old target becomes the new query and keeps the original query strand.
- For a `+` query, block order is preserved and each block's `dt`/`dq` are exchanged.
- For a `-` query, both new header intervals are derived from the chromosome sizes and block order is reversed. Because gaps sit *between* blocks, the gap following a reversed block is the one recorded on its successor in the reversed list.
- Score and chain ID are preserved.
- Output order matches input order; nothing is sorted implicitly. Pipe into `chaintools sort` if you need sorted output.
- `#` metadata lines are preserved in stream order.
- Chains are streamed one at a time, so memory does not grow with input size.

## Validation

A chain is rejected, rather than silently mistranslated, when:

- its target strand is not `+`;
- its block sizes and inner gaps do not sum to the target span;
- its block sizes and inner gaps do not sum to the query span;
- a header coordinate exceeds its declared sequence size.

## Differences from UCSC `chainSwap`

- Uses descriptive named arguments instead of positional UCSC arguments.
- Supports stdin/stdout and optional gzip-compressed output.
- Validates block/span consistency instead of assuming it.
