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


# chaintools sort

Sort chain records from a chain file or standard input.

## Input

- `--chain <PATH>`: read a chain file.
- No `--chain`: read chain data from standard input.
- `.gz` input paths are supported when the `gzip` feature is enabled.

## Output

- `--out-chain <PATH>`: write sorted chains to this path.
- No `--out-chain`: write sorted chains to standard output.
- `--gzip`: gzip-compress the output. Output is uncompressed unless this option
  is present. Requires the `gzip` feature.
- `--out-index <PATH>`: write an index of output offsets for the selected
  primary sort key.

## Sorting

- `--sort-by score`: sort by score descending.
- `--sort-by target`: sort by target name, then target start.
- `--sort-by query`: sort by query name, then query start.
- `--max-gb <GB>`: spill sorted runs to temporary files when the in-memory
  working set grows past this limit. The default is `8`.

## Differences from UCSC chainSort / chainMergeSort

- `sort` is the only user-facing sorting command. Large inputs are handled by
  internal spill-and-merge sorting instead of a separate `merge-sort` tool.
- Input can come from `--chain <PATH>` or standard input.
- Output can go to `--out-chain <PATH>` or standard output.
- Output gzip compression is explicit with `--gzip`.
- Sorting mode is selected with `--sort-by score|target|query` instead of
  separate UCSC boolean flags.
- Equal primary keys are normalized to a deterministic order. Ties are broken by
  chain id, then target fields, then query fields, then remaining chain content.
- The implementation preserves existing chain ids. It does not renumber them
  the way UCSC `chainMergeSort` does by default.
- `--out-index` is not allowed together with `--gzip` because index offsets are
  defined on uncompressed output bytes.
- Metadata lines beginning with `#` are copied to the beginning of the output in
  input encounter order before sorted chains are written.
