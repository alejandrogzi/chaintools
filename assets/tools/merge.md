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


# chaintools merge 

merges multiple chain files into one output file.

Inputs:
- `--chains <PATH>...`: explicit input chain paths in the given order
- `--file <PATH>`: text file with one input chain path per nonblank line

Output:
- `--out-chain <PATH>`: required output path
- `--gzip`: gzip-compress the output stream

Sorting:
- no `--sort-by`: merge input text in input order without reparsing
- `--sort-by score|id|reference|query`: parse all inputs, sort deterministically, and write canonical chain text

Renaming:
- `--rename`/`-r`: reassign chain IDs sequentially in sorted output order, so the first emitted chain gets `id 1`, the next `2`, and so on
- `--rename` implies sorting: it defaults to `--sort-by score` when no key is given, and respects an explicit `--sort-by <KEY>` when one is provided
- renaming is applied only to the final sorted output (both the in-memory and external-merge paths); original IDs are otherwise preserved

Notes:
- `reference` is the target/reference-name sort used elsewhere as target sort
- unsorted merge preserves input text order and only inserts record separators when needed between files
- sorted merge preserves metadata lines, then emits sorted chains
- sorted merge deduplicates metadata lines at the byte level: an identical metadata line appearing in more than one input (e.g. files produced by a previous split) is emitted once, in first-seen order
- `--max-gb <GB>` controls the in-memory working set for sorted merge and defaults to `8`
