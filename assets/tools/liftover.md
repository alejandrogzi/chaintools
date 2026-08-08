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


# chaintools liftover

Maps BED intervals through a chain, in one direction:

```text
chain reference/target  →  chain query
```

No genome sequence is required: a chain carries all the geometry.

## Usage

```bash
chaintools liftover \
  --chain map.chain \
  --bed input.bed \
  --type 3|6|12 \
  [--output lifted.bed] \
  [--unmapped unmapped.bed] \
  [--min-match 0.95] \
  [--multiple]
```

If `--output` is omitted, output goes to standard output. `--type` is required so no input column is silently ignored; columns beyond the declared width are copied through unlifted and a warning says so.

### Positive-strand example

```bash
$ cat plus.chain
chain 100 chr1 1000 + 100 200 qry1 2000 + 500 600 1
100

$ printf 'chr1\t120\t150\n' | tee in.bed
$ chaintools liftover --chain plus.chain --bed in.bed --type 3
qry1	520	550
```

### Reverse-strand example

The chain's query is on `-`, so coordinates flip around the query chromosome size and a BED6 strand flips with them:

```bash
$ cat minus.chain
chain 200 chr1 1000 + 100 200 qry1 2000 - 500 600 2
100

$ printf 'chr1\t120\t150\tfeature\t742\t+\n' > in6.bed
$ chaintools liftover --chain minus.chain --bed in6.bed --type 6
qry1	1450	1480	feature	0	-
```

### Reverse direction

There is no `--reverse`. Swap the chain first, which keeps one mapping direction in the engine:

```bash
chaintools swap --chain aToB.chain --out-chain bToA.chain
chaintools liftover --chain bToA.chain --bed on_b.bed --type 3 --output on_a.bed
```

## Supported BED widths

`BED3`, `BED6`, and `BED12`. Any other width is rejected.

## Mapping rules

- **`--min-match`** (default `0.95`) is the fraction of *input bases that land inside aligned chain blocks*. A chain's bounding span never counts, and at least one mapped base is always required — even at `--min-match 0`.
- An interval may cross chain gaps. BED3/BED6 still produce **one** output interval covering the mapped extent, not one record per chain block.
- **Mappings are never stitched across chains.** If chain A places 60% and chain B the other 40%, that is not a 100% mapping; each chain is judged on its own.
- **BED12** projects every block through the *same* chain, requires each block to contribute mapped sequence, and measures coverage over block bases only — intronic bases never count. Output blocks are re-sorted ascending by genomic coordinate (needed for `-` chains), `blockStarts` are recomputed, and `thickStart`/`thickEnd` are mapped through the same chain rather than copied.

## Multiple mappings

By default a record that several chains can independently place is **not** silently assigned to one of them; it is reported unmapped as `Duplicated in new`. `--multiple` emits every qualifying mapping instead, ordered deterministically by mapped bases, then chain score, query chromosome, query start, chain ID, and finally chain file order.

## Unmapped records

`--unmapped PATH` writes each rejected record preceded by its reason:

```text
Deleted in new              no chain aligns any base of the interval
Partially deleted in new    one relevant chain, below --min-match
Split in new                several chains touch it, none sufficient alone
Duplicated in new           several chains qualify and --multiple is off
```

Without `--unmapped`, rejected records are dropped — never silently: the summary always reports `unmapped=N` and a warning recommends the flag when `N > 0`.

## Known limitation

BED `score` and `itemRgb` are not preserved: the pinned `genepred` model does not carry them, so lifted records emit `0` and `0,0,0`. Coordinates, name, strand, thick region, and blocks all round-trip. This matches the existing `chaintools bed` output behavior.

## Not implemented

GFF/GTF/genePred/VCF/PSL input, position/interbase coordinates, `--min-blocks`, `--fudgeThick`, sequence validation, and automatic direction inversion.
