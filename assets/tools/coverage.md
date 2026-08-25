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
    <a href="https://img.shields.io/badge/version-0.0.13-green" target="_blank">
      <img alt="Version Badge" src="https://img.shields.io/badge/version-0.0.13-green">
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


# chaintools coverage

Measure the fraction of unique annotation feature bases covered by aligned
chain blocks. Chain gaps never count as coverage.

## Usage

```bash
chaintools coverage \
  --chains hg38ToMm39.over.chain.gz \
  --side reference \
  --intervals gencode.v50.annotation.gtf.gz \
  --feature cds \
  > coverage.tsv
```

Several chain files can be passed to `--chains`, listed one per line with
`--file`, or read from standard input when neither option is present. Their
coverage is combined; duplicate and overlapping mappings count each base once.

## Options

- `--chains <PATH>...` / `-c`: one or more chain files.
- `--file <PATH>` / `-f`: file containing one chain path per line. Conflicts
  with `--chains`.
- `--side reference|query`: chain side on which to measure coverage.
- `--intervals <PATH>`: BED, GTF, GFF, or GFF3 annotation.
- `--feature cds|exon|intron|utr`: required feature universe.
- `--threads <N>` / `-t`: global worker count; defaults to available CPUs.

`.gz` chain and annotation paths are supported when chaintools is built with
the `gzip` feature. `cargo install --all-features chaintools` enables it.

## Annotation semantics

Features are derived through `genepred`, then overlapping and book-ended
intervals are merged per chromosome before totals are calculated. Isoforms
therefore cannot double-count shared bases.

| Input | exon | cds / utr | intron |
|:---|:---:|:---:|:---:|
| BED3–6 | full interval | unavailable | unavailable |
| BED8/9 | full interval | thick bounds | unavailable |
| BED12 | blocks | blocks intersected with thick bounds | gaps between blocks |
| GTF/GFF | derived from transcript structure | derived from coding bounds | derived from exon structure |

A non-empty annotation that yields no requested intervals fails instead of
silently reporting zero coverage. Chromosomes are not filtered: the annotation
defines the complete denominator, including mitochondrial DNA and scaffolds.

## Chain coordinates

Only positive-length alignment blocks are marked. Full chain spans and the gaps
between blocks are never treated as aligned. `--side query` converts minus-strand
query blocks to forward genomic coordinates before intersecting the annotation.
The target/reference strand must be `+`, and malformed block spans are rejected.

## Output

Output is tab-separated and written to standard output:

```text
chrom   total_bases   covered_bases   coverage_fraction
chr1    3894019       3585875         0.920867
chr2    2799576       2623407         0.937073
total   6693595       6209282         0.927645
```

Rows are sorted by chromosome name. Fractions use six decimal places, and the
final `total` row is always present. An empty annotation reports `0.000000`,
never `NaN`.

## Memory and parallelism

Merged annotation intervals are flattened into compact feature space with one
atomic bit per feature base, so genomic gaps use no bitmap memory. Annotation
normalization is serial; interval indexing, chain parsing and marking, and
bitmap counting use the global thread pool. Parsed chain files are held in
memory one input file at a time.
