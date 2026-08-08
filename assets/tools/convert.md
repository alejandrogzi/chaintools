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


# chaintools convert

Converts chain alignments to VCF, MAF, or BAM.

A chain records alignment geometry but no bases, so both genomes are always required.

## Usage

```bash
chaintools convert \
  --to vcf|maf|bam \
  --reference target.2bit-or-fasta \
  --query query.2bit-or-fasta \
  [--chain input.chain] \
  [--output out.vcf] \
  [--gzip]
```

If `--chain` is omitted, input is read from standard input. If `--output` is omitted, output is written to standard output. The output type is decided by `--to`, so there is one generic `--output` rather than a per-format flag.

`--reference` and `--query` accept `.2bit`, `.fa`, `.fasta`, `.fna`, and gzipped FASTA variants.

`--to bam` requires a build with the `bam` feature (included in `--all-features`), and rejects `--gzip` because BAM output is already block-compressed.

## VCF

The chain target is the REF genome and the chain query is the ALT genome.

- Each mismatching aligned base becomes one record. Soft-masking is ignored (case-insensitive comparison) and a difference involving a non-`ACGT` base is not called.
- `dt > 0, dq == 0` becomes a deletion, `dt == 0, dq > 0` an insertion, both anchored on the preceding reference base — or on the following one when the event sits at the start of the chain's target span, so no coordinate underflows.
- `dt > 0 && dq > 0` becomes one replacement record carrying the whole reference gap against the whole query gap: a chain defines no base-level alignment inside a dual-sided gap and none is invented.
- Redundant shared suffix then prefix bases are trimmed while keeping both alleles non-empty. Repeat-aware left alignment is *not* performed; run `bcftools norm` if you need it.
- `INFO` carries `CHAIN_ID`, `QUERY_CHROM`, `QUERY_POS`, and `STRAND`. `QUAL` is `.` and `FILTER` is `PASS`. There are no sample or genotype columns.
- Records follow chain traversal order; nothing is buffered to coordinate-sort.

## MAF

Pairwise MAF, one stanza per continuously aligned segment.

- Aligned blocks and one-sided gaps extend the current stanza; a one-sided gap is emitted as sequence against `-` on the other row.
- A dual-sided gap ends the stanza and the next aligned block starts a new one, for the same reason as above.
- Sequence text is verbatim, so soft-masking is preserved and stripping `-` reproduces the declared source interval exactly.
- The target row is `+`; the query row keeps the chain's strand, and its `start`/`size` are in that orientation, as MAF requires.

## BAM

Each emitted record is an assembly-alignment segment, not a sequencing read.

- `@SQ` entries come from the reference genome, sorted by name: a BAM header must be complete before the first record, and the chain stream may be an unrewindable pipe. Every chain's declared target size is checked against that header, so contradictory sizes are rejected rather than silently accepted.
- `QNAME` is `query.chain_id`, suffixed `query.chain_id.1`, `.2`, … when a chain is split at a dual-sided gap. Distinct names mean no supplementary-alignment bookkeeping is needed.
- `CIGAR` uses `=`/`X` (the exact bases are known) plus `I`/`D` for one-sided chain gaps.
- `FLAG` carries the reverse bit for a `-` query and `SEQ` is the oriented query sequence. `MAPQ` is 0 and `QUAL` is absent. No `@RG`, sample names, read groups, or platform metadata are fabricated.

## Errors

Conversion fails clearly, rather than emitting partial or wrong records, on a missing reference or query sequence, chain coordinates outside a sequence, blocks that do not sum to the header span, an output path equal to the input chain, and — for BAM — an unavailable `bam` feature or a chromosome length that contradicts the reference.

## Non-goals

No liftover, PAF, SAM, AXT, PSL, BCF, tabix, bgzip-specific VCF, genotypes, full variant normalization, or local realignment inside chain gaps.
