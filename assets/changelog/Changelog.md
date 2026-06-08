# Changelog

All notable changes to **chaintools** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.5] - 2026-06-08

Full refactor of the **`antirepeat`** tool. On a ~200 MB soft-masked chain file with
`.2bit` reference/query it previously took ~12 h; it now completes in minutes, with
**byte-identical output**.

### Root cause

`antirepeat` used a lazy `.2bit` access path: for every chain below `--no-check-score`
it re-read the sequence from disk and, per fetch, the `twobit` reader scanned the
chromosome's soft-mask/N block list **from the start** (a linear `skip_while`, no binary
search). On repeat-masked genomes — hundreds of thousands of mask blocks per chromosome —
each of the ~millions of per-chain fetches re-scanned a large prefix, and a cost that
grows with chromosome length dominated the run. Parallelism was also off by default.

### Performance

- **Preload sequences into memory once.** `.2bit` reference/query are now fully decoded
  into memory at startup (the soft-mask/N scan is paid once per chromosome instead of once
  per chain), so every per-chain access is an in-memory lookup. This is the single biggest
  win and turns the ~12 h run into minutes.
- **Load only referenced chromosomes.** A cheap header-only pre-scan of the chain file
  loads just the sequences it references, bounding peak memory on fragmented assemblies
  (stdin input falls back to loading everything).
- **Parallel by default.** The `parallel` feature is now part of the `cli` build, so chains
  are filtered across all cores out of the box; the previous `--threads` startup error
  (when built without the feature) is gone.
- **Zero per-chain allocation.** The filter now borrows chromosome slices directly instead
  of copying each chain's span; minus-strand queries are reverse-complemented **on the fly**
  during the walk rather than copied and reversed. On large-span, repeat-driven chains this
  is a further ~9.6× faster and ~2× lower peak memory in benchmarking.
- **Fused filters.** The degeneracy and repeat-mask filters now share a single pass over a
  chain's aligned blocks.
- **I/O tuning.** Larger input read buffer (1 MB) and larger parallel batches.

### Changed

- `AntiRepeatEngine::chain_passes` no longer takes a `SequenceCache`; sequence access is now
  through the new `SequenceResolver::chromosome()` borrowing accessor.
- The `score` tool also benefits from the in-memory `.2bit` preload, as it shares the
  sequence resolver.

### Notes

- Output is verified byte-identical to v0.0.4 across plus/minus strands, soft-mask and N
  content, gzip input/output, and any thread count, via a randomized differential test and
  full old-vs-new output diffing.

## [0.0.4] - 2026-06-07

### Added

- Verbose, consistent logging system across **all** CLI tools (`-L/--level`,
  defaulting to `info` on stderr) with a uniform end-of-run summary line per subcommand,
  so logs never corrupt the stdout chain stream.

## [0.0.3] - 2026-05-31

### Added

- **`score`** subcommand: recompute chain scores from sequence
  (UCSC `chainScore`-compatible), including gap and substitution scoring.

### Changed

- Refactored the codebase into submodules (`io`, `model`, `parser`, `seq`, `cli`) for a
  clearer separation between the library and the CLI.

## [0.0.2] - 2026-04-13

First stable release.

### Added

- Command-line interface with the `filter`, `merge`, `sort`, and `split` subcommands.
- Reimplemented common chain parser with a streaming reader and an optional parallel reader.
- Zero-copy chain/block model backed by memory-mapped or owned buffers (`mmap` feature).
- Automatic gzip (`.chain.gz`) detection and decompression (`gzip` feature).
- Test suite, benchmark binary, CI workflows, Docker image, and rustdoc documentation.

[0.0.5]: https://github.com/alejandrogzi/chaintools/releases/tag/v0.0.5
[0.0.4]: https://github.com/alejandrogzi/chaintools/releases/tag/v0.0.4
[0.0.3]: https://github.com/alejandrogzi/chaintools/releases/tag/v0.0.3
[0.0.2]: https://github.com/alejandrogzi/chaintools/releases/tag/v0.0.2
