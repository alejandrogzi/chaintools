<p align="center">
  <p align="center">
    <img width=200 align="center" src="./assets/logo.png" >
  </p>

  <span>
    <h1 align="center">
        <code>chaintools</code>
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

  <p align="center">
    <samp>
        <span>work with .chain files in Rust</span>
        <br>
        <br>
        <a href="https://docs.rs/chaintools/0.0.13/chaintools/">docs</a> .
        <a href="https://github.com/alejandrogzi/chaintools/tree/master/assets/usage/usage.md">usage</a> .
        <a href="https://github.com/alejandrogzi/chaintools/tree/master/assets/tools">tools</a> .
        <a href="https://genome.ucsc.edu/goldenpath/help/chain.html">chains</a> 
    </samp>
  </p>

</p>


## Installation
### Binary
```bash
cargo install --all-features chaintools
```

### Docker
```bash
docker pull ghcr.io/alejandrogzi/chaintools:latest
```

### Conda
```bash
conda install -c bioconda chaintools
```

### Library
Add this to your `Cargo.toml`:

```toml
[dependencies]
chaintools = { version = "0.0.13", features = ["mmap", "gzip", "parallel"] }
```

## Benchmarks

<div align="center">

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `chaintools filter` | 7.205 ± 0.102 | 7.087 | 7.266 | 1.00 |
| `UCSC chainFilter` | 11.778 ± 0.043 | 11.729 | 11.812 | 1.63 ± 0.02 |
||
| `chaintools sort` | 12.434 ± 0.165 | 12.298 | 12.617 | 1.00 |
| `UCSC chainSort` | 18.279 ± 0.025 | 18.254 | 18.303 | 1.47 ± 0.02 |
||
| `chaintools merge` | 11.995 ± 0.045 | 11.943 | 12.024 | 1.00 |
| `UCSC chainMergeSort` | 17.616 ± 0.071 | 17.557 | 17.696 | 1.47 ± 0.01 |
||
| `chaintools swap` | 8.371 ± 0.163 | 8.183 | 8.475 | 1.00 |
| `UCSC chainSwap` | 14.152 ± 0.119 | 14.021 | 14.254 | 1.69 ± 0.04 |
||
| `chaintools liftover` | 4.380 ± 0.009 | 4.372 | 4.390 | 1.00 |
| `UCSC liftOver` | 16.540 ± 0.121 | 16.424 | 16.666 | 3.78 ± 0.03 |

</div>

*ran using hyperfine 1.18.0 on an AMD Ryzen 7 5700X with 128 GB of RAM and 16 cores with a 262 MB chain.gz file as input

**`swap` and `liftover` were measured on the same machine with the same methodology, on their own inputs: `swap` on a 344 MB chain.gz (hg38 vs HLrhiRex1, 3,049,608 chains) and `liftover` on a 6.5 MB `.over.chain.gz` (16,278 chains) lifting 244,296 BED3 intervals sampled from its aligned blocks. Both commands produced byte-identical output to their UCSC counterpart on every input tested. `bed`, `split` and `convert` have no equivalent UCSC tool to compare against, and `score`/`antirepeat`/`convert` require genome sequence, so they are not listed.
