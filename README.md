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

  <p align="center">
    <samp>
        <span>work with .chain files in Rust</span>
        <br>
        <br>
        <a href="https://docs.rs/chaintools/0.0.2/chaintools/">docs</a> .
        <a href="https://github.com/alejandrogzi/chaintools/tree/master/assets/usage/usage.md">usage</a> .
        <a href="https://github.com/alejandrogzi/chaintools/tree/master/assets/tools">tools</a> .
        <a href="https://github.com/alejandrogzi/chaintools?tab=readme-ov-file#Format">chains</a> 
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
chaintools = { version = "0.0.2", features = ["mmap", "gzip", "parallel"] }
```
## Benchmarks
| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `chaintools filter` | 7.205 ± 0.102 | 7.087 | 7.266 | 1.00 |
| `UCSC chainFilter` | 11.778 ± 0.043 | 11.729 | 11.812 | 1.63 ± 0.02 |
|:---|---:|---:|---:|---:|
| `chaintools sort` | 12.434 ± 0.165 | 12.298 | 12.617 | 1.00 |
| `UCSC chainSort` | 18.279 ± 0.025 | 18.254 | 18.303 | 1.47 ± 0.02 |
|:---|---:|---:|---:|---:|
| `chaintools merge` | 11.995 ± 0.045 | 11.943 | 12.024 | 1.00 |
| `UCSC chainMergeSort` | 17.616 ± 0.071 | 17.557 | 17.696 | 1.47 ± 0.01 |

