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

# chaintools split 

splits one chain input into multiple files under `<OUTDIR>/chains/`.

Inputs:
- `--chain <PATH>`: input chain file; if omitted, read from standard input

Required split mode:
- `--files <N>`: produce exactly `N` output files
- `--chunks <N>`: produce as many files as needed with at most `N` chains per file

Output:
- `--outdir <PATH>`: base output directory; files are always created under `<PATH>/chains/`
- `--gzip`: gzip-compress every split output file

Notes:
- output names use `part.00001.<basename>.chain` and add `.gz` when `--gzip` is used
- if the requested split threshold is larger than the number of chains, the tool produces a single output
- when possible, that single output is created as a symlink to the original input file
