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
</p>


## Overview

'chaintools' is a high-performance library designed for parsing chain files, which describe pairwise alignments between sequences commonly used in genomics. The library provides zero-copy parsing to minimize memory allocations and maximize performance when working with large alignment datasets.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
chaintools = { version = "0.0.2", features = ["mmap", "gzip"] }
```

### Features

- **Zero-copy parsing**: All string data is referenced without allocation for maximum performance
- **Memory mapping**: Optional `mmap` support for efficient handling of large files  
- **Parallel processing**: Multi-threaded parsing with the `rayon` feature
- **Streaming**: Low-memory streaming parser suitable for stdin and pipes
- **Indexing**: Random access to individual chains with the `index` feature
- **Compression**: Built-in gzip support with the `gzip` feature
- **Feature-gated dependencies**: Minimal footprint by enabling only needed features

## Usage

### Basic File Reading

```rust
use chaintools::Reader;

// Load a chain file (automatically uses mmap when available)
let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;

// Iterate over all chains
for chain in reader.chains() {
    println!("Chain {}: score={}", chain.id, chain.score);
    println!("  Reference: {}:{}-{} ({})", 
             chain.reference_name.as_str().unwrap_or("invalid"),
             chain.reference_start, chain.reference_end,
             if chain.reference_strand == chaintools::Strand::Plus { "+" } else { "-" });
    println!("  Query: {}:{}-{} ({})", 
             chain.query_name.as_str().unwrap_or("invalid"),
             chain.query_start, chain.query_end,
             if chain.query_strand == chaintools::Strand::Plus { "+" } else { "-" });
    println!("  Blocks: {}", chain.blocks.as_slice().len());
}

println!("Total chains: {}", reader.len());
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Streaming Large Files

```rust
use chaintools::stream::StreamingReader;

// Stream from a file (low memory usage)
let mut reader = StreamingReader::from_path("large.chain")?;

while let Some(chain) = reader.next_chain()? {
    println!("Processing chain with score: {}", chain.score);
    // Process chain without loading entire file into memory
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Parallel Processing

```rust
use chaintools::Reader;

// Parse large files faster using multiple threads
let reader = Reader::<chaintools::Chain>::from_path_parallel("huge.chain")?;

println!("Parsed {} chains in parallel", reader.len());
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Random Access with Indexing

```rust
use chaintools::ChainIndex;

// Build an index for fast random access
let index = ChainIndex::from_path("example.chain")?;

// Access specific chains without parsing the entire file
if let Some(chain_bytes) = index.chain_bytes(0) {
    println!("First chain is {} bytes", chain_bytes.len());
}

println!("Index contains {} chains", index.len());
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Format

Chain files use the following format:

```
chain score tName tSize tStrand tStart tEnd qName qSize qStrand qStart qEnd id
size
[dt dq]
size
[dt dq]
...
(blank line)
```

Where:
- `score`: Alignment score (signed 64-bit integer)
- `tName`, `qName`: Target and query sequence names
- `tSize`, `qSize`: Target and query sequence lengths
- `tStrand`, `qStrand`: Strand orientation (+ or -)
- `tStart`, `tEnd`, `qStart`, `qEnd`: Alignment coordinates
- `id`: Chain identifier
- `size`: Length of aligned region in bases
- `dt`, `dq`: Optional gap sizes on the reference and query sequences

In the Rust API, block gap fields are exposed as `gap_reference` and
`gap_query`.

In the Rust API, the `t*` header fields are exposed as `reference_*` and the
`q*` fields are exposed as `query_*`.

### Example Chain Record

```
chain 4900 chrY 58368225 + 25985403 25985638 chr5 151006098 - 43257292 43257528 1
9 1 0
10 0 5
61 4 0
16 0 4
42 3 0
16 0 8
14 1 0
3 7 0
48

```

## Data Structures

### Chain

The main data structure representing a parsed chain alignment:

```rust
use chaintools::{Chain, Strand, ByteSlice};

// Chain contains zero-copy references to sequence names
let chain = Chain {
    score: 100,
    reference_name: ByteSlice::from(b"chr1"),  // Zero-copy reference
    reference_size: 1000,
    reference_strand: Strand::Plus,
    reference_start: 0,
    reference_end: 100,
    query_name: ByteSlice::from(b"chr2"),  // Zero-copy reference
    query_size: 1000,
    query_strand: Strand::Plus,
    query_start: 0,
    query_end: 100,
    id: 1,
    blocks: BlockSlice::empty(),
};
```

### Block

Represents an aligned region with optional gaps:

```rust
use chaintools::Block;

let block = Block {
    size: 100,          // 100 bases aligned
    gap_reference: 50,  // 50 bases gap on the reference
    gap_query: 30,      // 30 bases gap on the query
};
```

## Performance Considerations

- **Memory mapping** (`mmap` feature) provides the best performance for large files
- **Parallel parsing** (`parallel` feature) speeds up processing of files with many chains
- **Streaming** mode uses minimal memory but is slower than batch parsing
- **Zero-copy** design minimizes allocations and improves cache efficiency

## API Reference

### Reader

Main interface for parsing complete chain files:

- `Reader::from_path(path)` - Load from file path
- `Reader::from_mmap(path)` - Load with memory mapping
- `Reader::from_path_parallel(path)` - Load with parallel parsing
- `Reader::from_owned_bytes(data)` - Parse from in-memory data
- `reader.chains()` - Iterator over all chains
- `reader.len()` - Number of chains
- `reader.is_empty()` - Check if empty

### StreamingReader

Low-memory streaming interface:

- `StreamingReader::from_path(path)` - Create from file
- `StreamingReader::new(reader)` - Create from any BufRead
- `reader.next_chain()` - Get next chain (returns None at EOF)

### ChainIndex

Random access indexing:

- `ChainIndex::from_path(path)` - Build index from file
- `index.len()` - Number of chains
- `index.spans()` - Get all chain spans
- `index.chain_bytes(idx)` - Get raw bytes of specific chain

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please ensure all tests pass and follow the existing code style.

Run tests with:

```bash
cargo test
```

Run tests with all features:

```bash
cargo test --all-features
`
