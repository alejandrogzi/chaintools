// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! # chaintools
//!
//! A high-performance library for parsing chain files, which describe pairwise alignments
//! between sequences commonly used in genomics. The library provides zero-copy parsing
//! to minimize memory allocations and maximize performance when working with large
//! alignment datasets.
//!
//! ## Features
//!
//! - **Zero-copy parsing**: All string data is referenced without allocation for maximum performance
//! - **Memory mapping**: Optional `mmap` support for efficient handling of large files  
//! - **Parallel processing**: Multi-threaded parsing with the `rayon` feature
//! - **Streaming**: Low-memory streaming parser suitable for stdin and pipes
//! - **Indexing**: Random access to individual chains with the `index` feature
//! - **Compression**: Built-in gzip support with the `gzip` feature
//! - **Feature-gated dependencies**: Minimal footprint by enabling only needed features
//!
//! ## Quick Start
//!
//! ```no_run
//! use chaintools::Reader;
//!
//! // Load a chain file (automatically uses mmap when available)
//! let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;
//!
//! // Iterate over all chains
//! for chain in reader.chains() {
//!     println!("Chain {}: score={}", chain.id, chain.score);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Examples
//!
//! ### Streaming large files
//!
//! ```no_run
//! use chaintools::stream::StreamingReader;
//!
//! // Stream from a file (low memory usage)
//! let mut reader = StreamingReader::from_path("large.chain")?;
//!
//! while let Some(chain) = reader.next_chain()? {
//!     println!("Processing chain with score: {}", chain.score);
//!     // Process chain without loading entire file into memory
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ### Parallel processing (`parallel` feature)
//!
//! ```no_run
//! # #[cfg(feature = "parallel")] {
//! use chaintools::Reader;
//!
//! // Parse large files faster using multiple threads
//! let reader = Reader::<chaintools::Chain>::from_path_parallel("huge.chain")?;
//!
//! println!("Parsed {} chains in parallel", reader.len());
//! # }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ### Random access with indexing (`index` feature)
//!
//! ```no_run
//! # #[cfg(feature = "index")] {
//! use chaintools::ChainIndex;
//!
//! // Build an index for fast random access
//! let index = ChainIndex::from_path("example.chain")?;
//!
//! // Access specific chains without parsing the entire file
//! if let Some(chain_bytes) = index.chain_bytes(0) {
//!     println!("First chain is {} bytes", chain_bytes.len());
//! }
//!
//! println!("Index contains {} chains", index.len());
//! # }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Feature flags
//!
//! - `mmap`: Memory mapping support for efficient handling of large files
//! - `gzip`: Built-in gzip compression support
//! - `index`: Random access indexing for chains
//! - `parallel`: Multi-threaded parsing with rayon
//! - `default`: Enables `mmap`
//!
//! ## Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! chaintools = { version = "0.0.2", features = ["mmap", "gzip"] }
//! ```

#[cfg(feature = "sequence")]
pub mod antirepeat;
pub mod block;
pub mod chain;
pub mod error;
#[cfg(feature = "index")]
pub mod index;
pub mod parser;
pub mod reader;
#[cfg(feature = "sequence")]
pub mod sequence;
pub mod storage;
pub mod stream;
#[cfg(feature = "sequence")]
pub mod writer;

pub use block::{Block, BlockSlice};
pub use chain::{Chain, Strand};
pub use error::ChainError;
#[cfg(feature = "index")]
pub use index::{ChainIndex, ChainSpan};
pub use reader::Reader;
pub use storage::ByteSlice;
pub use stream::{OwnedChain, OwnedChainHeader, StreamItem, StreamingReader};
#[cfg(feature = "sequence")]
pub use writer::{write_chain_dense, write_chain_header, write_dense_blocks};
