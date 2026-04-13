// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::path::Path;

use crate::error::ChainError;
use crate::parser::locate_chain_ranges;
use crate::storage::{SharedBytes, is_gz_path};

#[cfg(all(feature = "index", not(feature = "gzip")))]
use crate::storage::gzip_feature_error;

#[cfg(feature = "gzip")]
use flate2::read::MultiGzDecoder;

#[cfg(feature = "mmap")]
use memmap2::MmapOptions;

/// Byte span of a single chain within the source buffer.
///
/// Represents the location and size of a chain record in the original
/// file buffer. Used for random access to specific chains.
///
/// # Fields
///
/// * `offset` - Starting byte offset of the chain in the buffer
/// * `len` - Length of the chain in bytes
///
/// # Examples
///
/// ```
/// use chaintools::index::ChainSpan;
///
/// let span = ChainSpan { offset: 100, len: 50 };
/// assert_eq!(span.offset, 100);
/// assert_eq!(span.len, 50);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ChainSpan {
    pub offset: usize,
    pub len: usize,
}

/// Lightweight index that records the byte offsets of every chain.
///
/// Provides fast random access to individual chains without parsing
/// the entire file. Stores the original buffer and a vector of chain spans.
///
/// # Fields
///
/// * `bytes` - Shared storage for the entire file buffer
/// * `spans` - Vector of chain spans for random access
///
/// # Examples
///
/// ```no_run
/// use chaintools::index::ChainIndex;
///
/// let index = ChainIndex::from_path("example.chain")?;
/// println!("Found {} chains", index.len());
///
/// // Access bytes of the first chain
/// if let Some(chain_bytes) = index.chain_bytes(0) {
///     println!("First chain is {} bytes long", chain_bytes.len());
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ChainIndex {
    bytes: SharedBytes,
    spans: Vec<ChainSpan>,
}

impl ChainIndex {
    /// Build an index from a path. Uses mmap when available, otherwise owns the buffer.
    ///
    /// Automatically detects gzip files and decompresses them when the gzip feature
    /// is enabled. Prefers memory mapping for performance when available.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chain file (plain text or gzip compressed)
    ///
    /// # Output
    ///
    /// Returns `Ok(ChainIndex)` with all chain locations, or `Err(ChainError)`
    /// if the file cannot be read or indexed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chaintools::index::ChainIndex;
    ///
    /// // Index a plain text chain file
    /// let index = ChainIndex::from_path("example.chain")?;
    ///
    /// // Index a gzip compressed chain file
    /// let gz_index = ChainIndex::from_path("example.chain.gz")?;
    ///
    /// println!("Indexed {} chains", index.len());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let path = path.as_ref();
        if is_gz_path(path) {
            #[cfg(feature = "gzip")]
            {
                let file = std::fs::File::open(path)?;
                let mut decoder = MultiGzDecoder::new(file);
                let mut buffer = Vec::new();
                use std::io::Read;
                decoder.read_to_end(&mut buffer)?;
                return ChainIndex::from_owned(buffer);
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(gzip_feature_error());
            }
        }

        #[cfg(feature = "mmap")]
        {
            let file = std::fs::File::open(path)?;
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            return ChainIndex::from_bytes(SharedBytes::from_mmap(mmap));
        }

        #[cfg(not(feature = "mmap"))]
        {
            let mut buffer = Vec::new();
            std::fs::File::open(path)?.read_to_end(&mut buffer)?;
            ChainIndex::from_owned(buffer)
        }
    }

    /// Build an index from owned bytes.
    ///
    /// Creates an index from an in-memory byte vector. Useful when you
    /// already have the file contents loaded or working with dynamically
    /// generated chain data.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Byte vector containing chain file contents
    ///
    /// # Output
    ///
    /// Returns `Ok(ChainIndex)` with all chain locations, or `Err(ChainError)`
    /// if the data cannot be indexed
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::index::ChainIndex;
    ///
    /// let chain_data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\n";
    /// let index = ChainIndex::from_owned(chain_data.to_vec())?;
    ///
    /// assert_eq!(index.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_owned(bytes: Vec<u8>) -> Result<Self, ChainError> {
        ChainIndex::from_bytes(SharedBytes::from_owned(bytes))
    }

    /// Build an index from existing shared bytes (mmap or owned).
    ///
    /// Creates an index from already-loaded shared byte storage. Used
    /// internally by other constructors but available for advanced use cases.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Shared byte storage containing chain file contents
    ///
    /// # Output
    ///
    /// Returns `Ok(ChainIndex)` with all chain locations, or `Err(ChainError)`
    /// if the data cannot be indexed
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::index::ChainIndex;
    /// use chaintools::storage::SharedBytes;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\n";
    /// let shared = SharedBytes::from_owned(data.to_vec());
    /// let index = ChainIndex::from_bytes(shared)?;
    ///
    /// assert_eq!(index.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_bytes(bytes: SharedBytes) -> Result<Self, ChainError> {
        let spans = locate_chain_ranges(bytes.as_slice())?
            .into_iter()
            .map(|range| ChainSpan {
                offset: range.start,
                len: range.end - range.start,
            })
            .collect();
        Ok(ChainIndex { bytes, spans })
    }

    /// Returns a slice of all chain spans in the index.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the index
    ///
    /// # Output
    ///
    /// Returns a `&[ChainSpan]` containing all chain spans
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chaintools::index::ChainIndex;
    ///
    /// let index = ChainIndex::from_path("example.chain")?;
    /// let spans = index.spans();
    ///
    /// for (i, span) in spans.iter().enumerate() {
    ///     println!("Chain {}: offset={}, len={}", i, span.offset, span.len);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn spans(&self) -> &[ChainSpan] {
        &self.spans
    }

    /// Returns the number of chains in the index.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the index
    ///
    /// # Output
    ///
    /// Returns the count of chains as `usize`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chaintools::index::ChainIndex;
    ///
    /// let index = ChainIndex::from_path("example.chain")?;
    /// println!("Found {} chains", index.len());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// Returns true if the index contains no chains.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the index
    ///
    /// # Output
    ///
    /// Returns `true` if empty, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chaintools::index::ChainIndex;
    ///
    /// let index = ChainIndex::from_path("empty.chain")?;
    ///
    /// if index.is_empty() {
    ///     println!("No chains found in file");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Returns the raw bytes backing a chain span.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the index
    /// * `idx` - Index of the chain to retrieve
    ///
    /// # Output
    ///
    /// Returns `Some(&[u8])` with the chain bytes if the index is valid,
    /// or `None` if the index is out of bounds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chaintools::index::ChainIndex;
    ///
    /// let index = ChainIndex::from_path("example.chain")?;
    ///
    /// // Get bytes of the first chain
    /// if let Some(chain_bytes) = index.chain_bytes(0) {
    ///     println!("First chain: {} bytes", chain_bytes.len());
    /// }
    ///
    /// // Invalid index returns None
    /// assert!(index.chain_bytes(999).is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn chain_bytes(&self, idx: usize) -> Option<&[u8]> {
        let span = self.spans.get(idx)?;
        let data = self.bytes.as_slice();
        data.get(span.offset..span.offset + span.len)
    }
}
