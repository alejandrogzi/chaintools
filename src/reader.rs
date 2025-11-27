#[cfg(feature = "gzip")]
use std::io::BufReader;
#[cfg(any(feature = "gzip", not(feature = "mmap")))]
use std::io::Read;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use crate::block::{Block, BlockSlice};
use crate::chain::Chain;
use crate::error::ChainError;
use crate::parser::parse_chains_sequential;
#[cfg(not(feature = "gzip"))]
use crate::storage::gzip_feature_error;
use crate::storage::{is_gz_path, ByteSlice, SharedBytes};

#[cfg(feature = "parallel")]
use crate::parser::parse_chains_parallel;

#[cfg(feature = "gzip")]
use flate2::read::MultiGzDecoder;

#[cfg(feature = "mmap")]
use memmap2::MmapOptions;

/// Reader for chain files.
///
/// The generic parameter allows the API shape `Reader::<Chain>::from_path(...)`.
/// Provides zero-copy parsing with optional memory mapping and parallel processing.
///
/// # Fields
///
/// * `_bytes` - Shared byte storage for the entire file
/// * `_blocks` - Shared storage for all alignment blocks
/// * `chains` - Parsed chain records with zero-copy references
/// * `_marker` - PhantomData for the generic parameter
///
/// # Examples
///
/// ```ignore
/// use chaintools::Reader;
///
/// // Load a chain file (uses mmap when available)
/// let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;
///
/// // Iterate over all chains
/// for chain in reader.chains() {
///     println!("Chain score: {}", chain.score);
/// }
///
/// // Get chain count
/// println!("Total chains: {}", reader.len());
/// ```
#[derive(Debug)]
pub struct Reader<T = Chain> {
    _bytes: SharedBytes,
    _blocks: Arc<Vec<Block>>,
    chains: Vec<Chain>,
    _marker: PhantomData<T>,
}

impl Reader<Chain> {
    /// Load a chain file from a path. Uses mmap when available, falls back to owned buffer.
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
    /// Returns `Ok(Reader<Chain>)` containing all parsed chains, or `Err(ChainError)`
    /// if the file cannot be read or parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// // Load a plain text chain file
    /// let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;
    ///
    /// // Load a gzip compressed chain file
    /// let gz_reader = Reader::<chaintools::Chain>::from_path("example.chain.gz")?;
    ///
    /// println!("Loaded {} chains", reader.len());
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let path = path.as_ref();
        if is_gz_path(path) {
            #[cfg(feature = "gzip")]
            {
                let file = std::fs::File::open(path)?;
                let reader = BufReader::new(file);
                let mut decoder = MultiGzDecoder::new(reader);
                let mut buffer = Vec::new();
                decoder.read_to_end(&mut buffer)?;
                return Self::from_owned_bytes(buffer);
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(gzip_feature_error());
            }
        }

        #[cfg(feature = "mmap")]
        {
            Self::from_mmap(path)
        }

        #[cfg(not(feature = "mmap"))]
        {
            let mut data = Vec::new();
            std::fs::File::open(path)?.read_to_end(&mut data)?;
            Self::from_owned_bytes(data)
        }
    }

    /// Load a chain file using memory mapping (requires `mmap` feature).
    ///
    /// Maps the file directly into memory for zero-copy parsing without loading
    /// the entire file into RAM. Only works with uncompressed files.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the uncompressed chain file
    ///
    /// # Output
    ///
    /// Returns `Ok(Reader<Chain>)` with memory-mapped storage, or `Err(ChainError)`
    /// if the file cannot be mapped or parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// // Load with memory mapping for large files
    /// let reader = Reader::<chaintools::Chain>::from_mmap("large.chain")?;
    ///
    /// for chain in reader.chains() {
    ///     println!("Processing chain: {}", chain.id);
    /// }
    /// ```
    #[cfg(feature = "mmap")]
    pub fn from_mmap<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Self::build(SharedBytes::from_mmap(mmap), ParseStrategy::Sequential)
    }

    /// Same as [`from_path`] but always parses in parallel (requires `parallel` feature).
    ///
    /// Uses multiple threads to parse large files faster. Automatically detects
    /// gzip files and decompresses them when the gzip feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chain file (plain text or gzip compressed)
    ///
    /// # Output
    ///
    /// Returns `Ok(Reader<Chain>)` containing all parsed chains, or `Err(ChainError)`
    /// if the file cannot be read or parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// // Load a large file with parallel parsing
    /// let reader = Reader::<chaintools::Chain>::from_path_parallel("huge.chain")?;
    ///
    /// println!("Parsed {} chains in parallel", reader.len());
    /// ```
    #[cfg(feature = "parallel")]
    pub fn from_path_parallel<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let path = path.as_ref();
        if is_gz_path(path) {
            #[cfg(feature = "gzip")]
            {
                let file = std::fs::File::open(path)?;
                let reader = BufReader::new(file);
                let mut decoder = MultiGzDecoder::new(reader);
                let mut buffer = Vec::new();
                decoder.read_to_end(&mut buffer)?;
                return Self::from_owned_bytes_parallel(buffer);
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
            return Self::build(SharedBytes::from_mmap(mmap), ParseStrategy::Parallel);
        }

        #[cfg(not(feature = "mmap"))]
        {
            let mut data = Vec::new();
            std::fs::File::open(path)?.read_to_end(&mut data)?;
            Self::from_owned_bytes_parallel(data)
        }
    }

    /// Construct from an owned buffer (no mmap).
    ///
    /// Parses chain data from an in-memory byte vector using sequential parsing.
    /// Useful when you already have the file contents loaded or working with
    /// dynamically generated chain data.
    ///
    /// # Arguments
    ///
    /// * `data` - Byte vector containing chain file contents
    ///
    /// # Output
    ///
    /// Returns `Ok(Reader<Chain>)` containing all parsed chains, or `Err(ChainError)`
    /// if the data cannot be parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// let chain_data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
    /// let reader = Reader::<chaintools::Chain>::from_owned_bytes(chain_data.to_vec())?;
    ///
    /// println!("Parsed {} chains from memory", reader.len());
    /// ```
    pub fn from_owned_bytes(data: Vec<u8>) -> Result<Self, ChainError> {
        Self::build(SharedBytes::from_owned(data), ParseStrategy::Sequential)
    }

    /// Construct from an owned buffer using parallel parsing (requires `parallel` feature).
    ///
    /// Parses chain data from an in-memory byte vector using multiple threads.
    /// Best for large in-memory datasets where parallel processing can provide
    /// significant speedup over sequential parsing.
    ///
    /// # Arguments
    ///
    /// * `data` - Byte vector containing chain file contents
    ///
    /// # Output
    ///
    /// Returns `Ok(Reader<Chain>)` containing all parsed chains, or `Err(ChainError)`
    /// if the data cannot be parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// let chain_data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
    /// let reader = Reader::<chaintools::Chain>::from_owned_bytes_parallel(chain_data.to_vec())?;
    ///
    /// println!("Parsed {} chains in parallel from memory", reader.len());
    /// ```
    #[cfg(feature = "parallel")]
    pub fn from_owned_bytes_parallel(data: Vec<u8>) -> Result<Self, ChainError> {
        Self::build(SharedBytes::from_owned(data), ParseStrategy::Parallel)
    }

    fn build(bytes: SharedBytes, strategy: ParseStrategy) -> Result<Self, ChainError> {
        let buf = bytes.as_slice();
        let (metas, blocks) = match strategy {
            ParseStrategy::Sequential => parse_chains_sequential(buf)?,
            #[cfg(feature = "parallel")]
            ParseStrategy::Parallel => parse_chains_parallel(buf)?,
        };
        let blocks_arc: Arc<Vec<Block>> = Arc::new(blocks);
        let chains = metas
            .into_iter()
            .map(|meta| Chain {
                score: meta.score,
                t_name: ByteSlice::new(bytes.clone(), meta.t_name),
                t_size: meta.t_size,
                t_strand: meta.t_strand,
                t_start: meta.t_start,
                t_end: meta.t_end,
                q_name: ByteSlice::new(bytes.clone(), meta.q_name),
                q_size: meta.q_size,
                q_strand: meta.q_strand,
                q_start: meta.q_start,
                q_end: meta.q_end,
                id: meta.id,
                blocks: BlockSlice::new(blocks_arc.clone(), meta.blocks),
            })
            .collect();
        Ok(Reader {
            _bytes: bytes,
            _blocks: blocks_arc,
            chains,
            _marker: PhantomData,
        })
    }

    /// Returns an iterator over all chains in the reader.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the reader
    ///
    /// # Output
    ///
    /// Returns an iterator yielding `&Chain` references
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;
    ///
    /// for chain in reader.chains() {
    ///     println!("Chain {}: score={}", chain.id, chain.score);
    /// }
    /// ```
    pub fn chains(&self) -> impl Iterator<Item = &Chain> {
        self.chains.iter()
    }

    /// Returns the number of chains in the reader.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the reader
    ///
    /// # Output
    ///
    /// Returns the count of chains as `usize`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// let reader = Reader::<chaintools::Chain>::from_path("example.chain")?;
    /// println!("Found {} chains", reader.len());
    /// ```
    pub fn len(&self) -> usize {
        self.chains.len()
    }

    /// Returns true if the reader contains no chains.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the reader
    ///
    /// # Output
    ///
    /// Returns `true` if empty, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::Reader;
    ///
    /// let reader = Reader::<chaintools::Chain>::from_path("empty.chain")?;
    ///
    /// if reader.is_empty() {
    ///     println!("No chains found in file");
    /// }
    /// ```
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }
}

#[derive(Clone, Copy)]
enum ParseStrategy {
    Sequential,
    #[cfg(feature = "parallel")]
    Parallel,
}
