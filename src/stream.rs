// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::io::{BufRead, BufReader};
use std::path::Path;

#[cfg(feature = "gzip")]
use flate2::read::MultiGzDecoder;

use crate::block::Block;
use crate::chain::Strand;
use crate::error::ChainError;
use crate::parser::common::{is_blank, parse_block, parse_header_with_default_id};
#[cfg(not(feature = "gzip"))]
use crate::storage::gzip_feature_error;
use crate::storage::is_gz_path;

/// Owned chain representation for streaming mode.
///
/// Stores all data as owned `Vec<u8>` bytes instead of zero-copy references.
/// Use this when you need to process chains sequentially without keeping
/// the original file buffer in memory.
///
/// # Fields
///
/// * `score` - Alignment score
/// * `reference_name` - Reference sequence name
/// * `reference_size` - Reference sequence length
/// * `reference_strand` - Reference strand orientation
/// * `reference_start` - Reference start coordinate
/// * `reference_end` - Reference end coordinate
/// * `query_name` - Query sequence name
/// * `query_size` - Query sequence length
/// * `query_strand` - Query strand orientation
/// * `query_start` - Query start coordinate
/// * `query_end` - Query end coordinate
/// * `id` - Chain identifier
/// * `blocks` - Alignment blocks
///
/// # Examples
///
/// ```ignore
/// use chaintools::stream::OwnedChain;
///
/// let chain = OwnedChain {
///     score: 100,
///     reference_name: b"chr1".to_vec(),
///     reference_size: 1000,
///     reference_strand: chaintools::Strand::Plus,
///     reference_start: 0,
///     reference_end: 100,
///     query_name: b"chr2".to_vec(),
///     query_size: 1000,
///     query_strand: chaintools::Strand::Plus,
///     query_start: 0,
///     query_end: 100,
///     id: 1,
///     blocks: vec![],
/// };
/// ```
#[derive(Debug, Clone)]
pub struct OwnedChain {
    pub score: i64,
    pub reference_name: Vec<u8>,
    pub reference_size: u32,
    pub reference_strand: Strand,
    pub reference_start: u32,
    pub reference_end: u32,
    pub query_name: Vec<u8>,
    pub query_size: u32,
    pub query_strand: Strand,
    pub query_start: u32,
    pub query_end: u32,
    pub id: u64,
    pub blocks: Vec<Block>,
}

/// Owned chain header representation for streaming mode.
///
/// This is useful for callers that can decide whether to keep or discard a
/// record from header-level fields before parsing its block lines. Use this when
/// you only need metadata without the full chain blocks yet.
///
/// # Fields
///
/// * `offset` - Byte offset in the input stream
/// * `score` - Alignment score
/// * `reference_name` - Reference sequence name
/// * `reference_size` - Reference sequence length
/// * `reference_strand` - Reference strand orientation
/// * `reference_start` - Reference start coordinate
/// * `reference_end` - Reference end coordinate
/// * `query_name` - Query sequence name
/// * `query_size` - Query sequence length
/// * `query_strand` - Query strand orientation
/// * `query_start` - Query start coordinate
/// * `query_end` - Query end coordinate
/// * `id` - Chain identifier
#[derive(Debug, Clone)]
pub struct OwnedChainHeader {
    pub offset: usize,
    pub score: i64,
    pub reference_name: Vec<u8>,
    pub reference_size: u32,
    pub reference_strand: Strand,
    pub reference_start: u32,
    pub reference_end: u32,
    pub query_name: Vec<u8>,
    pub query_size: u32,
    pub query_strand: Strand,
    pub query_start: u32,
    pub query_end: u32,
    pub id: u64,
}

/// Next item encountered while streaming chain text.
///
/// Metadata lines are returned without their trailing newline characters.
/// Used by StreamingReader to yield either comment lines or chain headers.
///
/// # Variants
///
/// * `MetaLine` - A comment/metadata line (starting with #)
/// * `Header` - A complete chain header with metadata
///
/// # Examples
///
/// ```ignore
/// use chaintools::stream::{StreamingReader, StreamItem};
/// use std::io::BufReader;
///
/// let data = b"#comment\nchain 1 chr1 100 + 0 100 chr2 100 + 0 100 1\n10\n\n";
/// let reader = BufReader::new(&data[..]);
/// let mut stream = StreamingReader::new(reader);
///
/// while let Some(item) = stream.next_item()? {
///     match item {
///         StreamItem::MetaLine(line) => println!("Meta: {:?}", line),
///         StreamItem::Header(header) => println!("Chain: {}", header.score),
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub enum StreamItem {
    MetaLine(Vec<u8>),
    Header(OwnedChainHeader),
}

impl OwnedChainHeader {
    /// Attach parsed blocks and produce a complete owned chain.
    ///
    /// Combines the header metadata with parsed alignment blocks to create
    /// a full owned chain representation.
    ///
    /// # Arguments
    ///
    /// * `self` - Owned chain header
    /// * `blocks` - Parsed alignment blocks
    ///
    /// # Output
    ///
    /// Returns a complete `OwnedChain`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::stream::{OwnedChainHeader, StreamingReader};
    /// use std::io::BufReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    ///
    /// if let Some(header) = stream.next_header()? {
    ///     let blocks = stream.read_blocks(header.offset)?;
    ///     let chain = header.into_chain(blocks);
    /// }
    /// ```
    pub fn into_chain(self, blocks: Vec<Block>) -> OwnedChain {
        OwnedChain {
            score: self.score,
            reference_name: self.reference_name,
            reference_size: self.reference_size,
            reference_strand: self.reference_strand,
            reference_start: self.reference_start,
            reference_end: self.reference_end,
            query_name: self.query_name,
            query_size: self.query_size,
            query_strand: self.query_strand,
            query_start: self.query_start,
            query_end: self.query_end,
            id: self.id,
            blocks,
        }
    }
}

/// Streaming reader over any `BufRead`, suitable for stdin/pipes.
///
/// Provides low-memory sequential parsing of chain files without loading
/// the entire file into memory. Ideal for processing large files
/// via stdin/stdout pipes.
///
/// # Type Parameters
///
/// * `R` - A buffered reader type implementing `BufRead`
///
/// # Fields
///
/// * `reader` - The underlying buffered reader
/// * `buf` - Internal buffer for line reading (8KB capacity)
/// * `offset` - Current byte offset in the input
/// * `next_id` - Counter for auto-generated chain IDs
///
/// # Examples
///
/// ```ignore
/// use std::io::BufReader;
/// use chaintools::stream::StreamingReader;
///
/// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
/// let reader = BufReader::new(&data[..]);
/// let mut stream = StreamingReader::new(reader);
///
/// while let Some(chain) = stream.next_chain()? {
///     println!("Chain score: {}", chain.score);
/// }
/// ```
pub struct StreamingReader<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    offset: usize,
    next_id: u64,
}

impl<R: BufRead> StreamingReader<R> {
    /// Creates a new streaming reader with the given buffered reader.
    ///
    /// Initializes an internal buffer with 8KB capacity for efficient line reading
    /// and sets the byte offset to zero. Suitable for reading from stdin, pipes,
    /// or any other buffered input source.
    ///
    /// # Arguments
    ///
    /// * `reader` - A buffered reader implementing `BufRead` trait
    ///
    /// # Output
    ///
    /// Returns a new `StreamingReader<R>` instance ready to parse chain records
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream_reader = StreamingReader::new(reader);
    /// ```
    pub fn new(reader: R) -> Self {
        StreamingReader {
            reader,
            buf: Vec::with_capacity(8 * 1024),
            offset: 0,
            next_id: 1,
        }
    }

    /// Sets the next generated id used for chain headers that omit an id.
    ///
    /// Explicit ids in the input do not affect this counter. This mirrors UCSC's
    /// chain reader behavior and lets callers preserve generated-id continuity
    /// across multiple input streams when needed.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    /// * `next_id` - The next id to generate for headers without an explicit id
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    /// stream.set_next_generated_id(100);
    /// ```
    pub fn set_next_generated_id(&mut self, next_id: u64) {
        self.next_id = next_id;
    }

    /// Returns the next id that will be generated for a header without an id.
    ///
    /// # Arguments
    ///
    /// * `&self` - Reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns the next chain id that will be auto-generated
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let stream = StreamingReader::new(reader);
    ///
    /// assert_eq!(stream.next_generated_id(), 1);
    /// ```
    pub fn next_generated_id(&self) -> u64 {
        self.next_id
    }

    /// Pull the next chain from the stream. Returns `Ok(None)` at EOF.
    ///
    /// Reads and parses the next complete chain record from the input stream.
    /// Skips blank lines, parses the header line followed by
    /// alignment block lines until a blank line or EOF is encountered.
    /// Returns an error if a chain has no alignment blocks.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns `Ok(Some(OwnedChain))` when a chain is successfully parsed,
    /// `Ok(None)` when EOF is reached, or `Err(ChainError)` for parsing failures
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n10\n10\n\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream_reader = StreamingReader::new(reader);
    ///
    /// match stream_reader.next_chain()? {
    ///     Some(chain) => println!("Found chain with score: {}", chain.score),
    ///     None => println!("No chains found"),
    /// }
    /// ```
    pub fn next_chain(&mut self) -> Result<Option<OwnedChain>, ChainError> {
        let Some(header) = self.next_header()? else {
            return Ok(None);
        };
        let offset = header.offset;
        let blocks = self.read_blocks(offset)?;
        Ok(Some(header.into_chain(blocks)))
    }

    /// Pull the next chain header from the stream. Returns `Ok(None)` at EOF.
    ///
    /// After this returns `Some`, callers must either call `read_blocks` or
    /// `skip_blocks` before requesting another header. This allows
    /// callers to decide whether to keep the record based on header-level
    /// fields before committing to parsing the block lines.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns `Ok(Some(OwnedChainHeader))` with header metadata, or `Ok(None)` at EOF
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    ///
    /// while let Some(header) = stream.next_header()? {
    ///     println!("Chain score: {}", header.score);
    ///     let blocks = stream.read_blocks(header.offset)?;
    /// }
    /// ```
    pub fn next_header(&mut self) -> Result<Option<OwnedChainHeader>, ChainError> {
        while let Some(item) = self.next_item()? {
            if let StreamItem::Header(header) = item {
                return Ok(Some(header));
            }
        }
        Ok(None)
    }

    /// Pull the next metadata line or chain header from the stream.
    ///
    /// Blank lines are skipped. Metadata lines beginning with `#` are surfaced
    /// as `StreamItem::MetaLine`. Header items still require a subsequent call
    /// to `read_blocks` or `skip_blocks`.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns `Ok(Some(StreamItem))` with the next item, or `Ok(None)` at EOF
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::{StreamingReader, StreamItem};
    ///
    /// let data = b"#meta\nchain 1 chr1 100 + 0 100 chr2 100 + 0 100 1\n10\n\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    ///
    /// while let Some(item) = stream.next_item()? {
    ///     match item {
    ///         StreamItem::MetaLine(line) => println!("Meta: {:?}", line),
    ///         StreamItem::Header(h) => println!("Chain: {}", h.score),
    ///     }
    /// }
    /// ```
    pub fn next_item(&mut self) -> Result<Option<StreamItem>, ChainError> {
        loop {
            let Some((line_start, line)) = self.read_trimmed_line()? else {
                return Ok(None);
            };
            if line.is_empty() || is_blank(line) {
                continue;
            }
            if line[0] == b'#' {
                return Ok(Some(StreamItem::MetaLine(line.to_vec())));
            }
            let header_line = line.to_vec();
            let header = self.parse_header_line(line_start, &header_line)?;
            return Ok(Some(StreamItem::Header(header)));
        }
    }

    /// Read and parse block lines for the most recently returned header.
    ///
    /// Parses all block lines following the most recently returned header
    /// until a blank line or EOF. Must be called after `next_header`.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    /// * `header_offset` - Byte offset of the header (from the header struct)
    ///
    /// # Output
    ///
    /// Returns `Ok(Vec<Block>)` with all parsed blocks
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    ///
    /// if let Some(header) = stream.next_header()? {
    ///     let blocks = stream.read_blocks(header.offset)?;
    ///     println!("Parsed {} blocks", blocks.len());
    /// }
    /// ```
    pub fn read_blocks(&mut self, header_offset: usize) -> Result<Vec<Block>, ChainError> {
        let mut blocks = Vec::new();
        loop {
            let Some((line_start, line)) = self.read_trimmed_line()? else {
                break;
            };
            if is_blank(line) {
                break;
            }
            let block = parse_block(line, line_start)?;
            blocks.push(block);
        }

        if blocks.is_empty() {
            return Err(ChainError::Format {
                offset: header_offset,
                msg: "chain without any alignment blocks".into(),
            });
        }

        Ok(blocks)
    }

    /// Skip block lines for the most recently returned header without parsing.
    ///
    /// Advances past block lines without parsing them. Use this when you
    /// have decided to discard the chain from header-level filtering.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns `Ok(())` on success
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream = StreamingReader::new(reader);
    ///
    /// while let Some(header) = stream.next_header()? {
    ///     if header.score < 100 {
    ///         stream.skip_blocks()?;
    ///         continue;
    ///     }
    ///     let blocks = stream.read_blocks(header.offset)?;
    /// }
    /// ```
    pub fn skip_blocks(&mut self) -> Result<(), ChainError> {
        while let Some((_, line)) = self.read_trimmed_line()? {
            if is_blank(line) {
                break;
            }
        }
        Ok(())
    }

    /// Reads a line from the underlying reader and trims newline characters.
    ///
    /// Returns the starting byte offset and the line content without trailing
    /// newline or carriage return characters. Returns None when EOF is reached.
    /// The internal buffer is cleared and reused for each call to minimize allocations.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the streaming reader
    ///
    /// # Output
    ///
    /// Returns `Ok(Some((offset, line_slice)))` with the byte offset and trimmed line,
    /// `Ok(None)` when EOF is reached, or `Err(ChainError)` for read failures
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::io::BufReader;
    /// use chaintools::stream::StreamingReader;
    ///
    /// let data = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1\n";
    /// let reader = BufReader::new(&data[..]);
    /// let mut stream_reader = StreamingReader::new(reader);
    ///
    /// if let Some((offset, line)) = stream_reader.read_trimmed_line()? {
    ///     println!("Line at offset {}: {:?}", offset, line);
    /// }
    /// ```
    fn read_trimmed_line(&mut self) -> Result<Option<(usize, &[u8])>, ChainError> {
        self.buf.clear();
        let start = self.offset;
        let n = self.reader.read_until(b'\n', &mut self.buf)?;
        if n == 0 {
            return Ok(None);
        }
        self.offset += n;
        if let Some(b'\n') = self.buf.last() {
            self.buf.pop();
        }
        if let Some(b'\r') = self.buf.last() {
            self.buf.pop();
        }
        Ok(Some((start, self.buf.as_slice())))
    }

    fn parse_header_line(
        &mut self,
        header_offset: usize,
        header_line: &[u8],
    ) -> Result<OwnedChainHeader, ChainError> {
        let default_id = self.next_id;
        let (meta, has_explicit_id) =
            parse_header_with_default_id(header_line, header_offset, default_id)?;
        let reference_name = slice_name(header_line, header_offset, meta.reference_name.clone())?;
        let query_name = slice_name(header_line, header_offset, meta.query_name.clone())?;
        if !has_explicit_id {
            self.next_id = default_id
                .checked_add(1)
                .ok_or_else(|| ChainError::Format {
                    offset: header_offset,
                    msg: "generated chain id overflows u64".into(),
                })?;
        }

        Ok(OwnedChainHeader {
            offset: header_offset,
            score: meta.score,
            reference_name,
            reference_size: meta.reference_size,
            reference_strand: meta.reference_strand,
            reference_start: meta.reference_start,
            reference_end: meta.reference_end,
            query_name,
            query_size: meta.query_size,
            query_strand: meta.query_strand,
            query_start: meta.query_start,
            query_end: meta.query_end,
            id: meta.id,
        })
    }
}

impl StreamingReader<Box<dyn BufRead>> {
    /// Convenience to open a path for streaming (will decompress gzip if enabled).
    ///
    /// Opens the file at the given path and creates a buffered streaming reader.
    /// If the path has a .gz extension and the gzip feature is enabled, automatically
    /// wraps the file in a gzip decoder. Returns an error if gzip is required but
    /// the feature is not enabled.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the chain file (plain text or gzip compressed)
    ///
    /// # Output
    ///
    /// Returns `Ok(StreamingReader)` ready to parse the file, or `Err(ChainError)`
    /// if the file cannot be opened or gzip decompression is unavailable
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::stream::StreamingReader;
    ///
    /// // Open a plain text chain file
    /// let mut reader = StreamingReader::from_path("example.chain")?;
    ///
    /// // Open a gzip compressed chain file (requires gzip feature)
    /// let mut gz_reader = StreamingReader::from_path("example.chain.gz")?;
    ///
    /// while let Some(chain) = reader.next_chain()? {
    ///     println!("Processing chain: {}", chain.score);
    /// }
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let path = path.as_ref();
        if is_gz_path(path) {
            #[cfg(feature = "gzip")]
            {
                let file = std::fs::File::open(path)?;
                let reader = BufReader::new(file);
                let decoder = MultiGzDecoder::new(reader);
                return Ok(StreamingReader::new(Box::new(BufReader::new(decoder))));
            }
            #[cfg(not(feature = "gzip"))]
            {
                return Err(gzip_feature_error());
            }
        }

        let file = std::fs::File::open(path)?;
        Ok(StreamingReader::new(Box::new(BufReader::new(file))))
    }
}

/// Extracts a name slice from a line using absolute byte positions.
///
/// Converts absolute byte positions from the parser into relative positions
/// within the current line buffer, validates bounds, and returns the extracted
/// name as a new `Vec<u8>`. Returns an error if the slice would underflow or
/// exceed the line boundaries.
///
/// # Arguments
///
/// * `line` - The line buffer containing the name
/// * `line_start` - Absolute byte offset where this line starts in the input
/// * `range` - Absolute byte range of the name in the original input
///
/// # Output
///
/// Returns `Ok(Vec<u8>)` containing the extracted name bytes, or `Err(ChainError)`
/// if the slice calculation would underflow or exceed line boundaries
///
/// # Examples
///
/// ```ignore
/// use chaintools::stream::slice_name;
///
/// let line = b"chain 1 chr1 1000 + 0 1000 chr2 1000 + 0 1000 1";
/// let line_start = 100; // Assume this line starts at byte 100 in input
/// let name_range = 107..111; // chr1 is at bytes 107-111 in original input
///
/// let name = slice_name(line, line_start, name_range)?;
/// assert_eq!(name, b"chr1");
/// ```
fn slice_name(
    line: &[u8],
    line_start: usize,
    range: std::ops::Range<usize>,
) -> Result<Vec<u8>, ChainError> {
    let rel_start = range
        .start
        .checked_sub(line_start)
        .ok_or_else(|| ChainError::Format {
            offset: line_start,
            msg: "name slice underflow".into(),
        })?;
    let rel_end = range
        .end
        .checked_sub(line_start)
        .ok_or_else(|| ChainError::Format {
            offset: line_start,
            msg: "name slice underflow".into(),
        })?;
    if rel_end > line.len() || rel_start > rel_end {
        return Err(ChainError::Format {
            offset: line_start,
            msg: "name slice out of bounds".into(),
        });
    }
    Ok(line[rel_start..rel_end].to_vec())
}
