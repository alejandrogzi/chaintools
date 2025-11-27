use std::io::{BufRead, BufReader};
use std::path::Path;

#[cfg(feature = "gzip")]
use flate2::read::MultiGzDecoder;

use crate::block::Block;
use crate::chain::Strand;
use crate::error::ChainError;
use crate::parser::common::{is_blank, parse_block, parse_header};
#[cfg(not(feature = "gzip"))]
use crate::storage::gzip_feature_error;
use crate::storage::is_gz_path;

/// Owned representation for streaming mode.
#[derive(Debug, Clone)]
pub struct OwnedChain {
    pub score: i64,
    pub t_name: Vec<u8>,
    pub t_size: u32,
    pub t_strand: Strand,
    pub t_start: u32,
    pub t_end: u32,
    pub q_name: Vec<u8>,
    pub q_size: u32,
    pub q_strand: Strand,
    pub q_start: u32,
    pub q_end: u32,
    pub id: u64,
    pub blocks: Vec<Block>,
}

/// Streaming reader over any `BufRead`, suitable for stdin/pipes.
pub struct StreamingReader<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    offset: usize,
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
        }
    }

    /// Pull the next chain from the stream. Returns `Ok(None)` at EOF.
    ///
    /// Reads and parses the next complete chain record from the input stream.
    /// Skips blank lines and comments, parses the header line followed by
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
        // find next non-blank line
        let (header_offset, header_line) = loop {
            match self.read_trimmed_line()? {
                None => return Ok(None),
                Some((_, [])) => continue,
                Some((_start, line)) if is_blank(line) => continue,
                Some((start, line)) => break (start, line),
            }
        };

        let meta = parse_header(header_line, header_offset)?;
        let t_name = slice_name(header_line, header_offset, meta.t_name.clone())?;
        let q_name = slice_name(header_line, header_offset, meta.q_name.clone())?;

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

        Ok(Some(OwnedChain {
            score: meta.score,
            t_name,
            t_size: meta.t_size,
            t_strand: meta.t_strand,
            t_start: meta.t_start,
            t_end: meta.t_end,
            q_name,
            q_size: meta.q_size,
            q_strand: meta.q_strand,
            q_start: meta.q_start,
            q_end: meta.q_end,
            id: meta.id,
            blocks,
        }))
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
                let decoder = MultiGzDecoder::new(file);
                return Ok(StreamingReader::new(Box::new(BufReader::new(
                    decoder,
                ))));
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
/// name as a new Vec<u8>. Returns an error if the slice would underflow or
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
