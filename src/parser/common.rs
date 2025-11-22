use std::borrow::Cow;
use std::ops::Range;

use memchr::memchr;

use crate::{Block, ChainError, Strand};

/// Metadata for a parsed chain record.
///
/// Contains all the information from a chain header with byte ranges for
/// name fields to enable zero-copy string references. Used internally during
/// parsing before creating the final Chain struct.
///
/// # Fields
///
/// * `score` - Chain alignment score
/// * `t_name` - Byte range of target name in original buffer
/// * `t_size` - Target sequence size
/// * `t_strand` - Target strand orientation
/// * `t_start` - Target start coordinate
/// * `t_end` - Target end coordinate
/// * `q_name` - Byte range of query name in original buffer
/// * `q_size` - Query sequence size
/// * `q_strand` - Query strand orientation
/// * `q_start` - Query start coordinate
/// * `q_end` - Query end coordinate
/// * `id` - Chain identifier
/// * `blocks` - Range of block indices in the global block storage
#[derive(Debug)]
pub(crate) struct ChainMeta {
    pub score: i64,
    pub t_name: Range<usize>,
    pub t_size: u32,
    pub t_strand: Strand,
    pub t_start: u32,
    pub t_end: u32,
    pub q_name: Range<usize>,
    pub q_size: u32,
    pub q_strand: Strand,
    pub q_start: u32,
    pub q_end: u32,
    pub id: u64,
    pub blocks: Range<usize>,
}

/// Reads a line from a byte buffer starting at the given position.
///
/// Finds the next newline character and returns the position after it
/// along with the line content (without trailing newline or carriage return).
///
/// # Arguments
///
/// * `bytes` - Byte buffer to read from
/// * `start` - Starting position in the buffer
///
/// # Output
///
/// Returns a tuple of (next_position, line_slice) where next_position is
/// the position after the newline, and line_slice is the line content
/// without trailing newline/carriage return characters
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::read_line;
///
/// let data = b"hello\nworld\r\n";
/// let (pos, line) = read_line(data, 0);
/// assert_eq!(pos, 6);
/// assert_eq!(line, b"hello");
///
/// let (pos2, line2) = read_line(data, pos);
/// assert_eq!(pos2, 13);
/// assert_eq!(line2, b"world");
/// ```
pub(crate) fn read_line(bytes: &[u8], start: usize) -> (usize, &[u8]) {
    if start >= bytes.len() {
        return (bytes.len(), &bytes[bytes.len()..]);
    }
    match memchr(b'\n', &bytes[start..]) {
        Some(rel) => {
            let end = start + rel;
            let mut line = &bytes[start..end];
            if let Some(stripped) = line.strip_suffix(b"\r") {
                line = stripped;
            }
            (end + 1, line)
        }
        None => {
            let mut line = &bytes[start..];
            if let Some(stripped) = line.strip_suffix(b"\r") {
                line = stripped;
            }
            (bytes.len(), line)
        }
    }
}

/// Checks if a line contains only whitespace characters.
///
/// Returns true if all bytes in the line are ASCII whitespace characters
/// (space, tab, newline, carriage return, etc.).
///
/// # Arguments
///
/// * `line` - Byte slice representing a line
///
/// # Output
///
/// Returns `true` if the line is empty or contains only whitespace,
/// `false` if it contains any non-whitespace characters
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::is_blank;
///
/// assert!(is_blank(b""));
/// assert!(is_blank(b"   \t\n\r"));
/// assert!(!is_blank(b"hello world"));
/// assert!(!is_blank(b"  hello  "));
/// ```
pub(crate) fn is_blank(line: &[u8]) -> bool {
    line.iter().all(|b| b.is_ascii_whitespace())
}

/// Parses a chain header line into ChainMeta.
///
/// Parses the chain header format: "chain score tName tSize tStrand tStart tEnd qName qSize qStrand qStart qEnd id"
/// and extracts all the metadata with byte ranges for name fields.
///
/// # Arguments
///
/// * `line` - Header line bytes (without "chain" prefix validation)
/// * `offset` - Byte offset of this line in the original file for error reporting
///
/// # Output
///
/// Returns `Ok(ChainMeta)` with parsed metadata, or `Err(ChainError)` if
/// the format is invalid or required fields are missing
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::parse_header;
///
/// let header = b"chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1";
/// let meta = parse_header(header, 0)?;
///
/// assert_eq!(meta.score, 100);
/// assert_eq!(meta.t_size, 1000);
/// assert_eq!(meta.id, 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub(crate) fn parse_header(line: &[u8], offset: usize) -> Result<ChainMeta, ChainError> {
    let mut cursor = TokenCursor::new(line);
    let Some((kw_start, kw_end)) = cursor.next() else {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Borrowed("empty header line"),
        });
    };
    if &line[kw_start..kw_end] != b"chain" {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Borrowed("header does not start with 'chain'"),
        });
    }

    let score = parse_i64_token(&mut cursor, line, offset, "score")?;
    let t_name = parse_range_token(&mut cursor, offset, "tName")?;
    let t_size = parse_u32_token(&mut cursor, line, offset, "tSize")?;
    let t_strand = parse_strand_token(&mut cursor, line, offset, "tStrand")?;
    let t_start = parse_u32_token(&mut cursor, line, offset, "tStart")?;
    let t_end = parse_u32_token(&mut cursor, line, offset, "tEnd")?;

    let q_name = parse_range_token(&mut cursor, offset, "qName")?;
    let q_size = parse_u32_token(&mut cursor, line, offset, "qSize")?;
    let q_strand = parse_strand_token(&mut cursor, line, offset, "qStrand")?;
    let q_start = parse_u32_token(&mut cursor, line, offset, "qStart")?;
    let q_end = parse_u32_token(&mut cursor, line, offset, "qEnd")?;
    let id = parse_u64_token(&mut cursor, line, offset, "id")?;

    Ok(ChainMeta {
        score,
        t_name,
        t_size,
        t_strand,
        t_start,
        t_end,
        q_name,
        q_size,
        q_strand,
        q_start,
        q_end,
        id,
        blocks: 0..0,
    })
}

/// Parses a chain block line into a Block struct.
///
/// Parses block lines with format: "size [dt dq]" where dt and dq are optional
/// gap sizes on target and query sequences. If dt/dq are missing, they default to 0.
///
/// # Arguments
///
/// * `line` - Block line bytes
/// * `offset` - Byte offset of this line in the original file for error reporting
///
/// # Output
///
/// Returns `Ok(Block)` with parsed size and gap values, or `Err(ChainError)`
/// if the format is invalid
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::parse_block;
///
/// // Block with gaps
/// let block1 = parse_block(b"100 50 30", 0)?;
/// assert_eq!(block1.size, 100);
/// assert_eq!(block1.dt, 50);
/// assert_eq!(block1.dq, 30);
///
/// // Block without gaps (dt/dq default to 0)
/// let block2 = parse_block(b"200", 0)?;
/// assert_eq!(block2.size, 200);
/// assert_eq!(block2.dt, 0);
/// assert_eq!(block2.dq, 0);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub(crate) fn parse_block(line: &[u8], offset: usize) -> Result<Block, ChainError> {
    let mut cursor = TokenCursor::new(line);
    let size = parse_u32_token(&mut cursor, line, offset, "block size")?;
    let maybe_dt = cursor.next();
    if let Some((dt_s, dt_e)) = maybe_dt {
        let dt = parse_u32(&line[dt_s..dt_e], offset + dt_s, "dt")?;
        let Some((dq_s, dq_e)) = cursor.next() else {
            return Err(ChainError::Format {
                offset,
                msg: Cow::Borrowed("block line missing dq value"),
            });
        };
        let dq = parse_u32(&line[dq_s..dq_e], offset + dq_s, "dq")?;
        Ok(Block { size, dt, dq })
    } else {
        Ok(Block { size, dt: 0, dq: 0 })
    }
}

/// Parses a complete chain within a specific byte range.
///
/// Used by the parallel parser to parse individual chains in separate threads.
/// Parses the header and all block lines within the given range.
///
/// # Arguments
///
/// * `bytes` - Complete file buffer
/// * `range` - Byte range containing exactly one chain
///
/// # Output
///
/// Returns `Ok((ChainMeta, Vec<Block>))` with the parsed metadata and blocks,
/// or `Err(ChainError)` if parsing fails
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::parse_chain_in_range;
///
/// let data = b"chain 1 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n10\n\n";
/// let (meta, blocks) = parse_chain_in_range(data, 0..data.len())?;
///
/// assert_eq!(meta.score, 1);
/// assert_eq!(blocks.len(), 1);
/// ```
#[cfg(feature = "parallel")]
pub(crate) fn parse_chain_in_range(
    bytes: &[u8],
    range: Range<usize>,
) -> Result<(ChainMeta, Vec<Block>), ChainError> {
    let slice = &bytes[range.clone()];
    let mut pos = 0usize;
    let (next_pos, header_line) = read_line(slice, pos);
    let header_offset = range.start + pos;
    let mut meta = parse_header(header_line, header_offset)?;
    pos = next_pos;
    let mut blocks = Vec::new();
    while pos < slice.len() {
        let block_line_start = pos;
        let (next, line) = read_line(slice, pos);
        pos = next;
        if is_blank(line) {
            break;
        }
        let block = parse_block(line, range.start + block_line_start)?;
        blocks.push(block);
    }
    if blocks.is_empty() {
        return Err(ChainError::Format {
            offset: range.start,
            msg: Cow::Borrowed("chain without any alignment blocks"),
        });
    }
    meta.blocks = 0..blocks.len();
    Ok((meta, blocks))
}

/// Cursor for iterating over whitespace-separated tokens in a line.
///
/// Provides efficient tokenization of chain header and block lines by
/// tracking position and returning byte ranges for each token.
///
/// # Fields
///
/// * `line` - The line being tokenized
/// * `pos` - Current position in the line
///
/// # Examples
///
/// ```ignore
/// use chaintools::parser::common::TokenCursor;
///
/// let line = b"chain 100 chr1 1000";
/// let mut cursor = TokenCursor::new(line);
///
/// let token1 = cursor.next(); // Some((0, 5)) - "chain"
/// let token2 = cursor.next(); // Some((6, 9)) - "100"
/// let token3 = cursor.next(); // Some((10, 14)) - "chr1"
/// ```
pub(crate) struct TokenCursor<'a> {
    line: &'a [u8],
    pos: usize,
}

impl<'a> TokenCursor<'a> {
    /// Creates a new token cursor for the given line.
    ///
    /// # Arguments
    ///
    /// * `line` - Line bytes to tokenize
    ///
    /// # Output
    ///
    /// Returns a new `TokenCursor` starting at position 0
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::parser::common::TokenCursor;
    ///
    /// let line = b"hello world";
    /// let cursor = TokenCursor::new(line);
    /// ```
    fn new(line: &'a [u8]) -> Self {
        TokenCursor { line, pos: 0 }
    }

    /// Returns the next token range in the line.
    ///
    /// Skips leading whitespace and returns the start and end positions
    /// of the next token. Returns None when no more tokens are available.
    ///
    /// # Arguments
    ///
    /// * `&mut self` - Mutable reference to the cursor
    ///
    /// # Output
    ///
    /// Returns `Some((start, end))` for the next token, or `None` if at end
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::parser::common::TokenCursor;
    ///
    /// let line = b"  hello   world  ";
    /// let mut cursor = TokenCursor::new(line);
    ///
    /// let token1 = cursor.next(); // Some((2, 7)) - "hello"
    /// let token2 = cursor.next(); // Some((10, 15)) - "world"
    /// let token3 = cursor.next(); // None
    /// ```
    fn next(&mut self) -> Option<(usize, usize)> {
        let len = self.line.len();
        while self.pos < len && self.line[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
        if self.pos >= len {
            return None;
        }
        let start = self.pos;
        while self.pos < len && !self.line[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
        Some((start, self.pos))
    }
}

fn parse_i64_token(
    cursor: &mut TokenCursor<'_>,
    line: &[u8],
    offset: usize,
    label: &'static str,
) -> Result<i64, ChainError> {
    let Some((s, e)) = cursor.next() else {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} missing")),
        });
    };
    parse_i64(&line[s..e], offset + s, label)
}

fn parse_u64_token(
    cursor: &mut TokenCursor<'_>,
    line: &[u8],
    offset: usize,
    label: &'static str,
) -> Result<u64, ChainError> {
    let Some((s, e)) = cursor.next() else {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} missing")),
        });
    };
    parse_u64(&line[s..e], offset + s, label)
}

fn parse_u32_token(
    cursor: &mut TokenCursor<'_>,
    line: &[u8],
    offset: usize,
    label: &'static str,
) -> Result<u32, ChainError> {
    let val = parse_u64_token(cursor, line, offset, label)?;
    if val > u32::MAX as u64 {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} exceeds u32")),
        });
    }
    Ok(val as u32)
}

fn parse_range_token(
    cursor: &mut TokenCursor<'_>,
    offset: usize,
    label: &'static str,
) -> Result<Range<usize>, ChainError> {
    let Some((s, e)) = cursor.next() else {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} missing")),
        });
    };
    Ok((offset + s)..(offset + e))
}

fn parse_strand_token(
    cursor: &mut TokenCursor<'_>,
    line: &[u8],
    offset: usize,
    label: &'static str,
) -> Result<Strand, ChainError> {
    let Some((s, e)) = cursor.next() else {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} missing")),
        });
    };
    match line[s] {
        b'+' if e - s == 1 => Ok(Strand::Plus),
        b'-' if e - s == 1 => Ok(Strand::Minus),
        _ => Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{label} must be '+' or '-'")),
        }),
    }
}

fn parse_u64(data: &[u8], offset: usize, ctx: &str) -> Result<u64, ChainError> {
    let mut value: u64 = 0;
    if data.is_empty() {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{ctx} is empty")),
        });
    }
    for (i, &b) in data.iter().enumerate() {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return Err(ChainError::Format {
                offset: offset + i,
                msg: Cow::Owned(format!("{ctx} contains a non-digit")),
            });
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit as u64))
            .ok_or_else(|| ChainError::Format {
                offset: offset + i,
                msg: Cow::Owned(format!("{ctx} overflows u64")),
            })?;
    }
    Ok(value)
}

fn parse_u32(data: &[u8], offset: usize, ctx: &str) -> Result<u32, ChainError> {
    let val = parse_u64(data, offset, ctx)?;
    if val > u32::MAX as u64 {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{ctx} exceeds u32")),
        });
    }
    Ok(val as u32)
}

fn parse_i64(data: &[u8], offset: usize, ctx: &str) -> Result<i64, ChainError> {
    if data.is_empty() {
        return Err(ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{ctx} is empty")),
        });
    }
    let (negative, digits) = if data[0] == b'-' {
        (true, &data[1..])
    } else {
        (false, data)
    };
    let unsigned = parse_u64(digits, offset + if negative { 1 } else { 0 }, ctx)?;
    if negative {
        let val = (unsigned as i64)
            .checked_neg()
            .ok_or_else(|| ChainError::Format {
                offset,
                msg: Cow::Owned(format!("{ctx} underflows i64")),
            })?;
        Ok(val)
    } else {
        unsigned.try_into().map_err(|_| ChainError::Format {
            offset,
            msg: Cow::Owned(format!("{ctx} overflows i64")),
        })
    }
}
