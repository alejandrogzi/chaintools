// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::io::Write;

use crate::{Block, ChainError, OwnedChain, OwnedChainHeader, Strand};

/// Writes a chain in dense format to a writer.
///
/// Writes the chain header followed by block lines in the dense format:
/// `size [dt dq]` on each line, where dt and dq are only written for non-final blocks.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `chain` - The owned chain to write
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` on failure
///
/// # Examples
///
/// ```ignore
/// use chaintools::{io::stream::OwnedChain, io::writer::write_chain_dense};
/// use std::io::Cursor;
///
/// let chain = OwnedChain {
///     score: 100,
///     reference_name: b"chr1".to_vec(),
///     reference_size: 100,
///     reference_strand: chaintools::Strand::Plus,
///     reference_start: 0,
///     reference_end: 100,
///     query_name: b"chr2".to_vec(),
///     query_size: 100,
///     query_strand: chaintools::Strand::Plus,
///     query_start: 0,
///     query_end: 100,
///     id: 1,
///     blocks: vec![],
/// };
///
/// let mut buf = Vec::new();
/// write_chain_dense(&mut buf, &chain)?;
/// ```
pub fn write_chain_dense<W: Write>(writer: &mut W, chain: &OwnedChain) -> Result<(), ChainError> {
    write_chain_header(writer, chain)?;
    write_dense_blocks(writer, &chain.blocks)
}

/// Writes a chain in dense format with an overridden chain ID.
///
/// Identical to [`write_chain_dense`], except the header's `id` field is
/// replaced by the provided `id` rather than the chain's own id. This is used
/// by `merge --rename` to reassign sequential IDs without mutating the chain.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `chain` - The owned chain to write
/// * `id` - The chain ID to emit in the header
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` on failure
pub fn write_chain_dense_with_id<W: Write>(
    writer: &mut W,
    chain: &OwnedChain,
    id: u64,
) -> Result<(), ChainError> {
    write_chain_header_with_id(writer, chain, id)?;
    write_dense_blocks(writer, &chain.blocks)
}

/// Writes a chain header line.
///
/// Writes the "chain" header line with all chain metadata in the standard format:
/// `chain score tName tSize tStrand tStart tEnd qName qSize qStrand qStart qEnd id`
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `chain` - The chain-like object to write (any type implementing ChainLike)
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` on failure
///
/// # Examples
///
/// ```ignore
/// use chaintools::{io::stream::OwnedChainHeader, io::writer::write_chain_header};
/// use std::io::Cursor;
///
/// let header = OwnedChainHeader {
///     offset: 0,
///     score: 100,
///     reference_name: b"chr1".to_vec(),
///     reference_size: 100,
///     reference_strand: chaintools::Strand::Plus,
///     reference_start: 0,
///     reference_end: 100,
///     query_name: b"chr2".to_vec(),
///     query_size: 100,
///     query_strand: chaintools::Strand::Plus,
///     query_start: 0,
///     query_end: 100,
///     id: 1,
/// };
///
/// let mut buf = Vec::new();
/// write_chain_header(&mut buf, &header)?;
/// ```
pub fn write_chain_header<W, C>(writer: &mut W, chain: &C) -> Result<(), ChainError>
where
    W: Write,
    C: ChainLike,
{
    write_chain_header_with_id(writer, chain, chain.id())
}

/// Writes a chain header line with an overridden chain ID.
///
/// Identical to [`write_chain_header`], except the trailing `id` field is set
/// to the provided `id` instead of the chain's own id. The chain's other
/// fields are written unchanged.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `chain` - The chain-like object to write (any type implementing ChainLike)
/// * `id` - The chain ID to emit in the header
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` on failure
pub fn write_chain_header_with_id<W, C>(
    writer: &mut W,
    chain: &C,
    id: u64,
) -> Result<(), ChainError>
where
    W: Write,
    C: ChainLike,
{
    write!(writer, "chain {} ", chain.score())?;
    writer.write_all(chain.reference_name())?;
    write!(
        writer,
        " {} {} {} {} ",
        chain.reference_size(),
        strand_to_byte(chain.reference_strand()) as char,
        chain.reference_start(),
        chain.reference_end()
    )?;
    writer.write_all(chain.query_name())?;
    writeln!(
        writer,
        " {} {} {} {} {}",
        chain.query_size(),
        strand_to_byte(chain.query_strand()) as char,
        chain.query_start(),
        chain.query_end(),
        id
    )?;
    Ok(())
}

/// Writes block lines in dense format.
///
/// Writes each block on its own line: `size [dt dq]` where dt (target gap)
/// and dq (query gap) are only included for non-final blocks. Adds a blank
/// line after the last block.
///
/// # Arguments
///
/// * `writer` - Output writer
/// * `blocks` - Slice of blocks to write
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(ChainError)` on failure
///
/// # Examples
///
/// ```ignore
/// use chaintools::{Block, io::writer::write_dense_blocks};
/// use std::io::Cursor;
///
/// let blocks = vec![
///     Block { size: 100, gap_reference: 10, gap_query: 5 },
///     Block { size: 50, gap_reference: 0, gap_query: 0 },
/// ];
///
/// let mut buf = Vec::new();
/// write_dense_blocks(&mut buf, &blocks)?;
/// ```
pub fn write_dense_blocks<W: Write>(writer: &mut W, blocks: &[Block]) -> Result<(), ChainError> {
    for (index, block) in blocks.iter().enumerate() {
        write!(writer, "{}", block.size)?;
        if index + 1 < blocks.len() {
            write!(writer, "\t{}\t{}", block.gap_reference, block.gap_query)?;
        }
        writer.write_all(b"\n")?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

/// Writes one metadata/comment line followed by a newline.
///
/// The line is not modified: callers that want comment output should include
/// the leading `#` themselves. Embedded newlines are rejected so one call cannot
/// accidentally produce multiple output records.
pub fn write_metadata_line<W: Write>(writer: &mut W, line: &[u8]) -> Result<(), ChainError> {
    if line.contains(&b'\n') {
        return Err(writer_error("metadata line contains an embedded newline"));
    }
    writer.write_all(line)?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Writes multiple metadata/comment lines.
pub fn write_metadata_lines<W, I, B>(writer: &mut W, lines: I) -> Result<(), ChainError>
where
    W: Write,
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    for line in lines {
        write_metadata_line(writer, line.as_ref())?;
    }
    Ok(())
}

/// Trait for types that can be written as chain records.
///
/// Provides a generic interface for writing both `OwnedChain` and
/// `OwnedChainHeader` as chain records. Implementors must provide all
/// the metadata fields from the chain header line.
///
/// # Examples
///
/// ```ignore
/// use chaintools::io::writer::ChainLike;
/// use chaintools::{Strand, Block};
///
/// struct MyChain {
///     score: i64,
///     reference_name: Vec<u8>,
///     // ... other fields
/// }
///
/// impl ChainLike for MyChain {
///     fn score(&self) -> i64 { self.score }
///     fn reference_name(&self) -> &[u8] { &self.reference_name }
///     // ... implement other methods
/// }
/// ```
pub trait ChainLike {
    fn score(&self) -> i64;
    fn reference_name(&self) -> &[u8];
    fn reference_size(&self) -> u32;
    fn reference_strand(&self) -> Strand;
    fn reference_start(&self) -> u32;
    fn reference_end(&self) -> u32;
    fn query_name(&self) -> &[u8];
    fn query_size(&self) -> u32;
    fn query_strand(&self) -> Strand;
    fn query_start(&self) -> u32;
    fn query_end(&self) -> u32;
    fn id(&self) -> u64;
}

impl ChainLike for OwnedChain {
    fn score(&self) -> i64 {
        self.score
    }

    fn reference_name(&self) -> &[u8] {
        &self.reference_name
    }

    fn reference_size(&self) -> u32 {
        self.reference_size
    }

    fn reference_strand(&self) -> Strand {
        self.reference_strand
    }

    fn reference_start(&self) -> u32 {
        self.reference_start
    }

    fn reference_end(&self) -> u32 {
        self.reference_end
    }

    fn query_name(&self) -> &[u8] {
        &self.query_name
    }

    fn query_size(&self) -> u32 {
        self.query_size
    }

    fn query_strand(&self) -> Strand {
        self.query_strand
    }

    fn query_start(&self) -> u32 {
        self.query_start
    }

    fn query_end(&self) -> u32 {
        self.query_end
    }

    fn id(&self) -> u64 {
        self.id
    }
}

impl ChainLike for OwnedChainHeader {
    fn score(&self) -> i64 {
        self.score
    }

    fn reference_name(&self) -> &[u8] {
        &self.reference_name
    }

    fn reference_size(&self) -> u32 {
        self.reference_size
    }

    fn reference_strand(&self) -> Strand {
        self.reference_strand
    }

    fn reference_start(&self) -> u32 {
        self.reference_start
    }

    fn reference_end(&self) -> u32 {
        self.reference_end
    }

    fn query_name(&self) -> &[u8] {
        &self.query_name
    }

    fn query_size(&self) -> u32 {
        self.query_size
    }

    fn query_strand(&self) -> Strand {
        self.query_strand
    }

    fn query_start(&self) -> u32 {
        self.query_start
    }

    fn query_end(&self) -> u32 {
        self.query_end
    }

    fn id(&self) -> u64 {
        self.id
    }
}

/// Converts a Strand to a byte representation.
fn strand_to_byte(strand: Strand) -> u8 {
    match strand {
        Strand::Plus => b'+',
        Strand::Minus => b'-',
    }
}

fn writer_error(message: impl Into<String>) -> ChainError {
    ChainError::Format {
        offset: 0,
        msg: message.into().into(),
    }
}
