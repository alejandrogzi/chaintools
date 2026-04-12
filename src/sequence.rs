// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::ChainError;
#[cfg(feature = "gzip")]
use flate2::read::MultiGzDecoder;
use twobit::{TwoBitFile, TwoBitPhysicalFile};

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const TWOBIT_MAGIC: [u8; 4] = [0x43, 0x27, 0x41, 0x1a];
const TWOBIT_REV_MAGIC: [u8; 4] = [0x1a, 0x41, 0x27, 0x43];

/// A map of sequence names to sequence data.
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::SequenceMap;
///
/// let mut map = SequenceMap::new();
/// map.insert(b"chr1".to_vec(), b"ACGT".to_vec());
/// ```
pub type SequenceMap = HashMap<Vec<u8>, Vec<u8>>;

/// Source of sequence data for random-access filtering.
///
/// # Variants
///
/// * `TwoBit` - 2bit file path for lazy loading
/// * `Loaded` - Pre-loaded FASTA sequences in memory
#[derive(Debug, Clone)]
enum SequenceSource {
    TwoBit(PathBuf),
    Loaded {
        path: PathBuf,
        sequences: Arc<SequenceMap>,
    },
}

/// Supported sequence file formats.
///
/// # Variants
///
/// * `TwoBit` - 2bit binary format
/// * `Fasta` - FASTA text format (including .fa, .fasta, .fna, and gzipped variants)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SequenceFormat {
    TwoBit,
    Fasta,
}

/// Resolves sequence file inputs for random-access filtering.
///
/// Provides lazy loading for 2bit files and pre-loaded storage for FASTA files.
/// Automatically detects file format based on extension.
///
/// # Fields
///
/// * `source` - The sequence source (2bit path or loaded FASTA)
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::SequenceResolver;
///
/// // Open a 2bit file (lazy loading)
/// let resolver = SequenceResolver::new("genome.2bit")?;
/// ```
#[derive(Debug, Clone)]
pub struct SequenceResolver {
    source: SequenceSource,
}

impl SequenceResolver {
    /// Creates a new sequence resolver from a file path.
    ///
    /// Automatically detects the sequence format (2bit or FASTA) based on
    /// the file extension. 2bit files are lazy-loaded, FASTA files are
    /// loaded entirely into memory.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the sequence file
    ///
    /// # Output
    ///
    /// Returns `Ok(SequenceResolver)` or `Err(ChainError)` if unsupported format
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::sequence::SequenceResolver;
    ///
    /// // From 2bit file
    /// let resolver = SequenceResolver::new("genome.2bit")?;
    ///
    /// // From FASTA file
    /// let resolver = SequenceResolver::new("sequences.fa")?;
    /// ```
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        let path = path.as_ref().to_path_buf();
        match detect_sequence_format(&path) {
            Some(SequenceFormat::TwoBit) => Ok(Self {
                source: SequenceSource::TwoBit(path),
            }),
            Some(SequenceFormat::Fasta) => Ok(Self {
                source: SequenceSource::Loaded {
                    path: path.clone(),
                    sequences: Arc::new(get_sequences(&path)?),
                },
            }),
            None => Err(sequence_error(format!(
                "unsupported sequence format for {} (expected .2bit, .fa, .fasta, .fna, or gzipped FASTA)",
                path.display()
            ))),
        }
    }

    /// Fetches a sequence range from the resolver.
    ///
    /// Retrieves a subsequence from the resolved sequences, supporting both
    /// 2bit lazy loading and pre-loaded FASTA sequences.
    ///
    /// # Arguments
    ///
    /// * `cache` - Per-worker cache for 2bit file handles
    /// * `seq_name` - Name of the sequence to fetch
    /// * `start` - Start position of the range
    /// * `length` - Length of the range to fetch
    ///
    /// # Output
    ///
    /// Returns `Ok(Vec<u8>)` containing the fetched sequence, or `Err(ChainError)`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::sequence::{SequenceResolver, SequenceCache};
    ///
    /// let resolver = SequenceResolver::new("genome.2bit")?;
    /// let mut cache = SequenceCache::default();
    ///
    /// // Fetch 100 bases from chr1 starting at position 0
    /// let seq = resolver.fetch(&mut cache, b"chr1", 0, 100)?;
    /// ```
    pub fn fetch(
        &self,
        cache: &mut SequenceCache,
        seq_name: &[u8],
        start: u32,
        length: u32,
    ) -> Result<Vec<u8>, ChainError> {
        match &self.source {
            SequenceSource::TwoBit(path) => cache.fetch_twobit(path, seq_name, start, length),
            SequenceSource::Loaded { path, sequences } => {
                fetch_loaded_sequence(path, sequences, seq_name, start, length)
            }
        }
    }
}

/// Per-worker cache of open 2bit files.
///
/// Maintains open file handles to 2bit files for efficient repeated access.
/// Each worker thread should have its own cache instance.
///
/// # Fields
///
/// * `files` - Hash map of path to open 2bit file reader
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::SequenceCache;
///
/// let mut cache = SequenceCache::default();
/// // Cache is used internally by SequenceResolver::fetch()
/// ```
#[derive(Default)]
pub struct SequenceCache {
    files: HashMap<PathBuf, TwoBitPhysicalFile>,
}

impl SequenceCache {
    fn fetch_twobit(
        &mut self,
        path: &Path,
        seq_name: &[u8],
        start: u32,
        length: u32,
    ) -> Result<Vec<u8>, ChainError> {
        let seq_name = bytes_to_utf8(seq_name, "2bit sequence name")?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| sequence_error("requested 2bit range overflows u32"))?;

        let reader = match self.files.entry(path.to_path_buf()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let file = TwoBitFile::open(path)
                    .map_err(|err| {
                        sequence_error(format!("cannot open 2bit {}: {err}", path.display()))
                    })?
                    .enable_softmask(true);
                entry.insert(file)
            }
        };

        let sequence = reader
            .read_sequence(seq_name, start as usize..end as usize)
            .map_err(|err| {
                sequence_error(format!(
                    "cannot read {seq_name}:{start}-{end} from {}: {err}",
                    path.display()
                ))
            })?;
        if sequence.len() != length as usize {
            return Err(sequence_error(format!(
                "sequence range {seq_name}:{start}-{end} exceeds {}",
                path.display()
            )));
        }
        Ok(sequence.into_bytes())
    }
}

/// Loads all sequences from a FASTA or 2bit input.
///
/// If the path is `-`, data is read from stdin. Gzipped FASTA requires the `gzip`
/// feature. Soft-masked bases from 2bit inputs are preserved.
///
/// # Arguments
///
/// * `sequence` - Path to the sequence file, or "-" for stdin
///
/// # Output
///
/// Returns `Ok(SequenceMap)` containing all sequences, or `Err(ChainError)`
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::get_sequences;
///
/// // Load from 2bit file
/// let sequences = get_sequences("genome.2bit")?;
/// ```
///
/// ```ignore
/// use chaintools::sequence::get_sequences;
///
/// // Load from FASTA file
/// let sequences = get_sequences("sequences.fa")?;
/// ```
pub fn get_sequences<P: AsRef<Path>>(sequence: P) -> Result<SequenceMap, ChainError> {
    let path = sequence.as_ref();
    if path == Path::new("-") {
        return from_stdin();
    }

    match detect_sequence_format(path) {
        Some(SequenceFormat::TwoBit) => from_2bit(path),
        Some(SequenceFormat::Fasta) => from_fa(path),
        None => Err(sequence_error(format!(
            "cannot determine supported sequence format for {}",
            path.display()
        ))),
    }
}

/// Loads sequences from standard input.
///
/// Detects the format (gzip, 2bit, or FASTA) and parses accordingly.
///
/// # Output
///
/// Returns `Ok(SequenceMap)` or `Err(ChainError)` on failure
fn from_stdin() -> Result<SequenceMap, ChainError> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    if input.is_empty() {
        return Err(sequence_error(
            "missing sequence input and standard input is empty",
        ));
    }

    if input.starts_with(&GZIP_MAGIC) {
        #[cfg(feature = "gzip")]
        {
            return parse_fasta_reader(
                BufReader::new(MultiGzDecoder::new(Cursor::new(input))),
                "stdin",
            );
        }
        #[cfg(not(feature = "gzip"))]
        {
            return Err(sequence_error(
                "gzip-compressed sequence input requires the `gzip` feature",
            ));
        }
    }

    if input.starts_with(&TWOBIT_MAGIC) || input.starts_with(&TWOBIT_REV_MAGIC) {
        return from_2bit_buf(input, "stdin");
    }

    if input
        .iter()
        .copied()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| b == b'>')
    {
        return parse_fasta_reader(BufReader::new(Cursor::new(input)), "stdin");
    }

    Err(sequence_error("unsupported standard input sequence format"))
}

/// Loads sequences from a 2bit file.
///
/// Opens the specified 2bit file and loads all sequences into memory.
/// Enables soft-masking for lowercase representation of repeat regions.
///
/// # Arguments
///
/// * `path` - Path to the 2bit file
///
/// # Output
///
/// Returns `Ok(SequenceMap)` containing all sequences, or `Err(ChainError)`
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::from_2bit;
///
/// let sequences = from_2bit("genome.2bit")?;
/// ```
pub fn from_2bit<P: AsRef<Path>>(path: P) -> Result<SequenceMap, ChainError> {
    let path = path.as_ref();
    let genome = TwoBitFile::open(path)
        .map_err(|err| sequence_error(format!("cannot open 2bit {}: {err}", path.display())))?
        .enable_softmask(true);
    let source = format!("file {}", path.display());
    collect_2bit_sequences(genome, &source)
}

/// Parses 2bit format from a buffer.
///
/// Helper function to parse 2bit format from an in-memory buffer.
///
/// # Arguments
///
/// * `buf` - 2bit file bytes
/// * `source` - Source identifier for error messages
///
/// # Output
///
/// Returns `Ok(SequenceMap)` or `Err(ChainError)` on failure
fn from_2bit_buf(buf: Vec<u8>, source: &str) -> Result<SequenceMap, ChainError> {
    let genome = TwoBitFile::from_buf(buf)
        .map_err(|err| sequence_error(format!("cannot read 2bit from {source}: {err}")))?
        .enable_softmask(true);
    collect_2bit_sequences(genome, source)
}

/// Collects all sequences from a 2bit file.
///
/// Iterates over all chromosomes in the 2bit file and loads their sequences.
///
/// # Arguments
///
/// * `genome` - TwoBitFile handle
/// * `source` - Source identifier for error messages
///
/// # Output
///
/// Returns `Ok(SequenceMap)` with all sequences
fn collect_2bit_sequences<R: Read + std::io::Seek>(
    mut genome: TwoBitFile<R>,
    source: &str,
) -> Result<SequenceMap, ChainError> {
    let mut sequences = HashMap::new();
    for chr in genome.chrom_names() {
        let seq = genome
            .read_sequence(&chr, ..)
            .map_err(|err| sequence_error(format!("cannot read {chr} from {source}: {err}")))?;
        sequences.insert(chr.into_bytes(), seq.into_bytes());
    }
    Ok(sequences)
}

/// Loads sequences from a FASTA file.
///
/// Opens the specified FASTA file and loads all sequences into memory.
/// Automatically detects and handles gzip compression based on file extension.
///
/// # Arguments
///
/// * `path` - Path to the FASTA file (.fa, .fasta, .fna, or gzipped variants)
///
/// # Output
///
/// Returns `Ok(SequenceMap)` containing all sequences, or `Err(ChainError)`
///
/// # Examples
///
/// ```ignore
/// use chaintools::sequence::from_fa;
///
/// let sequences = from_fa("sequences.fa")?;
/// ```
pub fn from_fa<P: AsRef<Path>>(path: P) -> Result<SequenceMap, ChainError> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let source = format!("file {}", path.display());
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        #[cfg(feature = "gzip")]
        {
            return parse_fasta_reader(BufReader::new(MultiGzDecoder::new(file)), &source);
        }

        #[cfg(not(feature = "gzip"))]
        {
            return Err(sequence_error(
                "gzip-compressed FASTA requires the `gzip` feature",
            ));
        }
    }

    parse_fasta_reader(BufReader::new(file), &source)
}

/// Parses FASTA format from a buffered reader.
///
/// Reads FASTA records from the reader and builds a sequence map.
///
/// # Arguments
///
/// * `reader` - Buffered reader for FASTA content
/// * `source` - Source identifier for error messages
///
/// # Output
///
/// Returns `Ok(SequenceMap)` or `Err(ChainError)` on failure
fn parse_fasta_reader<R: BufRead>(mut reader: R, source: &str) -> Result<SequenceMap, ChainError> {
    let mut acc = HashMap::new();
    let mut line = Vec::new();
    let mut header: Option<Vec<u8>> = None;
    let mut seq = Vec::new();

    loop {
        line.clear();
        let bytes_read = reader.read_until(b'\n', &mut line)?;
        if bytes_read == 0 {
            break;
        }

        trim_line_endings(&mut line);
        if line.is_empty() {
            continue;
        }

        if line[0] == b'>' {
            let record_name = fasta_record_name(&line[1..]);
            if record_name.is_empty() {
                return Err(sequence_error(format!(
                    "invalid FASTA in {source}: empty record name"
                )));
            }
            if let Some(prev_header) = header.replace(record_name.to_vec()) {
                acc.insert(prev_header, std::mem::take(&mut seq));
            }
        } else {
            if header.is_none() {
                return Err(sequence_error(format!(
                    "invalid FASTA in {source}: sequence data before header"
                )));
            }
            seq.extend_from_slice(&line);
        }
    }

    if let Some(last_header) = header {
        acc.insert(last_header, seq);
        Ok(acc)
    } else {
        Err(sequence_error(format!(
            "no FASTA records found in {source}"
        )))
    }
}

/// Removes trailing newline and carriage return characters from a line.
fn trim_line_endings(line: &mut Vec<u8>) {
    if line.ends_with(b"\n") {
        line.pop();
    }
    if line.ends_with(b"\r") {
        line.pop();
    }
}

/// Extracts the record name from a FASTA header line.
///
/// Returns the portion of the header before the first whitespace.
///
/// # Arguments
///
/// * `header` - FASTA header line (without the '>' prefix)
///
/// # Output
///
/// Returns the record name as a byte slice
fn fasta_record_name(header: &[u8]) -> &[u8] {
    let mut start = 0usize;
    while start < header.len() && header[start].is_ascii_whitespace() {
        start += 1;
    }
    let mut end = start;
    while end < header.len() && !header[end].is_ascii_whitespace() {
        end += 1;
    }
    &header[start..end]
}

/// Detects the sequence format based on file extension.
///
/// # Arguments
///
/// * `path` - Path to the sequence file
///
/// # Output
///
/// Returns `Some(SequenceFormat)` if detected, or `None`
fn detect_sequence_format(path: &Path) -> Option<SequenceFormat> {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("2bit"))
    {
        return Some(SequenceFormat::TwoBit);
    }

    let ext = path.extension().and_then(|ext| ext.to_str())?;
    if is_fasta_extension(ext) {
        return Some(SequenceFormat::Fasta);
    }
    if ext.eq_ignore_ascii_case("gz") {
        let stem = Path::new(path.file_stem()?);
        let stem_ext = stem.extension().and_then(|inner| inner.to_str())?;
        if is_fasta_extension(stem_ext) {
            return Some(SequenceFormat::Fasta);
        }
    }
    None
}

/// Checks if a file extension is a FASTA variant.
fn is_fasta_extension(ext: &str) -> bool {
    ext.eq_ignore_ascii_case("fa")
        || ext.eq_ignore_ascii_case("fasta")
        || ext.eq_ignore_ascii_case("fna")
}

/// Fetches a subsequence from loaded sequences.
///
/// Retrieves a slice of a sequence by name, start position, and length.
///
/// # Arguments
///
/// * `path` - Source file path (for error messages)
/// * `sequences` - Map of sequence names to sequence data
/// * `seq_name` - Name of the sequence to fetch
/// * `start` - Start position (0-based)
/// * `length` - Number of bases to fetch
///
/// # Output
///
/// Returns `Ok(Vec<u8>)` with the fetched subsequence or `Err(ChainError)` on failure
fn fetch_loaded_sequence(
    path: &Path,
    sequences: &SequenceMap,
    seq_name: &[u8],
    start: u32,
    length: u32,
) -> Result<Vec<u8>, ChainError> {
    let sequence = sequences.get(seq_name).ok_or_else(|| {
        sequence_error(format!(
            "sequence {} not found in {}",
            String::from_utf8_lossy(seq_name),
            path.display()
        ))
    })?;
    let start = start as usize;
    let end = start
        .checked_add(length as usize)
        .ok_or_else(|| sequence_error("requested sequence range overflows usize"))?;
    if end > sequence.len() {
        return Err(sequence_error(format!(
            "sequence range {}:{}-{} exceeds {}",
            String::from_utf8_lossy(seq_name),
            start,
            end,
            path.display()
        )));
    }
    Ok(sequence[start..end].to_vec())
}

/// Converts a byte slice to a UTF-8 string, with context for errors.
///
/// # Arguments
///
/// * `value` - Byte slice to convert
/// * `context` - Context string for error messages
///
/// # Output
///
/// Returns `Ok(&str)` or `Err(ChainError)` if not valid UTF-8
fn bytes_to_utf8<'a>(value: &'a [u8], context: &str) -> Result<&'a str, ChainError> {
    std::str::from_utf8(value).map_err(|_| sequence_error(format!("{context} must be valid UTF-8")))
}

/// Creates a sequence error with a custom message.
///
/// Helper function to create an unsupported ChainError with a custom message.
///
/// # Arguments
///
/// * `message` - Error message
///
/// # Output
///
/// Returns a `ChainError::Unsupported` with the message
fn sequence_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}
