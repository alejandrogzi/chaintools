// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::model::error::ChainError;
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
/// use chaintools::seq::sequence::SequenceMap;
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
    // Retained so `fetch`/`SequenceCache` keep a stable shape for other callers
    // (e.g. `score`); the anti-repeat path now always preloads into `Loaded`.
    #[allow(dead_code)]
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
/// use chaintools::seq::sequence::SequenceResolver;
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
    /// use chaintools::seq::sequence::SequenceResolver;
    ///
    /// // From 2bit file
    /// let resolver = SequenceResolver::new("genome.2bit")?;
    ///
    /// // From FASTA file
    /// let resolver = SequenceResolver::new("sequences.fa")?;
    /// ```
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ChainError> {
        Self::new_filtered(path, None)
    }

    /// Preloads either all sequences or only the named 2bit sequences.
    ///
    /// This is a clearer alias for [`SequenceResolver::new_filtered`] when
    /// callers want predictable zero-copy full-sequence access through
    /// [`SequenceResolver::sequence`].
    pub fn preload<P: AsRef<Path>>(
        path: P,
        names: Option<&HashSet<Vec<u8>>>,
    ) -> Result<Self, ChainError> {
        Self::new_filtered(path, names)
    }

    /// Creates a resolver, preloading either all sequences or only those named.
    ///
    /// Both 2bit and FASTA inputs are decoded fully into memory up front (the
    /// 2bit soft-mask/N block scan is paid once per sequence at load time rather
    /// than once per `fetch`), so every later `fetch` is an in-memory slice.
    /// When `names` is `Some`, only the listed 2bit sequences are loaded; FASTA
    /// inputs are always loaded in full because they are parsed in a single
    /// pass. Sequences referenced but absent from the file are simply not loaded
    /// and surface as [`ChainError::MissingSequence`] at fetch time.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the sequence file
    /// * `names` - Sequence names to load, or `None` to load every sequence
    ///
    /// # Output
    ///
    /// Returns `Ok(SequenceResolver)` or `Err(ChainError)` if unsupported format
    pub fn new_filtered<P: AsRef<Path>>(
        path: P,
        names: Option<&HashSet<Vec<u8>>>,
    ) -> Result<Self, ChainError> {
        let path = path.as_ref().to_path_buf();
        match detect_sequence_format(&path) {
            Some(SequenceFormat::TwoBit) => Ok(Self {
                source: SequenceSource::Loaded {
                    path: path.clone(),
                    sequences: Arc::new(from_2bit_filtered(&path, names)?),
                },
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
    /// use chaintools::seq::sequence::{SequenceResolver, SequenceCache};
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

    /// Borrows a full decoded chromosome from the preloaded sequence map.
    ///
    /// Unlike [`SequenceResolver::fetch`], this returns a zero-copy slice into
    /// the in-memory sequence instead of allocating a copy of a sub-range,
    /// letting callers index aligned regions directly. Only the preloaded
    /// (`Loaded`) source supports it; a missing name yields
    /// [`ChainError::MissingSequence`], matching `fetch`'s behavior.
    ///
    /// # Arguments
    ///
    /// * `seq_name` - Name of the sequence to borrow
    ///
    /// # Output
    ///
    /// Returns `Ok(&[u8])` with the full sequence, or `Err(ChainError)`
    pub fn chromosome(&self, seq_name: &[u8]) -> Result<&[u8], ChainError> {
        self.sequence(seq_name)
    }

    /// Borrows a full decoded sequence from the preloaded sequence map.
    ///
    /// This returns a zero-copy slice into the in-memory sequence instead of
    /// allocating a copied range. A missing name yields
    /// [`ChainError::MissingSequence`], matching [`SequenceResolver::fetch`].
    pub fn sequence(&self, seq_name: &[u8]) -> Result<&[u8], ChainError> {
        match &self.source {
            SequenceSource::Loaded { sequences, .. } => sequences
                .get(seq_name)
                .map(Vec::as_slice)
                .ok_or_else(|| ChainError::MissingSequence {
                    name: String::from_utf8_lossy(seq_name).into_owned().into(),
                }),
            SequenceSource::TwoBit(_) => {
                Err(sequence_error("chromosome() requires preloaded sequences"))
            }
        }
    }

    /// Returns every preloaded sequence name with its length, sorted by name.
    ///
    /// Formats that must declare their sequences before any record is written
    /// (a BAM `@SQ` header, for instance) need the whole set up front, while
    /// chains are still being streamed. The sort makes that declaration
    /// deterministic across runs, which a `HashMap` iteration order is not.
    ///
    /// # Output
    ///
    /// Returns `Ok(Vec<(name, length)>)`, or `Err(ChainError)` if a length
    /// exceeds the `u32` coordinate space or the source is not preloaded.
    pub fn sequences(&self) -> Result<Vec<(&[u8], u32)>, ChainError> {
        match &self.source {
            SequenceSource::Loaded { sequences, .. } => {
                let mut entries = Vec::with_capacity(sequences.len());
                for (name, sequence) in sequences.iter() {
                    let len = u32::try_from(sequence.len()).map_err(|_| {
                        sequence_error("sequence length exceeds u32 coordinate space")
                    })?;
                    entries.push((name.as_slice(), len));
                }
                entries.sort_unstable_by_key(|(name, _)| *name);
                Ok(entries)
            }
            SequenceSource::TwoBit(_) => {
                Err(sequence_error("sequences() requires preloaded sequences"))
            }
        }
    }

    /// Returns the full decoded sequence length.
    pub fn sequence_len(&self, seq_name: &[u8]) -> Result<u32, ChainError> {
        let len = self.sequence(seq_name)?.len();
        u32::try_from(len)
            .map_err(|_| sequence_error("sequence length exceeds u32 coordinate space"))
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
/// use chaintools::seq::sequence::SequenceCache;
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

        let sequence = match reader.read_sequence(seq_name, start as usize..end as usize) {
            Ok(sequence) => sequence,
            // A missing chromosome is reported distinctly so callers can skip
            // the affected chains instead of aborting the whole run.
            Err(twobit::Error::MissingName(name)) => {
                return Err(ChainError::MissingSequence { name: name.into() });
            }
            Err(err) => {
                return Err(sequence_error(format!(
                    "cannot read {seq_name}:{start}-{end} from {}: {err}",
                    path.display()
                )));
            }
        };
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
/// use chaintools::seq::sequence::get_sequences;
///
/// // Load from 2bit file
/// let sequences = get_sequences("genome.2bit")?;
/// ```
///
/// ```ignore
/// use chaintools::seq::sequence::get_sequences;
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
/// use chaintools::seq::sequence::from_2bit;
///
/// let sequences = from_2bit("genome.2bit")?;
/// ```
pub fn from_2bit<P: AsRef<Path>>(path: P) -> Result<SequenceMap, ChainError> {
    from_2bit_filtered(path, None)
}

/// Loads sequences from a 2bit file, optionally restricted to named sequences.
///
/// Behaves like [`from_2bit`] but, when `names` is `Some`, only loads the 2bit
/// sequences whose name appears in the set. Names in the set that are absent
/// from the file are skipped (they surface later as
/// [`ChainError::MissingSequence`] at fetch time). `None` loads everything.
///
/// # Arguments
///
/// * `path` - Path to the 2bit file
/// * `names` - Sequence names to load, or `None` for all
///
/// # Output
///
/// Returns `Ok(SequenceMap)` containing the selected sequences, or `Err(ChainError)`
pub fn from_2bit_filtered<P: AsRef<Path>>(
    path: P,
    names: Option<&HashSet<Vec<u8>>>,
) -> Result<SequenceMap, ChainError> {
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|err| sequence_error(format!("cannot open 2bit {}: {err}", path.display())))?;
    let source = format!("file {}", path.display());
    read_2bit(file, &source, names)
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
    read_2bit(Cursor::new(buf), source, None)
}

/// 2bit signature (kent `twoBitSig`) as read in the file's own byte order.
const TWOBIT_SIGNATURE: u32 = 0x1A41_2743;

/// Read buffer for 2bit loading: large enough that walking the records of a
/// fragmented assembly in file order stays one sequential read.
const TWOBIT_READ_BUFFER: usize = 1 << 20;

/// Packed-DNA bytes decoded per read while loading one sequence.
const TWOBIT_DNA_CHUNK: usize = 1 << 16;

/// Four bases per packed byte, most significant pair first (T=0, C=1, A=2, G=3).
const TWOBIT_QUADS: [[u8; 4]; 256] = twobit_quads();

const fn twobit_quads() -> [[u8; 4]; 256] {
    const NUC: &[u8; 4] = b"TCAG";
    let mut table = [[0_u8; 4]; 256];
    let mut byte = 0;
    while byte < 256 {
        table[byte] = [
            NUC[(byte >> 6) & 3],
            NUC[(byte >> 4) & 3],
            NUC[(byte >> 2) & 3],
            NUC[byte & 3],
        ];
        byte += 1;
    }
    table
}

/// Loads every sequence of a 2bit file, or only the named ones, in one pass.
///
/// The index is read once into a name -> offset map, and the selected records
/// are then decoded in file-offset order, so a load costs one sequential read
/// of the file however many sequences it holds. The `twobit` crate looks a
/// sequence up by scanning its whole index, which made a full load quadratic
/// in the sequence count: about 7 h for a 2.07 M-scaffold assembly.
///
/// Output matches the `twobit` crate with soft-masking enabled: uppercase
/// bases, `N` over hard-masked blocks, lowercase over soft-masked blocks, and
/// the first index entry wins when a name repeats. Both byte orders and
/// version 1 files (64-bit offsets, as written by `faToTwoBit -long`) are read.
///
/// # Arguments
///
/// * `reader` - The 2bit file or buffer
/// * `source` - Source identifier for error messages
/// * `names` - Sequence names to load, or `None` for all
///
/// # Output
///
/// Returns `Ok(SequenceMap)` with the selected sequences, or `Err(ChainError)`
fn read_2bit<R: Read + Seek>(
    reader: R,
    source: &str,
    names: Option<&HashSet<Vec<u8>>>,
) -> Result<SequenceMap, ChainError> {
    let mut input = TwoBitInput::new(reader);
    let context = |err: std::io::Error| sequence_error(format!("cannot read 2bit {source}: {err}"));

    let signature = input.bytes4().map_err(context)?;
    input.big_endian = if u32::from_le_bytes(signature) == TWOBIT_SIGNATURE {
        false
    } else if u32::from_be_bytes(signature) == TWOBIT_SIGNATURE {
        true
    } else {
        return Err(sequence_error(format!(
            "{source} does not start with a 2bit signature"
        )));
    };
    let version = input.u32().map_err(context)?;
    if version > 1 {
        return Err(sequence_error(format!(
            "unsupported 2bit version {version} in {source}"
        )));
    }
    let count = input.u32().map_err(context)?;
    let _reserved = input.u32().map_err(context)?;

    // Index: name -> data offset, first entry wins (as `twobit` lookups do).
    let mut offsets: HashMap<Vec<u8>, u64> = HashMap::new();
    for _ in 0..count {
        let name_len = usize::from(input.byte().map_err(context)?);
        let mut name = vec![0_u8; name_len];
        input.read_exact(&mut name).map_err(context)?;
        let offset = if version == 1 {
            input.u64().map_err(context)?
        } else {
            u64::from(input.u32().map_err(context)?)
        };
        if std::str::from_utf8(&name).is_err() {
            return Err(sequence_error(format!(
                "2bit sequence name is not UTF-8 in {source}"
            )));
        }
        // Skip sequences the caller did not ask for. A requested name that is
        // absent from the file is simply never loaded and surfaces as
        // MissingSequence at fetch time, matching the load-everything behavior.
        if names.is_some_and(|filter| !filter.contains(&name)) {
            continue;
        }
        offsets.entry(name).or_insert(offset);
    }

    let mut records: Vec<(Vec<u8>, u64)> = offsets.into_iter().collect();
    records.sort_unstable_by_key(|&(_, offset)| offset);
    let mut sequences = HashMap::with_capacity(records.len());
    for (name, offset) in records {
        let sequence = input.sequence(offset).map_err(|err| {
            sequence_error(format!(
                "cannot read {} from 2bit {source}: {err}",
                String::from_utf8_lossy(&name)
            ))
        })?;
        sequences.insert(name, sequence);
    }
    Ok(sequences)
}

/// Buffered 2bit reader that tracks its position, so moving to the next record
/// keeps the read buffer whenever the record is already in it.
struct TwoBitInput<R: Read + Seek> {
    reader: BufReader<R>,
    position: u64,
    big_endian: bool,
    dna: Vec<u8>,
}

impl<R: Read + Seek> TwoBitInput<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(TWOBIT_READ_BUFFER, reader),
            position: 0,
            big_endian: false,
            dna: Vec::new(),
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.reader.read_exact(buf)?;
        self.position += buf.len() as u64;
        Ok(())
    }

    fn byte(&mut self) -> std::io::Result<u8> {
        let mut buf = [0_u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn bytes4(&mut self) -> std::io::Result<[u8; 4]> {
        let mut buf = [0_u8; 4];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn u32(&mut self) -> std::io::Result<u32> {
        let buf = self.bytes4()?;
        Ok(if self.big_endian {
            u32::from_be_bytes(buf)
        } else {
            u32::from_le_bytes(buf)
        })
    }

    fn u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0_u8; 8];
        self.read_exact(&mut buf)?;
        Ok(if self.big_endian {
            u64::from_be_bytes(buf)
        } else {
            u64::from_le_bytes(buf)
        })
    }

    fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        if offset != self.position {
            let delta = i64::try_from(offset).map_err(std::io::Error::other)?
                - i64::try_from(self.position).map_err(std::io::Error::other)?;
            self.reader.seek_relative(delta)?;
            self.position = offset;
        }
        Ok(())
    }

    /// `(start, end)` blocks: a count, then that many starts, then the sizes.
    fn blocks(&mut self) -> std::io::Result<Vec<(usize, usize)>> {
        let count = self.u32()? as usize;
        // Capped: a corrupt count then fails at end of file, not in the allocator.
        let mut starts = Vec::with_capacity(count.min(1 << 16));
        for _ in 0..count {
            starts.push(self.u32()? as usize);
        }
        let mut blocks = Vec::with_capacity(starts.len());
        for start in starts {
            blocks.push((start, start + self.u32()? as usize));
        }
        Ok(blocks)
    }

    /// Decodes the record at `offset`: size, N blocks, mask blocks, a reserved
    /// word, then the packed bases.
    fn sequence(&mut self, offset: u64) -> std::io::Result<Vec<u8>> {
        self.seek_to(offset)?;
        let size = self.u32()? as usize;
        let n_blocks = self.blocks()?;
        let mask_blocks = self.blocks()?;
        let _reserved = self.u32()?;

        let mut sequence = Vec::with_capacity(size + 3);
        let mut packed = size.div_ceil(4);
        let mut dna = std::mem::take(&mut self.dna);
        dna.resize(packed.min(TWOBIT_DNA_CHUNK), 0);
        while packed > 0 {
            let chunk = packed.min(TWOBIT_DNA_CHUNK);
            self.read_exact(&mut dna[..chunk])?;
            for &byte in &dna[..chunk] {
                sequence.extend_from_slice(&TWOBIT_QUADS[usize::from(byte)]);
            }
            packed -= chunk;
        }
        self.dna = dna;
        sequence.truncate(size);

        apply_2bit_blocks(&mut sequence, &n_blocks, |_| b'N');
        apply_2bit_blocks(&mut sequence, &mask_blocks, |base| {
            base.to_ascii_lowercase()
        });
        Ok(sequence)
    }
}

/// Applies `mask` over each block clipped to the sequence. Like the `twobit`
/// crate, which expects sorted blocks, the walk stops at the first block that
/// starts at or past the end of the sequence.
fn apply_2bit_blocks(sequence: &mut [u8], blocks: &[(usize, usize)], mask: impl Fn(u8) -> u8) {
    let len = sequence.len();
    for &(start, end) in blocks.iter().take_while(|&&(start, _)| start < len) {
        for base in &mut sequence[start..end.min(len)] {
            *base = mask(*base);
        }
    }
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
/// use chaintools::seq::sequence::from_fa;
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
    let sequence = match sequences.get(seq_name) {
        Some(sequence) => sequence,
        // A missing sequence name is reported distinctly so callers can skip
        // the affected chains instead of aborting the whole run.
        None => {
            return Err(ChainError::MissingSequence {
                name: String::from_utf8_lossy(seq_name).into_owned().into(),
            });
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn preload_sequence_and_len_from_fasta() {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "chaintools-sequence-test-{}-{id}.fa",
            std::process::id()
        ));
        std::fs::write(&path, b">chr1 description\nACGTnn\n").expect("write FASTA");

        let resolver = SequenceResolver::preload(&path, None).expect("preload FASTA");
        assert_eq!(resolver.sequence(b"chr1").unwrap(), b"ACGTnn");
        assert_eq!(resolver.chromosome(b"chr1").unwrap(), b"ACGTnn");
        assert_eq!(resolver.sequence_len(b"chr1").unwrap(), 6);

        std::fs::remove_file(path).expect("remove FASTA");
    }

    /// The previous loader: the `twobit` crate with soft-masking, one name
    /// lookup per sequence. The new loader must reproduce it exactly.
    fn legacy_2bit_sequences<R: Read + Seek>(
        mut genome: TwoBitFile<R>,
        names: Option<&HashSet<Vec<u8>>>,
    ) -> SequenceMap {
        let mut sequences = HashMap::new();
        for chr in genome.chrom_names() {
            if names.is_some_and(|filter| !filter.contains(chr.as_bytes())) {
                continue;
            }
            let seq = genome
                .read_sequence(&chr, ..)
                .expect("twobit crate reads the sequence");
            sequences.insert(chr.into_bytes(), seq.into_bytes());
        }
        sequences
    }

    fn legacy_2bit_buf(buf: &[u8], names: Option<&HashSet<Vec<u8>>>) -> SequenceMap {
        let genome = TwoBitFile::from_buf(buf.to_vec())
            .expect("twobit crate opens the 2bit")
            .enable_softmask(true);
        legacy_2bit_sequences(genome, names)
    }

    fn load_buf(buf: &[u8], names: Option<&HashSet<Vec<u8>>>) -> SequenceMap {
        read_2bit(Cursor::new(buf.to_vec()), "test", names).expect("load 2bit")
    }

    /// One 2bit record: bases to pack (N and anything else pack as T, as in
    /// kent) and raw `(start, size)` N and soft-mask blocks.
    struct Record {
        name: String,
        bases: Vec<u8>,
        n_blocks: Vec<(u32, u32)>,
        mask_blocks: Vec<(u32, u32)>,
    }

    /// Runs of `pred` in `text` as `(start, size)` blocks.
    fn runs(text: &[u8], pred: impl Fn(u8) -> bool) -> Vec<(u32, u32)> {
        let mut blocks = Vec::new();
        let mut i = 0;
        while i < text.len() {
            if pred(text[i]) {
                let start = i;
                while i < text.len() && pred(text[i]) {
                    i += 1;
                }
                blocks.push((start as u32, (i - start) as u32));
            } else {
                i += 1;
            }
        }
        blocks
    }

    /// A record whose decoded sequence is `text` itself (N/n hard-masked,
    /// lowercase soft-masked), as `faToTwoBit` would write it.
    fn masked(name: &str, text: &str) -> Record {
        let text = text.as_bytes();
        Record {
            name: name.to_owned(),
            bases: text.to_vec(),
            n_blocks: runs(text, |b| b.eq_ignore_ascii_case(&b'N')),
            mask_blocks: runs(text, |b| b.is_ascii_lowercase()),
        }
    }

    /// Writes a 2bit: header, index, then the records in `data_order`.
    fn encode_2bit(
        records: &[Record],
        big_endian: bool,
        version: u32,
        data_order: &[usize],
    ) -> Vec<u8> {
        let word = |out: &mut Vec<u8>, v: u32| {
            out.extend_from_slice(&if big_endian {
                v.to_be_bytes()
            } else {
                v.to_le_bytes()
            })
        };
        let mut data = vec![Vec::new(); records.len()];
        for (rec, out) in records.iter().zip(&mut data) {
            word(out, rec.bases.len() as u32);
            for blocks in [&rec.n_blocks, &rec.mask_blocks] {
                word(out, blocks.len() as u32);
                for &(start, _) in blocks.iter() {
                    word(out, start);
                }
                for &(_, size) in blocks.iter() {
                    word(out, size);
                }
            }
            word(out, 0);
            for quad in rec.bases.chunks(4) {
                let mut byte = 0_u8;
                for i in 0..4 {
                    let code = match quad.get(i).map(u8::to_ascii_uppercase) {
                        Some(b'C') => 1,
                        Some(b'A') => 2,
                        Some(b'G') => 3,
                        _ => 0,
                    };
                    byte = (byte << 2) | code;
                }
                out.push(byte);
            }
        }
        let offset_size = if version == 1 { 8 } else { 4 };
        let index_len: usize = records
            .iter()
            .map(|rec| 1 + rec.name.len() + offset_size)
            .sum();
        let mut offsets = vec![0_u64; records.len()];
        let mut next = (16 + index_len) as u64;
        for &i in data_order {
            offsets[i] = next;
            next += data[i].len() as u64;
        }

        let mut out = Vec::new();
        word(&mut out, TWOBIT_SIGNATURE);
        word(&mut out, version);
        word(&mut out, records.len() as u32);
        word(&mut out, 0);
        for (rec, &offset) in records.iter().zip(&offsets) {
            out.push(rec.name.len() as u8);
            out.extend_from_slice(rec.name.as_bytes());
            if version == 1 {
                out.extend_from_slice(&if big_endian {
                    offset.to_be_bytes()
                } else {
                    offset.to_le_bytes()
                });
            } else {
                word(&mut out, offset as u32);
            }
        }
        for &i in data_order {
            out.extend_from_slice(&data[i]);
        }
        out
    }

    fn in_order(records: &[Record]) -> Vec<usize> {
        (0..records.len()).collect()
    }

    /// xorshift64*: deterministic test data without a dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Random records: lengths 0..400 (short ones often), runs of N and of
    /// lowercase, plus raw blocks that overlap, run past the end, sit empty at
    /// position 0, or come unsorted; repeated names and shuffled data order.
    fn random_genome(rng: &mut Rng) -> (Vec<Record>, Vec<usize>) {
        let count = 1 + rng.below(60);
        let mut records = Vec::with_capacity(count);
        for i in 0..count {
            let len = if rng.below(4) == 0 {
                rng.below(6)
            } else {
                rng.below(400)
            };
            let mut text = Vec::with_capacity(len);
            while text.len() < len {
                let run = 1 + rng.below(30);
                let kind = rng.below(10);
                for _ in 0..run.min(len - text.len()) {
                    let base = b"ACGT"[rng.below(4)];
                    text.push(match kind {
                        0 => b'N',
                        1 => b'n',
                        2..=4 => base.to_ascii_lowercase(),
                        _ => base,
                    });
                }
            }
            let name = if i > 0 && rng.below(15) == 0 {
                records
                    .get(rng.below(i))
                    .map_or_else(|| format!("seq{i}"), |r: &Record| r.name.clone())
            } else {
                format!("scaffold_{i}_{}", rng.below(1000))
            };
            let mut rec = masked(&name, std::str::from_utf8(&text).unwrap());
            if rng.below(3) == 0 {
                let extra = (rng.below(len + 10) as u32, rng.below(20) as u32);
                rec.n_blocks.push(extra);
                rec.mask_blocks
                    .push((rng.below(len + 10) as u32, rng.below(50) as u32));
                if rng.below(2) == 0 {
                    rec.n_blocks.insert(0, (0, 0));
                }
                if rng.below(2) == 0 {
                    rec.mask_blocks
                        .sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                }
            }
            records.push(rec);
        }
        let mut order = in_order(&records);
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i + 1));
        }
        (records, order)
    }

    #[test]
    fn twobit_loader_matches_twobit_crate_on_random_genomes() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..200 {
            let (records, order) = random_genome(&mut rng);
            let buf = encode_2bit(&records, false, 0, &order);
            assert_eq!(load_buf(&buf, None), legacy_2bit_buf(&buf, None));

            let mut subset: HashSet<Vec<u8>> = records
                .iter()
                .filter(|_| rng.below(3) == 0)
                .map(|r| r.name.as_bytes().to_vec())
                .collect();
            subset.insert(b"not_in_file".to_vec());
            assert_eq!(
                load_buf(&buf, Some(&subset)),
                legacy_2bit_buf(&buf, Some(&subset))
            );
        }
    }

    #[test]
    fn twobit_loader_decodes_masks_and_short_records() {
        let records = vec![
            masked("chr1", "ACGTacgtNNnnACG"),
            masked("empty", ""),
            masked("one", "g"),
            masked("allN", "NNNNNNN"),
        ];
        let buf = encode_2bit(&records, false, 0, &[2, 0, 3, 1]);
        let loaded = load_buf(&buf, None);
        assert_eq!(loaded.len(), 4);
        assert_eq!(loaded[b"chr1".as_slice()], b"ACGTacgtNNnnACG");
        assert_eq!(loaded[b"empty".as_slice()], b"");
        assert_eq!(loaded[b"one".as_slice()], b"g");
        assert_eq!(loaded[b"allN".as_slice()], b"NNNNNNN");
        assert_eq!(loaded, legacy_2bit_buf(&buf, None));
    }

    #[test]
    fn twobit_loader_reads_both_byte_orders_and_version_1() {
        let mut rng = Rng(42);
        for _ in 0..20 {
            let (records, order) = random_genome(&mut rng);
            let expected = legacy_2bit_buf(&encode_2bit(&records, false, 0, &order), None);
            let big = encode_2bit(&records, true, 0, &order);
            assert_eq!(legacy_2bit_buf(&big, None), expected);
            assert_eq!(load_buf(&big, None), expected);
            for big_endian in [false, true] {
                let long = encode_2bit(&records, big_endian, 1, &order);
                assert_eq!(load_buf(&long, None), expected);
            }
        }
    }

    #[test]
    fn twobit_loader_rejects_malformed_files() {
        let records = vec![masked("chr1", "ACGTACGTACGT"), masked("chr2", "ACGT")];
        let good = encode_2bit(&records, false, 0, &in_order(&records));

        let mut bad_signature = good.clone();
        bad_signature[0] ^= 0xFF;
        assert!(read_2bit(Cursor::new(bad_signature), "test", None).is_err());

        let mut version_2 = good.clone();
        version_2[4..8].copy_from_slice(&2_u32.to_le_bytes());
        assert!(read_2bit(Cursor::new(version_2), "test", None).is_err());

        for cut in [3, 10, 20, good.len() - 1] {
            assert!(read_2bit(Cursor::new(good[..cut].to_vec()), "test", None).is_err());
        }
    }

    #[test]
    fn from_2bit_filtered_reads_a_file() {
        let records = vec![masked("chrA", "ACGTNNacgt"), masked("chrB", "TTTTcccc")];
        let buf = encode_2bit(&records, false, 0, &in_order(&records));
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "chaintools-sequence-test-{}-{id}.2bit",
            std::process::id()
        ));
        std::fs::write(&path, &buf).expect("write 2bit");

        let only_b: HashSet<Vec<u8>> = [b"chrB".to_vec()].into_iter().collect();
        let loaded = from_2bit_filtered(&path, Some(&only_b)).expect("load 2bit");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[b"chrB".as_slice()], b"TTTTcccc");
        assert_eq!(from_2bit(&path).expect("load 2bit"), load_buf(&buf, None));

        std::fs::remove_file(path).expect("remove 2bit");
    }

    /// Real-genome check against the previous loader:
    /// `CHAINTOOLS_2BIT_ORACLE=a.2bit:b.2bit cargo test --release -- --ignored twobit_oracle`.
    /// The previous loader is quadratic in the sequence count, so pick files
    /// with up to a few hundred thousand sequences.
    #[test]
    #[ignore = "needs CHAINTOOLS_2BIT_ORACLE"]
    fn twobit_oracle_real_files() {
        let paths = std::env::var("CHAINTOOLS_2BIT_ORACLE").expect("set CHAINTOOLS_2BIT_ORACLE");
        for path in paths.split(':').filter(|p| !p.is_empty()) {
            let started = std::time::Instant::now();
            let loaded = from_2bit(path).expect("load 2bit");
            let new_time = started.elapsed();
            let started = std::time::Instant::now();
            let genome = TwoBitFile::open(path)
                .expect("twobit crate opens the 2bit")
                .enable_softmask(true);
            let legacy = legacy_2bit_sequences(genome, None);
            let legacy_time = started.elapsed();
            let bases: usize = loaded.values().map(Vec::len).sum();
            eprintln!(
                "{path}: {} sequences, {bases} bp; new {new_time:.2?}, twobit crate {legacy_time:.2?}",
                loaded.len()
            );
            assert_eq!(loaded, legacy, "{path}");
        }
    }
}
