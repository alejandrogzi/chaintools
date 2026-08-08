// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::fs::File;
use std::io::{BufRead, BufWriter, Write};
use std::path::PathBuf;

use chaintools::seq::revcomp::reverse_complement_in_place;
use chaintools::seq::sequence::{SequenceCache, SequenceResolver};
use chaintools::{ChainError, OwnedChain, Strand, StreamingReader};
use clap::{Args, ValueEnum};
#[cfg(feature = "gzip")]
use flate2::{Compression, write::GzEncoder};

use super::CliError;

const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;

/// Command-line arguments for the convert subcommand.
///
/// Converts chain alignments into a sequence-aware format. A chain records
/// alignment geometry but no bases, so both genomes are required.
///
/// # Examples
///
/// ```bash
/// chaintools convert --to vcf -r reference.2bit -q query.2bit -c input.chain -o out.vcf
/// ```
#[derive(Debug, Args)]
pub struct ConvertArgs {
    #[arg(
        long = "to",
        value_name = "FORMAT",
        value_enum,
        help = "Output format. The generic --output option carries the result."
    )]
    to: ConvertTarget,

    #[arg(
        short = 'r',
        long = "reference",
        value_name = "PATH",
        help = "Path to the reference (target) sequence file (.2bit, .fa, .fasta, .fna, and gzip variants)."
    )]
    reference: PathBuf,

    #[arg(
        short = 'q',
        long = "query",
        value_name = "PATH",
        help = "Path to the query sequence file (.2bit, .fa, .fasta, .fna, and gzip variants)."
    )]
    query: PathBuf,

    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to the input .chain file. If not provided, chain data is read from standard input."
    )]
    chain: Option<PathBuf>,

    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        help = "Path for the converted output. If not provided, output is written to standard output."
    )]
    output: Option<PathBuf>,

    #[arg(
        short = 'G',
        long = "gzip",
        help = "Compress convert output with gzip. Requires the `gzip` feature."
    )]
    gzip: bool,
}

/// Conversion targets supported by `chaintools convert`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConvertTarget {
    Vcf,
    Maf,
    Bam,
}

impl std::fmt::Display for ConvertTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertTarget::Vcf => f.write_str("vcf"),
            ConvertTarget::Maf => f.write_str("maf"),
            ConvertTarget::Bam => f.write_str("bam"),
        }
    }
}

/// Runs the convert subcommand.
///
/// # Arguments
///
/// * `args` - Convert arguments
/// * `stdin` - Input stream (used if no --chain provided)
/// * `stdout` - Output stream (used if no --output provided)
/// * `_stderr` - Error/logging output
///
/// # Output
///
/// Returns `Ok(())` on success or `Err(CliError)` on failure
pub fn run<R, W, E>(
    args: ConvertArgs,
    stdin: &mut R,
    stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    validate_output_args(&args)?;
    super::ensure_inputs_exist(
        &[("reference", &args.reference), ("query", &args.query)],
        &[("input chain", args.chain.as_deref())],
    )?;
    if let Some(output) = &args.output {
        super::validate_distinct_paths("--output", output, args.chain.as_deref())?;
    }

    let input_desc = args
        .chain
        .as_deref()
        .map_or_else(|| "<stdin>".to_owned(), |path| path.display().to_string());
    let output_desc = args
        .output
        .as_deref()
        .map_or_else(|| "<stdout>".to_owned(), |path| path.display().to_string());
    log::info!(
        "convert: to={}, reference={}, query={}, input={input_desc}, output={output_desc}",
        args.to,
        args.reference.display(),
        args.query.display()
    );

    if let Some(path) = &args.output {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file);
        if args.gzip {
            run_gzip_output(&args, stdin, writer)?;
        } else {
            let mut writer = writer;
            run_to_writer(&args, stdin, &mut writer)?;
            writer.flush()?;
        }
    } else if args.gzip {
        run_gzip_output(&args, stdin, stdout)?;
    } else {
        run_to_writer(&args, stdin, stdout)?;
        stdout.flush()?;
    }

    Ok(())
}

#[cfg(feature = "gzip")]
fn run_gzip_output<R, W>(args: &ConvertArgs, stdin: &mut R, writer: W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let mut writer = GzEncoder::new(writer, Compression::fast());
    run_to_writer(args, stdin, &mut writer)?;
    writer.try_finish()?;
    writer.get_mut().flush()?;
    Ok(())
}

#[cfg(not(feature = "gzip"))]
fn run_gzip_output<R, W>(_args: &ConvertArgs, _stdin: &mut R, _writer: W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    Err(CliError::Message(
        "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
    ))
}

fn validate_output_args(args: &ConvertArgs) -> Result<(), CliError> {
    if args.gzip && args.to == ConvertTarget::Bam {
        // BAM is block-compressed by its own writer; wrapping it in gzip would
        // produce a file no BAM reader accepts.
        return Err(CliError::Message(
            "--gzip is not supported with --to bam: BAM output is already block-compressed"
                .to_owned(),
        ));
    }

    #[cfg(not(feature = "gzip"))]
    if args.gzip {
        return Err(CliError::Message(
            "--gzip requires chaintools to be built with the `gzip` feature".to_owned(),
        ));
    }
    Ok(())
}

fn run_to_writer<R, W>(args: &ConvertArgs, stdin: &mut R, writer: &mut W) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    let sequences = Sequences {
        reference: SequenceResolver::new(&args.reference)?,
        query: SequenceResolver::new(&args.query)?,
        cache: SequenceCache::default(),
    };

    match args.to {
        ConvertTarget::Vcf => convert_vcf(args, stdin, writer, sequences),
        ConvertTarget::Maf => convert_maf(args, stdin, writer, sequences),
        ConvertTarget::Bam => convert_bam(args, stdin, writer, sequences),
    }
}

/// Streams chains and writes BAM assembly-alignment records.
///
/// `@SQ` entries are taken from the reference genome rather than from a pre-scan
/// of the chains: a BAM header must be complete before the first record, and the
/// chain stream may be an unrewindable pipe. Every chain's declared target size
/// is checked against that header, so contradictory sizes are rejected instead
/// of silently accepted.
#[cfg(feature = "bam")]
fn convert_bam<R, W>(
    args: &ConvertArgs,
    stdin: &mut R,
    writer: &mut W,
    mut sequences: Sequences,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    use noodles_bam as bam;
    use noodles_sam::alignment::io::Write as _;

    let header = bam_header(&sequences.reference)?;
    let mut bam_writer = bam::io::Writer::new(writer);
    bam_writer.write_header(&header)?;

    let mut records = 0u64;
    let chains = for_each_chain(args, stdin, |chain, offset| {
        let reference_id = reference_sequence_id(&header, chain, offset)?;
        let (reference_seq, query_seq) = sequences.fetch_chain(chain)?;

        // One chain's segments are buffered so the QNAME suffix can say whether
        // the chain was split at all; nothing beyond one chain is held.
        let mut segments = Vec::new();
        walk_segments(chain, offset, &reference_seq, &query_seq, |segment| {
            segments.push(segment);
            Ok(1)
        })?;

        let split = segments.len() > 1;
        for (index, segment) in segments.iter().enumerate() {
            let record = bam_record(chain, reference_id, segment, split.then_some(index + 1))?;
            bam_writer.write_alignment_record(&header, &record)?;
            records += 1;
        }
        Ok(())
    })?;

    bam_writer.try_finish()?;
    super::log_summary("convert", &[("chains", chains), ("records", records)]);
    Ok(())
}

/// Builds a `@HD`/`@SQ`-only header from the reference genome's sequences.
#[cfg(feature = "bam")]
fn bam_header(reference: &SequenceResolver) -> Result<noodles_sam::Header, CliError> {
    use std::num::NonZero;

    use noodles_sam::{
        self as sam,
        header::record::value::{Map, map::ReferenceSequence},
    };

    let mut builder = sam::Header::builder().set_header(Default::default());
    for (name, length) in reference.sequences()? {
        let length = NonZero::new(length as usize).ok_or_else(|| {
            CliError::Message(format!(
                "reference sequence {} is empty and cannot be a BAM @SQ entry",
                String::from_utf8_lossy(name)
            ))
        })?;
        builder =
            builder.add_reference_sequence(name.to_vec(), Map::<ReferenceSequence>::new(length));
    }
    Ok(builder.build())
}

/// Resolves a chain's target contig to its `@SQ` index, checking its length.
#[cfg(feature = "bam")]
fn reference_sequence_id(
    header: &noodles_sam::Header,
    chain: &OwnedChain,
    offset: usize,
) -> Result<usize, CliError> {
    let (index, _, reference_sequence) = header
        .reference_sequences()
        .get_full(chain.reference_name.as_slice())
        .ok_or_else(|| {
            CliError::Chain(ChainError::MissingSequence {
                name: String::from_utf8_lossy(&chain.reference_name)
                    .into_owned()
                    .into(),
            })
        })?;

    if usize::from(reference_sequence.length()) != chain.reference_size as usize {
        return Err(CliError::Chain(format_error(
            offset,
            format!(
                "chain declares target {} as {} bases but the reference has {}",
                String::from_utf8_lossy(&chain.reference_name),
                chain.reference_size,
                reference_sequence.length()
            ),
        )));
    }
    Ok(index)
}

/// Builds one BAM record from an aligned segment.
///
/// The record is an assembly-alignment segment, not a sequencing read: MAPQ is
/// 0, qualities are absent, and no read-group or platform metadata is invented.
/// A chain split into several segments yields several records with
/// deterministically suffixed names, so no supplementary bookkeeping is needed.
#[cfg(feature = "bam")]
fn bam_record(
    chain: &OwnedChain,
    reference_id: usize,
    segment: &AlignedSegment,
    segment_index: Option<usize>,
) -> Result<noodles_sam::alignment::RecordBuf, CliError> {
    use noodles_core::Position;
    use noodles_sam::{
        alignment::RecordBuf,
        alignment::record::{Flags, MappingQuality},
    };

    let mut name = chain.query_name.clone();
    write!(&mut name, ".{}", chain.id).expect("writing to Vec cannot fail");
    if let Some(index) = segment_index {
        write!(&mut name, ".{index}").expect("writing to Vec cannot fail");
    }

    let start = Position::new(segment.reference_start as usize + 1).ok_or_else(|| {
        CliError::Message("BAM alignment start overflows a 1-based position".to_owned())
    })?;
    let flags = match chain.query_strand {
        Strand::Plus => Flags::empty(),
        Strand::Minus => Flags::REVERSE_COMPLEMENTED,
    };
    // BAM encodes SEQ in 4 bits per base, which has no lowercase form.
    let bases: Vec<u8> = segment
        .query_text
        .iter()
        .filter(|base| **base != b'-')
        .map(u8::to_ascii_uppercase)
        .collect();

    Ok(RecordBuf::builder()
        .set_name(name)
        .set_flags(flags)
        .set_reference_sequence_id(reference_id)
        .set_alignment_start(start)
        .set_mapping_quality(MappingQuality::new(0).expect("0 is a valid mapping quality"))
        .set_cigar(segment_cigar(segment).into())
        .set_sequence(bases.into())
        .build())
}

/// Derives an `=`/`X`/`I`/`D` CIGAR from a segment's two aligned rows.
///
/// The exact bases are known, so matches and mismatches are distinguished
/// instead of being collapsed into `M`. One-sided chain gaps appear as `-` in one
/// row and become `I`/`D`; no secondary alignment is computed.
#[cfg(feature = "bam")]
fn segment_cigar(segment: &AlignedSegment) -> Vec<noodles_sam::alignment::record::cigar::Op> {
    use noodles_sam::alignment::record::cigar::{Op, op::Kind};

    let mut ops: Vec<Op> = Vec::new();
    for (&reference_base, &query_base) in segment.reference_text.iter().zip(&segment.query_text) {
        let kind = if reference_base == b'-' {
            Kind::Insertion
        } else if query_base == b'-' {
            Kind::Deletion
        } else if reference_base.eq_ignore_ascii_case(&query_base) {
            Kind::SequenceMatch
        } else {
            Kind::SequenceMismatch
        };

        match ops.last_mut() {
            Some(last) if last.kind() == kind => *last = Op::new(kind, last.len() + 1),
            _ => ops.push(Op::new(kind, 1)),
        }
    }
    ops
}

#[cfg(not(feature = "bam"))]
fn convert_bam<R, W>(
    _args: &ConvertArgs,
    _stdin: &mut R,
    _writer: &mut W,
    _sequences: Sequences,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    Err(CliError::Message(
        "--to bam requires chaintools to be built with the `bam` feature".to_owned(),
    ))
}

/// Reference and query sequence access for one conversion run.
struct Sequences {
    reference: SequenceResolver,
    query: SequenceResolver,
    cache: SequenceCache,
}

impl Sequences {
    /// Fetches a chain's target span and its strand-oriented query span.
    ///
    /// The target is always read on `+`; a `-` query is fetched from its forward
    /// coordinates and reverse-complemented, so the returned query bases run in
    /// the same direction as the chain's own query coordinates. This mirrors
    /// [`chaintools::seq::score::chainscore::ChainScorer::score_chain`].
    fn fetch_chain(&mut self, chain: &OwnedChain) -> Result<(Vec<u8>, Vec<u8>), ChainError> {
        let reference_len = span(chain.reference_start, chain.reference_end, "target")?;
        let query_len = span(chain.query_start, chain.query_end, "query")?;

        let reference_seq = self.reference.fetch(
            &mut self.cache,
            &chain.reference_name,
            chain.reference_start,
            reference_len,
        )?;
        let query_seq = match chain.query_strand {
            Strand::Plus => self.query.fetch(
                &mut self.cache,
                &chain.query_name,
                chain.query_start,
                query_len,
            )?,
            Strand::Minus => {
                let start = chain
                    .query_size
                    .checked_sub(chain.query_end)
                    .ok_or_else(|| convert_error("query minus-strand fetch underflows"))?;
                let mut seq =
                    self.query
                        .fetch(&mut self.cache, &chain.query_name, start, query_len)?;
                reverse_complement_in_place(&mut seq);
                seq
            }
        };

        Ok((reference_seq, query_seq))
    }
}

/// Streams chains and writes one VCF describing the query as the ALT genome.
fn convert_vcf<R, W>(
    args: &ConvertArgs,
    stdin: &mut R,
    writer: &mut W,
    mut sequences: Sequences,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    write_vcf_header(writer)?;

    let mut records = 0u64;
    let chains = for_each_chain(args, stdin, |chain, offset| {
        let (reference_seq, query_seq) = sequences.fetch_chain(chain)?;
        records += write_chain_variants(writer, chain, offset, &reference_seq, &query_seq)?;
        Ok(())
    })?;

    super::log_summary("convert", &[("chains", chains), ("records", records)]);
    Ok(())
}

/// Streams chains from the file input or standard input, returning the count.
///
/// One chain is held at a time: it is read, converted, and dropped before the
/// next header is parsed.
fn for_each_chain<R, F>(args: &ConvertArgs, stdin: &mut R, mut emit: F) -> Result<u64, CliError>
where
    R: BufRead,
    F: FnMut(&OwnedChain, usize) -> Result<(), CliError>,
{
    if let Some(path) = &args.chain {
        drive_chains(&mut StreamingReader::from_path(path)?, &mut emit)
    } else {
        drive_chains(&mut StreamingReader::new(stdin), &mut emit)
    }
}

fn drive_chains<R, F>(reader: &mut StreamingReader<R>, emit: &mut F) -> Result<u64, CliError>
where
    R: BufRead,
    F: FnMut(&OwnedChain, usize) -> Result<(), CliError>,
{
    let mut chains = 0u64;
    while let Some(header) = reader.next_header()? {
        let offset = header.offset;
        let blocks = reader.read_blocks(offset)?;
        emit(&header.into_chain(blocks), offset)?;
        chains += 1;
    }
    Ok(chains)
}

fn write_vcf_header<W: Write>(writer: &mut W) -> Result<(), CliError> {
    writer.write_all(b"##fileformat=VCFv4.2\n")?;
    writeln!(writer, "##source=chaintools {}", env!("CARGO_PKG_VERSION"))?;
    writer.write_all(
        b"##INFO=<ID=CHAIN_ID,Number=1,Type=Integer,Description=\"Source chain id\">\n\
          ##INFO=<ID=QUERY_CHROM,Number=1,Type=String,Description=\"Query sequence name\">\n\
          ##INFO=<ID=QUERY_POS,Number=1,Type=Integer,Description=\"1-based query position in the chain query orientation\">\n\
          ##INFO=<ID=STRAND,Number=1,Type=String,Description=\"Chain query strand\">\n\
          #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
    )?;
    Ok(())
}

/// Emits the VCF records for one chain, returning how many were written.
///
/// Walks the chain block by block against the already-fetched target and
/// oriented query spans: aligned bases yield one record per mismatching base,
/// and each gap between blocks yields one indel or replacement record. Records
/// follow chain traversal order and are never buffered for sorting.
fn write_chain_variants<W: Write>(
    writer: &mut W,
    chain: &OwnedChain,
    offset: usize,
    reference_seq: &[u8],
    query_seq: &[u8],
) -> Result<u64, CliError> {
    let mut records = 0u64;
    let mut reference_pos = chain.reference_start;
    let mut query_pos = chain.query_start;
    let last = chain.blocks.len().saturating_sub(1);

    for (index, block) in chain.blocks.iter().enumerate() {
        let size = block.size as usize;
        let reference_off = (reference_pos - chain.reference_start) as usize;
        let query_off = (query_pos - chain.query_start) as usize;
        let reference_block = slice(reference_seq, reference_off, size, offset, "target")?;
        let query_block = slice(query_seq, query_off, size, offset, "query")?;

        for (step, (&reference_base, &query_base)) in
            reference_block.iter().zip(query_block).enumerate()
        {
            let reference_base = reference_base.to_ascii_uppercase();
            let query_base = query_base.to_ascii_uppercase();
            // Soft-masked bases differ only in case, and a substitution against
            // an ambiguous base is not a callable variant.
            if reference_base == query_base || !is_acgt(reference_base) || !is_acgt(query_base) {
                continue;
            }
            let step = step as u32;
            write_vcf_record(
                writer,
                chain,
                u64::from(reference_pos + step) + 1,
                &[reference_base],
                &[query_base],
                query_pos + step,
            )?;
            records += 1;
        }

        if index < last {
            records += write_gap_variant(
                writer,
                chain,
                offset,
                GapContext {
                    reference_seq,
                    query_seq,
                    reference_off: reference_off + size,
                    query_off: query_off + size,
                    reference_pos: reference_pos + block.size,
                    query_pos: query_pos + block.size,
                    reference_gap: block.gap_reference as usize,
                    query_gap: block.gap_query as usize,
                },
            )?;
        }

        reference_pos += block.size + block.gap_reference;
        query_pos += block.size + block.gap_query;
    }

    verify_walk_end(chain, offset, reference_pos, query_pos)?;
    Ok(records)
}

/// Checks that a completed block walk landed exactly on both header ends.
fn verify_walk_end(
    chain: &OwnedChain,
    offset: usize,
    reference_pos: u32,
    query_pos: u32,
) -> Result<(), CliError> {
    if reference_pos != chain.reference_end || query_pos != chain.query_end {
        return Err(CliError::Chain(format_error(
            offset,
            "chain blocks do not sum to the header span",
        )));
    }
    Ok(())
}

/// One inter-block gap, resolved against the fetched chain spans.
struct GapContext<'a> {
    reference_seq: &'a [u8],
    query_seq: &'a [u8],
    reference_off: usize,
    query_off: usize,
    reference_pos: u32,
    query_pos: u32,
    reference_gap: usize,
    query_gap: usize,
}

/// Writes the record for one inter-block gap, returning how many were written.
///
/// A target-only gap is a deletion, a query-only gap is an insertion, and a gap
/// on both sides is emitted as a whole-interval replacement: a chain does not
/// define a base-level alignment inside a dual-sided gap, so none is invented.
fn write_gap_variant<W: Write>(
    writer: &mut W,
    chain: &OwnedChain,
    offset: usize,
    gap: GapContext<'_>,
) -> Result<u64, CliError> {
    if gap.reference_gap == 0 && gap.query_gap == 0 {
        return Ok(0);
    }

    let deleted = slice(
        gap.reference_seq,
        gap.reference_off,
        gap.reference_gap,
        offset,
        "target gap",
    )?
    .to_ascii_uppercase();
    let inserted = slice(
        gap.query_seq,
        gap.query_off,
        gap.query_gap,
        offset,
        "query gap",
    )?
    .to_ascii_uppercase();

    let (position, mut reference_allele, mut alternate_allele) =
        if gap.reference_gap > 0 && gap.query_gap > 0 {
            // Both alleles are non-empty, so no anchor base is needed.
            (u64::from(gap.reference_pos) + 1, deleted, inserted)
        } else if gap.reference_off > 0 {
            // Anchor on the preceding target base (the last base of the block
            // just walked), which is always inside the fetched span.
            let anchor = gap.reference_seq[gap.reference_off - 1].to_ascii_uppercase();
            let mut reference_allele = vec![anchor];
            let mut alternate_allele = vec![anchor];
            reference_allele.extend_from_slice(&deleted);
            alternate_allele.extend_from_slice(&inserted);
            (
                u64::from(gap.reference_pos),
                reference_allele,
                alternate_allele,
            )
        } else {
            // The gap starts at the first base of the chain's target span, so
            // there is no preceding base to anchor on: use the following one
            // rather than underflowing the coordinate.
            let anchor = slice(
                gap.reference_seq,
                gap.reference_off + gap.reference_gap,
                1,
                offset,
                "target anchor",
            )?[0]
                .to_ascii_uppercase();
            let mut reference_allele = deleted;
            let mut alternate_allele = inserted;
            reference_allele.push(anchor);
            alternate_allele.push(anchor);
            (
                u64::from(gap.reference_pos) + 1,
                reference_allele,
                alternate_allele,
            )
        };

    let position = trim_alleles(position, &mut reference_allele, &mut alternate_allele);
    if reference_allele == alternate_allele {
        return Ok(0);
    }

    write_vcf_record(
        writer,
        chain,
        position,
        &reference_allele,
        &alternate_allele,
        gap.query_pos,
    )?;
    Ok(1)
}

/// Trims redundant shared suffix then prefix bases, keeping both alleles non-empty.
///
/// Returns the (possibly advanced) 1-based reference position. This is the
/// cheap, always-safe part of normalization; repeat-aware left alignment is
/// deliberately left to dedicated VCF tooling such as `bcftools norm`.
fn trim_alleles(position: u64, reference: &mut Vec<u8>, alternate: &mut Vec<u8>) -> u64 {
    while reference.len() > 1 && alternate.len() > 1 && reference.last() == alternate.last() {
        reference.pop();
        alternate.pop();
    }

    let mut trimmed = 0usize;
    while reference.len() > trimmed + 1
        && alternate.len() > trimmed + 1
        && reference[trimmed] == alternate[trimmed]
    {
        trimmed += 1;
    }
    reference.drain(..trimmed);
    alternate.drain(..trimmed);
    position + trimmed as u64
}

/// Writes one VCF data line. `position` is 1-based; `query_pos` is 0-based.
fn write_vcf_record<W: Write>(
    writer: &mut W,
    chain: &OwnedChain,
    position: u64,
    reference_allele: &[u8],
    alternate_allele: &[u8],
    query_pos: u32,
) -> Result<(), CliError> {
    writer.write_all(&chain.reference_name)?;
    write!(writer, "\t{position}\t.\t")?;
    writer.write_all(reference_allele)?;
    writer.write_all(b"\t")?;
    writer.write_all(alternate_allele)?;
    write!(writer, "\t.\tPASS\tCHAIN_ID={};QUERY_CHROM=", chain.id)?;
    writer.write_all(&chain.query_name)?;
    writeln!(
        writer,
        ";QUERY_POS={};STRAND={}",
        u64::from(query_pos) + 1,
        strand_symbol(chain.query_strand)
    )?;
    Ok(())
}

/// Streams chains and writes a pairwise MAF.
fn convert_maf<R, W>(
    args: &ConvertArgs,
    stdin: &mut R,
    writer: &mut W,
    mut sequences: Sequences,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
{
    writeln!(
        writer,
        "##maf version=1 scoring=chain\n# chaintools {}\n",
        env!("CARGO_PKG_VERSION")
    )?;

    let mut stanzas = 0u64;
    let chains = for_each_chain(args, stdin, |chain, offset| {
        let (reference_seq, query_seq) = sequences.fetch_chain(chain)?;
        stanzas += walk_segments(chain, offset, &reference_seq, &query_seq, |segment| {
            flush_stanza(writer, chain, &segment)
        })?;
        Ok(())
    })?;

    super::log_summary("convert", &[("chains", chains), ("records", stanzas)]);
    Ok(())
}

/// One gap-aware pairwise alignment segment of a chain.
///
/// This is the unit both MAF and BAM emit: a run of the chain that has a
/// continuous base-level alignment, ending wherever the chain stops defining one.
/// Text is accumulated verbatim from the fetched sequences (soft-masking
/// included) so that stripping `-` reproduces the declared source interval
/// exactly. `start`/`size` are in each row's declared orientation: for a `-`
/// query that is the chain's own query coordinate space, which is already what
/// MAF asks for.
struct AlignedSegment {
    reference_start: u32,
    query_start: u32,
    reference_size: u32,
    query_size: u32,
    reference_text: Vec<u8>,
    query_text: Vec<u8>,
}

impl AlignedSegment {
    fn new(reference_start: u32, query_start: u32) -> Self {
        AlignedSegment {
            reference_start,
            query_start,
            reference_size: 0,
            query_size: 0,
            reference_text: Vec::new(),
            query_text: Vec::new(),
        }
    }

    fn push_aligned(&mut self, reference: &[u8], query: &[u8]) {
        self.reference_text.extend_from_slice(reference);
        self.query_text.extend_from_slice(query);
        self.reference_size += reference.len() as u32;
        self.query_size += query.len() as u32;
    }

    /// Adds unaligned reference bases against `-` on the query row.
    fn push_reference_gap(&mut self, reference: &[u8]) {
        self.reference_text.extend_from_slice(reference);
        self.query_text
            .extend(std::iter::repeat_n(b'-', reference.len()));
        self.reference_size += reference.len() as u32;
    }

    /// Adds unaligned query bases against `-` on the reference row.
    fn push_query_gap(&mut self, query: &[u8]) {
        self.query_text.extend_from_slice(query);
        self.reference_text
            .extend(std::iter::repeat_n(b'-', query.len()));
        self.query_size += query.len() as u32;
    }

    fn is_empty(&self) -> bool {
        self.reference_text.is_empty() && self.query_text.is_empty()
    }
}

/// Walks a chain's blocks and hands each aligned segment to `emit`.
///
/// Aligned blocks and one-sided gaps extend the current segment; a gap on both
/// sides ends it, because a chain records no alignment path through such a gap.
/// Returns the sum of the counts `emit` reports. MAF and BAM split identically,
/// so they share this walk and differ only in what they do with a segment.
fn walk_segments<F>(
    chain: &OwnedChain,
    offset: usize,
    reference_seq: &[u8],
    query_seq: &[u8],
    mut emit: F,
) -> Result<u64, CliError>
where
    F: FnMut(AlignedSegment) -> Result<u64, CliError>,
{
    let mut emitted = 0u64;
    let mut segment = AlignedSegment::new(chain.reference_start, chain.query_start);
    let mut reference_pos = chain.reference_start;
    let mut query_pos = chain.query_start;
    let last = chain.blocks.len().saturating_sub(1);

    for (index, block) in chain.blocks.iter().enumerate() {
        let size = block.size as usize;
        let reference_off = (reference_pos - chain.reference_start) as usize;
        let query_off = (query_pos - chain.query_start) as usize;
        segment.push_aligned(
            slice(reference_seq, reference_off, size, offset, "target")?,
            slice(query_seq, query_off, size, offset, "query")?,
        );

        let reference_gap = block.gap_reference as usize;
        let query_gap = block.gap_query as usize;
        reference_pos += block.size + block.gap_reference;
        query_pos += block.size + block.gap_query;

        if index == last {
            break;
        }
        if reference_gap > 0 && query_gap > 0 {
            // ponytail: chain does not define an alignment path through a
            // dual-sided gap; split here rather than inventing one. Upgrade path
            // is a local aligner over the two unaligned intervals, which is a
            // different tool's job.
            let finished =
                std::mem::replace(&mut segment, AlignedSegment::new(reference_pos, query_pos));
            emitted += emit_non_empty(&mut emit, finished)?;
        } else if reference_gap > 0 {
            segment.push_reference_gap(slice(
                reference_seq,
                reference_off + size,
                reference_gap,
                offset,
                "target gap",
            )?);
        } else if query_gap > 0 {
            segment.push_query_gap(slice(
                query_seq,
                query_off + size,
                query_gap,
                offset,
                "query gap",
            )?);
        }
    }

    verify_walk_end(chain, offset, reference_pos, query_pos)?;
    emitted += emit_non_empty(&mut emit, segment)?;
    Ok(emitted)
}

/// Hands a segment to `emit` unless it carries no alignment columns.
fn emit_non_empty<F>(emit: &mut F, segment: AlignedSegment) -> Result<u64, CliError>
where
    F: FnMut(AlignedSegment) -> Result<u64, CliError>,
{
    if segment.is_empty() {
        return Ok(0);
    }
    emit(segment)
}

/// Writes one MAF stanza.
fn flush_stanza<W: Write>(
    writer: &mut W,
    chain: &OwnedChain,
    stanza: &AlignedSegment,
) -> Result<u64, CliError> {
    writeln!(writer, "a score={} chain_id={}", chain.score, chain.id)?;
    write_maf_row(
        writer,
        &chain.reference_name,
        stanza.reference_start,
        stanza.reference_size,
        chain.reference_strand,
        chain.reference_size,
        &stanza.reference_text,
    )?;
    write_maf_row(
        writer,
        &chain.query_name,
        stanza.query_start,
        stanza.query_size,
        chain.query_strand,
        chain.query_size,
        &stanza.query_text,
    )?;
    writer.write_all(b"\n")?;
    Ok(1)
}

fn write_maf_row<W: Write>(
    writer: &mut W,
    src: &[u8],
    start: u32,
    size: u32,
    strand: Strand,
    src_size: u32,
    text: &[u8],
) -> Result<(), CliError> {
    writer.write_all(b"s ")?;
    writer.write_all(src)?;
    write!(
        writer,
        " {start} {size} {} {src_size} ",
        strand_symbol(strand)
    )?;
    writer.write_all(text)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn strand_symbol(strand: Strand) -> char {
    match strand {
        Strand::Plus => '+',
        Strand::Minus => '-',
    }
}

fn is_acgt(base: u8) -> bool {
    matches!(base, b'A' | b'C' | b'G' | b'T')
}

/// Borrows `length` bases at `offset`, erroring instead of panicking or wrapping.
fn slice<'a>(
    sequence: &'a [u8],
    offset: usize,
    length: usize,
    chain_offset: usize,
    label: &str,
) -> Result<&'a [u8], CliError> {
    let end = offset.checked_add(length).ok_or_else(|| {
        CliError::Chain(format_error(
            chain_offset,
            format!("{label} range overflows"),
        ))
    })?;
    sequence.get(offset..end).ok_or_else(|| {
        CliError::Chain(format_error(
            chain_offset,
            format!("{label} range exceeds the fetched chain sequence"),
        ))
    })
}

fn span(start: u32, end: u32, label: &str) -> Result<u32, ChainError> {
    end.checked_sub(start)
        .ok_or_else(|| convert_error(format!("{label} span is inverted")))
}

fn format_error(offset: usize, message: impl Into<String>) -> ChainError {
    ChainError::Format {
        offset,
        msg: message.into().into(),
    }
}

fn convert_error(message: impl Into<String>) -> ChainError {
    ChainError::Unsupported {
        msg: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Parser)]
    struct ConvertHarness {
        #[command(flatten)]
        args: ConvertArgs,
    }

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    /// Two tiny FASTA genomes in a self-cleaning temp directory.
    struct Genomes {
        dir: PathBuf,
        reference: PathBuf,
        query: PathBuf,
    }

    impl Drop for Genomes {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Writes `reference` and `query` as single-record FASTA genomes.
    ///
    /// The reference record is named `chr1` and the query record `qry1`.
    fn genomes(reference: &str, query: &str) -> Genomes {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chaintools-convert-test-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let reference_path = dir.join("reference.fa");
        let query_path = dir.join("query.fa");
        std::fs::write(&reference_path, format!(">chr1\n{reference}\n")).expect("write reference");
        std::fs::write(&query_path, format!(">qry1\n{query}\n")).expect("write query");
        Genomes {
            dir,
            reference: reference_path,
            query: query_path,
        }
    }

    fn convert_bytes(genomes: &Genomes, to: ConvertTarget, chain: &str) -> Vec<u8> {
        let mut stdin = Cursor::new(chain.as_bytes());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            ConvertArgs {
                to,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect("convert run");
        stdout
    }

    fn convert(genomes: &Genomes, to: ConvertTarget, chain: &str) -> String {
        String::from_utf8(convert_bytes(genomes, to, chain)).expect("utf8 output")
    }

    /// Runs a VCF conversion and returns only the data lines.
    fn vcf_records(genomes: &Genomes, chain: &str) -> Vec<String> {
        convert(genomes, ConvertTarget::Vcf, chain)
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(str::to_owned)
            .collect()
    }

    /// Asserts every record's REF equals the reference genome slice at POS.
    ///
    /// This is the high-value VCF invariant: a record whose REF does not match
    /// the genome it claims to describe is unusable downstream.
    fn assert_ref_matches_genome(reference: &str, records: &[String]) {
        for record in records {
            let fields: Vec<&str> = record.split('\t').collect();
            let position: usize = fields[1].parse().expect("numeric POS");
            let reference_allele = fields[3];
            let start = position - 1;
            assert_eq!(
                reference[start..start + reference_allele.len()].to_ascii_uppercase(),
                reference_allele,
                "REF mismatch in {record}"
            );
        }
    }

    #[test]
    fn parses_minimal_args() {
        let cli = ConvertHarness::try_parse_from([
            "chaintools",
            "--to",
            "vcf",
            "--reference",
            "reference.2bit",
            "--query",
            "query.2bit",
        ])
        .expect("convert arguments should parse");
        assert_eq!(cli.args.to, ConvertTarget::Vcf);
        assert!(cli.args.chain.is_none());
        assert!(cli.args.output.is_none());
    }

    #[test]
    fn requires_both_genomes() {
        for missing in ["--reference", "--query"] {
            let args: Vec<&str> = ["chaintools", "--to", "vcf", missing, "genome.2bit"].to_vec();
            let err = ConvertHarness::try_parse_from(args)
                .expect_err("a single genome should not be enough");
            assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        }
    }

    #[test]
    fn vcf_header_is_emitted_and_identical_alignment_has_no_records() {
        let genomes = genomes("ACGTACGT", "ACGTACGT");
        let output = convert(
            &genomes,
            ConvertTarget::Vcf,
            "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n",
        );
        assert!(output.starts_with("##fileformat=VCFv4.2\n"));
        assert!(output.contains("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"));
        assert!(!output.lines().any(|line| !line.starts_with('#')));
    }

    #[test]
    fn vcf_emits_one_record_per_substitution() {
        let genomes = genomes("ACGTACGT", "ACGTTCGT");
        let records = vcf_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(
            records,
            vec![
                "chr1\t5\t.\tA\tT\t.\tPASS\tCHAIN_ID=1;QUERY_CHROM=qry1;QUERY_POS=5;STRAND=+"
                    .to_owned()
            ]
        );
        assert_ref_matches_genome("ACGTACGT", &records);
    }

    #[test]
    fn vcf_emits_adjacent_substitutions_separately() {
        let genomes = genomes("ACGTACGT", "AGTTACGT");
        let records = vcf_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(records.len(), 2, "{records:?}");
        assert!(records[0].starts_with("chr1\t2\t.\tC\tG\t"));
        assert!(records[1].starts_with("chr1\t3\t.\tG\tT\t"));
        assert_ref_matches_genome("ACGTACGT", &records);
    }

    #[test]
    fn vcf_ignores_soft_masking_and_ambiguous_bases() {
        // Case differs on every base and the query has an N: neither is a
        // callable substitution.
        let genomes = genomes("ACGTACGT", "acgtNcgt");
        let records = vcf_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert!(records.is_empty(), "{records:?}");
    }

    #[test]
    fn vcf_anchors_insertions_on_the_preceding_base() {
        let genomes = genomes("ACGTACGT", "ACGTGACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 8 + 0 8 qry1 9 + 0 9 1\n4\t0\t1\n4\n\n",
        );
        assert_eq!(
            records,
            vec![
                "chr1\t4\t.\tT\tTG\t.\tPASS\tCHAIN_ID=1;QUERY_CHROM=qry1;QUERY_POS=5;STRAND=+"
                    .to_owned()
            ]
        );
        assert_ref_matches_genome("ACGTACGT", &records);
    }

    #[test]
    fn vcf_encodes_multi_base_insertions() {
        let genomes = genomes("ACGTACGT", "ACGTGGGACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 8 + 0 8 qry1 11 + 0 11 1\n4\t0\t3\n4\n\n",
        );
        assert!(
            records[0].starts_with("chr1\t4\t.\tT\tTGGG\t"),
            "{records:?}"
        );
        assert_ref_matches_genome("ACGTACGT", &records);
    }

    #[test]
    fn vcf_encodes_one_base_deletions() {
        let genomes = genomes("ACGTGACGT", "ACGTACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 9 + 0 9 qry1 8 + 0 8 1\n4\t1\t0\n4\n\n",
        );
        assert_eq!(
            records,
            vec![
                "chr1\t4\t.\tTG\tT\t.\tPASS\tCHAIN_ID=1;QUERY_CHROM=qry1;QUERY_POS=5;STRAND=+"
                    .to_owned()
            ]
        );
        assert_ref_matches_genome("ACGTGACGT", &records);
    }

    #[test]
    fn vcf_encodes_multi_base_deletions() {
        let genomes = genomes("ACGTGGGACGT", "ACGTACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 11 + 0 11 qry1 8 + 0 8 1\n4\t3\t0\n4\n\n",
        );
        assert!(
            records[0].starts_with("chr1\t4\t.\tTGGG\tT\t"),
            "{records:?}"
        );
        assert_ref_matches_genome("ACGTGGGACGT", &records);
    }

    #[test]
    fn vcf_encodes_dual_sided_gaps_as_replacements() {
        let genomes = genomes("ACGTGGACGT", "ACGTTTTACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 10 + 0 10 qry1 11 + 0 11 1\n4\t2\t3\n4\n\n",
        );
        assert_eq!(
            records,
            vec![
                "chr1\t5\t.\tGG\tTTT\t.\tPASS\tCHAIN_ID=1;QUERY_CHROM=qry1;QUERY_POS=5;STRAND=+"
                    .to_owned()
            ]
        );
        assert_ref_matches_genome("ACGTGGACGT", &records);
    }

    #[test]
    fn vcf_trims_redundant_bases_from_replacements() {
        // Reference gap "GA" against query gap "TA": the shared trailing A is
        // redundant and is trimmed away.
        let genomes = genomes("ACGTGAACGT", "ACGTTAACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 10 + 0 10 qry1 10 + 0 10 1\n4\t2\t2\n4\n\n",
        );
        assert_eq!(records.len(), 1, "{records:?}");
        assert!(records[0].starts_with("chr1\t5\t.\tG\tT\t"), "{records:?}");
        assert_ref_matches_genome("ACGTGAACGT", &records);
    }

    #[test]
    fn vcf_handles_negative_query_strand() {
        // Query forward "TTGACTAA": the chain's query interval [2,6) read on the
        // minus strand is revcomp("GACT") = "AGTC", which differs from the
        // reference "AGTG" at its last base.
        let genomes = genomes("AGTG", "TTGACTAA");
        let records = vcf_records(&genomes, "chain 100 chr1 4 + 0 4 qry1 8 - 2 6 5\n4\n\n");
        assert_eq!(
            records,
            vec![
                "chr1\t4\t.\tG\tC\t.\tPASS\tCHAIN_ID=5;QUERY_CHROM=qry1;QUERY_POS=6;STRAND=-"
                    .to_owned()
            ]
        );
        assert_ref_matches_genome("AGTG", &records);
    }

    #[test]
    fn vcf_handles_a_variant_at_reference_position_zero() {
        let genomes = genomes("ACGT", "TCGT");
        let records = vcf_records(&genomes, "chain 100 chr1 4 + 0 4 qry1 4 + 0 4 1\n4\n\n");
        assert!(records[0].starts_with("chr1\t1\t.\tA\tT\t"), "{records:?}");
        assert_ref_matches_genome("ACGT", &records);
    }

    #[test]
    fn vcf_uses_a_following_anchor_at_the_start_of_the_target_span() {
        // A leading zero-length block puts the gap at target offset 0, where
        // there is no preceding base to anchor on.
        let genomes = genomes("ACGT", "GACGT");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 4 + 0 4 qry1 5 + 0 5 1\n0\t0\t1\n4\n\n",
        );
        assert_eq!(records.len(), 1, "{records:?}");
        assert!(records[0].starts_with("chr1\t1\t.\tA\tGA\t"), "{records:?}");
        assert_ref_matches_genome("ACGT", &records);
    }

    #[test]
    fn vcf_emits_a_variant_adjacent_to_the_final_block() {
        let genomes = genomes("ACGTGT", "ACGTA");
        let records = vcf_records(
            &genomes,
            "chain 100 chr1 6 + 0 6 qry1 5 + 0 5 1\n4\t1\t0\n1\n\n",
        );
        assert_eq!(records.len(), 2, "{records:?}");
        assert!(records[0].starts_with("chr1\t4\t.\tTG\tT\t"), "{records:?}");
        assert!(records[1].starts_with("chr1\t6\t.\tT\tA\t"), "{records:?}");
        assert_ref_matches_genome("ACGTGT", &records);
    }

    /// Runs a MAF conversion and returns its stanzas as line groups.
    fn maf_stanzas(genomes: &Genomes, chain: &str) -> Vec<Vec<String>> {
        convert(genomes, ConvertTarget::Maf, chain)
            .lines()
            .skip_while(|line| line.starts_with('#') || line.is_empty())
            .collect::<Vec<_>>()
            .split(|line| line.is_empty())
            .filter(|stanza| !stanza.is_empty())
            .map(|stanza| stanza.iter().map(|line| (*line).to_owned()).collect())
            .collect()
    }

    /// Splits an `s` row into its MAF fields.
    fn maf_row(line: &str) -> (String, u32, u32, char, u32, String) {
        let fields: Vec<&str> = line.split(' ').collect();
        assert_eq!(fields[0], "s", "not an s row: {line}");
        (
            fields[1].to_owned(),
            fields[2].parse().expect("numeric start"),
            fields[3].parse().expect("numeric size"),
            fields[4].chars().next().expect("strand"),
            fields[5].parse().expect("numeric srcSize"),
            fields[6].to_owned(),
        )
    }

    /// Asserts a row's gap-free text reproduces its declared source interval.
    ///
    /// `source` is the forward sequence; a `-` row is checked against the
    /// reverse complement, as MAF declares coordinates in the row's own
    /// orientation.
    fn assert_row_matches_source(source: &str, line: &str) {
        let (_, start, size, strand, src_size, text) = maf_row(line);
        assert_eq!(src_size as usize, source.len(), "srcSize mismatch: {line}");
        let bases: String = text.chars().filter(|base| *base != '-').collect();
        assert_eq!(bases.len(), size as usize, "size mismatch: {line}");

        let oriented = match strand {
            '+' => source.as_bytes().to_vec(),
            _ => {
                let mut reversed = source.as_bytes().to_vec();
                reverse_complement_in_place(&mut reversed);
                reversed
            }
        };
        let start = start as usize;
        assert_eq!(
            bases.as_bytes(),
            &oriented[start..start + size as usize],
            "text does not reproduce the declared interval: {line}"
        );
    }

    #[test]
    fn maf_emits_a_pairwise_stanza_for_an_identical_alignment() {
        let genomes = genomes("ACGTACGT", "ACGTACGT");
        let output = convert(
            &genomes,
            ConvertTarget::Maf,
            "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 3\n8\n\n",
        );
        assert!(output.starts_with("##maf version=1 scoring=chain\n"));
        let stanzas = maf_stanzas(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 3\n8\n\n");
        assert_eq!(
            stanzas,
            vec![vec![
                "a score=100 chain_id=3".to_owned(),
                "s chr1 0 8 + 8 ACGTACGT".to_owned(),
                "s qry1 0 8 + 8 ACGTACGT".to_owned(),
            ]]
        );
    }

    #[test]
    fn maf_carries_substitutions_in_the_aligned_text() {
        let genomes = genomes("ACGTACGT", "ACGTTCGT");
        let stanzas = maf_stanzas(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(stanzas[0][1], "s chr1 0 8 + 8 ACGTACGT");
        assert_eq!(stanzas[0][2], "s qry1 0 8 + 8 ACGTTCGT");
        assert_row_matches_source("ACGTACGT", &stanzas[0][1]);
        assert_row_matches_source("ACGTTCGT", &stanzas[0][2]);
    }

    #[test]
    fn maf_emits_reference_only_gaps_as_query_dashes() {
        let genomes = genomes("ACGTGACGT", "ACGTACGT");
        let stanzas = maf_stanzas(
            &genomes,
            "chain 100 chr1 9 + 0 9 qry1 8 + 0 8 1\n4\t1\t0\n4\n\n",
        );
        assert_eq!(stanzas.len(), 1, "{stanzas:?}");
        assert_eq!(stanzas[0][1], "s chr1 0 9 + 9 ACGTGACGT");
        assert_eq!(stanzas[0][2], "s qry1 0 8 + 8 ACGT-ACGT");
        assert_row_matches_source("ACGTGACGT", &stanzas[0][1]);
        assert_row_matches_source("ACGTACGT", &stanzas[0][2]);
    }

    #[test]
    fn maf_emits_query_only_gaps_as_reference_dashes() {
        let genomes = genomes("ACGTACGT", "ACGTGACGT");
        let stanzas = maf_stanzas(
            &genomes,
            "chain 100 chr1 8 + 0 8 qry1 9 + 0 9 1\n4\t0\t1\n4\n\n",
        );
        assert_eq!(stanzas[0][1], "s chr1 0 8 + 8 ACGT-ACGT");
        assert_eq!(stanzas[0][2], "s qry1 0 9 + 9 ACGTGACGT");
        assert_row_matches_source("ACGTACGT", &stanzas[0][1]);
        assert_row_matches_source("ACGTGACGT", &stanzas[0][2]);
    }

    #[test]
    fn maf_keeps_several_one_sided_gaps_in_one_stanza() {
        let genomes = genomes("ACGTGACGTACGT", "ACGTACGTTTACGT");
        let stanzas = maf_stanzas(
            &genomes,
            "chain 100 chr1 13 + 0 13 qry1 14 + 0 14 1\n4\t1\t0\n4\t0\t2\n4\n\n",
        );
        assert_eq!(stanzas.len(), 1, "{stanzas:?}");
        assert_eq!(stanzas[0][1], "s chr1 0 13 + 13 ACGTGACGT--ACGT");
        assert_eq!(stanzas[0][2], "s qry1 0 14 + 14 ACGT-ACGTTTACGT");
        assert_row_matches_source("ACGTGACGTACGT", &stanzas[0][1]);
        assert_row_matches_source("ACGTACGTTTACGT", &stanzas[0][2]);
    }

    #[test]
    fn maf_splits_at_dual_sided_gaps() {
        let genomes = genomes("ACGTGGACGT", "ACGTTTTACGT");
        let stanzas = maf_stanzas(
            &genomes,
            "chain 100 chr1 10 + 0 10 qry1 11 + 0 11 1\n4\t2\t3\n4\n\n",
        );
        assert_eq!(stanzas.len(), 2, "{stanzas:?}");
        assert_eq!(stanzas[0][1], "s chr1 0 4 + 10 ACGT");
        assert_eq!(stanzas[0][2], "s qry1 0 4 + 11 ACGT");
        assert_eq!(stanzas[1][1], "s chr1 6 4 + 10 ACGT");
        assert_eq!(stanzas[1][2], "s qry1 7 4 + 11 ACGT");
        for stanza in &stanzas {
            assert_row_matches_source("ACGTGGACGT", &stanza[1]);
            assert_row_matches_source("ACGTTTTACGT", &stanza[2]);
        }
    }

    #[test]
    fn maf_keeps_minus_strand_query_coordinates_and_orientation() {
        // Query forward "TTGACTAA"; the chain's query interval [2,6) on the
        // minus strand is revcomp("GACT") = "AGTC". The row must declare the
        // minus-strand coordinates, not forward ones relabelled `+`.
        let genomes = genomes("AGTC", "TTGACTAA");
        let stanzas = maf_stanzas(&genomes, "chain 100 chr1 4 + 0 4 qry1 8 - 2 6 9\n4\n\n");
        assert_eq!(
            stanzas,
            vec![vec![
                "a score=100 chain_id=9".to_owned(),
                "s chr1 0 4 + 4 AGTC".to_owned(),
                "s qry1 2 4 - 8 AGTC".to_owned(),
            ]]
        );
        assert_row_matches_source("TTGACTAA", &stanzas[0][2]);
    }

    #[test]
    fn maf_preserves_soft_masking() {
        let genomes = genomes("acgtACGT", "acgtACGT");
        let stanzas = maf_stanzas(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(stanzas[0][1], "s chr1 0 8 + 8 acgtACGT");
    }

    /// Renders each BAM record as `name flags pos cigar seq` for comparison.
    #[cfg(feature = "bam")]
    fn bam_records(genomes: &Genomes, chain: &str) -> (Vec<String>, Vec<(String, u32)>) {
        use noodles_bam as bam;
        use noodles_sam::alignment::record::cigar::op::Kind;

        let bytes = convert_bytes(genomes, ConvertTarget::Bam, chain);
        let mut reader = bam::io::Reader::new(Cursor::new(bytes));
        let header = reader.read_header().expect("read BAM header");
        let references = header
            .reference_sequences()
            .iter()
            .map(|(name, map)| {
                (
                    String::from_utf8_lossy(name).into_owned(),
                    map.length().get() as u32,
                )
            })
            .collect();

        let mut rendered = Vec::new();
        for record in reader.records() {
            let record = record.expect("read BAM record");
            let cigar: String = record
                .cigar()
                .iter()
                .map(|op| {
                    let op = op.expect("valid CIGAR op");
                    let kind = match op.kind() {
                        Kind::SequenceMatch => '=',
                        Kind::SequenceMismatch => 'X',
                        Kind::Insertion => 'I',
                        Kind::Deletion => 'D',
                        other => panic!("unexpected CIGAR kind {other:?}"),
                    };
                    format!("{}{kind}", op.len())
                })
                .collect();
            rendered.push(format!(
                "{} {} {} {cigar} {} {}",
                String::from_utf8_lossy(record.name().expect("record name")),
                record.flags().bits(),
                usize::from(
                    record
                        .alignment_start()
                        .expect("mapped record")
                        .expect("valid position")
                ),
                String::from_utf8_lossy(&record.sequence().iter().collect::<Vec<u8>>()),
                record
                    .mapping_quality()
                    .map_or(255, |quality| quality.get()),
            ));
        }
        (rendered, references)
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_derives_sq_entries_and_emits_one_record_per_chain() {
        let genomes = genomes("ACGTACGT", "ACGTACGT");
        let (records, references) =
            bam_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 4\n8\n\n");
        assert_eq!(references, vec![("chr1".to_owned(), 8)]);
        assert_eq!(records, vec!["qry1.4 0 1 8= ACGTACGT 0".to_owned()]);
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_distinguishes_matches_from_mismatches() {
        let genomes = genomes("ACGTACGT", "ACGTTCGT");
        let (records, _) = bam_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(records, vec!["qry1.1 0 1 4=1X3= ACGTTCGT 0".to_owned()]);
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_encodes_one_sided_gaps_in_the_cigar() {
        let deletion = genomes("ACGTGACGT", "ACGTACGT");
        let (records, _) = bam_records(
            &deletion,
            "chain 100 chr1 9 + 0 9 qry1 8 + 0 8 1\n4\t1\t0\n4\n\n",
        );
        assert_eq!(records, vec!["qry1.1 0 1 4=1D4= ACGTACGT 0".to_owned()]);

        let insertion = genomes("ACGTACGT", "ACGTGACGT");
        let (records, _) = bam_records(
            &insertion,
            "chain 100 chr1 8 + 0 8 qry1 9 + 0 9 1\n4\t0\t1\n4\n\n",
        );
        assert_eq!(records, vec!["qry1.1 0 1 4=1I4= ACGTGACGT 0".to_owned()]);
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_splits_dual_sided_gaps_into_suffixed_records() {
        let genomes = genomes("ACGTGGACGT", "ACGTTTTACGT");
        let (records, _) = bam_records(
            &genomes,
            "chain 100 chr1 10 + 0 10 qry1 11 + 0 11 2\n4\t2\t3\n4\n\n",
        );
        assert_eq!(
            records,
            vec![
                "qry1.2.1 0 1 4= ACGT 0".to_owned(),
                "qry1.2.2 0 7 4= ACGT 0".to_owned(),
            ]
        );
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_flags_and_orients_a_minus_strand_query() {
        let genomes = genomes("AGTC", "TTGACTAA");
        let (records, _) = bam_records(&genomes, "chain 100 chr1 4 + 0 4 qry1 8 - 2 6 6\n4\n\n");
        // FLAG 16 is REVERSE; SEQ is the oriented query, revcomp("GACT").
        assert_eq!(records, vec!["qry1.6 16 1 4= AGTC 0".to_owned()]);
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_uppercases_soft_masked_query_bases() {
        let genomes = genomes("acgtACGT", "acgtACGT");
        let (records, _) = bam_records(&genomes, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n");
        assert_eq!(records, vec!["qry1.1 0 1 8= ACGTACGT 0".to_owned()]);
    }

    #[cfg(feature = "bam")]
    #[test]
    fn bam_rejects_a_chain_target_size_that_contradicts_the_reference() {
        let genomes = genomes("ACGTACGT", "ACGTACGT");
        let mut stdin = Cursor::new(b"chain 100 chr1 9 + 0 8 qry1 8 + 0 8 1\n8\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Bam,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("contradictory target size should be rejected");
        assert!(err.to_string().contains("but the reference has 8"), "{err}");
    }

    #[test]
    fn rejects_gzip_for_bam_output() {
        let genomes = genomes("ACGT", "ACGT");
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Bam,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: true,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("gzip-wrapped BAM should be rejected");
        assert!(
            err.to_string().contains("already block-compressed"),
            "{err}"
        );
    }

    #[test]
    fn rejects_blocks_that_do_not_sum_to_the_header_span() {
        let genomes = genomes("ACGTACGT", "ACGTACGT");
        let mut stdin = Cursor::new(b"chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n4\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Vcf,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("truncated block list should be rejected");
        assert!(err.to_string().contains("do not sum to the header span"));
    }

    #[test]
    fn rejects_coordinates_outside_the_reference() {
        let genomes = genomes("ACGT", "ACGT");
        let mut stdin = Cursor::new(b"chain 100 chr1 8 + 0 8 qry1 4 + 0 4 1\n8\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Vcf,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("out-of-bounds chain should be rejected");
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn rejects_a_missing_query_sequence() {
        let genomes = genomes("ACGT", "ACGT");
        let mut stdin = Cursor::new(b"chain 100 chr1 4 + 0 4 absent 4 + 0 4 1\n4\n\n".to_vec());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Vcf,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: None,
                output: None,
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("missing query sequence should be rejected");
        assert!(err.to_string().contains("missing sequence"), "{err}");
    }

    #[test]
    fn rejects_output_equal_to_the_input_chain() {
        let genomes = genomes("ACGT", "ACGT");
        let chain_path = genomes.dir.join("in.chain");
        std::fs::write(&chain_path, "chain 100 chr1 4 + 0 4 qry1 4 + 0 4 1\n4\n\n")
            .expect("write chain");
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let err = run(
            ConvertArgs {
                to: ConvertTarget::Vcf,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: Some(chain_path.clone()),
                output: Some(chain_path),
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect_err("overwriting the input chain should be rejected");
        assert!(
            err.to_string().contains("must not be the same path"),
            "{err}"
        );
    }

    #[test]
    fn reads_chains_from_a_file_input() {
        let genomes = genomes("ACGTACGT", "ACGTTCGT");
        let chain_path = genomes.dir.join("in.chain");
        std::fs::write(&chain_path, "chain 100 chr1 8 + 0 8 qry1 8 + 0 8 1\n8\n\n")
            .expect("write chain");
        let output_path = genomes.dir.join("out.vcf");
        let mut stdin = Cursor::new(Vec::<u8>::new());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            ConvertArgs {
                to: ConvertTarget::Vcf,
                reference: genomes.reference.clone(),
                query: genomes.query.clone(),
                chain: Some(chain_path),
                output: Some(output_path.clone()),
                gzip: false,
            },
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )
        .expect("convert run");

        let written = std::fs::read_to_string(&output_path).expect("read output");
        assert!(written.contains("chr1\t5\t.\tA\tT\t"), "{written}");
        assert!(stdout.is_empty(), "file output should not touch stdout");
        let _ = Path::new(&output_path);
    }
}
