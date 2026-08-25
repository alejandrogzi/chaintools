// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chaintools::{Chain, Reader, Strand};
use clap::{Args, ValueEnum};
use genepred::cli::feature::{FeatureKind, FeatureOptions, run as extract};
#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::{CliError, bed::BedSide};

const INPUT_LIST_BUFFER_CAPACITY: usize = 1024 * 1024;

#[derive(Debug, Args)]
pub struct CoverageArgs {
    #[arg(
        short = 'c',
        long = "chains",
        value_name = "PATH",
        num_args = 1..,
        conflicts_with = "file",
        help = "Input chain files. If not provided, chain data is read from standard input."
    )]
    chains: Option<Vec<PathBuf>>,

    #[arg(
        short = 'f',
        long = "file",
        value_name = "PATH",
        conflicts_with = "chains",
        help = "Path to a file listing one input chain path per line"
    )]
    file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "SIDE",
        value_enum,
        help = "Chain side to measure in forward genomic coordinates"
    )]
    side: BedSide,

    #[arg(
        long,
        value_name = "PATH",
        help = "BED, GTF, or GFF annotation whose feature bases define the denominator"
    )]
    intervals: PathBuf,

    #[arg(long, value_enum, help = "Annotation feature to measure")]
    feature: Feature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Feature {
    Cds,
    Exon,
    Intron,
    Utr,
}

impl std::fmt::Display for Feature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Feature::Cds => f.write_str("cds"),
            Feature::Exon => f.write_str("exon"),
            Feature::Intron => f.write_str("intron"),
            Feature::Utr => f.write_str("utr"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Interval {
    start: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy)]
struct IndexedInterval {
    start: u64,
    end: u64,
    offset: usize,
}

#[derive(Debug)]
struct ChromCoverage {
    intervals: Vec<IndexedInterval>,
    bitmap: Vec<AtomicU64>,
    total_bases: u64,
}

struct FeatureSpace {
    chroms: BTreeMap<Vec<u8>, ChromCoverage>,
    records_in: u64,
    intervals_out: u64,
}

pub fn run<R, W, E>(
    args: CoverageArgs,
    stdin: &mut R,
    stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let inputs = collect_input_paths(&args)?;
    super::ensure_inputs_exist(&[("intervals", &args.intervals)], &[])?;
    let chain_inputs = inputs
        .iter()
        .map(|path| ("input chain", path.as_path()))
        .collect::<Vec<_>>();
    super::ensure_inputs_exist(&chain_inputs, &[])?;

    log::info!(
        "coverage: side={}, feature={}, intervals={}, input={}",
        args.side,
        args.feature,
        args.intervals.display(),
        if inputs.is_empty() {
            "<stdin>".to_owned()
        } else {
            format!("{} file(s)", inputs.len())
        }
    );

    let feature_space = load_feature_space(&args)?;
    let chains = mark_inputs(stdin, &inputs, &feature_space.chroms, args.side)?;
    let (total_bases, covered_bases) = write_report(stdout, &feature_space.chroms)?;
    stdout.flush()?;

    super::log_summary(
        "coverage",
        &[
            (
                "files",
                if inputs.is_empty() {
                    1
                } else {
                    inputs.len() as u64
                },
            ),
            ("chains", chains),
            ("annotation_records", feature_space.records_in),
            ("feature_intervals", feature_space.intervals_out),
            ("total_bases", total_bases),
            ("covered_bases", covered_bases),
        ],
    );
    Ok(())
}

fn load_feature_space(args: &CoverageArgs) -> Result<FeatureSpace, CliError> {
    let mut bed3 = Vec::new();
    let summary = extract(
        &args.intervals,
        &mut bed3,
        &FeatureOptions {
            kind: match args.feature {
                Feature::Cds => FeatureKind::Cds,
                Feature::Exon => FeatureKind::Exons,
                Feature::Intron => FeatureKind::Introns,
                Feature::Utr => FeatureKind::Utr,
            },
            bed_type: 3,
            additional_fields: None,
            unique: true,
        },
    )
    .map_err(|err| CliError::Message(err.to_string()))?;

    if summary.records_in > 0 && summary.intervals_out == 0 {
        return Err(no_feature_intervals(args));
    }

    let chroms = index_intervals(parse_bed3(&bed3)?)?;
    if summary.records_in > 0 && chroms.values().all(|chrom| chrom.total_bases == 0) {
        return Err(no_feature_intervals(args));
    }

    Ok(FeatureSpace {
        chroms,
        records_in: summary.records_in,
        intervals_out: summary.intervals_out,
    })
}

fn no_feature_intervals(args: &CoverageArgs) -> CliError {
    CliError::Message(format!(
        "{} yielded no {} intervals: BED3/6 carries no CDS bounds or exon structure",
        args.intervals.display(),
        args.feature
    ))
}

fn parse_bed3(bytes: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<Interval>>, CliError> {
    let mut by_chrom = BTreeMap::<Vec<u8>, Vec<Interval>>::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(|byte| *byte == b'\t');
        let chrom = fields.next().filter(|field| !field.is_empty());
        let start = fields.next().and_then(parse_u64);
        let end = fields.next().and_then(parse_u64);
        if chrom.is_none() || start.is_none() || end.is_none() || fields.next().is_some() {
            return Err(extracted_bed_error(index + 1));
        }
        let (chrom, start, end) = (chrom.unwrap(), start.unwrap(), end.unwrap());
        if start >= end {
            return Err(extracted_bed_error(index + 1));
        }
        by_chrom
            .entry(chrom.to_vec())
            .or_default()
            .push(Interval { start, end });
    }
    Ok(by_chrom)
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn extracted_bed_error(line: usize) -> CliError {
    CliError::Message(format!(
        "genepred emitted an invalid BED3 interval on line {line}"
    ))
}

fn index_intervals(
    raw: BTreeMap<Vec<u8>, Vec<Interval>>,
) -> Result<BTreeMap<Vec<u8>, ChromCoverage>, CliError> {
    let entries = raw.into_iter().collect::<Vec<_>>();

    #[cfg(feature = "parallel")]
    let indexed = entries
        .into_par_iter()
        .map(index_chrom)
        .collect::<Result<Vec<_>, _>>()?;

    #[cfg(not(feature = "parallel"))]
    let indexed = entries
        .into_iter()
        .map(index_chrom)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(indexed.into_iter().collect())
}

fn index_chrom(
    (chrom, intervals): (Vec<u8>, Vec<Interval>),
) -> Result<(Vec<u8>, ChromCoverage), CliError> {
    let mut offset = 0usize;
    let mut total_bases = 0u64;
    let mut indexed = Vec::new();

    for interval in merge_intervals(intervals) {
        let length = interval.end - interval.start;
        indexed.push(IndexedInterval {
            start: interval.start,
            end: interval.end,
            offset,
        });
        offset = offset
            .checked_add(usize::try_from(length).map_err(|_| feature_space_too_large(&chrom))?)
            .ok_or_else(|| feature_space_too_large(&chrom))?;
        total_bases = total_bases
            .checked_add(length)
            .ok_or_else(|| feature_space_too_large(&chrom))?;
    }

    let words = offset.div_ceil(u64::BITS as usize);
    let mut bitmap = Vec::new();
    bitmap
        .try_reserve_exact(words)
        .map_err(|_| feature_space_too_large(&chrom))?;
    bitmap.extend((0..words).map(|_| AtomicU64::new(0)));

    Ok((
        chrom,
        ChromCoverage {
            intervals: indexed,
            bitmap,
            total_bases,
        },
    ))
}

fn feature_space_too_large(chrom: &[u8]) -> CliError {
    CliError::Message(format!(
        "feature space for {} is too large to index",
        String::from_utf8_lossy(chrom)
    ))
}

fn merge_intervals(mut intervals: Vec<Interval>) -> Vec<Interval> {
    intervals.sort_unstable_by_key(|interval| (interval.start, interval.end));
    let mut intervals = intervals.into_iter();
    let Some(mut current) = intervals.next() else {
        return Vec::new();
    };

    let mut merged = Vec::new();
    for interval in intervals {
        if interval.start <= current.end {
            current.end = current.end.max(interval.end);
        } else {
            merged.push(current);
            current = interval;
        }
    }
    merged.push(current);
    merged
}

fn collect_input_paths(args: &CoverageArgs) -> Result<Vec<PathBuf>, CliError> {
    if let Some(paths) = &args.chains {
        return Ok(paths.clone());
    }

    let Some(list_path) = &args.file else {
        return Ok(Vec::new());
    };
    let file = File::open(list_path)?;
    let mut reader = BufReader::with_capacity(INPUT_LIST_BUFFER_CAPACITY, file);
    let mut line = String::new();
    let mut paths = Vec::new();

    while reader.read_line(&mut line)? != 0 {
        let path = line.trim();
        if !path.is_empty() {
            paths.push(PathBuf::from(path));
        }
        line.clear();
    }

    if paths.is_empty() {
        return Err(CliError::Message(format!(
            "{} does not list any input chain files",
            list_path.display()
        )));
    }
    Ok(paths)
}

fn mark_inputs<R: BufRead>(
    stdin: &mut R,
    inputs: &[PathBuf],
    coverage: &BTreeMap<Vec<u8>, ChromCoverage>,
    side: BedSide,
) -> Result<u64, CliError> {
    if inputs.is_empty() {
        let mut bytes = Vec::new();
        stdin.read_to_end(&mut bytes)?;
        return mark_reader(read_chain_bytes(bytes)?, coverage, side);
    }

    let mut chains = 0u64;
    for path in inputs {
        log::debug!("measuring {}", path.display());
        chains += mark_reader(read_chain_path(path)?, coverage, side)?;
    }
    Ok(chains)
}

#[cfg(feature = "parallel")]
fn read_chain_path(path: &Path) -> Result<Reader<Chain>, CliError> {
    Ok(Reader::<Chain>::from_path_parallel(path)?)
}

#[cfg(not(feature = "parallel"))]
fn read_chain_path(path: &Path) -> Result<Reader<Chain>, CliError> {
    Ok(Reader::<Chain>::from_path(path)?)
}

#[cfg(feature = "parallel")]
fn read_chain_bytes(bytes: Vec<u8>) -> Result<Reader<Chain>, CliError> {
    Ok(Reader::<Chain>::from_owned_bytes_parallel(bytes)?)
}

#[cfg(not(feature = "parallel"))]
fn read_chain_bytes(bytes: Vec<u8>) -> Result<Reader<Chain>, CliError> {
    Ok(Reader::<Chain>::from_owned_bytes(bytes)?)
}

fn mark_reader(
    reader: Reader<Chain>,
    coverage: &BTreeMap<Vec<u8>, ChromCoverage>,
    side: BedSide,
) -> Result<u64, CliError> {
    let count = reader.len() as u64;

    #[cfg(feature = "parallel")]
    {
        // ponytail: whole-file parse held in RAM; batch through StreamingReader if peak RSS matters.
        reader
            .chains()
            .collect::<Vec<_>>()
            .into_par_iter()
            .try_for_each(|chain| mark_chain(coverage, chain, side))?;
    }

    #[cfg(not(feature = "parallel"))]
    reader
        .chains()
        .try_for_each(|chain| mark_chain(coverage, chain, side))?;

    Ok(count)
}

fn mark_chain(
    coverage: &BTreeMap<Vec<u8>, ChromCoverage>,
    chain: &Chain,
    side: BedSide,
) -> Result<(), CliError> {
    let name = match side {
        BedSide::Reference => chain.reference_name.as_bytes(),
        BedSide::Query => chain.query_name.as_bytes(),
    };
    let Some(chrom) = coverage.get(name) else {
        return Ok(());
    };
    validate_chain(chain)?;

    let mut reference = u64::from(chain.reference_start);
    let mut query = u64::from(chain.query_start);
    for block in chain.blocks.as_slice() {
        let size = u64::from(block.size);
        let reference_end = reference + size;
        let query_end = query + size;
        let (start, end) = match side {
            BedSide::Reference => (reference, reference_end),
            BedSide::Query => query_block(chain, query, query_end)?,
        };
        mark_overlap(chrom, start, end);

        reference = reference_end + u64::from(block.gap_reference);
        query = query_end + u64::from(block.gap_query);
    }

    if reference != u64::from(chain.reference_end) || query != u64::from(chain.query_end) {
        return Err(chain_error(
            chain,
            "chain block coordinates do not match header end",
        ));
    }
    Ok(())
}

fn validate_chain(chain: &Chain) -> Result<(), CliError> {
    if chain.reference_strand != Strand::Plus {
        return Err(chain_error(
            chain,
            "chain target strand must be + for canonical chain coordinates",
        ));
    }
    if chain.reference_start >= chain.reference_end || chain.query_start >= chain.query_end {
        return Err(chain_error(chain, "chain span is empty or inverted"));
    }
    if chain.reference_end > chain.reference_size || chain.query_end > chain.query_size {
        return Err(chain_error(chain, "chain span exceeds its sequence size"));
    }
    if chain.blocks.as_slice().iter().any(|block| block.size == 0) {
        return Err(chain_error(
            chain,
            "chain contains an empty alignment block",
        ));
    }
    Ok(())
}

fn query_block(
    chain: &Chain,
    oriented_start: u64,
    oriented_end: u64,
) -> Result<(u64, u64), CliError> {
    match chain.query_strand {
        Strand::Plus => Ok((oriented_start, oriented_end)),
        Strand::Minus => {
            let size = u64::from(chain.query_size);
            let start = size
                .checked_sub(oriented_end)
                .ok_or_else(|| chain_error(chain, "query block exceeds its sequence size"))?;
            let end = size
                .checked_sub(oriented_start)
                .ok_or_else(|| chain_error(chain, "query block exceeds its sequence size"))?;
            Ok((start, end))
        }
    }
}

fn chain_error(chain: &Chain, message: &str) -> CliError {
    CliError::Message(format!("chain {}: {message}", chain.id))
}

fn mark_overlap(coverage: &ChromCoverage, block_start: u64, block_end: u64) {
    let mut index = coverage
        .intervals
        .partition_point(|interval| interval.end <= block_start);

    while index < coverage.intervals.len() && coverage.intervals[index].start < block_end {
        let interval = coverage.intervals[index];
        let overlap_start = block_start.max(interval.start);
        let overlap_end = block_end.min(interval.end);
        if overlap_start < overlap_end {
            set_range(
                &coverage.bitmap,
                interval.offset + (overlap_start - interval.start) as usize,
                interval.offset + (overlap_end - interval.start) as usize,
            );
        }
        index += 1;
    }
}

fn set_range(bits: &[AtomicU64], start: usize, end: usize) {
    if start >= end {
        return;
    }
    debug_assert!(end <= bits.len() * u64::BITS as usize);

    let first_word = start / u64::BITS as usize;
    let last_word = (end - 1) / u64::BITS as usize;
    let first_mask = u64::MAX << (start % u64::BITS as usize);
    let end_bit = end % u64::BITS as usize;
    let last_mask = if end_bit == 0 {
        u64::MAX
    } else {
        (1u64 << end_bit) - 1
    };

    if first_word == last_word {
        bits[first_word].fetch_or(first_mask & last_mask, Ordering::Relaxed);
        return;
    }

    bits[first_word].fetch_or(first_mask, Ordering::Relaxed);
    for word in &bits[first_word + 1..last_word] {
        word.fetch_or(u64::MAX, Ordering::Relaxed);
    }
    bits[last_word].fetch_or(last_mask, Ordering::Relaxed);
}

fn covered_bases(coverage: &ChromCoverage) -> u64 {
    coverage
        .bitmap
        .iter()
        .map(|word| u64::from(word.load(Ordering::Relaxed).count_ones()))
        .sum()
}

fn write_report<W: Write>(
    writer: &mut W,
    coverage: &BTreeMap<Vec<u8>, ChromCoverage>,
) -> Result<(u64, u64), CliError> {
    let entries = coverage.iter().collect::<Vec<_>>();

    #[cfg(feature = "parallel")]
    let covered = entries
        .par_iter()
        .map(|(_, chrom)| covered_bases(chrom))
        .collect::<Vec<_>>();

    #[cfg(not(feature = "parallel"))]
    let covered = entries
        .iter()
        .map(|(_, chrom)| covered_bases(chrom))
        .collect::<Vec<_>>();

    writeln!(
        writer,
        "chrom\ttotal_bases\tcovered_bases\tcoverage_fraction"
    )?;
    let mut total_bases = 0u64;
    let mut total_covered = 0u64;
    for ((name, chrom), covered_bases) in entries.into_iter().zip(covered) {
        writer.write_all(name)?;
        writeln!(
            writer,
            "\t{}\t{}\t{:.6}",
            chrom.total_bases,
            covered_bases,
            fraction(covered_bases, chrom.total_bases)
        )?;
        total_bases += chrom.total_bases;
        total_covered += covered_bases;
    }
    writeln!(
        writer,
        "total\t{total_bases}\t{total_covered}\t{:.6}",
        fraction(total_covered, total_bases)
    )?;
    Ok((total_bases, total_covered))
}

fn fraction(covered: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        covered as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{BufReader, Cursor};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    struct TempPath(PathBuf);

    impl TempPath {
        fn with_contents(suffix: &str, contents: &[u8]) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "chaintools-coverage-test-{}-{id}.{suffix}",
                std::process::id()
            ));
            fs::write(&path, contents).expect("write temp file");
            Self(path)
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn args(intervals: &TempPath, side: BedSide, feature: Feature) -> CoverageArgs {
        CoverageArgs {
            chains: None,
            file: None,
            side,
            intervals: intervals.0.clone(),
            feature,
        }
    }

    fn run_stdin(
        intervals: &TempPath,
        side: BedSide,
        feature: Feature,
        chains: &str,
    ) -> Result<String, CliError> {
        let mut stdin = BufReader::new(Cursor::new(chains.as_bytes()));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run(
            args(intervals, side, feature),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        )?;
        Ok(String::from_utf8(stdout).expect("coverage output is UTF-8"))
    }

    #[test]
    fn merge_combines_overlapping_and_bookended_intervals() {
        assert_eq!(
            merge_intervals(vec![
                Interval {
                    start: 400,
                    end: 500,
                },
                Interval {
                    start: 100,
                    end: 200,
                },
                Interval {
                    start: 250,
                    end: 300,
                },
                Interval {
                    start: 150,
                    end: 250,
                },
            ]),
            vec![
                Interval {
                    start: 100,
                    end: 300,
                },
                Interval {
                    start: 400,
                    end: 500,
                },
            ]
        );
    }

    #[test]
    fn bitmap_ranges_are_wordwise_and_idempotent() {
        let bits = (0..2).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        set_range(&bits, 1, 5);
        set_range(&bits, 1, 5);
        set_range(&bits, 60, 68);
        assert_eq!(
            bits.iter()
                .map(|word| word.load(Ordering::Relaxed).count_ones())
                .sum::<u32>(),
            12
        );

        set_range(&bits, 64, 128);
        assert_eq!(
            bits.iter()
                .map(|word| word.load(Ordering::Relaxed).count_ones())
                .sum::<u32>(),
            72
        );
    }

    #[test]
    fn aligned_blocks_do_not_cover_the_chain_gap() {
        let intervals = TempPath::with_contents("bed", b"chr1\t140\t260\n");
        let chains = "chain 100 chr1 1000 + 110 290 q1 1000 + 0 70 1\n\
                      30 120 10\n\
                      30\n\n";

        assert_eq!(
            run_stdin(&intervals, BedSide::Reference, Feature::Exon, chains).unwrap(),
            "chrom\ttotal_bases\tcovered_bases\tcoverage_fraction\n\
             chr1\t120\t0\t0.000000\n\
             total\t120\t0\t0.000000\n"
        );
    }

    #[test]
    fn duplicate_chains_do_not_double_count_and_total_is_reported() {
        let intervals = TempPath::with_contents(
            "bed",
            b"chr1\t100\t300\ttx1\t0\t+\t120\t280\t0,0,0\t2\t50,50,\t0,150,\n\
              chr2\t10\t20\ttx2\t0\t+\t10\t20\t0,0,0\t1\t10,\t0,\n",
        );
        let chain = "chain 100 chr1 1000 + 110 290 q1 1000 + 0 70 1\n\
                     30 120 10\n\
                     30\n\n";
        let chains = format!("{chain}{chain}");

        assert_eq!(
            run_stdin(&intervals, BedSide::Reference, Feature::Exon, &chains).unwrap(),
            "chrom\ttotal_bases\tcovered_bases\tcoverage_fraction\n\
             chr1\t100\t60\t0.600000\n\
             chr2\t10\t0\t0.000000\n\
             total\t110\t60\t0.545455\n"
        );
    }

    #[test]
    fn minus_query_is_marked_in_forward_genomic_coordinates() {
        let intervals = TempPath::with_contents("bed", b"qry1\t435\t490\n");
        let chains = "chain 100 chr1 1000 + 100 160 qry1 500 - 10 65 7\n\
                      20 10 5\n\
                      30\n\n";

        assert_eq!(
            run_stdin(&intervals, BedSide::Query, Feature::Exon, chains).unwrap(),
            "chrom\ttotal_bases\tcovered_bases\tcoverage_fraction\n\
             qry1\t55\t50\t0.909091\n\
             total\t55\t50\t0.909091\n"
        );
    }

    #[test]
    fn bed3_cds_fails_loudly() {
        let intervals = TempPath::with_contents("bed", b"chr1\t0\t10\n");
        let err = run_stdin(&intervals, BedSide::Reference, Feature::Cds, "")
            .expect_err("BED3 has no CDS bounds");
        assert!(err.to_string().contains("yielded no cds intervals"));
    }

    #[test]
    fn malformed_chain_block_span_is_rejected() {
        let intervals = TempPath::with_contents("bed", b"chr1\t0\t10\n");
        let chains = "chain 1 chr1 100 + 0 10 q 100 + 0 10 9\n5\n\n";
        let err = run_stdin(&intervals, BedSide::Reference, Feature::Exon, chains)
            .expect_err("block span must match the header");
        assert!(
            err.to_string()
                .contains("chain block coordinates do not match header end")
        );
    }

    #[test]
    fn minus_reference_strand_is_rejected() {
        let intervals = TempPath::with_contents("bed", b"chr1\t0\t10\n");
        let chains = "chain 1 chr1 100 - 0 10 q 100 + 0 10 9\n10\n\n";
        let err = run_stdin(&intervals, BedSide::Reference, Feature::Exon, chains)
            .expect_err("minus target strand is invalid");
        assert!(err.to_string().contains("chain target strand must be +"));
    }
}
