// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use chaintools::{Strand, StreamingReader};
use clap::Args;

use super::CliError;
use super::stats::{self, ChainStats, TargetStats};

// ponytail: in-memory ceiling, configurable via --memory-ceiling; add per-key external sort if real inputs exceed it.
const DEFAULT_MEMORY_CEILING_GIB: u64 = 16;
const ESTIMATED_SEGMENT_BYTES: u128 = 32;

#[derive(Debug, Args)]
pub struct CompareArgs {
    #[arg(short = 'a', long = "chain-a", value_name = "PATH")]
    chain_a: PathBuf,

    #[arg(short = 'b', long = "chain-b", value_name = "PATH")]
    chain_b: PathBuf,

    #[arg(long, help = "Add per-target agreement, fragmentation, and top chains")]
    by_sequence: bool,

    #[arg(
        long,
        value_name = "N",
        default_value_t = 3,
        help = "Top scored chains to show per target with --by-sequence"
    )]
    top: usize,

    #[arg(
        short = 'M',
        long = "memory-ceiling",
        value_name = "GIB",
        default_value_t = DEFAULT_MEMORY_CEILING_GIB,
        help = "Memory ceiling in GiB for canonical mappings before compare fails"
    )]
    memory_ceiling: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MappingKey {
    target: u32,
    query: u32,
    strand: u8,
    diagonal: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Segment {
    key: MappingKey,
    start: u32,
    end: u32,
}

#[derive(Default)]
struct NameInterner {
    target_ids: HashMap<Vec<u8>, u32>,
    query_ids: HashMap<Vec<u8>, u32>,
    target_names: Vec<Vec<u8>>,
    query_names: Vec<Vec<u8>>,
}

impl NameInterner {
    fn target(&mut self, name: &[u8]) -> Result<u32, CliError> {
        intern_name(&mut self.target_ids, &mut self.target_names, name)
    }

    fn query(&mut self, name: &[u8]) -> Result<u32, CliError> {
        intern_name(&mut self.query_ids, &mut self.query_names, name)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct CoverageStats {
    target_covered_bp: u64,
    query_covered_bp: u64,
    target_multi_bp: u64,
    query_multi_bp: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct TargetAgreement {
    a_pair_bp: u64,
    b_pair_bp: u64,
    shared_pair_bp: u64,
}

#[derive(Debug, Default)]
struct MappingAgreement {
    a_pair_bp: u64,
    b_pair_bp: u64,
    shared_pair_bp: u64,
    targets: Vec<TargetAgreement>,
}

#[derive(Debug, Clone, Copy)]
struct Event {
    sequence: u32,
    position: u32,
    delta: i8,
}

pub fn run<R, W, E>(
    args: CompareArgs,
    _stdin: &mut R,
    stdout: &mut W,
    _stderr: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    if args.top == 0 {
        return Err(CliError::Message(
            "--top must be greater than zero".to_owned(),
        ));
    }
    super::ensure_inputs_exist(
        &[("chain A", &args.chain_a), ("chain B", &args.chain_b)],
        &[],
    )?;

    let stats_a = analyze_path(&args.chain_a)?;
    let stats_b = analyze_path(&args.chain_b)?;
    ensure_compatible(&stats_a, &stats_b)?;
    check_memory_budget(stats_a.blocks, stats_b.blocks, args.memory_ceiling)?;

    let mut names = NameInterner::default();
    let mappings_a = canonicalize_path(&args.chain_a, stats_a.blocks, &mut names)?;
    let mappings_b = canonicalize_path(&args.chain_b, stats_b.blocks, &mut names)?;
    let agreement = mapping_agreement(&mappings_a, &mappings_b, names.target_names.len());
    let coverage_a = coverage(&mappings_a)?;
    let coverage_b = coverage(&mappings_b)?;

    write_comparison(
        stdout,
        &stats_a,
        &stats_b,
        &mappings_a,
        &mappings_b,
        &agreement,
        coverage_a,
        coverage_b,
        &names,
        args.by_sequence,
        args.top,
    )?;
    stdout.flush()?;
    super::log_summary(
        "compare",
        &[
            ("a_chains", stats_a.chains),
            ("b_chains", stats_b.chains),
            ("shared_pair_bp", agreement.shared_pair_bp),
        ],
    );
    Ok(())
}

fn analyze_path(path: &Path) -> Result<ChainStats, CliError> {
    stats::analyze(&mut StreamingReader::from_path(path)?)
}

fn ensure_compatible(a: &ChainStats, b: &ChainStats) -> Result<(), CliError> {
    check_sizes("target", &a.target_sizes, &b.target_sizes)?;
    check_sizes("query", &a.query_sizes, &b.query_sizes)
}

fn check_sizes(
    side: &str,
    a: &BTreeMap<Vec<u8>, u32>,
    b: &BTreeMap<Vec<u8>, u32>,
) -> Result<(), CliError> {
    for (name, size_a) in a {
        if let Some(size_b) = b.get(name)
            && size_a != size_b
        {
            return Err(CliError::Message(format!(
                "conflicting {side} sizes for {}: A={size_a}, B={size_b}",
                String::from_utf8_lossy(name)
            )));
        }
    }
    Ok(())
}

fn check_memory_budget(a_blocks: u64, b_blocks: u64, ceiling_gib: u64) -> Result<(), CliError> {
    let estimated = (u128::from(a_blocks) + u128::from(b_blocks)) * ESTIMATED_SEGMENT_BYTES * 2;
    let budget = u128::from(ceiling_gib) * 1024 * 1024 * 1024;
    if estimated > budget {
        return Err(CliError::Message(format!(
            "canonical mapping estimate ({:.2} GiB) exceeds compare's {ceiling_gib} GiB memory ceiling; per-key external sort is the upgrade path and is not implemented",
            estimated as f64 / (1024.0 * 1024.0 * 1024.0)
        )));
    }
    Ok(())
}

fn intern_name(
    ids: &mut HashMap<Vec<u8>, u32>,
    names: &mut Vec<Vec<u8>>,
    name: &[u8],
) -> Result<u32, CliError> {
    if let Some(id) = ids.get(name) {
        return Ok(*id);
    }
    let id = u32::try_from(names.len())
        .map_err(|_| CliError::Message("more than 2^32 distinct sequence names".to_owned()))?;
    let owned = name.to_vec();
    ids.insert(owned.clone(), id);
    names.push(owned);
    Ok(id)
}

fn canonicalize_path(
    path: &Path,
    expected_blocks: u64,
    names: &mut NameInterner,
) -> Result<Vec<Segment>, CliError> {
    canonicalize(
        &mut StreamingReader::from_path(path)?,
        expected_blocks,
        names,
    )
}

fn canonicalize<R: BufRead>(
    reader: &mut StreamingReader<R>,
    expected_blocks: u64,
    names: &mut NameInterner,
) -> Result<Vec<Segment>, CliError> {
    let capacity = usize::try_from(expected_blocks)
        .map_err(|_| CliError::Message("block count exceeds addressable memory".to_owned()))?;
    let mut segments = Vec::new();
    segments.try_reserve_exact(capacity).map_err(|_| {
        CliError::Message("unable to reserve memory for canonical mapping segments".to_owned())
    })?;

    while let Some(header) = reader.next_header()? {
        let offset = header.offset;
        let blocks = reader.read_blocks(offset)?;
        let chain = header.into_chain(blocks);
        stats::validate_chain(&chain, offset)?;
        let target = names.target(&chain.reference_name)?;
        let query = names.query(&chain.query_name)?;
        let strand = match chain.query_strand {
            Strand::Plus => 0,
            Strand::Minus => 1,
        };
        let mut target_position = u64::from(chain.reference_start);
        let mut query_position = u64::from(chain.query_start);
        let last = chain.blocks.len().saturating_sub(1);

        for (index, block) in chain.blocks.iter().enumerate() {
            let size = u64::from(block.size);
            let target_end = target_position.checked_add(size).ok_or_else(|| {
                CliError::Message("target block coordinate overflows u64".to_owned())
            })?;
            let query_end = query_position.checked_add(size).ok_or_else(|| {
                CliError::Message("query block coordinate overflows u64".to_owned())
            })?;
            let diagonal = match chain.query_strand {
                Strand::Plus => query_position as i64 - target_position as i64,
                Strand::Minus => {
                    let query_forward_end = u64::from(chain.query_size)
                        .checked_sub(query_position)
                        .ok_or_else(|| {
                            CliError::Message(
                                "query block coordinate exceeds its sequence size".to_owned(),
                            )
                        })?;
                    i64::try_from(target_position + query_forward_end).map_err(|_| {
                        CliError::Message("minus-strand mapping diagonal overflows i64".to_owned())
                    })?
                }
            };
            segments.push(Segment {
                key: MappingKey {
                    target,
                    query,
                    strand,
                    diagonal,
                },
                start: u32::try_from(target_position)
                    .map_err(|_| CliError::Message("target block start exceeds u32".to_owned()))?,
                end: u32::try_from(target_end)
                    .map_err(|_| CliError::Message("target block end exceeds u32".to_owned()))?,
            });
            target_position = target_end;
            query_position = query_end;
            if index < last {
                target_position += u64::from(block.gap_reference);
                query_position += u64::from(block.gap_query);
            }
        }
    }

    segments.sort_unstable();
    let mut kept = 0usize;
    for index in 0..segments.len() {
        let segment = segments[index];
        if kept > 0
            && segments[kept - 1].key == segment.key
            && segment.start <= segments[kept - 1].end
        {
            segments[kept - 1].end = segments[kept - 1].end.max(segment.end);
        } else {
            segments[kept] = segment;
            kept += 1;
        }
    }
    segments.truncate(kept);
    Ok(segments)
}

fn mapping_agreement(a: &[Segment], b: &[Segment], target_count: usize) -> MappingAgreement {
    let mut result = MappingAgreement {
        targets: vec![TargetAgreement::default(); target_count],
        ..MappingAgreement::default()
    };
    for segment in a {
        let length = u64::from(segment.end - segment.start);
        result.a_pair_bp += length;
        result.targets[segment.key.target as usize].a_pair_bp += length;
    }
    for segment in b {
        let length = u64::from(segment.end - segment.start);
        result.b_pair_bp += length;
        result.targets[segment.key.target as usize].b_pair_bp += length;
    }

    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].key.cmp(&b[j].key) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal if a[i].end <= b[j].start => i += 1,
            Ordering::Equal if b[j].end <= a[i].start => j += 1,
            Ordering::Equal => {
                let shared = u64::from(a[i].end.min(b[j].end) - a[i].start.max(b[j].start));
                result.shared_pair_bp += shared;
                result.targets[a[i].key.target as usize].shared_pair_bp += shared;
                match a[i].end.cmp(&b[j].end) {
                    Ordering::Less => i += 1,
                    Ordering::Greater => j += 1,
                    Ordering::Equal => {
                        i += 1;
                        j += 1;
                    }
                }
            }
        }
    }
    result
}

fn coverage(segments: &[Segment]) -> Result<CoverageStats, CliError> {
    let (target_covered_bp, target_multi_bp) = projected_coverage(segments, false)?;
    let (query_covered_bp, query_multi_bp) = projected_coverage(segments, true)?;
    Ok(CoverageStats {
        target_covered_bp,
        query_covered_bp,
        target_multi_bp,
        query_multi_bp,
    })
}

fn projected_coverage(segments: &[Segment], query_side: bool) -> Result<(u64, u64), CliError> {
    let capacity = segments
        .len()
        .checked_mul(2)
        .ok_or_else(|| CliError::Message("coverage event count overflows usize".to_owned()))?;
    let mut events = Vec::new();
    events
        .try_reserve_exact(capacity)
        .map_err(|_| CliError::Message("unable to reserve memory for coverage sweep".to_owned()))?;
    for segment in segments {
        let (sequence, start, end) = if query_side {
            let (start, end) = query_interval(*segment)?;
            (segment.key.query, start, end)
        } else {
            (segment.key.target, segment.start, segment.end)
        };
        events.push(Event {
            sequence,
            position: start,
            delta: 1,
        });
        events.push(Event {
            sequence,
            position: end,
            delta: -1,
        });
    }
    events.sort_unstable_by_key(|event| (event.sequence, event.position));

    let mut covered = 0u64;
    let mut multi = 0u64;
    let mut current_sequence = None;
    let mut previous_position = 0u32;
    let mut depth = 0i64;
    let mut index = 0usize;
    while index < events.len() {
        let sequence = events[index].sequence;
        let position = events[index].position;
        if current_sequence == Some(sequence) {
            let length = u64::from(position - previous_position);
            if depth > 0 {
                covered += length;
            }
            if depth > 1 {
                multi += length;
            }
        } else {
            if current_sequence.is_some() && depth != 0 {
                return Err(CliError::Message(
                    "unbalanced coverage intervals during sweep".to_owned(),
                ));
            }
            current_sequence = Some(sequence);
            depth = 0;
        }

        let mut delta = 0i64;
        while index < events.len()
            && events[index].sequence == sequence
            && events[index].position == position
        {
            delta += i64::from(events[index].delta);
            index += 1;
        }
        depth += delta;
        if depth < 0 {
            return Err(CliError::Message(
                "negative coverage depth during sweep".to_owned(),
            ));
        }
        previous_position = position;
    }
    if depth != 0 {
        return Err(CliError::Message(
            "unbalanced coverage intervals during sweep".to_owned(),
        ));
    }
    Ok((covered, multi))
}

fn query_interval(segment: Segment) -> Result<(u32, u32), CliError> {
    let (start, end) = if segment.key.strand == 0 {
        (
            i64::from(segment.start) + segment.key.diagonal,
            i64::from(segment.end) + segment.key.diagonal,
        )
    } else {
        (
            segment.key.diagonal - i64::from(segment.end),
            segment.key.diagonal - i64::from(segment.start),
        )
    };
    if start < 0 || end <= start || end > i64::from(u32::MAX) {
        return Err(CliError::Message(
            "canonical mapping produced an invalid query interval".to_owned(),
        ));
    }
    Ok((start as u32, end as u32))
}

#[allow(clippy::too_many_arguments)]
fn write_comparison<W: Write>(
    writer: &mut W,
    a: &ChainStats,
    b: &ChainStats,
    mappings_a: &[Segment],
    mappings_b: &[Segment],
    agreement: &MappingAgreement,
    coverage_a: CoverageStats,
    coverage_b: CoverageStats,
    names: &NameInterner,
    by_sequence: bool,
    top: usize,
) -> Result<(), CliError> {
    let a_only = agreement.a_pair_bp - agreement.shared_pair_bp;
    let b_only = agreement.b_pair_bp - agreement.shared_pair_bp;

    writeln!(writer, "MAPPING AGREEMENT")?;
    writeln!(writer, "A_unique_pair_bp\t{}", agreement.a_pair_bp)?;
    writeln!(writer, "B_unique_pair_bp\t{}", agreement.b_pair_bp)?;
    writeln!(writer, "shared_pair_bp\t{}", agreement.shared_pair_bp)?;
    writeln!(writer, "A_only_pair_bp\t{a_only}")?;
    writeln!(writer, "B_only_pair_bp\t{b_only}")?;
    writeln!(
        writer,
        "A_retained_by_B\t{}",
        percent_ratio(agreement.shared_pair_bp, agreement.a_pair_bp)
    )?;
    writeln!(
        writer,
        "B_retained_by_A\t{}",
        percent_ratio(agreement.shared_pair_bp, agreement.b_pair_bp)
    )?;
    writeln!(
        writer,
        "pair_Dice\t{}",
        percent_ratio_u128(
            u128::from(agreement.shared_pair_bp) * 2,
            u128::from(agreement.a_pair_bp) + u128::from(agreement.b_pair_bp)
        )
    )?;
    writeln!(
        writer,
        "pair_Jaccard\t{}",
        percent_ratio_u128(
            u128::from(agreement.shared_pair_bp),
            u128::from(agreement.a_pair_bp) + u128::from(agreement.b_pair_bp)
                - u128::from(agreement.shared_pair_bp)
        )
    )?;

    writeln!(writer, "\nCOVERAGE / AMBIGUITY")?;
    writeln!(writer, "metric\tA\tB")?;
    writeln!(
        writer,
        "unique_target_covered_bp\t{}\t{}",
        coverage_a.target_covered_bp, coverage_b.target_covered_bp
    )?;
    writeln!(
        writer,
        "unique_query_covered_bp\t{}\t{}",
        coverage_a.query_covered_bp, coverage_b.query_covered_bp
    )?;
    writeln!(
        writer,
        "target_bp_mapped_more_than_once\t{}\t{}",
        coverage_a.target_multi_bp, coverage_b.target_multi_bp
    )?;
    writeln!(
        writer,
        "query_bp_mapped_more_than_once\t{}\t{}",
        coverage_a.query_multi_bp, coverage_b.query_multi_bp
    )?;

    writeln!(writer, "\nCONTINUITY")?;
    writeln!(writer, "metric\tA\tB")?;
    writeln!(writer, "chains\t{}\t{}", a.chains, b.chains)?;
    writeln!(writer, "blocks\t{}\t{}", a.blocks, b.blocks)?;
    writeln!(
        writer,
        "chain_aligned_bp_n50\t{}\t{}",
        a.chain_aligned_n50, b.chain_aligned_n50
    )?;
    writeln!(
        writer,
        "chain_aligned_bp_l50\t{}\t{}",
        a.chain_aligned_l50, b.chain_aligned_l50
    )?;
    writeln!(
        writer,
        "largest_chain_aligned_bp\t{}\t{}",
        a.largest_chain_aligned_bp, b.largest_chain_aligned_bp
    )?;
    writeln!(
        writer,
        "median_blocks_per_chain\t{:.1}\t{:.1}",
        a.median_blocks_per_chain, b.median_blocks_per_chain
    )?;
    writeln!(
        writer,
        "median_target_top1_share\t{}\t{}",
        percent(a.median_target_top1_share),
        percent(b.median_target_top1_share)
    )?;
    writeln!(
        writer,
        "median_target_top3_share\t{}\t{}",
        percent(a.median_target_top3_share),
        percent(b.median_target_top3_share)
    )?;
    writeln!(
        writer,
        "median_target_chains_to_90pct\t{:.1}\t{:.1}",
        a.median_target_chains_90, b.median_target_chains_90
    )?;
    writeln!(
        writer,
        "max_target_chains_to_90pct\t{}\t{}",
        a.max_target_chains_90, b.max_target_chains_90
    )?;

    writeln!(writer, "\nGAPS")?;
    writeln!(writer, "metric\tA\tB\tabsolute_delta\tpercent_delta")?;
    write_delta_row(writer, "target_gap_bp", a.target_gap_bp, b.target_gap_bp)?;
    write_delta_row(writer, "query_gap_bp", a.query_gap_bp, b.query_gap_bp)?;
    write_delta_row(
        writer,
        "target_only_gap_events",
        a.target_only_gap_events,
        b.target_only_gap_events,
    )?;
    write_delta_row(
        writer,
        "query_only_gap_events",
        a.query_only_gap_events,
        b.query_only_gap_events,
    )?;
    write_delta_row(
        writer,
        "dual_gap_events",
        a.dual_gap_events,
        b.dual_gap_events,
    )?;
    write_delta_row(
        writer,
        "largest_target_gap",
        a.largest_target_gap,
        b.largest_target_gap,
    )?;
    write_delta_row(
        writer,
        "largest_query_gap",
        a.largest_query_gap,
        b.largest_query_gap,
    )?;

    write_interpretation(writer, a, b, agreement, coverage_a, coverage_b)?;
    if by_sequence {
        write_by_sequence(writer, a, b, agreement, names, top)?;
    }

    debug_assert_eq!(
        mappings_a.iter().map(segment_len).sum::<u64>(),
        agreement.a_pair_bp
    );
    debug_assert_eq!(
        mappings_b.iter().map(segment_len).sum::<u64>(),
        agreement.b_pair_bp
    );
    Ok(())
}

fn write_delta_row<W: Write>(writer: &mut W, label: &str, a: u64, b: u64) -> Result<(), CliError> {
    let delta = i128::from(b) - i128::from(a);
    let percent_delta = if a == 0 {
        if b == 0 {
            "0.00%".to_owned()
        } else {
            "n/a".to_owned()
        }
    } else {
        format!("{:.2}%", delta as f64 * 100.0 / a as f64)
    };
    writeln!(writer, "{label}\t{a}\t{b}\t{delta:+}\t{percent_delta}")?;
    Ok(())
}

fn write_interpretation<W: Write>(
    writer: &mut W,
    a: &ChainStats,
    b: &ChainStats,
    agreement: &MappingAgreement,
    coverage_a: CoverageStats,
    coverage_b: CoverageStats,
) -> Result<(), CliError> {
    writeln!(writer, "\nINTERPRETATION")?;
    let identical = agreement.a_pair_bp == agreement.shared_pair_bp
        && agreement.b_pair_bp == agreement.shared_pair_bp;
    if identical && more_fragmented(b, a) {
        writeln!(
            writer,
            "Canonical mappings are identical, but B is more fragmented."
        )?;
    } else if identical && more_fragmented(a, b) {
        writeln!(
            writer,
            "Canonical mappings are identical, but A is more fragmented."
        )?;
    } else if identical {
        writeln!(writer, "Canonical base-pair mappings are identical.")?;
    } else {
        writeln!(
            writer,
            "Canonical mappings differ (pair Dice {}); continuity alone cannot identify a winner.",
            percent_ratio_u128(
                u128::from(agreement.shared_pair_bp) * 2,
                u128::from(agreement.a_pair_bp) + u128::from(agreement.b_pair_bp)
            )
        )?;
    }

    if broader_without_more_ambiguity(coverage_b, coverage_a) {
        writeln!(
            writer,
            "B has broader target/query coverage without more multi-mapped target/query bp."
        )?;
    } else if broader_without_more_ambiguity(coverage_a, coverage_b) {
        writeln!(
            writer,
            "A has broader target/query coverage without more multi-mapped target/query bp."
        )?;
    }
    Ok(())
}

fn more_fragmented(candidate: &ChainStats, other: &ChainStats) -> bool {
    candidate.chain_aligned_n50 < other.chain_aligned_n50
        && candidate.median_target_top1_share < other.median_target_top1_share
        && candidate.median_target_chains_90 > other.median_target_chains_90
}

fn broader_without_more_ambiguity(candidate: CoverageStats, other: CoverageStats) -> bool {
    candidate.target_covered_bp >= other.target_covered_bp
        && candidate.query_covered_bp >= other.query_covered_bp
        && (candidate.target_covered_bp > other.target_covered_bp
            || candidate.query_covered_bp > other.query_covered_bp)
        && candidate.target_multi_bp <= other.target_multi_bp
        && candidate.query_multi_bp <= other.query_multi_bp
}

fn write_by_sequence<W: Write>(
    writer: &mut W,
    a: &ChainStats,
    b: &ChainStats,
    agreement: &MappingAgreement,
    names: &NameInterner,
    top: usize,
) -> Result<(), CliError> {
    let target_names = a
        .targets
        .keys()
        .chain(b.targets.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    writeln!(writer, "\nBY TARGET")?;
    writeln!(
        writer,
        "target\tA_chains\tB_chains\tA_top1\tB_top1\tA_top3\tB_top3\tA_N50\tB_N50\tA_chains_to_90pct\tB_chains_to_90pct\tA_only_pair_bp\tB_only_pair_bp\tshared_pair_bp\tpair_Dice"
    )?;
    for name in &target_names {
        let target_a = a.targets.get(name);
        let target_b = b.targets.get(name);
        let pair = names
            .target_ids
            .get(name.as_slice())
            .and_then(|id| agreement.targets.get(*id as usize))
            .copied()
            .unwrap_or_default();
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            String::from_utf8_lossy(name),
            target_a.map_or(0, TargetStats::chain_count),
            target_b.map_or(0, TargetStats::chain_count),
            percent(target_a.map_or(0.0, |target| target.top1_share)),
            percent(target_b.map_or(0.0, |target| target.top1_share)),
            percent(target_a.map_or(0.0, |target| target.top3_share)),
            percent(target_b.map_or(0.0, |target| target.top3_share)),
            target_a.map_or(0, |target| target.chain_n50),
            target_b.map_or(0, |target| target.chain_n50),
            target_a.map_or(0, |target| target.chains_90),
            target_b.map_or(0, |target| target.chains_90),
            pair.a_pair_bp - pair.shared_pair_bp,
            pair.b_pair_bp - pair.shared_pair_bp,
            pair.shared_pair_bp,
            percent_ratio_u128(
                u128::from(pair.shared_pair_bp) * 2,
                u128::from(pair.a_pair_bp) + u128::from(pair.b_pair_bp)
            )
        )?;
    }

    for name in &target_names {
        if let Some(target) = a.targets.get(name) {
            writeln!(writer, "\nTOP CHAINS A\t{}", String::from_utf8_lossy(name))?;
            stats::write_top_chains(writer, &target.chain_summaries, top)?;
        }
        if let Some(target) = b.targets.get(name) {
            writeln!(writer, "\nTOP CHAINS B\t{}", String::from_utf8_lossy(name))?;
            stats::write_top_chains(writer, &target.chain_summaries, top)?;
        }
    }
    Ok(())
}

fn segment_len(segment: &Segment) -> u64 {
    u64::from(segment.end - segment.start)
}

fn percent(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn percent_ratio(numerator: u64, denominator: u64) -> String {
    percent_ratio_u128(u128::from(numerator), u128::from(denominator))
}

fn percent_ratio_u128(numerator: u128, denominator: u128) -> String {
    if denominator == 0 {
        "n/a".to_owned()
    } else {
        format!("{:.2}%", numerator as f64 * 100.0 / denominator as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Cursor;

    #[derive(Debug, Parser)]
    struct CompareHarness {
        #[command(flatten)]
        args: CompareArgs,
    }

    fn parse_compare_args(argv: &[&str]) -> CompareArgs {
        CompareHarness::try_parse_from(argv).expect("args parse").args
    }

    fn stats_text(text: &str) -> ChainStats {
        stats::analyze(&mut StreamingReader::new(Cursor::new(text.as_bytes())))
            .expect("valid stats fixture")
    }

    fn canonical_text(text: &str, names: &mut NameInterner) -> Vec<Segment> {
        canonicalize(
            &mut StreamingReader::new(Cursor::new(text.as_bytes())),
            16,
            names,
        )
        .expect("valid canonical fixture")
    }

    #[test]
    fn split_chains_keep_exact_mapping_but_expose_fragmentation() {
        let a = "chain 100 chr1 100 + 0 24 qry1 100 + 0 24 7\n12 0 0\n12\n\n";
        let b = "chain 50 chr1 100 + 0 12 qry1 100 + 0 12 99\n12\n\n\
                 chain 50 chr1 100 + 12 24 qry1 100 + 12 24 42\n12\n\n";
        let stats_a = stats_text(a);
        let stats_b = stats_text(b);
        let mut names = NameInterner::default();
        let mappings_a = canonical_text(a, &mut names);
        let mappings_b = canonical_text(b, &mut names);
        let agreement = mapping_agreement(&mappings_a, &mappings_b, names.target_names.len());
        let target_a = stats_a.targets.get(b"chr1".as_slice()).expect("A target");
        let target_b = stats_b.targets.get(b"chr1".as_slice()).expect("B target");

        assert_eq!(agreement.a_pair_bp, 24);
        assert_eq!(agreement.b_pair_bp, 24);
        assert_eq!(agreement.shared_pair_bp, 24);
        assert_eq!(target_a.top1_share, 1.0);
        assert_eq!(target_b.top1_share, 0.5);
        assert_eq!((target_a.chains_90, target_b.chains_90), (1, 2));
        assert!(more_fragmented(&stats_b, &stats_a));
    }

    #[test]
    fn canonicalization_ignores_chain_ids_order_splits_and_duplicates() {
        let a = "chain 1 chr1 100 + 0 10 q1 100 + 0 10 1\n10\n\n\
                 chain 1 chr1 100 + 20 30 q1 100 + 20 30 2\n10\n\n";
        let b = "chain 9 chr1 100 + 20 30 q1 100 + 20 30 77\n10\n\n\
                 chain 9 chr1 100 + 0 10 q1 100 + 0 10 88\n5 0 0\n5\n\n\
                 chain 9 chr1 100 + 0 10 q1 100 + 0 10 99\n10\n\n";
        let mut names = NameInterner::default();
        let mappings_a = canonical_text(a, &mut names);
        let mappings_b = canonical_text(b, &mut names);

        assert_eq!(mappings_a, mappings_b);
        assert_eq!(mappings_a.iter().map(segment_len).sum::<u64>(), 20);
    }

    #[test]
    fn intersection_distinguishes_query_mapping_and_partial_overlap() {
        let duplicated = "chain 1 chr1 100 + 0 10 q1 100 + 0 10 1\n10\n\n\
                          chain 1 chr1 100 + 0 10 q1 100 + 0 10 2\n10\n\n";
        let shifted = "chain 1 chr1 100 + 5 15 q1 100 + 5 15 3\n10\n\n";
        let different_query = "chain 1 chr1 100 + 0 10 q1 100 + 1 11 4\n10\n\n";
        let mut names = NameInterner::default();
        let a = canonical_text(duplicated, &mut names);
        let b = canonical_text(shifted, &mut names);
        let c = canonical_text(different_query, &mut names);
        let overlap = mapping_agreement(&a, &b, names.target_names.len());
        let mismatch = mapping_agreement(&a, &c, names.target_names.len());

        assert_eq!(
            (overlap.a_pair_bp, overlap.b_pair_bp, overlap.shared_pair_bp),
            (10, 10, 5)
        );
        assert_eq!(mismatch.shared_pair_bp, 0);
    }

    #[test]
    fn negative_strand_split_is_exact() {
        let a = "chain 1 chr1 100 + 0 10 q1 100 - 20 30 1\n10\n\n";
        let b = "chain 1 chr1 100 + 5 10 q1 100 - 25 30 3\n5\n\n\
                 chain 1 chr1 100 + 0 5 q1 100 - 20 25 2\n5\n\n";
        let mut names = NameInterner::default();
        let mappings_a = canonical_text(a, &mut names);
        let mappings_b = canonical_text(b, &mut names);
        let agreement = mapping_agreement(&mappings_a, &mappings_b, names.target_names.len());

        assert_eq!(mappings_a, mappings_b);
        assert_eq!(agreement.shared_pair_bp, 10);
    }

    #[test]
    fn compatibility_rejects_conflicting_sizes() {
        let a = stats_text("chain 1 chr1 100 + 0 10 q1 100 + 0 10 1\n10\n\n");
        let b = stats_text("chain 1 chr1 101 + 0 10 q1 100 + 0 10 2\n10\n\n");
        let error = ensure_compatible(&a, &b).expect_err("size conflict");

        assert!(error.to_string().contains("conflicting target sizes"));
    }

    #[test]
    fn coverage_sweep_counts_target_and_query_multimapping() {
        let text = "chain 1 chr1 100 + 0 10 q1 100 + 0 10 1\n10\n\n\
                    chain 1 chr1 100 + 0 10 q2 100 + 0 10 2\n10\n\n\
                    chain 1 chr2 100 + 0 10 q1 100 + 0 10 3\n10\n\n";
        let mut names = NameInterner::default();
        let mappings = canonical_text(text, &mut names);
        let coverage = coverage(&mappings).expect("coverage");

        assert_eq!(coverage.target_covered_bp, 20);
        assert_eq!(coverage.target_multi_bp, 10);
        assert_eq!(coverage.query_covered_bp, 20);
        assert_eq!(coverage.query_multi_bp, 10);
    }

    #[test]
    fn memory_budget_respects_ceiling() {
        // estimate = (a_blocks + b_blocks) * 32 bytes * 2 files = 64 bytes/block.
        // The 16 GiB default admits exactly 2^28 blocks and rejects one more;
        // a raised ceiling admits the overflow case.
        assert!(check_memory_budget(268_435_456, 0, 16).is_ok());
        assert!(check_memory_budget(268_435_457, 0, 16).is_err());
        assert!(check_memory_budget(268_435_457, 0, 17).is_ok());
    }

    #[test]
    fn parses_memory_ceiling_flag() {
        let args = parse_compare_args(&[
            "compare", "-a", "a.chain", "-b", "b.chain", "--memory-ceiling", "32",
        ]);
        assert_eq!(args.memory_ceiling, 32);

        let args = parse_compare_args(&["compare", "-a", "a.chain", "-b", "b.chain", "-M", "4"]);
        assert_eq!(args.memory_ceiling, 4);

        let args = parse_compare_args(&["compare", "-a", "a.chain", "-b", "b.chain"]);
        assert_eq!(args.memory_ceiling, DEFAULT_MEMORY_CEILING_GIB);
    }
}
