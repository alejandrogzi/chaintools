// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;

use chaintools::{ChainError, OwnedChain, Strand, StreamingReader};
use clap::Args;

use super::CliError;

#[derive(Debug, Args)]
pub struct StatsArgs {
    #[arg(
        short = 'c',
        long = "chain",
        value_name = "PATH",
        help = "Path to the input .chain file. If omitted, read standard input"
    )]
    chain: Option<PathBuf>,

    #[arg(long, help = "Add per-target fragmentation and top-chain tables")]
    by_sequence: bool,

    #[arg(
        long,
        value_name = "N",
        default_value_t = 3,
        help = "Top scored chains to show per target with --by-sequence"
    )]
    top: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ChainSummary {
    pub(crate) id: u64,
    pub(crate) score: i64,
    pub(crate) aligned_bp: u64,
    pub(crate) target_span: u64,
    pub(crate) query_span: u64,
    pub(crate) blocks: u64,
    pub(crate) target_gap_bp: u64,
    pub(crate) query_gap_bp: u64,
    pub(crate) largest_gap: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TargetStats {
    pub(crate) total_aligned_bp: u64,
    pub(crate) top1_share: f64,
    pub(crate) top3_share: f64,
    pub(crate) chains_50: u64,
    pub(crate) chains_90: u64,
    pub(crate) chains_95: u64,
    pub(crate) chain_n50: u64,
    pub(crate) chain_summaries: Vec<ChainSummary>,
}

impl TargetStats {
    pub(crate) fn chain_count(&self) -> u64 {
        self.chain_summaries.len() as u64
    }

    fn finish(&mut self) {
        let mut aligned = self
            .chain_summaries
            .iter()
            .map(|chain| chain.aligned_bp)
            .collect::<Vec<_>>();
        aligned.sort_unstable_by(|a, b| b.cmp(a));
        self.total_aligned_bp = aligned.iter().sum();
        self.top1_share = ratio(aligned.first().copied().unwrap_or(0), self.total_aligned_bp);
        self.top3_share = ratio(aligned.iter().take(3).copied().sum(), self.total_aligned_bp);
        self.chains_50 = chains_for_fraction(&aligned, self.total_aligned_bp, 50);
        self.chains_90 = chains_for_fraction(&aligned, self.total_aligned_bp, 90);
        self.chains_95 = chains_for_fraction(&aligned, self.total_aligned_bp, 95);
        self.chain_n50 = n50_l50(&aligned).0;
        self.chain_summaries.sort_unstable_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| b.aligned_bp.cmp(&a.aligned_bp))
                .then_with(|| a.id.cmp(&b.id))
        });
    }
}

#[derive(Debug, Default)]
pub(crate) struct ChainStats {
    pub(crate) chains: u64,
    pub(crate) blocks: u64,
    pub(crate) aligned_pair_bp: u64,
    pub(crate) target_span_bp: u64,
    pub(crate) query_span_bp: u64,
    pub(crate) target_gap_bp: u64,
    pub(crate) query_gap_bp: u64,
    pub(crate) target_only_gap_events: u64,
    pub(crate) query_only_gap_events: u64,
    pub(crate) dual_gap_events: u64,
    pub(crate) largest_target_gap: u64,
    pub(crate) largest_query_gap: u64,
    pub(crate) plus_chains: u64,
    pub(crate) minus_chains: u64,
    pub(crate) plus_aligned_bp: u64,
    pub(crate) minus_aligned_bp: u64,
    pub(crate) score_sum: i128,
    pub(crate) largest_chain_aligned_bp: u64,
    pub(crate) chain_aligned_n50: u64,
    pub(crate) chain_aligned_l50: u64,
    pub(crate) median_chain_aligned_bp: f64,
    pub(crate) median_blocks_per_chain: f64,
    pub(crate) median_target_top1_share: f64,
    pub(crate) median_target_top3_share: f64,
    pub(crate) median_target_chains_90: f64,
    pub(crate) max_target_chains_90: u64,
    pub(crate) target_sizes: BTreeMap<Vec<u8>, u32>,
    pub(crate) query_sizes: BTreeMap<Vec<u8>, u32>,
    pub(crate) targets: BTreeMap<Vec<u8>, TargetStats>,
    chain_aligned_bps: Vec<u64>,
    chain_block_counts: Vec<u64>,
}

impl ChainStats {
    fn add_chain(&mut self, chain: &OwnedChain) -> Result<(), CliError> {
        record_size(
            &mut self.target_sizes,
            &chain.reference_name,
            chain.reference_size,
            "target",
        )?;
        record_size(
            &mut self.query_sizes,
            &chain.query_name,
            chain.query_size,
            "query",
        )?;

        let aligned_bp = chain.blocks.iter().map(|block| u64::from(block.size)).sum();
        let target_span = u64::from(chain.reference_end - chain.reference_start);
        let query_span = u64::from(chain.query_end - chain.query_start);
        let mut target_gap_bp = 0u64;
        let mut query_gap_bp = 0u64;
        let mut largest_gap = 0u64;

        for block in chain
            .blocks
            .iter()
            .take(chain.blocks.len().saturating_sub(1))
        {
            let target_gap = u64::from(block.gap_reference);
            let query_gap = u64::from(block.gap_query);
            target_gap_bp += target_gap;
            query_gap_bp += query_gap;
            self.largest_target_gap = self.largest_target_gap.max(target_gap);
            self.largest_query_gap = self.largest_query_gap.max(query_gap);
            largest_gap = largest_gap.max(target_gap).max(query_gap);
            match (target_gap > 0, query_gap > 0) {
                (true, false) => self.target_only_gap_events += 1,
                (false, true) => self.query_only_gap_events += 1,
                (true, true) => self.dual_gap_events += 1,
                (false, false) => {}
            }
        }

        self.chains += 1;
        self.blocks += chain.blocks.len() as u64;
        self.aligned_pair_bp += aligned_bp;
        self.target_span_bp += target_span;
        self.query_span_bp += query_span;
        self.target_gap_bp += target_gap_bp;
        self.query_gap_bp += query_gap_bp;
        self.score_sum += i128::from(chain.score);
        self.largest_chain_aligned_bp = self.largest_chain_aligned_bp.max(aligned_bp);
        self.chain_aligned_bps.push(aligned_bp);
        self.chain_block_counts.push(chain.blocks.len() as u64);

        match chain.query_strand {
            Strand::Plus => {
                self.plus_chains += 1;
                self.plus_aligned_bp += aligned_bp;
            }
            Strand::Minus => {
                self.minus_chains += 1;
                self.minus_aligned_bp += aligned_bp;
            }
        }

        self.targets
            .entry(chain.reference_name.clone())
            .or_default()
            .chain_summaries
            .push(ChainSummary {
                id: chain.id,
                score: chain.score,
                aligned_bp,
                target_span,
                query_span,
                blocks: chain.blocks.len() as u64,
                target_gap_bp,
                query_gap_bp,
                largest_gap,
            });
        Ok(())
    }

    fn finish(&mut self) {
        let (n50, l50) = n50_l50(&self.chain_aligned_bps);
        self.chain_aligned_n50 = n50;
        self.chain_aligned_l50 = l50;
        self.median_chain_aligned_bp = median_u64(&self.chain_aligned_bps);
        self.median_blocks_per_chain = median_u64(&self.chain_block_counts);

        for target in self.targets.values_mut() {
            target.finish();
        }
        self.median_target_top1_share =
            median_f64(self.targets.values().map(|target| target.top1_share));
        self.median_target_top3_share =
            median_f64(self.targets.values().map(|target| target.top3_share));
        self.median_target_chains_90 = median_u64(
            &self
                .targets
                .values()
                .map(|target| target.chains_90)
                .collect::<Vec<_>>(),
        );
        self.max_target_chains_90 = self
            .targets
            .values()
            .map(|target| target.chains_90)
            .max()
            .unwrap_or(0);
    }
}

pub fn run<R, W, E>(
    args: StatsArgs,
    stdin: &mut R,
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
    super::ensure_inputs_exist(&[], &[("input chain", args.chain.as_deref())])?;

    let stats = if let Some(path) = &args.chain {
        analyze(&mut StreamingReader::from_path(path)?)?
    } else {
        analyze(&mut StreamingReader::new(stdin))?
    };
    write_stats(stdout, &stats, args.by_sequence, args.top)?;
    stdout.flush()?;
    super::log_summary(
        "stats",
        &[("chains", stats.chains), ("blocks", stats.blocks)],
    );
    Ok(())
}

/// Streams one chain input and returns all statistics needed by both commands.
pub(crate) fn analyze<R: BufRead>(reader: &mut StreamingReader<R>) -> Result<ChainStats, CliError> {
    let mut stats = ChainStats::default();
    while let Some(header) = reader.next_header()? {
        let offset = header.offset;
        let blocks = reader.read_blocks(offset)?;
        let chain = header.into_chain(blocks);
        validate_chain(&chain, offset)?;
        stats.add_chain(&chain)?;
    }
    stats.finish();
    Ok(stats)
}

pub(crate) fn validate_chain(chain: &OwnedChain, offset: usize) -> Result<(), CliError> {
    if chain.reference_strand != Strand::Plus {
        return Err(format_error(
            offset,
            "chain target strand must be + for canonical chain coordinates",
        ));
    }
    if chain.reference_start >= chain.reference_end || chain.query_start >= chain.query_end {
        return Err(format_error(offset, "chain span is empty or inverted"));
    }
    if chain.reference_end > chain.reference_size || chain.query_end > chain.query_size {
        return Err(format_error(offset, "chain span exceeds its sequence size"));
    }
    if chain.blocks.iter().any(|block| block.size == 0) {
        return Err(format_error(
            offset,
            "chain contains an empty alignment block",
        ));
    }
    super::validate_block_spans(chain, offset)?;
    Ok(())
}

fn record_size(
    sizes: &mut BTreeMap<Vec<u8>, u32>,
    name: &[u8],
    size: u32,
    side: &str,
) -> Result<(), CliError> {
    if let Some(previous) = sizes.get(name) {
        if *previous != size {
            return Err(CliError::Message(format!(
                "conflicting {side} sizes for {}: {previous} and {size}",
                String::from_utf8_lossy(name)
            )));
        }
    } else {
        sizes.insert(name.to_vec(), size);
    }
    Ok(())
}

fn format_error(offset: usize, message: impl Into<String>) -> CliError {
    CliError::Chain(ChainError::Format {
        offset,
        msg: message.into().into(),
    })
}

fn write_stats<W: Write>(
    writer: &mut W,
    stats: &ChainStats,
    by_sequence: bool,
    top: usize,
) -> Result<(), CliError> {
    writeln!(writer, "GENERAL")?;
    writeln!(writer, "chains\t{}", stats.chains)?;
    writeln!(writer, "blocks\t{}", stats.blocks)?;
    writeln!(writer, "target_sequences\t{}", stats.target_sizes.len())?;
    writeln!(writer, "query_sequences\t{}", stats.query_sizes.len())?;

    writeln!(writer, "\nALIGNMENT")?;
    writeln!(writer, "aligned_pair_bp\t{}", stats.aligned_pair_bp)?;
    writeln!(writer, "target_span_bp\t{}", stats.target_span_bp)?;
    writeln!(writer, "query_span_bp\t{}", stats.query_span_bp)?;

    writeln!(writer, "\nCONTINUITY")?;
    writeln!(
        writer,
        "largest_chain_aligned_bp\t{}",
        stats.largest_chain_aligned_bp
    )?;
    writeln!(writer, "chain_aligned_bp_n50\t{}", stats.chain_aligned_n50)?;
    writeln!(writer, "chain_aligned_bp_l50\t{}", stats.chain_aligned_l50)?;
    writeln!(
        writer,
        "median_chain_aligned_bp\t{:.1}",
        stats.median_chain_aligned_bp
    )?;
    writeln!(
        writer,
        "median_blocks_per_chain\t{:.1}",
        stats.median_blocks_per_chain
    )?;
    writeln!(
        writer,
        "median_target_top1_share\t{}",
        percent(stats.median_target_top1_share)
    )?;
    writeln!(
        writer,
        "median_target_top3_share\t{}",
        percent(stats.median_target_top3_share)
    )?;
    writeln!(
        writer,
        "median_target_chains_to_90pct\t{:.1}",
        stats.median_target_chains_90
    )?;
    writeln!(
        writer,
        "max_target_chains_to_90pct\t{}",
        stats.max_target_chains_90
    )?;

    writeln!(writer, "\nGAPS")?;
    writeln!(writer, "target_gap_bp\t{}", stats.target_gap_bp)?;
    writeln!(writer, "query_gap_bp\t{}", stats.query_gap_bp)?;
    writeln!(
        writer,
        "target_only_gap_events\t{}",
        stats.target_only_gap_events
    )?;
    writeln!(
        writer,
        "query_only_gap_events\t{}",
        stats.query_only_gap_events
    )?;
    writeln!(writer, "dual_gap_events\t{}", stats.dual_gap_events)?;
    writeln!(writer, "largest_target_gap\t{}", stats.largest_target_gap)?;
    writeln!(writer, "largest_query_gap\t{}", stats.largest_query_gap)?;

    writeln!(writer, "\nSTRAND")?;
    writeln!(writer, "plus_chains\t{}", stats.plus_chains)?;
    writeln!(writer, "minus_chains\t{}", stats.minus_chains)?;
    writeln!(writer, "plus_aligned_bp\t{}", stats.plus_aligned_bp)?;
    writeln!(writer, "minus_aligned_bp\t{}", stats.minus_aligned_bp)?;

    writeln!(writer, "\nSCORE")?;
    writeln!(writer, "score_sum\t{}", stats.score_sum)?;
    writeln!(
        writer,
        "mean_score_per_chain\t{:.2}",
        if stats.chains == 0 {
            0.0
        } else {
            stats.score_sum as f64 / stats.chains as f64
        }
    )?;

    if by_sequence {
        writeln!(writer, "\nBY TARGET")?;
        writeln!(
            writer,
            "target\tchains\taligned_bp\ttop1_share\ttop3_share\tchains_to_50pct\tchains_to_90pct\tchains_to_95pct\tchain_n50"
        )?;
        for (name, target) in &stats.targets {
            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                String::from_utf8_lossy(name),
                target.chain_count(),
                target.total_aligned_bp,
                percent(target.top1_share),
                percent(target.top3_share),
                target.chains_50,
                target.chains_90,
                target.chains_95,
                target.chain_n50
            )?;
        }
        for (name, target) in &stats.targets {
            writeln!(writer, "\nTOP CHAINS\t{}", String::from_utf8_lossy(name))?;
            write_top_chains(writer, &target.chain_summaries, top)?;
        }
    }
    Ok(())
}

pub(crate) fn write_top_chains<W: Write>(
    writer: &mut W,
    chains: &[ChainSummary],
    top: usize,
) -> Result<(), CliError> {
    writeln!(
        writer,
        "rank\tchain_id\tscore\taligned_bp\ttarget_span\tquery_span\tblocks\tdensity\ttarget_gap_bp\tquery_gap_bp\tlargest_gap"
    )?;
    for (index, chain) in chains.iter().take(top).enumerate() {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            index + 1,
            chain.id,
            chain.score,
            chain.aligned_bp,
            chain.target_span,
            chain.query_span,
            chain.blocks,
            percent(ratio(chain.aligned_bp, chain.target_span)),
            chain.target_gap_bp,
            chain.query_gap_bp,
            chain.largest_gap
        )?;
    }
    Ok(())
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn percent(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn n50_l50(values: &[u64]) -> (u64, u64) {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    let total = sorted.iter().map(|&value| u128::from(value)).sum::<u128>();
    let mut cumulative = 0u128;
    for (index, value) in sorted.into_iter().enumerate() {
        cumulative += u128::from(value);
        if cumulative * 2 >= total {
            return (value, index as u64 + 1);
        }
    }
    (0, 0)
}

fn chains_for_fraction(sorted: &[u64], total: u64, percent: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    let mut cumulative = 0u128;
    for (index, value) in sorted.iter().copied().enumerate() {
        cumulative += u128::from(value);
        if cumulative * 100 >= u128::from(total) * u128::from(percent) {
            return index as u64 + 1;
        }
    }
    sorted.len() as u64
}

fn median_u64(values: &[u64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => 0.0,
        length if length % 2 == 1 => sorted[middle] as f64,
        _ => (sorted[middle - 1] as f64 + sorted[middle] as f64) / 2.0,
    }
}

fn median_f64(values: impl Iterator<Item = f64>) -> f64 {
    let mut sorted = values.collect::<Vec<_>>();
    sorted.sort_unstable_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => 0.0,
        length if length % 2 == 1 => sorted[middle],
        _ => (sorted[middle - 1] + sorted[middle]) / 2.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn analyze_text(text: &str) -> ChainStats {
        analyze(&mut StreamingReader::new(Cursor::new(text.as_bytes())))
            .expect("valid chain fixture")
    }

    #[test]
    fn collects_alignment_gaps_and_strands() {
        let stats = analyze_text(
            "chain 100 chr1 1000 + 0 46 qry1 1000 + 0 48 1\n\
             10 2 0\n10 0 3\n10 4 5\n10\n\n\
             chain 50 chr2 100 + 0 10 qry2 100 - 20 30 2\n10\n\n",
        );

        assert_eq!(stats.chains, 2);
        assert_eq!(stats.blocks, 5);
        assert_eq!(stats.aligned_pair_bp, 50);
        assert_eq!((stats.target_gap_bp, stats.query_gap_bp), (6, 8));
        assert_eq!(stats.target_only_gap_events, 1);
        assert_eq!(stats.query_only_gap_events, 1);
        assert_eq!(stats.dual_gap_events, 1);
        assert_eq!((stats.largest_target_gap, stats.largest_query_gap), (4, 5));
        assert_eq!((stats.plus_chains, stats.minus_chains), (1, 1));
        assert_eq!((stats.plus_aligned_bp, stats.minus_aligned_bp), (40, 10));
    }

    #[test]
    fn computes_n50_and_target_concentration_from_aligned_bp() {
        let stats = analyze_text(
            "chain 4 chrN 1000 + 0 40 q1 1000 + 0 40 1\n40\n\n\
             chain 3 chrN 1000 + 100 130 q1 1000 + 100 130 2\n30\n\n\
             chain 2 chrN 1000 + 200 220 q1 1000 + 200 220 3\n20\n\n\
             chain 1 chrN 1000 + 300 310 q1 1000 + 300 310 4\n10\n\n",
        );
        let target = stats.targets.get(b"chrN".as_slice()).expect("target");

        assert_eq!((stats.chain_aligned_n50, stats.chain_aligned_l50), (30, 2));
        assert_eq!(stats.median_chain_aligned_bp, 25.0);
        assert!((target.top1_share - 0.4).abs() < f64::EPSILON);
        assert!((target.top3_share - 0.9).abs() < f64::EPSILON);
        assert_eq!(
            (target.chains_50, target.chains_90, target.chains_95),
            (2, 3, 4)
        );
        assert_eq!(target.chain_n50, 30);
    }
}
