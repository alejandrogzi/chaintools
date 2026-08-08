use clap::Parser;
use std::process::{Command, ExitStatus, Stdio};

const STDOUT: &str = "out.chain";

const SORTING: [&str; 2] = [
    "target/release/chaintools sort -c {chain} -o out.chain --sort-by score --threads 16",
    "./bench/chainSort {chain} out.chain",
];

const FILTER: [&str; 2] = [
    "target/release/chaintools filter -c {chain} -o out.chain --min-score 15000 --min-target-start 12400 --threads 16",
    "./bench/chainFilter {chain} -minScore=15000 -tStartMin=12400 > out.chain",
];

const MERGE_SORT: [&str; 2] = [
    "target/release/chaintools split -c {chain} --files 10 -o . && target/release/chaintools merge -c chains/* -o out.chain --threads 16 -S score && rm -rf chains",
    "target/release/chaintools split -c {chain} --files 10 -o . && ./bench/chainMergeSort chains/* > out.chain && rm -rf chains",
];

const SWAP: [&str; 2] = [
    "target/release/chaintools swap -c {chain} -o out.chain",
    "./bench/chainSwap {chain} out.chain",
];

// Both tools default to a 0.95 minimum match and to rejecting records that
// several chains can place, so no flags are needed to line them up. `bench.bed`
// is built once by `prepare_liftover_bed`; regenerating it per run would time
// the setup instead of the liftover.
const LIFTOVER: [&str; 2] = [
    "target/release/chaintools liftover -c {chain} -b bench.bed --type 3 -o out.bed -u unmapped.bed",
    "./bench/liftOver bench.bed {chain} out.bed unmapped.bed",
];

/// BED workload for the liftover benchmark.
const LIFTOVER_BED: &str = "bench.bed";

/// Records which chain the BED workload was built from.
///
/// A BED built from one chain but benchmarked against another would silently
/// measure nothing useful, so the source is written next to it and checked.
const LIFTOVER_BED_SOURCE: &str = "bench.bed.source";

/// Keep every Nth aligned chain block when building the liftover workload.
const LIFTOVER_BED_STRIDE: u64 = 10;

/// Command line arguments for the benchmark utility.
///
/// This struct defines configuration options for running performance benchmarks
/// of various chromosome size extraction tools using hyperfine.
#[derive(Debug, Parser)]
pub struct Args {
    /// Path to the reference directory containing test assemblies
    #[clap(short = 'c', long = "chain", help = "Path to chain file")]
    chain: String,

    /// Additional arguments to pass to the hyperfine benchmarking tool
    #[clap(short = 'a',
        value_delimiter = ',',
        num_args = 1..,
        help = "Extra arguments to pass to hyperfine"
    )]
    hyperfine_args: Vec<String>,

    /// Run only the named benchmarks instead of all of them.
    ///
    /// Each benchmark needs its UCSC baseline binary present in `bench/`, so
    /// this allows running the ones whose baseline is installed.
    #[clap(
        long = "only",
        value_delimiter = ',',
        num_args = 1..,
        help = "Only run the named benchmarks (sorting, filter, merge_sort, swap, liftover)"
    )]
    only: Vec<String>,
}

/// Configuration for hyperfine benchmark execution.
///
/// This struct encapsulates all parameters needed to run hyperfine benchmarks,
/// including warmup runs, execution limits, output formats, and commands to test.
pub struct HyperfineCall {
    /// Number of warmup runs before actual benchmarking
    pub warmup: u32,
    /// Minimum number of benchmark runs
    pub min_runs: u32,
    /// Maximum number of benchmark runs (optional)
    pub max_runs: Option<u32>,
    /// Path to export results in CSV format
    pub export_csv: Option<String>,
    /// Path to export results in Markdown format
    pub export_markdown: Option<String>,
    /// Parameterized variables for command substitution
    pub parameters: Vec<(String, Vec<String>)>,
    /// Setup command to run before each benchmark
    pub setup: Option<String>,
    /// Cleanup command to run after each benchmark
    pub cleanup: Option<String>,
    /// List of commands to benchmark
    pub commands: Vec<String>,
    /// Additional hyperfine command line arguments
    pub extras: Vec<String>,
}

impl Default for HyperfineCall {
    /// Creates a default HyperfineCall with sensible baseline settings.
    ///
    /// Sets up reasonable defaults for benchmarking with 3 warmup runs
    /// and 5 minimum runs, suitable for most performance testing scenarios.
    fn default() -> Self {
        Self {
            warmup: 3,
            min_runs: 5,
            max_runs: None,
            export_csv: None,
            export_markdown: None,
            parameters: Vec::new(),
            setup: None,
            cleanup: None,
            commands: Vec::new(),
            extras: Vec::new(),
        }
    }
}

impl HyperfineCall {
    /// Executes the hyperfine benchmark with the configured parameters.
    ///
    /// This method builds and executes a hyperfine command using the struct's
    /// configuration. It sets up all command line arguments including warmup,
    /// runs, exports, parameters, setup/cleanup commands, and the actual
    /// benchmark commands.
    ///
    /// # Returns
    ///
    /// ExitStatus from the hyperfine process execution
    ///
    /// # Panics
    ///
    /// Panics if hyperfine command cannot be executed
    pub fn invoke(&self) -> ExitStatus {
        let mut command = Command::new("hyperfine");

        command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .stdin(Stdio::null());

        command.arg("--warmup").arg(self.warmup.to_string());
        command.arg("--min-runs").arg(self.min_runs.to_string());
        if let Some(export_csv) = &self.export_csv {
            command.arg("--export-csv").arg(export_csv);
        }
        if let Some(export_markdown) = &self.export_markdown {
            command.arg("--export-markdown").arg(export_markdown);
        }
        for (flag, values) in &self.parameters {
            command.arg("-L").arg(flag).arg(values.join(","));
        }
        if let Some(setup) = &self.setup {
            command.arg("--setup").arg(setup);
        }
        if let Some(cleanup) = &self.cleanup {
            command.arg("--cleanup").arg(cleanup);
        }
        if let Some(max_runs) = self.max_runs {
            command.arg("--max-runs").arg(max_runs.to_string());
        }
        if !self.extras.is_empty() {
            command.args(&self.extras);
        }

        for cmd in &self.commands {
            command.arg(cmd);
        }

        command.status().expect("Failed to run hyperfine")
    }
}

/// Runs comprehensive benchmarks for chromosome size extraction tools.
///
/// This function sets up and executes hyperfine benchmarks comparing multiple
/// tools across various genome assemblies. It configures warmup runs, output formats,
/// parameterized assembly testing, and cleanup operations.
///
/// # Returns
///
/// Tuple containing (csv_path, markdown_path) for the benchmark results
///
/// # Errors
///
/// Returns error if benchmark fails or if file system operations fail
///
/// # Examples
///
/// ```ignore
/// let (csv, md) = benchmark()?;
/// println!("Results: {} {}", csv, md);
/// ```
fn benchmark() {
    let args = Args::parse();

    std::fs::create_dir_all("runs").unwrap_or_else(|e| panic!("{}", e));
    prepare_liftover_bed(&args.chain);

    // (input, commands, run name, warmup, min runs, max runs). `swap` and
    // `liftover` read every chain of a multi-hundred-megabyte input on each run,
    // so they get fewer repetitions than the lighter benchmarks.
    let triplets = vec![
        (&args.chain, SORTING.to_vec(), "sorting", 3, 3, 10),
        (&args.chain, FILTER.to_vec(), "filter", 3, 3, 10),
        (&args.chain, MERGE_SORT.to_vec(), "merge_sort", 3, 3, 10),
        (&args.chain, SWAP.to_vec(), "swap", 3, 3, 10),
        (&args.chain, LIFTOVER.to_vec(), "liftover", 3, 3, 10),
    ];

    for (chain, tools, run_name, warmup, min_runs, max_runs) in triplets {
        if !args.only.is_empty() && !args.only.iter().any(|name| name == run_name) {
            continue;
        }
        let csv = format!("bench_{}.csv", run_name);
        let md = format!("bench_{}.md", run_name);

        #[allow(clippy::needless_update)]
        let code = HyperfineCall {
            warmup,
            min_runs,
            max_runs: Some(max_runs),
            export_csv: Some(format!("runs/{}", csv).to_string()),
            export_markdown: Some(format!("runs/{}", md).to_string()),
            parameters: vec![("chain".to_string(), vec![chain.to_string()])],
            setup: Some("cargo build --release --all-features".to_string()),
            // Deliberately does not remove the liftover BED: that is an input.
            cleanup: Some(format!(
                "rm -rf output {} chains out.bed unmapped.bed",
                STDOUT
            )),
            commands: tools
                .iter()
                .map(|cmd| cmd.to_string())
                .collect::<Vec<String>>(),
            extras: args
                .hyperfine_args
                .iter()
                .map(|s| format!("--{}", s))
                .collect(),
            ..Default::default()
        }
        .invoke()
        .code()
        .expect("Benchmark terminated unexpectedly");

        if code != 0 {
            eprintln!("Benchmark failed with exit code {}", code);
        }
    }
}

/// Builds the BED workload for the liftover benchmark, once.
///
/// The intervals are the chain's own aligned blocks, sampled every
/// [`LIFTOVER_BED_STRIDE`] blocks, so the records genuinely map and the
/// benchmark measures candidate lookup plus mapping rather than the unmapped
/// path. Whole chain spans would be the wrong workload: a multi-megabase span
/// cannot meet a 0.95 minimum match. Skipped when the file already exists, so
/// repeated benchmark runs reuse the same input.
fn prepare_liftover_bed(chain: &str) {
    let built_from_this_chain = std::fs::read_to_string(LIFTOVER_BED_SOURCE)
        .is_ok_and(|source| source.trim() == chain.trim());
    if std::path::Path::new(LIFTOVER_BED).exists() && built_from_this_chain {
        eprintln!("reusing existing {LIFTOVER_BED}");
        return;
    }

    build_release();
    eprintln!("building {LIFTOVER_BED} from {chain}");
    let script = format!(
        "target/release/chaintools --level off bed -c {chain} --side reference --type 12 \
         | awk 'BEGIN{{OFS=\"\\t\"}} \
           {{n=split($11,sz,\",\"); split($12,st,\",\"); \
             for(i=1;i<=n;i++) if(sz[i]!=\"\") {{k++; if(k%{LIFTOVER_BED_STRIDE}==0) \
               print $1, $2+st[i], $2+st[i]+sz[i]}}}}' > {LIFTOVER_BED}"
    );
    let status = Command::new("sh")
        .arg("-c")
        .arg(&script)
        .status()
        .expect("Failed to build the liftover BED workload");
    assert!(status.success(), "building {LIFTOVER_BED} failed");
    std::fs::write(LIFTOVER_BED_SOURCE, chain).expect("Failed to record the BED source chain");
}

/// Builds the release binaries the benchmark commands invoke.
fn build_release() {
    let status = Command::new("cargo")
        .args(["build", "--release", "--all-features"])
        .status()
        .expect("Failed to run cargo build");
    assert!(
        status.success(),
        "cargo build --release --all-features failed"
    );
}

/// Main entry point for the benchmark utility.
///
/// This function parses command line arguments, runs the benchmark suite,
/// and reports the location of result files or any errors that occurred.
fn main() {
    benchmark();
}
