use super::cli::BenchConfig;
use super::metrics::{BenchmarkSummary, CommandStats};
use super::runner::BenchStats;

pub fn print_results(config: &BenchConfig, stats: &[BenchStats]) {
    if config.csv {
        for stat in stats {
            print_csv(stat);
        }
        return;
    }
    if config.quiet {
        for stat in stats {
            println!("{:.2} requests per second", stat.rps);
        }
        return;
    }

    println!("====== Valkey Benchmark ======");
    println!("Host: {}:{}", config.host, config.port);
    println!("Clients: {}, Requests: {}", config.clients, config.requests);
    if config.pipeline > 1 {
        println!("Pipeline: {}", config.pipeline);
    }
    if let Some(duration) = config.duration {
        println!("Duration: {} seconds", duration);
    }
    println!();
    for stat in stats {
        print_console(stat);
    }
}

pub fn print_help() {
    println!("Usage: versal-benchmark [OPTIONS] [--] [COMMAND ARG...]");
    println!();
    println!("Options:");
    println!("  -h <host>        Server hostname (default: 127.0.0.1)");
    println!("  -p <port>        Server port (default: 6379)");
    println!("  -s <socket>      Server Unix socket");
    println!("  -a <password>    Password to use when connecting");
    println!("  --user <user>    Username for ACL authentication");
    println!("  -u <uri>         Server URI, for example valkey://user:pass@host:port");
    println!("  -c <clients>     Number of parallel clients");
    println!("  -n <requests>    Total number of requests");
    println!("  --duration <sec> Run benchmark for N seconds");
    println!("  --warmup <sec>   Warmup duration per selected test");
    println!("  -d <size>        Data size for __data__");
    println!("  --dbnum <db>     Database number");
    println!("  -r <keyspace>    Keyspace length");
    println!("  --sequential     Use sequential key replacement");
    println!("  -P <pipeline>    Pipeline depth");
    println!("  -t <tests>       Comma-separated test names");
    println!("  -q               Quiet mode");
    println!("  --csv            Output CSV rows");
    println!("  -I               Idle connection mode");
    println!("  -x               Read last command argument from stdin");
    println!("  --dataset <path> Load CSV/TSV dataset");
    println!("  --maxdocs <n>    Limit dataset documents");
    println!("  --ratio <w:r>    Memtier Redis SET/GET ratio");
    println!("  --requests <n>   Memtier Redis total request count");
    println!("  --test-time <s>  Memtier Redis time-based run duration");
    println!("  --run-count <n>  Memtier Redis repeated run count");
    println!("  --clients <n>    Memtier Redis clients per thread");
    println!("  --pipeline <n>   Memtier Redis pipeline depth");
    println!("  --key-pattern <w:r> Memtier key pattern: R, S, or P");
    println!("  --key-prefix <s> Memtier key prefix");
    println!("  --key-minimum <n> Memtier minimum key id");
    println!("  --key-maximum <n> Memtier maximum key id");
    println!("  --data-size <n>  Memtier SET payload size");
    println!("  --random-data    Generate random Memtier SET payloads");
    println!("  --data-size-range <min-max> Memtier payload size range");
    println!("  --rate-limiting <rps> Memtier per-connection request rate limit");
    println!("  --command <line> Memtier Redis command template");
    println!("  --command-ratio <n> Ratio for the most recent --command");
    println!("  --command-key-pattern <R|S|P> Key pattern for the most recent --command");
    println!("  --protocol <redis|resp2|resp3> Memtier Redis protocol");
    println!("  --json-out-file <path> Write memtier-compatible JSON summary");
    println!("  --print-percentiles <list> Comma-separated latency percentiles");
    println!("  --help           Show this help message");
    println!("  -v, --version    Show version information");
}

pub fn write_json_summary(path: &str, summary: &BenchmarkSummary) -> Result<(), String> {
    std::fs::write(path, summary.to_json_string()).map_err(|err| err.to_string())
}

pub fn print_summary_or_json(
    config: &BenchConfig,
    summary: &BenchmarkSummary,
) -> Result<(), String> {
    if let Some(path) = config.memtier.json_out_file.as_deref() {
        write_json_summary(path, summary)?;
    }
    if config.csv {
        for command in &summary.commands {
            print_summary_csv(command);
        }
        return Ok(());
    }
    if config.quiet {
        let rps = if summary.duration_secs > 0.0 {
            summary.total_requests as f64 / summary.duration_secs
        } else {
            0.0
        };
        println!("{rps:.2} requests per second");
        return Ok(());
    }

    println!("====== Memtier Redis Benchmark ======");
    println!("Host: {}:{}", config.host, config.port);
    println!(
        "Clients: {}, Requests: {}",
        config.clients, summary.total_requests
    );
    if config.pipeline > 1 {
        println!("Pipeline: {}", config.pipeline);
    }
    println!("Errors: {}", summary.total_errors);
    println!("Duration: {:.2} seconds", summary.duration_secs);
    println!();
    for command in &summary.commands {
        print_summary_command(command);
    }
    Ok(())
}

pub fn benchmark_summary_from_stats(stats: &[BenchStats]) -> BenchmarkSummary {
    BenchmarkSummary {
        total_requests: stats.iter().map(|stat| stat.requests).sum(),
        total_errors: 0,
        duration_secs: stats.iter().map(|stat| stat.elapsed.as_secs_f64()).sum(),
        commands: stats
            .iter()
            .map(|stat| CommandStats {
                name: stat.name.clone(),
                requests: stat.requests,
                errors: 0,
                rps: stat.rps,
                percentiles: stat.percentiles.clone(),
            })
            .collect(),
    }
}

fn print_console(stat: &BenchStats) {
    println!("====== {} ======", stat.name);
    println!(
        "{} requests completed in {:.2} seconds",
        stat.requests,
        stat.elapsed.as_secs_f64()
    );
    println!("{:.2} requests per second", stat.rps);
    println!(
        "latency min/avg/max: {}/{}/{} usec",
        stat.min_us, stat.p50_us, stat.max_us
    );
    println!(
        "percentiles: p50={} p95={} p99={} p99.9={}",
        stat.p50_us, stat.p95_us, stat.p99_us, stat.p999_us
    );
}

fn print_csv(stat: &BenchStats) {
    println!(
        "\"{}\",\"{}\",\"{:.2}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"",
        stat.name,
        stat.requests,
        stat.rps,
        stat.min_us,
        stat.p50_us,
        stat.p95_us,
        stat.p99_us,
        stat.max_us
    );
}

fn print_summary_command(command: &CommandStats) {
    println!("====== {} ======", command.name);
    println!("{} requests completed", command.requests);
    println!("{:.2} requests per second", command.rps);
    if !command.percentiles.is_empty() {
        let rendered = command
            .percentiles
            .iter()
            .map(|(pct, latency_us)| format!("p{pct}={latency_us}"))
            .collect::<Vec<_>>()
            .join(" ");
        println!("percentiles: {rendered}");
    }
}

fn print_summary_csv(command: &CommandStats) {
    println!(
        "\"{}\",\"{}\",\"{:.2}\",\"{}\"",
        command.name, command.requests, command.rps, command.errors
    );
}
