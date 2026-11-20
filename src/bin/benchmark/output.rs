use std::io::{self, Write};

use super::cli::BenchConfig;
use super::metrics::{BenchmarkSummary, CommandStats};
use super::runner::BenchStats;

pub fn print_results(config: &BenchConfig, stats: &[BenchStats]) {
    write_results(&mut io::stdout().lock(), config, stats)
        .expect("Failed to write benchmark results");
}

fn write_results(
    out: &mut impl Write,
    config: &BenchConfig,
    stats: &[BenchStats],
) -> io::Result<()> {
    if config.csv {
        writeln!(
            out,
            "\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\
             \"p95_latency_ms\",\"p99_latency_ms\",\"p999_latency_ms\",\"max_latency_ms\""
        )?;
        for stat in stats {
            write_csv(out, stat)?;
        }
        return Ok(());
    }
    if config.quiet {
        for stat in stats {
            writeln!(
                out,
                "{}: {:.2} requests per second, p50={:.3} msec",
                stat.name,
                stat.rps,
                stat.p50_us as f64 / 1000.0
            )?;
        }
        return Ok(());
    }

    writeln!(out, "====== Valkey Benchmark ======")?;
    writeln!(out, "Host: {}:{}", config.host, config.port)?;
    writeln!(
        out,
        "Clients: {}, Requests: {}",
        config.clients, config.requests
    )?;
    if config.pipeline > 1 {
        writeln!(out, "Pipeline: {}", config.pipeline)?;
    }
    if let Some(duration) = config.duration {
        writeln!(out, "Duration: {} seconds", duration)?;
    }
    writeln!(out)?;
    for stat in stats {
        write_console(out, config, stat)?;
    }
    Ok(())
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

fn write_console(out: &mut impl Write, config: &BenchConfig, stat: &BenchStats) -> io::Result<()> {
    writeln!(out, "====== {} ======", stat.name)?;
    writeln!(
        out,
        "{} requests completed in {:.2} seconds",
        stat.requests,
        stat.elapsed.as_secs_f64()
    )?;
    if let (Some(target), Some(rps)) = (config.rps, &stat.rps_distribution) {
        writeln!(out, "RPS Summary:")?;
        writeln!(out, "  target RPS: {:.2}", target as f64)?;
        writeln!(out, "  RPS distribution (reqs/sec):")?;
        write_distribution(
            out,
            [
                rps.avg_rps,
                rps.min_rps,
                rps.p50_rps,
                rps.p95_rps,
                rps.p99_rps,
                rps.p999_rps,
                rps.max_rps,
            ],
        )?;
    }
    writeln!(out, "Latency by percentile distribution:")?;
    for (pct, latency_us) in [
        (0.0, stat.min_us),
        (50.0, stat.p50_us),
        (95.0, stat.p95_us),
        (99.0, stat.p99_us),
        (99.9, stat.p999_us),
        (100.0, stat.max_us),
    ] {
        writeln!(
            out,
            "{pct:.3}% <= {:.3} milliseconds",
            latency_us as f64 / 1000.0
        )?;
    }
    writeln!(out, "Summary:")?;
    writeln!(
        out,
        "  throughput summary: {:.2} requests per second",
        stat.rps
    )?;
    writeln!(out, "  latency summary (msec):")?;
    write_distribution(
        out,
        [
            stat.avg_us,
            stat.min_us as f64,
            stat.p50_us as f64,
            stat.p95_us as f64,
            stat.p99_us as f64,
            stat.p999_us as f64,
            stat.max_us as f64,
        ]
        .map(|value| value / 1000.0),
    )
}

fn write_distribution(out: &mut impl Write, values: [f64; 7]) -> io::Result<()> {
    writeln!(
        out,
        "    {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "avg", "min", "p50", "p95", "p99", "p99.9", "max"
    )?;
    writeln!(
        out,
        "    {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3} {:9.3}",
        values[0], values[1], values[2], values[3], values[4], values[5], values[6]
    )
}

fn write_csv(out: &mut impl Write, stat: &BenchStats) -> io::Result<()> {
    writeln!(
        out,
        "\"{}\",\"{:.2}\",\"{:.3}\",\"{:.3}\",\"{:.3}\",\"{:.3}\",\"{:.3}\",\"{:.3}\",\"{:.3}\"",
        stat.name.replace('"', "\"\""),
        stat.rps,
        stat.avg_us / 1000.0,
        stat.min_us as f64 / 1000.0,
        stat.p50_us as f64 / 1000.0,
        stat.p95_us as f64 / 1000.0,
        stat.p99_us as f64 / 1000.0,
        stat.p999_us as f64 / 1000.0,
        stat.max_us as f64 / 1000.0
    )
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

#[cfg(test)]
mod tests {
    use super::super::runner::RpsStats;
    use super::*;
    use std::time::Duration;

    fn sample_stats() -> BenchStats {
        BenchStats {
            name: "SET".to_string(),
            requests: 10,
            elapsed: Duration::from_secs(1),
            rps: 10.0,
            avg_us: 2_500.0,
            min_us: 1_000,
            p50_us: 2_000,
            p95_us: 3_000,
            p99_us: 4_000,
            p999_us: 5_000,
            max_us: 6_000,
            percentiles: vec![(50.0, 2_000), (99.9, 5_000)],
            rps_distribution: Some(RpsStats {
                avg_rps: 9.0,
                min_rps: 4.0,
                p50_rps: 8.0,
                p95_rps: 10.0,
                p99_rps: 11.0,
                p999_rps: 12.0,
                max_rps: 13.0,
            }),
        }
    }

    fn report(config: &BenchConfig) -> String {
        let mut out = Vec::new();
        write_results(&mut out, config, &[sample_stats()]).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn console_summary_reports_mean_and_percentiles_in_milliseconds() {
        let out = report(&BenchConfig::default());
        let summary = out.split("latency summary (msec):\n").nth(1).unwrap();
        let mut lines = summary.lines();
        assert_eq!(
            lines.next().unwrap().split_whitespace().collect::<Vec<_>>(),
            ["avg", "min", "p50", "p95", "p99", "p99.9", "max"]
        );
        assert_eq!(
            lines.next().unwrap().split_whitespace().collect::<Vec<_>>(),
            ["2.500", "1.000", "2.000", "3.000", "4.000", "5.000", "6.000"]
        );
    }

    #[test]
    fn rps_summary_precedes_real_latency_percentiles() {
        let config = BenchConfig {
            rps: Some(10),
            ..Default::default()
        };
        let out = report(&config);
        let summary = out.split("RPS Summary:\n").nth(1).unwrap();
        let (rps, latency) = summary.split_once("Latency by percentile").unwrap();
        assert!(rps.contains("p99.9"));
        assert_eq!(
            rps.lines()
                .last()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>(),
            ["9.000", "4.000", "8.000", "10.000", "11.000", "12.000", "13.000"]
        );
        assert!(latency.contains("99.900% <= 5.000 milliseconds"));
    }

    #[test]
    fn csv_reports_nine_quoted_columns_without_console_diagnostics() {
        let config = BenchConfig {
            csv: true,
            quiet: true,
            rps: Some(10),
            ..Default::default()
        };
        let out = report(&config);
        assert_eq!(
            out,
            "\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\
             \"p95_latency_ms\",\"p99_latency_ms\",\"p999_latency_ms\",\"max_latency_ms\"\n\
             \"SET\",\"10.00\",\"2.500\",\"1.000\",\"2.000\",\"3.000\",\"4.000\",\"5.000\",\"6.000\"\n"
        );
        assert!(!out.contains('\r'));
        assert!(!out.contains("requests per second"));
        assert!(!out.contains("RPS Summary"));
    }

    #[test]
    fn quiet_reports_only_command_throughput_and_median() {
        let config = BenchConfig {
            quiet: true,
            rps: Some(10),
            ..Default::default()
        };
        assert_eq!(
            report(&config),
            "SET: 10.00 requests per second, p50=2.000 msec\n"
        );
    }
}
