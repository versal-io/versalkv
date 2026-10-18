use std::io::Read;
use std::time::{Duration, Instant};

use super::cli::{BenchConfig, BenchMode};
use super::dataset::Dataset;
use super::metrics;
use super::metrics::BenchmarkSummary;
use super::plan::BenchmarkPlan;
use super::protocol;
use super::scheduler;
use super::templates::{CommandGenerator, CommandPlan, TestTemplate};
use super::transport::{ConnectionOptions, RespConnection};

#[derive(Clone, Debug)]
pub struct BenchStats {
    pub name: String,
    pub requests: u64,
    pub elapsed: Duration,
    pub rps: f64,
    pub min_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub percentiles: Vec<(f64, u64)>,
}

pub enum RunOutput {
    Legacy(Vec<BenchStats>),
    Summary(BenchmarkSummary),
}

pub fn run_benchmark(
    config: &BenchConfig,
    benchmark_plan: &BenchmarkPlan,
    command_plan: &CommandPlan,
    dataset: Option<&Dataset>,
) -> Result<RunOutput, String> {
    if config.mode == BenchMode::MemtierRedis {
        return scheduler::run_memtier(config, benchmark_plan, dataset).map(RunOutput::Summary);
    }

    run(config, command_plan, dataset).map(RunOutput::Legacy)
}

pub fn run(
    config: &BenchConfig,
    plan: &CommandPlan,
    dataset: Option<&Dataset>,
) -> Result<Vec<BenchStats>, String> {
    if config.idle {
        return run_idle(config);
    }

    let mut results = Vec::new();
    loop {
        if is_fcall_selected(config) {
            prepare_fcall_function(config)?;
        }
        for test in plan.tests() {
            run_warmup(config, test, dataset)?;
            results.push(run_test(config, test, dataset)?);
        }
        if !config.loop_mode {
            break;
        }
    }
    Ok(results)
}

fn is_fcall_selected(config: &BenchConfig) -> bool {
    config
        .tests
        .as_ref()
        .map(|names| names.iter().any(|n| n.eq_ignore_ascii_case("FCALL")))
        .unwrap_or(false)
}

/// Load the single-function library that FCALL benchmarks target so the
/// server has `foo1` registered before the measurement starts. Mirrors
/// `valkey-benchmark.c:2727`.
fn prepare_fcall_function(config: &BenchConfig) -> Result<(), String> {
    let options = ConnectionOptions::from_config(config);
    let mut stream = RespConnection::connect(&options)?;
    let script = super::templates::generate_function_script(1, config.num_keys_in_fcall > 0);
    stream.send_request(&[
        b"FUNCTION".to_vec(),
        b"LOAD".to_vec(),
        b"REPLACE".to_vec(),
        script.into_bytes(),
    ])?;
    Ok(())
}

fn run_idle(config: &BenchConfig) -> Result<Vec<BenchStats>, String> {
    let options = ConnectionOptions::from_config(config);
    if let Some(maxclients) = fetch_maxclients(config)? {
        if config.clients > maxclients {
            return Err(format!(
                "maxclients limit reached: requested {} idle clients, server allows {}",
                config.clients, maxclients
            ));
        }
    }
    let mut streams = Vec::with_capacity(config.clients);
    for _ in 0..config.clients {
        let mut stream = RespConnection::connect(&options)?;
        stream.write_all(&protocol::serialize_resp(&[b"PING".to_vec()]))?;
        streams.push(stream);
    }
    std::thread::sleep(Duration::from_millis(100));
    drop(streams);
    Ok(Vec::new())
}

fn fetch_maxclients(config: &BenchConfig) -> Result<Option<usize>, String> {
    let options = ConnectionOptions::from_config(config);
    let mut stream = RespConnection::connect(&options)?;
    let reply =
        stream.send_request(&[b"CONFIG".to_vec(), b"GET".to_vec(), b"maxclients".to_vec()])?;
    Ok(parse_config_get_numeric(&reply))
}

fn parse_config_get_numeric(reply: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(reply);
    let marker = "\r\n$";
    let last_bulk = text.rfind(marker)?;
    let value_start = text[last_bulk + marker.len()..].find("\r\n")? + last_bulk + marker.len() + 2;
    let value_end = text[value_start..].find("\r\n")? + value_start;
    text[value_start..value_end].parse().ok()
}

fn run_warmup(
    config: &BenchConfig,
    test: &TestTemplate,
    dataset: Option<&Dataset>,
) -> Result<(), String> {
    if config.warmup == 0 {
        return Ok(());
    }
    let end = Instant::now() + Duration::from_secs(config.warmup);
    let options = ConnectionOptions::from_config(config);
    let mut stream = RespConnection::connect(&options)?;
    let mut generator = CommandGenerator::new(config, dataset);
    while Instant::now() < end {
        for command in generator.build_sequence(test)? {
            stream.send_request_with_redirect(&command)?;
        }
    }
    Ok(())
}

fn run_test(
    config: &BenchConfig,
    test: &TestTemplate,
    dataset: Option<&Dataset>,
) -> Result<BenchStats, String> {
    let options = ConnectionOptions::from_config(config);
    let mut stream = RespConnection::connect(&options)?;
    if config.enable_tracking && config.resp3 {
        stream.send_request(&[b"CLIENT".to_vec(), b"TRACKING".to_vec(), b"ON".to_vec()])?;
    }
    let mut generator = CommandGenerator::new(config, dataset);
    let mut latencies = Vec::new();
    let mut completed = 0u64;
    let start = Instant::now();
    let duration = config.duration.map(Duration::from_secs);
    let rps_sleep = config
        .rps
        .filter(|&rps| rps > 0)
        .map(|rps| Duration::from_micros(1_000_000 / rps));

    while should_continue(config, completed, start, duration) {
        if config.pipeline == 1 {
            let sent_at = Instant::now();
            let mut sent = 0u64;
            for command in generator.build_sequence(test)? {
                if !should_continue(config, completed + sent, start, duration) {
                    break;
                }
                stream.send_request_with_redirect(&command)?;
                sent += 1;
                if let Some(sleep) = rps_sleep {
                    std::thread::sleep(sleep);
                }
            }
            if sent == 0 {
                break;
            }
            let per_reply = ((sent_at.elapsed().as_micros() as u64) / sent).max(1);
            for _ in 0..sent {
                latencies.push(per_reply);
            }
            completed += sent;
            continue;
        }

        let mut batch = Vec::new();
        let mut replies = 0usize;
        for _ in 0..config.pipeline {
            if !should_continue(config, completed + replies as u64, start, duration) {
                break;
            }
            for command in generator.build_sequence(test)? {
                batch.extend_from_slice(&protocol::serialize_resp(&command));
                replies += 1;
            }
        }
        if replies == 0 {
            break;
        }

        let sent_at = Instant::now();
        stream.write_all(&batch)?;
        for _ in 0..replies {
            stream.read_one_reply()?;
        }
        let elapsed_us = sent_at.elapsed().as_micros() as u64;
        let per_reply = (elapsed_us / replies as u64).max(1);
        for _ in 0..replies {
            latencies.push(per_reply);
        }
        completed += replies as u64;
    }

    let elapsed = if let Some(duration) = duration {
        duration
    } else {
        start.elapsed()
    };
    Ok(compute_stats(
        &test.name,
        &latencies,
        elapsed,
        &config.memtier.percentiles,
    ))
}

fn should_continue(
    config: &BenchConfig,
    completed: u64,
    start: Instant,
    duration: Option<Duration>,
) -> bool {
    match duration {
        Some(duration) => start.elapsed() < duration,
        None => completed < config.requests,
    }
}

fn compute_stats(
    name: &str,
    latencies: &[u64],
    elapsed: Duration,
    percentiles: &[f64],
) -> BenchStats {
    if latencies.is_empty() {
        return BenchStats {
            name: name.to_string(),
            requests: 0,
            elapsed,
            rps: 0.0,
            min_us: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            p999_us: 0,
            max_us: 0,
            percentiles: percentiles.iter().map(|pct| (*pct, 0)).collect(),
        };
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let rps = if elapsed.as_secs_f64() > 0.0 {
        latencies.len() as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    BenchStats {
        name: name.to_string(),
        requests: latencies.len() as u64,
        elapsed,
        rps,
        min_us: sorted[0],
        p50_us: legacy_percentile(&sorted, 50.0),
        p95_us: legacy_percentile(&sorted, 95.0),
        p99_us: legacy_percentile(&sorted, 99.0),
        p999_us: legacy_percentile(&sorted, 99.9),
        max_us: sorted[sorted.len() - 1],
        percentiles: percentiles
            .iter()
            .map(|pct| (*pct, metrics::percentile(&sorted, *pct)))
            .collect(),
    }
}

fn legacy_percentile(sorted: &[u64], pct: f64) -> u64 {
    let idx = ((sorted.len() as f64) * pct / 100.0).ceil() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn append_stdin_lastarg(config: &mut BenchConfig) -> Result<(), String> {
    if !config.stdin_lastarg {
        return Ok(());
    }
    let mut input = Vec::new();
    std::io::stdin()
        .read_to_end(&mut input)
        .map_err(|err| err.to_string())?;
    config
        .command
        .push(String::from_utf8_lossy(&input).into_owned());
    Ok(())
}
