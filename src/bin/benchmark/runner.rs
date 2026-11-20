use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
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

const WARMUP_GRACE: Duration = Duration::from_millis(500);
const RPS_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct BenchStats {
    pub name: String,
    pub requests: u64,
    pub elapsed: Duration,
    pub rps: f64,
    pub avg_us: f64,
    pub min_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub percentiles: Vec<(f64, u64)>,
    pub rps_distribution: Option<RpsStats>,
}

#[derive(Clone, Debug)]
pub struct RpsStats {
    pub avg_rps: f64,
    pub min_rps: f64,
    pub p50_rps: f64,
    pub p95_rps: f64,
    pub p99_rps: f64,
    pub p999_rps: f64,
    pub max_rps: f64,
}

#[derive(Default)]
struct RpsRecorder {
    samples: Vec<f64>,
    previous_elapsed: Duration,
    previous_completed: u64,
}

impl RpsRecorder {
    fn record(&mut self, completed: u64, elapsed: Duration) {
        if elapsed - self.previous_elapsed >= RPS_SAMPLE_INTERVAL {
            self.record_window(completed, elapsed);
        }
    }

    fn record_window(&mut self, completed: u64, elapsed: Duration) {
        let window = elapsed - self.previous_elapsed;
        if !window.is_zero() {
            self.samples
                .push((completed - self.previous_completed) as f64 / window.as_secs_f64());
            self.previous_elapsed = elapsed;
            self.previous_completed = completed;
        }
    }

    fn finish(mut self, completed: u64, elapsed: Duration) -> Option<RpsStats> {
        if completed > self.previous_completed {
            self.record_window(completed, elapsed);
        }
        if self.samples.is_empty() {
            return None;
        }
        self.samples.sort_unstable_by(f64::total_cmp);
        let at = |pct| self.samples[legacy_percentile_index(self.samples.len(), pct)];
        Some(RpsStats {
            avg_rps: self.samples.iter().sum::<f64>() / self.samples.len() as f64,
            min_rps: self.samples[0],
            p50_rps: at(50.0),
            p95_rps: at(95.0),
            p99_rps: at(99.0),
            p999_rps: at(99.9),
            max_rps: self.samples[self.samples.len() - 1],
        })
    }
}

struct RpsSampler {
    completed: Arc<AtomicU64>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    worker: Option<JoinHandle<RpsRecorder>>,
    start: Instant,
}

impl RpsSampler {
    fn start(start: Instant, interval: Duration) -> Result<Self, String> {
        let completed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_completed = Arc::clone(&completed);
        let worker_stop = Arc::clone(&stop);
        let worker = thread::Builder::new()
            .name("benchmark-rps".to_string())
            .spawn(move || {
                let mut recorder = RpsRecorder::default();
                let (lock, wake) = &*worker_stop;
                let mut should_stop = lock.lock().unwrap_or_else(|err| err.into_inner());
                while !*should_stop {
                    let (guard, _) = wake
                        .wait_timeout_while(should_stop, interval, |should_stop| !*should_stop)
                        .unwrap_or_else(|err| err.into_inner());
                    should_stop = guard;
                    if !*should_stop {
                        recorder.record(worker_completed.load(Ordering::Relaxed), start.elapsed());
                    }
                }
                recorder
            })
            .map_err(|err| format!("Unable to start RPS sampler: {err}"))?;
        Ok(Self {
            completed,
            stop,
            worker: Some(worker),
            start,
        })
    }

    fn stop_and_join(&mut self) -> Result<(RpsRecorder, Duration), String> {
        let worker = self
            .worker
            .take()
            .ok_or_else(|| "RPS sampler already stopped".to_string())?;
        let (lock, wake) = &*self.stop;
        let mut should_stop = lock.lock().unwrap_or_else(|err| err.into_inner());
        *should_stop = true;
        // Capture the endpoint under the sampling lock so no timer sample
        // can fall after it. Thread shutdown time is not measurement time.
        let elapsed = self.start.elapsed();
        wake.notify_one();
        drop(should_stop);
        worker
            .join()
            .map(|recorder| (recorder, elapsed))
            .map_err(|_| "RPS sampler thread panicked".to_string())
    }

    fn finish(mut self) -> Result<Option<RpsStats>, String> {
        let (recorder, elapsed) = self.stop_and_join()?;
        Ok(recorder.finish(self.completed.load(Ordering::Relaxed), elapsed))
    }
}

impl Drop for RpsSampler {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.stop_and_join();
        }
    }
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
    if config.idle {
        return run(config, command_plan, dataset).map(RunOutput::Legacy);
    }
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
        streams.push(RespConnection::connect(&options)?);
    }
    println!(
        "Creating {} idle connections and waiting forever (Ctrl+C when done)",
        streams.len()
    );
    loop {
        std::thread::park();
    }
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
    let end = Instant::now() + warmup_duration(config.warmup);
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

fn warmup_duration(seconds: u64) -> Duration {
    Duration::from_secs(seconds) + WARMUP_GRACE
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
    let rps_sampler = if config.rps.is_some_and(|rps| rps > 0) {
        Some(RpsSampler::start(start, RPS_SAMPLE_INTERVAL)?)
    } else {
        None
    };

    while should_continue(config, completed, start, duration) {
        if config.pipeline == 1 {
            let mut request_elapsed = Duration::ZERO;
            let mut sent = 0u64;
            for command in generator.build_sequence(test)? {
                if !should_continue(config, completed + sent, start, duration) {
                    break;
                }
                let sent_at = Instant::now();
                stream.send_request_with_redirect(&command)?;
                request_elapsed += sent_at.elapsed();
                sent += 1;
                if let Some(sampler) = &rps_sampler {
                    sampler.completed.fetch_add(1, Ordering::Relaxed);
                }
                if let Some(sleep) = rps_sleep {
                    std::thread::sleep(sleep);
                }
            }
            if sent == 0 {
                break;
            }
            // Rate-limit waits affect throughput, not measured request latency.
            let per_reply = ((request_elapsed.as_micros() as u64) / sent).max(1);
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
            if let Some(sampler) = &rps_sampler {
                sampler.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
        let elapsed_us = sent_at.elapsed().as_micros() as u64;
        let per_reply = (elapsed_us / replies as u64).max(1);
        for _ in 0..replies {
            latencies.push(per_reply);
        }
        completed += replies as u64;
    }

    let measured_elapsed = start.elapsed();
    let rps_distribution = match rps_sampler {
        Some(sampler) => sampler.finish()?,
        None => None,
    };
    let elapsed = if let Some(duration) = duration {
        duration
    } else {
        measured_elapsed
    };
    let mut stats = compute_stats(&test.name, &latencies, elapsed, &config.memtier.percentiles);
    stats.rps_distribution = rps_distribution;
    Ok(stats)
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
            avg_us: 0.0,
            min_us: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            p999_us: 0,
            max_us: 0,
            percentiles: percentiles.iter().map(|pct| (*pct, 0)).collect(),
            rps_distribution: None,
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
        avg_us: latencies.iter().map(|&value| value as u128).sum::<u128>() as f64
            / latencies.len() as f64,
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
        rps_distribution: None,
    }
}

fn legacy_percentile(sorted: &[u64], pct: f64) -> u64 {
    sorted[legacy_percentile_index(sorted.len(), pct)]
}

fn legacy_percentile_index(count: usize, pct: f64) -> usize {
    // Match hdr_value_at_percentile's one-based rounded rank.
    let rank = ((count as f64) * pct / 100.0 + 0.5) as usize;
    rank.saturating_sub(1).min(count - 1)
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    #[test]
    fn rps_timer_records_zero_windows_without_completion_callbacks() {
        let start = Instant::now();
        let sampler = super::RpsSampler::start(start, super::RPS_SAMPLE_INTERVAL).unwrap();
        std::thread::sleep(Duration::from_millis(800));
        sampler.completed.fetch_add(1, Ordering::Relaxed);
        let stats = sampler.finish().unwrap().unwrap();
        assert!(start.elapsed() >= Duration::from_millis(800));
        assert_eq!(stats.min_rps, 0.0);
        assert_eq!(stats.p50_rps, 0.0);
        assert!(stats.max_rps > 0.0);
    }

    #[test]
    fn rps_timer_records_actual_windows_without_backfilling() {
        let mut recorder = super::RpsRecorder::default();
        recorder.record(0, Duration::from_millis(250));
        recorder.record(0, Duration::from_millis(500));
        recorder.record(1, Duration::from_millis(1_500));
        assert_eq!(recorder.samples, vec![0.0, 0.0, 1.0]);
    }

    #[test]
    fn rps_timer_keeps_zero_only_observations() {
        let sampler = super::RpsSampler::start(Instant::now(), super::RPS_SAMPLE_INTERVAL).unwrap();
        std::thread::sleep(Duration::from_millis(800));
        let stats = sampler.finish().unwrap().unwrap();
        assert_eq!(stats.avg_rps, 0.0);
        assert_eq!(stats.min_rps, 0.0);
        assert_eq!(stats.p999_rps, 0.0);
        assert_eq!(stats.max_rps, 0.0);
    }

    #[test]
    fn rps_sampler_finish_interrupts_wait_and_joins_empty_run() {
        let start = Instant::now();
        let sampler = super::RpsSampler::start(Instant::now(), Duration::from_secs(60)).unwrap();
        let completed = Arc::clone(&sampler.completed);
        let stop = Arc::clone(&sampler.stop);
        std::thread::sleep(Duration::from_millis(50));
        let stats = sampler.finish().unwrap();
        assert!(stats.is_none());
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(Arc::strong_count(&completed), 1);
        assert_eq!(Arc::strong_count(&stop), 1);
    }

    #[test]
    fn rps_sampler_drop_interrupts_wait_and_joins_on_error() {
        let mut completed = None;
        let mut stop = None;
        let start = Instant::now();
        let result: Result<(), String> = (|| {
            let sampler = super::RpsSampler::start(Instant::now(), Duration::from_secs(60))?;
            completed = Some(Arc::clone(&sampler.completed));
            stop = Some(Arc::clone(&sampler.stop));
            std::thread::sleep(Duration::from_millis(50));
            Err("request failed".to_string())
        })();
        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(Arc::strong_count(&completed.unwrap()), 1);
        assert_eq!(Arc::strong_count(&stop.unwrap()), 1);
    }

    #[test]
    fn rps_sampler_starts_with_fresh_counts_after_previous_run() {
        let first = super::RpsSampler::start(Instant::now(), Duration::from_secs(60)).unwrap();
        first.completed.fetch_add(50, Ordering::Relaxed);
        assert!(first.finish().unwrap().unwrap().min_rps > 0.0);
        let second = super::RpsSampler::start(Instant::now(), Duration::from_secs(60)).unwrap();
        assert_eq!(second.completed.load(Ordering::Relaxed), 0);
        assert!(second.finish().unwrap().is_none());
    }

    fn start_reply_server(sessions: Vec<Vec<(Duration, &'static [u8])>>) -> (u16, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let worker = std::thread::spawn(move || {
            for replies in sessions {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut scratch = Vec::new();
                for (delay, reply) in replies {
                    super::protocol::read_one_reply(&mut stream, &mut scratch).unwrap();
                    std::thread::sleep(delay);
                    stream.write_all(reply).unwrap();
                }
            }
        });
        (port, worker)
    }

    fn ping_template() -> super::TestTemplate {
        super::TestTemplate {
            name: "PING".to_string(),
            sequence: vec![vec!["PING".to_string()]],
        }
    }

    #[test]
    fn blocking_sync_replies_preserve_zero_throughput_windows() {
        let (port, server) = start_reply_server(vec![vec![
            (Duration::from_millis(600), b"+PONG\r\n"),
            (Duration::from_millis(600), b"+PONG\r\n"),
        ]]);
        let config = super::BenchConfig {
            port,
            requests: 2,
            rps: Some(10),
            ..super::BenchConfig::default()
        };
        let result = super::run_test(&config, &ping_template(), None);
        server.join().unwrap();
        let stats = result.unwrap();
        assert_eq!(stats.requests, 2);
        assert!(stats.min_us >= 600_000);
        let rps = stats.rps_distribution.unwrap();
        assert_eq!(rps.min_rps, 0.0);
        assert!(rps.max_rps > 0.0);
    }

    #[test]
    fn pipeline_replies_preserve_zero_throughput_windows() {
        let (port, server) = start_reply_server(vec![vec![
            (Duration::ZERO, b"+PONG\r\n"),
            (Duration::from_millis(800), b"+PONG\r\n"),
        ]]);
        let config = super::BenchConfig {
            port,
            requests: 2,
            pipeline: 2,
            rps: Some(10),
            ..super::BenchConfig::default()
        };
        let result = super::run_test(&config, &ping_template(), None);
        server.join().unwrap();
        let stats = result.unwrap();
        assert_eq!(stats.requests, 2);
        assert!(stats.min_us >= 400_000);
        let rps = stats.rps_distribution.unwrap();
        assert_eq!(rps.min_rps, 0.0);
        assert!(rps.max_rps > 0.0);
    }

    #[test]
    fn rate_limit_waits_affect_rps_but_not_request_latency() {
        let (port, server) = start_reply_server(vec![vec![(Duration::ZERO, b"+PONG\r\n")]]);
        let config = super::BenchConfig {
            port,
            requests: 1,
            rps: Some(1),
            ..super::BenchConfig::default()
        };
        let result = super::run_test(&config, &ping_template(), None);
        server.join().unwrap();
        let stats = result.unwrap();
        assert_eq!(stats.requests, 1);
        assert!(stats.elapsed >= Duration::from_secs(1));
        assert!(stats.max_us < 500_000);
        let rps = stats.rps_distribution.unwrap();
        assert_eq!(rps.min_rps, 0.0);
        assert!(rps.max_rps > 0.0);
    }

    #[test]
    fn empty_run_and_request_error_stop_the_sampler() {
        let (port, server) = start_reply_server(vec![
            vec![],
            vec![(Duration::ZERO, b"-ERR request failed\r\n")],
        ]);
        let mut config = super::BenchConfig {
            port,
            requests: 0,
            rps: Some(10),
            ..super::BenchConfig::default()
        };
        let start = Instant::now();
        let empty = super::run_test(&config, &ping_template(), None).unwrap();
        assert_eq!(empty.requests, 0);
        assert!(empty.rps_distribution.is_none());
        config.requests = 1;
        assert!(super::run_test(&config, &ping_template(), None)
            .unwrap_err()
            .contains("request failed"));
        server.join().unwrap();
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn measurement_excludes_warmup_completions_and_zero_windows() {
        let (port, server) = start_reply_server(vec![
            vec![(Duration::from_millis(1_600), b"+PONG\r\n")],
            vec![(Duration::ZERO, b"+PONG\r\n")],
        ]);
        let config = super::BenchConfig {
            port,
            requests: 1,
            warmup: 1,
            rps: Some(1_000_000),
            ..super::BenchConfig::default()
        };
        let template = ping_template();
        super::run_warmup(&config, &template, None).unwrap();
        let result = super::run_test(&config, &template, None);
        server.join().unwrap();
        let stats = result.unwrap();
        assert_eq!(stats.requests, 1);
        assert!(stats.elapsed < super::RPS_SAMPLE_INTERVAL);
        assert!(stats.rps_distribution.unwrap().min_rps > 0.0);
    }

    #[test]
    fn summary_uses_mean_and_one_based_percentile_ranks() {
        let stats = super::compute_stats(
            "SET",
            &[20_000, 1_000, 4_000, 2_000, 3_000],
            Duration::from_secs(2),
            &[50.0, 99.9],
        );
        assert_eq!(stats.requests, 5);
        assert_eq!(stats.rps, 2.5);
        assert_eq!(stats.avg_us, 6_000.0);
        assert_eq!(stats.min_us, 1_000);
        assert_eq!(stats.p50_us, 3_000);
        assert_eq!(stats.p95_us, 20_000);
        assert_eq!(stats.p99_us, 20_000);
        assert_eq!(stats.p999_us, 20_000);
        assert_eq!(stats.max_us, 20_000);
    }

    #[test]
    fn legacy_percentiles_use_upstream_histogram_rank_rounding() {
        let sorted = [10, 20, 30, 40];
        assert_eq!(super::legacy_percentile(&sorted, 0.0), 10);
        assert_eq!(super::legacy_percentile(&sorted, 50.0), 20);
        assert_eq!(super::legacy_percentile(&sorted, 60.0), 20);
        assert_eq!(super::legacy_percentile(&sorted, 99.9), 40);
        assert_eq!(super::legacy_percentile(&sorted, 100.0), 40);
    }

    #[test]
    fn empty_summary_has_zero_latency_metrics() {
        let stats = super::compute_stats("GET", &[], Duration::ZERO, &[50.0, 99.9]);
        assert_eq!(stats.requests, 0);
        assert_eq!(stats.rps, 0.0);
        assert_eq!(stats.avg_us, 0.0);
        assert_eq!(stats.p999_us, 0);
        assert_eq!(stats.percentiles, vec![(50.0, 0), (99.9, 0)]);
    }

    #[test]
    fn mean_does_not_overflow_for_large_latency_samples() {
        let stats = super::compute_stats("GET", &[u64::MAX, u64::MAX], Duration::from_secs(1), &[]);
        assert_eq!(stats.avg_us, u64::MAX as f64);
    }

    #[test]
    fn rps_distribution_uses_completed_deltas_and_measured_windows() {
        let mut recorder = super::RpsRecorder::default();
        recorder.record(1, Duration::from_millis(100));
        recorder.record(4, Duration::from_millis(500));
        recorder.record(10, Duration::from_millis(1_000));
        let stats = recorder.finish(11, Duration::from_millis(1_250)).unwrap();
        assert_eq!(stats.avg_rps, 8.0);
        assert_eq!(stats.min_rps, 4.0);
        assert_eq!(stats.p50_rps, 8.0);
        assert_eq!(stats.p95_rps, 12.0);
        assert_eq!(stats.p99_rps, 12.0);
        assert_eq!(stats.p999_rps, 12.0);
        assert_eq!(stats.max_rps, 12.0);
    }

    #[test]
    fn short_rps_run_records_its_actual_partial_window() {
        let stats = super::RpsRecorder::default()
            .finish(1, Duration::from_millis(100))
            .unwrap();
        assert_eq!(stats.avg_rps, 10.0);
        assert_eq!(stats.p999_rps, 10.0);
        assert!(super::RpsRecorder::default()
            .finish(0, Duration::ZERO)
            .is_none());
    }

    #[test]
    fn warmup_duration_includes_guard_time_for_wall_clock_assertions() {
        assert_eq!(
            super::warmup_duration(2),
            Duration::from_secs(2) + Duration::from_millis(500)
        );
    }
}
