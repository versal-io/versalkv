use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::cli::{BenchConfig, BenchMode, KeyPattern};
use super::datagen::{DataGenConfig, DataGenerator};
use super::dataset::Dataset;
use super::keygen::{KeyGenConfig, KeyGenerator};
use super::metrics::{BenchmarkSummary, CommandStats, LatencyRecorder};
use super::plan::{BenchmarkPlan, RunBudget, WorkloadPlan};
use super::protocol;
use super::transport::{ConnectionOptions, RespConnection};
use super::workload::{MemtierCommandWorkload, MemtierRatioWorkload, Workload};

pub struct Scheduler;

#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    pub threads: usize,
    pub clients_per_thread: usize,
    pub pipeline: usize,
    pub budget: RunBudget,
    pub run_count: u32,
    pub warmup: Duration,
    pub rate_limit_per_connection: Option<u64>,
}

impl Scheduler {
    pub fn run(
        &self,
        config: &BenchConfig,
        plan: &BenchmarkPlan,
    ) -> Result<BenchmarkSummary, String> {
        run_plan(config, plan)
    }
}

pub fn run_memtier(
    config: &BenchConfig,
    plan: &BenchmarkPlan,
    dataset: Option<&Dataset>,
) -> Result<BenchmarkSummary, String> {
    if config.mode != BenchMode::MemtierRedis || plan.mode != BenchMode::MemtierRedis {
        return Err("scheduler memtier route requires memtier Redis mode".to_string());
    }
    if dataset.is_some() {
        return Err("memtier dataset workloads are not wired in this phase".to_string());
    }
    run_plan(config, plan)
}

pub fn run_plan(config: &BenchConfig, plan: &BenchmarkPlan) -> Result<BenchmarkSummary, String> {
    let scheduler_config = SchedulerConfig::from_config(config, plan);
    validate_plan(plan, &scheduler_config)?;

    let options = Arc::new(ConnectionOptions::from_config(config));
    let total_clients = planned_total_clients_for_plan(plan, scheduler_config);
    let mut combined = SummaryAccumulator::default();

    for run_idx in 0..scheduler_config.run_count.max(1) {
        if !scheduler_config.warmup.is_zero() {
            run_workers(WorkerRun {
                config,
                plan,
                scheduler_config,
                budget: RunBudget::Time(scheduler_config.warmup),
                options: options.clone(),
                total_clients,
                run_idx,
                collect_metrics: false,
            })?;
        }

        let run_summary = run_workers(WorkerRun {
            config,
            plan,
            scheduler_config,
            budget: scheduler_config.budget,
            options: options.clone(),
            total_clients,
            run_idx,
            collect_metrics: true,
        })?;
        combined.merge(run_summary);
    }

    finish_memtier_summary(combined, &config.memtier.percentiles)
}

impl SchedulerConfig {
    fn from_config(config: &BenchConfig, plan: &BenchmarkPlan) -> Self {
        let threads = plan.threads.max(1);
        let requested_clients = plan.clients.max(1);
        let clients_per_thread = match plan.mode {
            BenchMode::MemtierRedis => requested_clients,
            BenchMode::Valkey => requested_clients.div_ceil(threads).max(1),
        };
        Self {
            threads,
            clients_per_thread,
            pipeline: plan.pipeline.max(1),
            budget: plan.budget,
            run_count: plan.run_count.max(1),
            warmup: Duration::from_secs(config.warmup),
            rate_limit_per_connection: config.memtier.rate_limit_per_connection,
        }
    }
}

fn validate_plan(plan: &BenchmarkPlan, config: &SchedulerConfig) -> Result<(), String> {
    if plan.workloads.is_empty() {
        return Err("benchmark plan contains no workloads".to_string());
    }
    if config.threads == 0 || config.clients_per_thread == 0 {
        return Err("scheduler requires at least one thread and one client".to_string());
    }
    for workload in &plan.workloads {
        match workload {
            WorkloadPlan::MemtierRatio(_) | WorkloadPlan::MemtierCommands => {}
            WorkloadPlan::ValkeyTemplates | WorkloadPlan::ValkeyCommandSequence => {
                return Err(
                    "scheduler only supports memtier Redis workloads in this phase".to_string(),
                );
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct WorkerRun<'a> {
    config: &'a BenchConfig,
    plan: &'a BenchmarkPlan,
    scheduler_config: SchedulerConfig,
    budget: RunBudget,
    options: Arc<ConnectionOptions>,
    total_clients: usize,
    run_idx: u32,
    collect_metrics: bool,
}

fn run_workers(run: WorkerRun<'_>) -> Result<WorkerSummary, String> {
    run_workers_with_start_count(run).map(|(summary, _started_clients)| summary)
}

fn run_workers_with_start_count(run: WorkerRun<'_>) -> Result<(WorkerSummary, u64), String> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started_clients = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(run.scheduler_config.threads);

    for thread_idx in 0..run.scheduler_config.threads {
        let worker = ThreadWorker {
            config: run.config.clone(),
            plan: run.plan.clone(),
            scheduler_config: run.scheduler_config,
            budget: run.budget,
            options: run.options.clone(),
            completed: completed.clone(),
            stop: stop.clone(),
            start,
            thread_idx,
            total_clients: run.total_clients,
            run_idx: run.run_idx,
            collect_metrics: run.collect_metrics,
            started_clients: started_clients.clone(),
        };
        handles.push(thread::spawn(move || worker.run()));
    }

    let mut summary = WorkerSummary::default();
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(worker_summary)) => summary.merge(worker_summary),
            Ok(Err(err)) => errors.push(err),
            Err(_) => errors.push("scheduler worker thread panicked".to_string()),
        }
    }
    summary.duration = start.elapsed();
    let started = started_clients.load(Ordering::Relaxed);
    finish_joined_client_summaries(summary, errors).map(|summary| (summary, started))
}

struct ThreadWorker {
    config: BenchConfig,
    plan: BenchmarkPlan,
    scheduler_config: SchedulerConfig,
    budget: RunBudget,
    options: Arc<ConnectionOptions>,
    completed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    start: Instant,
    thread_idx: usize,
    total_clients: usize,
    run_idx: u32,
    collect_metrics: bool,
    started_clients: Arc<AtomicU64>,
}

impl ThreadWorker {
    fn run(self) -> Result<WorkerSummary, String> {
        let clients_per_thread = self.scheduler_config.clients_per_thread;
        let client_indices =
            planned_client_indices(self.thread_idx, clients_per_thread, self.total_clients);
        let mut handles = Vec::with_capacity(client_indices.len());
        for client_idx in client_indices {
            let client_worker = self.clone();
            handles.push(thread::spawn(move || client_worker.run_client(client_idx)));
        }

        let mut summary = WorkerSummary::default();
        let mut errors = Vec::new();
        for handle in handles {
            match handle.join() {
                Ok(Ok(client_summary)) => summary.merge(client_summary),
                Ok(Err(err)) => errors.push(err),
                Err(_) => errors.push("scheduler client thread panicked".to_string()),
            }
        }
        finish_joined_client_summaries(summary, errors)
    }

    fn run_client(&self, client_idx: usize) -> Result<WorkerSummary, String> {
        self.started_clients.fetch_add(1, Ordering::Relaxed);
        let budget = BudgetState::new(self.budget);
        if !budget.should_send(self.completed.load(Ordering::Relaxed), self.start) {
            self.stop.store(true, Ordering::Relaxed);
            return Ok(WorkerSummary::default());
        }

        let mut connection = RespConnection::connect(&self.options)?;
        let mut workload = self.build_workload(client_idx)?;
        let mut limiter = RateLimiter::new(self.scheduler_config.rate_limit_per_connection);
        let mut summary = WorkerSummary::default();

        loop {
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            let mut batch = Vec::new();
            let mut labels = Vec::with_capacity(self.scheduler_config.pipeline);
            for _ in 0..self.scheduler_config.pipeline {
                let current = self.completed.load(Ordering::Relaxed);
                if !budget.should_send(current, self.start) {
                    self.stop.store(true, Ordering::Relaxed);
                    break;
                }
                match self.completed.compare_exchange(
                    current,
                    current + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        limiter.wait_before_next();
                        let request = workload.next_request(current);
                        batch.extend_from_slice(&protocol::serialize_resp(&request.args));
                        labels.push(request.label);
                    }
                    Err(_) => continue,
                }
            }

            if labels.is_empty() {
                break;
            }

            let sent_at = Instant::now();
            connection.write_all(&batch)?;
            let mut reply_errors = Vec::with_capacity(labels.len());
            for _ in 0..labels.len() {
                reply_errors.push(connection.read_one_reply().is_err());
            }

            if self.collect_metrics {
                let per_reply = (sent_at.elapsed().as_micros() as u64 / labels.len() as u64).max(1);
                summary.record_batch(labels, per_reply, reply_errors);
            }
        }

        Ok(summary)
    }

    fn build_workload(&self, client_idx: usize) -> Result<Box<dyn Workload>, String> {
        let workload = self
            .plan
            .workloads
            .first()
            .ok_or_else(|| "benchmark plan contains no workloads".to_string())?;
        let seed_base = ((self.run_idx as u64) << 32) ^ client_idx as u64 ^ 0x5eed_1234;
        match workload {
            WorkloadPlan::MemtierRatio(ratio) => {
                let (write_pattern, read_pattern) = self.key_patterns();
                Ok(Box::new(MemtierRatioWorkload::new(
                    *ratio,
                    KeyGenerator::new(self.key_config(write_pattern, client_idx), seed_base | 1),
                    KeyGenerator::new(self.key_config(read_pattern, client_idx), seed_base | 2),
                    DataGenerator::new(self.data_config(), seed_base | 3),
                )))
            }
            WorkloadPlan::MemtierCommands => Ok(Box::new(MemtierCommandWorkload::new(
                self.config.memtier.command_specs.clone(),
                self.key_config(KeyPattern::Sequential, client_idx),
                seed_base | 4,
                DataGenerator::new(self.data_config(), seed_base | 5),
            )?)),
            WorkloadPlan::ValkeyTemplates | WorkloadPlan::ValkeyCommandSequence => {
                Err("scheduler only supports memtier Redis workloads in this phase".to_string())
            }
        }
    }

    fn key_patterns(&self) -> (KeyPattern, KeyPattern) {
        self.config
            .memtier
            .key_pattern
            .unwrap_or((KeyPattern::Random, KeyPattern::Random))
    }

    fn key_config(&self, pattern: KeyPattern, client_idx: usize) -> KeyGenConfig {
        KeyGenConfig {
            prefix: self.config.memtier.key_prefix.clone(),
            minimum: self.config.memtier.key_minimum,
            maximum: self.config.memtier.key_maximum,
            pattern,
            client_id: client_idx.min(self.total_clients.saturating_sub(1)),
            total_clients: self.total_clients.max(1),
        }
    }

    fn data_config(&self) -> DataGenConfig {
        if let Some((min, max)) = self.config.memtier.data_size_range {
            DataGenConfig::range(min, max, self.config.memtier.random_data)
        } else {
            DataGenConfig {
                size: self
                    .config
                    .memtier
                    .data_size
                    .unwrap_or(self.config.datasize),
                random: self.config.memtier.random_data,
                range: None,
            }
        }
    }

    #[cfg(test)]
    fn zero_budget_worker_run(
        config: &BenchConfig,
        plan: &BenchmarkPlan,
        scheduler_config: SchedulerConfig,
        total_clients: usize,
    ) -> Result<u64, String> {
        let worker_run = WorkerRun {
            config,
            plan,
            scheduler_config,
            budget: RunBudget::Requests(0),
            options: Arc::new(ConnectionOptions {
                host: "127.0.0.1".to_string(),
                port: 1,
                unix_socket: None,
                user: None,
                password: None,
                dbnum: None,
                resp3: false,
            }),
            total_clients,
            run_idx: 0,
            collect_metrics: true,
        };
        run_workers_with_start_count(worker_run).map(|(_summary, started)| started)
    }

    #[cfg(test)]
    #[allow(clippy::field_reassign_with_default)]
    fn run_zero_budget_client_start_count(clients_per_thread: usize) -> Result<u64, String> {
        let started_clients = Arc::new(AtomicU64::new(0));
        let mut config = BenchConfig::default();
        config.mode = BenchMode::MemtierRedis;
        config.memtier.requests = Some(0);
        let plan = BenchmarkPlan::from_config(&config)?;
        let worker = ThreadWorker {
            config,
            plan,
            scheduler_config: SchedulerConfig {
                threads: 1,
                clients_per_thread,
                pipeline: 1,
                budget: RunBudget::Requests(0),
                run_count: 1,
                warmup: Duration::ZERO,
                rate_limit_per_connection: None,
            },
            budget: RunBudget::Requests(0),
            options: Arc::new(ConnectionOptions {
                host: "127.0.0.1".to_string(),
                port: 1,
                unix_socket: None,
                user: None,
                password: None,
                dbnum: None,
                resp3: false,
            }),
            completed: Arc::new(AtomicU64::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
            start: Instant::now(),
            thread_idx: 0,
            total_clients: clients_per_thread,
            run_idx: 0,
            collect_metrics: true,
            started_clients: started_clients.clone(),
        };
        worker.run()?;
        Ok(started_clients.load(Ordering::Relaxed))
    }
}

fn finish_joined_client_summaries(
    summary: WorkerSummary,
    errors: Vec<String>,
) -> Result<WorkerSummary, String> {
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(errors.join("; "))
    }
}

fn planned_client_indices(
    thread_idx: usize,
    clients_per_thread: usize,
    total_clients: usize,
) -> Vec<usize> {
    let first = thread_idx * clients_per_thread;
    let end = (first + clients_per_thread).min(total_clients);
    (first..end).collect()
}

fn planned_total_clients(config: SchedulerConfig) -> usize {
    config
        .threads
        .saturating_mul(config.clients_per_thread)
        .max(1)
}

fn planned_total_clients_for_plan(plan: &BenchmarkPlan, config: SchedulerConfig) -> usize {
    match plan.mode {
        BenchMode::MemtierRedis => planned_total_clients(config),
        BenchMode::Valkey => plan.clients.max(1),
    }
}

fn finish_memtier_summary(
    combined: SummaryAccumulator,
    percentiles: &[f64],
) -> Result<BenchmarkSummary, String> {
    let summary = combined.finish(percentiles);
    if summary.total_errors > 0 {
        return Err(format!(
            "benchmark failed with {} Redis command {}",
            summary.total_errors,
            if summary.total_errors == 1 {
                "error"
            } else {
                "errors"
            }
        ));
    }
    Ok(summary)
}

impl Clone for ThreadWorker {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            plan: self.plan.clone(),
            scheduler_config: self.scheduler_config,
            budget: self.budget,
            options: self.options.clone(),
            completed: self.completed.clone(),
            stop: self.stop.clone(),
            start: self.start,
            thread_idx: self.thread_idx,
            total_clients: self.total_clients,
            run_idx: self.run_idx,
            collect_metrics: self.collect_metrics,
            started_clients: self.started_clients.clone(),
        }
    }
}

#[derive(Clone, Copy)]
struct BudgetState {
    budget: RunBudget,
}

impl BudgetState {
    fn new(budget: RunBudget) -> Self {
        Self { budget }
    }

    fn should_send(&self, completed: u64, start: Instant) -> bool {
        match self.budget {
            RunBudget::Requests(requests) => completed < requests,
            RunBudget::Time(duration) => start.elapsed() < duration,
        }
    }
}

struct RateLimiter {
    delay: Option<Duration>,
    next_send: Option<Instant>,
}

impl RateLimiter {
    fn new(rate_per_sec: Option<u64>) -> Self {
        Self {
            delay: rate_per_sec.and_then(delay_for_rate),
            next_send: None,
        }
    }

    #[cfg(test)]
    fn delay_between_requests(&self) -> Option<Duration> {
        self.delay
    }

    fn wait_before_next(&mut self) {
        let Some(delay) = self.delay else {
            return;
        };
        let now = Instant::now();
        if let Some(next_send) = self.next_send {
            if next_send > now {
                thread::sleep(next_send - now);
            }
            self.next_send = Some(next_send + delay);
        } else {
            self.next_send = Some(now + delay);
        }
    }
}

fn delay_for_rate(rate_per_sec: u64) -> Option<Duration> {
    if rate_per_sec == 0 {
        return None;
    }
    Some(Duration::from_secs_f64(1.0 / rate_per_sec as f64))
}

#[derive(Default)]
struct WorkerSummary {
    total_requests: u64,
    total_errors: u64,
    duration: Duration,
    commands: BTreeMap<String, CommandAccumulator>,
}

impl WorkerSummary {
    fn record(&mut self, label: String, latency_us: u64, errors: u64) {
        self.total_requests += 1;
        self.total_errors += errors;
        self.commands
            .entry(label)
            .or_default()
            .record(latency_us, errors);
    }

    fn record_batch(&mut self, labels: Vec<String>, latency_us: u64, reply_errors: Vec<bool>) {
        for (idx, label) in labels.into_iter().enumerate() {
            let errors = u64::from(reply_errors.get(idx).copied().unwrap_or(false));
            self.record(label, latency_us, errors);
        }
    }

    fn merge(&mut self, other: Self) {
        self.total_requests += other.total_requests;
        self.total_errors += other.total_errors;
        self.duration = self.duration.max(other.duration);
        for (name, command) in other.commands {
            self.commands.entry(name).or_default().merge(command);
        }
    }
}

#[derive(Default)]
struct CommandAccumulator {
    requests: u64,
    errors: u64,
    latencies: LatencyRecorder,
}

impl CommandAccumulator {
    fn record(&mut self, latency_us: u64, errors: u64) {
        self.requests += 1;
        self.errors += errors;
        self.latencies.record(latency_us);
    }

    fn merge(&mut self, mut other: Self) {
        self.requests += other.requests;
        self.errors += other.errors;
        self.latencies.append(&mut other.latencies);
    }
}

#[derive(Default)]
struct SummaryAccumulator {
    total_requests: u64,
    total_errors: u64,
    duration_secs: f64,
    commands: BTreeMap<String, CommandAccumulator>,
}

impl SummaryAccumulator {
    fn merge(&mut self, summary: WorkerSummary) {
        self.total_requests += summary.total_requests;
        self.total_errors += summary.total_errors;
        self.duration_secs += summary.duration.as_secs_f64();
        for (name, command) in summary.commands {
            self.commands.entry(name).or_default().merge(command);
        }
    }

    fn finish(self, percentiles: &[f64]) -> BenchmarkSummary {
        let mut commands = Vec::with_capacity(self.commands.len());
        for (name, mut command) in self.commands {
            command.latencies.sort_unstable();
            let rps = if self.duration_secs > 0.0 {
                command.requests as f64 / self.duration_secs
            } else {
                0.0
            };
            commands.push(CommandStats {
                name,
                requests: command.requests,
                errors: command.errors,
                rps,
                percentiles: percentiles
                    .iter()
                    .map(|pct| (*pct, command.latencies.percentile_sorted(*pct)))
                    .collect(),
            });
        }
        BenchmarkSummary {
            total_requests: self.total_requests,
            total_errors: self.total_errors,
            duration_secs: self.duration_secs,
            commands,
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::plan::RunBudget;
    use std::time::Instant;

    #[test]
    fn request_budget_stops_at_configured_count() {
        let budget = BudgetState::new(RunBudget::Requests(3));
        assert!(budget.should_send(0, Instant::now()));
        assert!(budget.should_send(2, Instant::now()));
        assert!(!budget.should_send(3, Instant::now()));
    }

    #[test]
    fn rate_limiter_computes_spacing() {
        let limiter = RateLimiter::new(Some(100));
        assert_eq!(
            limiter.delay_between_requests(),
            Some(Duration::from_millis(10))
        );
    }

    #[test]
    fn zero_request_run_plan_returns_empty_summary_without_connecting() {
        let mut config = BenchConfig::default();
        config.mode = BenchMode::MemtierRedis;
        config.clients = 1;
        config.threads = 1;
        config.memtier.requests = Some(0);
        let plan = BenchmarkPlan::from_config(&config).unwrap();
        let summary = run_plan(&config, &plan).unwrap();
        assert_eq!(summary.total_requests, 0);
        assert_eq!(summary.total_errors, 0);
        assert!(summary.commands.is_empty());
    }

    #[test]
    fn rate_limiter_waits_after_first_request() {
        let mut limiter = RateLimiter::new(Some(1_000_000));
        limiter.wait_before_next();
        limiter.wait_before_next();
    }

    #[test]
    fn worker_summary_merges_command_metrics() {
        let mut left = WorkerSummary::default();
        left.record("GET".to_string(), 5, 0);
        let mut right = WorkerSummary::default();
        right.record("GET".to_string(), 7, 1);
        left.merge(right);

        let mut combined = SummaryAccumulator::default();
        combined.merge(left);
        let summary = combined.finish(&[50.0, 100.0]);
        assert_eq!(summary.total_requests, 2);
        assert_eq!(summary.total_errors, 1);
        assert_eq!(summary.commands[0].requests, 2);
        assert_eq!(summary.commands[0].errors, 1);
    }

    #[test]
    fn zero_budget_worker_starts_every_configured_client_loop() {
        let started = ThreadWorker::run_zero_budget_client_start_count(3).unwrap();
        assert_eq!(started, 3);
    }

    #[test]
    fn memtier_clients_are_planned_per_thread() {
        let mut config = BenchConfig::default();
        config.mode = BenchMode::MemtierRedis;
        config.clients = 3;
        config.threads = 2;
        config.memtier.requests = Some(0);
        let plan = BenchmarkPlan::from_config(&config).unwrap();
        let scheduler_config = SchedulerConfig::from_config(&config, &plan);
        assert_eq!(scheduler_config.threads, 2);
        assert_eq!(scheduler_config.clients_per_thread, 3);
        assert_eq!(planned_total_clients(scheduler_config), 6);
    }

    #[test]
    fn valkey_clients_preserve_requested_total_when_not_divisible_by_threads() {
        let mut config = BenchConfig::default();
        config.clients = 3;
        config.threads = 2;
        let plan = BenchmarkPlan::from_config(&config).unwrap();
        let scheduler_config = SchedulerConfig::from_config(&config, &plan);
        assert_eq!(scheduler_config.threads, 2);
        assert_eq!(scheduler_config.clients_per_thread, 2);
        assert_eq!(planned_total_clients_for_plan(&plan, scheduler_config), 3);
    }

    #[test]
    fn memtier_total_clients_remains_threads_times_clients_per_thread() {
        let mut config = BenchConfig::default();
        config.mode = BenchMode::MemtierRedis;
        config.clients = 3;
        config.threads = 2;
        config.memtier.requests = Some(0);
        let plan = BenchmarkPlan::from_config(&config).unwrap();
        let scheduler_config = SchedulerConfig::from_config(&config, &plan);
        assert_eq!(planned_total_clients_for_plan(&plan, scheduler_config), 6);
    }

    #[test]
    fn zero_budget_run_plan_starts_threads_times_clients_without_connecting() {
        let mut config = BenchConfig::default();
        config.mode = BenchMode::MemtierRedis;
        config.clients = 2;
        config.threads = 3;
        config.memtier.requests = Some(0);
        let plan = BenchmarkPlan::from_config(&config).unwrap();
        let scheduler_config = SchedulerConfig::from_config(&config, &plan);
        let started = ThreadWorker::zero_budget_worker_run(
            &config,
            &plan,
            scheduler_config,
            planned_total_clients(scheduler_config),
        )
        .unwrap();
        assert_eq!(started, 6);
    }

    #[test]
    fn planned_client_indices_do_not_exceed_requested_clients() {
        assert_eq!(planned_client_indices(0, 2, 3), vec![0, 1]);
        assert_eq!(planned_client_indices(1, 2, 3), vec![2]);
        assert!(planned_client_indices(2, 2, 3).is_empty());
    }

    #[test]
    fn batch_error_accounting_records_per_command_errors() {
        let mut summary = WorkerSummary::default();
        summary.record_batch(
            vec!["GET".to_string(), "SET".to_string(), "GET".to_string()],
            11,
            vec![true, false, true],
        );

        let mut combined = SummaryAccumulator::default();
        combined.merge(summary);
        let summary = combined.finish(&[100.0]);
        assert_eq!(summary.total_errors, 2);
        let get = summary
            .commands
            .iter()
            .find(|command| command.name == "GET")
            .unwrap();
        let set = summary
            .commands
            .iter()
            .find(|command| command.name == "SET")
            .unwrap();
        assert_eq!(get.errors, 2);
        assert_eq!(set.errors, 0);
    }

    #[test]
    fn partial_client_error_is_not_swallowed_after_progress() {
        let mut summary = WorkerSummary::default();
        summary.record("GET".to_string(), 5, 0);
        let result = finish_joined_client_summaries(summary, vec!["connect failed".to_string()]);
        let Err(err) = result else {
            panic!("partial client errors must fail the scheduler run");
        };
        assert!(err.contains("connect failed"));
    }

    #[test]
    fn memtier_summary_with_runtime_errors_fails_run() {
        let mut summary = WorkerSummary::default();
        summary.record("GET".to_string(), 5, 1);
        let mut combined = SummaryAccumulator::default();
        combined.merge(summary);
        let result = finish_memtier_summary(combined, &[50.0]);
        let Err(err) = result else {
            panic!("runtime Redis errors must fail memtier runs");
        };
        assert!(err.contains("1 Redis command error"));
    }
}
