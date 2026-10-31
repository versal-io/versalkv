use std::collections::HashMap;
use std::fmt::Write;
use std::time::Instant;

const ERRORSTATS_LIMIT: usize = 128;
const ERRORSTATS_OVERFLOW: &str = "ERRORSTATS_OVERFLOW";
const STANDARD_ERROR_CODES: &[&str] = &[
    "ERR",
    "WRONGTYPE",
    "NOSCRIPT",
    "MOVED",
    "CLUSTERDOWN",
    "LOADING",
    "READONLY",
    "MASTERDOWN",
    "NOAUTH",
    "WRONGPASS",
    "NOPERM",
    "BUSY",
    "NOPROTO",
    "NOKEY",
    "EXECABORT",
    "CROSSSLOT",
    "TRYAGAIN",
    "ASK",
    "UNBLOCKED",
    "OOM",
    "NOREPLICAS",
    "MISCONF",
    "NOGROUP",
];

const LATENCY_EXACT_USEC_LIMIT: usize = 2048;
const LATENCY_LOG_SUB_BUCKETS: usize = 16;
const LATENCY_LOG_BUCKETS: usize = 53 * LATENCY_LOG_SUB_BUCKETS;
const INSTANTANEOUS_METRIC_SAMPLES: usize = 16;
const INSTANTANEOUS_METRIC_PERIOD_MS: i64 = 100;

#[derive(Default)]
struct InstantaneousMetric {
    samples: [u64; INSTANTANEOUS_METRIC_SAMPLES],
    next_sample: usize,
    last_base: Option<u64>,
    last_value: u64,
}

impl InstantaneousMetric {
    fn track(&mut self, current_value: u64, current_base: u64, factor: u64) {
        if let Some(last_base) = self.last_base {
            let base_delta = current_base.saturating_sub(last_base);
            let value_delta = current_value.saturating_sub(self.last_value);
            let sample = value_delta
                .saturating_mul(factor)
                .checked_div(base_delta)
                .unwrap_or(0);
            self.samples[self.next_sample] = sample;
            self.next_sample = (self.next_sample + 1) % INSTANTANEOUS_METRIC_SAMPLES;
        }
        self.last_base = Some(current_base);
        self.last_value = current_value;
    }

    fn average(&self) -> u64 {
        self.samples.iter().sum::<u64>() / INSTANTANEOUS_METRIC_SAMPLES as u64
    }
}

/// Compact, per-command latency distribution. Low microsecond values retain
/// exact buckets; larger values use sixteen sub-buckets per power-of-two range.
/// This keeps every command's telemetry bounded while avoiding the previous
/// average-derived pseudo-percentiles.
#[derive(Debug, Clone, PartialEq)]
struct LatencyHistogram {
    exact_usec: Box<[u64; LATENCY_EXACT_USEC_LIMIT]>,
    logarithmic_usec: [u64; LATENCY_LOG_BUCKETS],
    count: u64,
}

impl LatencyHistogram {
    fn record(&mut self, duration_usec: u64) {
        let duration_usec = duration_usec.max(1);
        if (duration_usec as usize) < LATENCY_EXACT_USEC_LIMIT {
            self.exact_usec[duration_usec as usize] =
                self.exact_usec[duration_usec as usize].saturating_add(1);
        } else {
            let exponent = 63usize.saturating_sub(duration_usec.leading_zeros() as usize);
            let base = 1u64 << exponent;
            let sub_width = (base / LATENCY_LOG_SUB_BUCKETS as u64).max(1);
            let sub_bucket = ((duration_usec - base) / sub_width)
                .min(LATENCY_LOG_SUB_BUCKETS as u64 - 1) as usize;
            let bucket = (exponent.saturating_sub(11) * LATENCY_LOG_SUB_BUCKETS + sub_bucket)
                .min(LATENCY_LOG_BUCKETS - 1);
            self.logarithmic_usec[bucket] = self.logarithmic_usec[bucket].saturating_add(1);
        }
        self.count = self.count.saturating_add(1);
    }

    fn percentile_usec(&self, percentile: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let percentile = percentile.clamp(0.0, 100.0);
        let rank = ((self.count as f64 * percentile / 100.0).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (duration_usec, count) in self.exact_usec.iter().enumerate().skip(1) {
            seen = seen.saturating_add(*count);
            if seen >= rank {
                return Some(duration_usec as u64);
            }
        }
        for (bucket, count) in self.logarithmic_usec.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= rank {
                let exponent = bucket / LATENCY_LOG_SUB_BUCKETS + 11;
                let base = 1u64 << exponent;
                let sub_width = (base / LATENCY_LOG_SUB_BUCKETS as u64).max(1);
                let sub_bucket = (bucket % LATENCY_LOG_SUB_BUCKETS) as u64;
                return Some(
                    base.saturating_add(
                        sub_width
                            .saturating_mul(sub_bucket)
                            .saturating_add(sub_width / 2),
                    ),
                );
            }
        }
        Some(1u64 << 63)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            exact_usec: Box::new([0; LATENCY_EXACT_USEC_LIMIT]),
            logarithmic_usec: [0; LATENCY_LOG_BUCKETS],
            count: 0,
        }
    }
}

fn generate_run_id() -> String {
    let mut id = String::with_capacity(40);
    let mut state: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xdeadbeef);
    for _ in 0..20 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let _ = write!(id, "{:02x}", (state >> 56) as u8);
    }
    id
}

#[derive(Debug, Clone, Default)]
pub struct CmdStat {
    pub calls: u64,
    pub usec: u64,
    pub rejected_calls: u64,
    pub failed_calls: u64,
    latency_histogram: Option<Box<LatencyHistogram>>,
}

impl CmdStat {
    pub fn record_latency_usec(&mut self, duration_usec: u64) {
        self.latency_histogram
            .get_or_insert_with(Default::default)
            .record(duration_usec);
    }

    pub fn percentile_usec(&self, percentile: f64) -> Option<u64> {
        self.latency_histogram
            .as_deref()
            .and_then(|histogram| histogram.percentile_usec(percentile))
    }
}

pub struct ServerStats {
    pub cluster_io_threaded_reads_processed: u64,
    pub cluster_io_threaded_writes_processed: u64,
    pub cluster_io_main_thread_fallbacks: u64,
    pub start_time: Instant,
    pub connections_received: u64,
    pub connected_clients: u64,
    pub connected_priority_clients: u64,
    pub rejected_connections: u64,
    pub rejected_priority_connections: u64,
    pub cmd_stats: HashMap<String, CmdStat>,
    pub active_time_usec: u64,
    pub tcp_port: u16,
    pub clients_hz: u64,
    pub sync_full: u64,
    pub sync_partial_ok: u64,
    pub sync_partial_err: u64,
    pub expired_keys: u64,
    pub expired_fields: u64,
    pub evicted_keys: u64,
    pub evicted_clients: u64,
    pub client_query_buffer_limit_disconnections: u64,
    pub client_output_buffer_limit_disconnections: u64,
    pub total_error_replies: u64,
    pub error_stats: HashMap<String, u64>,
    pub latency_histos: HashMap<String, u64>,
    pub lazyfree_pending_objects: u64,
    pub lazyfreed_objects: u64,
    pub io_threaded_total_prefetch_batches: u64,
    pub io_threaded_total_prefetch_entries: u64,
    pub active_defrag_hits: u64,
    pub active_defrag_misses: u64,
    pub active_defrag_key_hits: u64,
    pub active_defrag_key_misses: u64,
    pub total_active_defrag_time: u64,
    pub current_active_defrag_time: u64,
    pub active_defrag_running: bool,
    pub total_commands_processed: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub total_net_repl_input_bytes: u64,
    pub total_net_repl_output_bytes: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub total_forks: u64,
    pub latest_fork_usec: u64,
    pub instantaneous_ops_per_sec: u64,
    pub instantaneous_input_kbps: f64,
    pub instantaneous_output_kbps: f64,
    pub eventloop_cycles: u64,
    pub eventloop_duration_sum: u64,
    pub eventloop_duration_cmd_sum: u64,
    pub eventloop_priority_cycles: u64,
    pub eventloop_priority_duration_sum: u64,
    pub eventloop_priority_duration_cmd_sum: u64,
    pub eventloop_priority_duration_max: u64,
    pub eventloop_priority_cmd_per_cycle_max: u64,
    pub(crate) priority_commands_processed: u64,
    pub(crate) reset_generation: u64,
    pub(crate) last_reset_at: Instant,
    is_priority_command: bool,
    pub eventloop_duration_aof_sum: u64,
    pub eventloop_duration_cron_sum: u64,
    pub eventloop_events_collected: u64,
    pub eventloop_events_handled: u64,
    pub eventloop_events_deferred: u64,
    pub eventloop_events_requeued: u64,
    pub eventloop_events_stale: u64,
    pub eventloop_events_owner_closed: u64,
    pub eventloop_events_dispatch_latency_sum: u64,
    pub eventloop_duration_max: u64,
    pub eventloop_cmd_per_cycle_max: u64,
    pub instantaneous_eventloop_cycles_per_sec: u64,
    pub instantaneous_eventloop_duration_usec: u64,
    pub run_id: String,
    pub acl_access_denied_auth: u64,
    pub acl_access_denied_tls_cert: u64,
    pub acl_access_denied_cmd: u64,
    pub acl_access_denied_key: u64,
    pub acl_access_denied_channel: u64,
    pub acl_access_denied_db: u64,
    last_cmd_start: Option<Instant>,
    pending_latency_name: Option<String>,
    last_sample_time_ms: Option<i64>,
    ops_metric: InstantaneousMetric,
    input_metric: InstantaneousMetric,
    output_metric: InstantaneousMetric,
    eventloop_cycle_metric: InstantaneousMetric,
    eventloop_duration_metric: InstantaneousMetric,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventLoopEventOutcome {
    Handled,
    Deferred,
    Requeued,
    Stale,
    OwnerClosed,
}

impl ServerStats {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cluster_io_threaded_reads_processed: 0,
            cluster_io_threaded_writes_processed: 0,
            cluster_io_main_thread_fallbacks: 0,
            start_time: Instant::now(),
            connections_received: 0,
            connected_clients: 0,
            connected_priority_clients: 0,
            rejected_connections: 0,
            rejected_priority_connections: 0,
            cmd_stats: HashMap::new(),
            active_time_usec: 0,
            tcp_port: 0,
            clients_hz: 10,
            sync_full: 0,
            sync_partial_ok: 0,
            sync_partial_err: 0,
            expired_keys: 0,
            expired_fields: 0,
            evicted_keys: 0,
            evicted_clients: 0,
            client_query_buffer_limit_disconnections: 0,
            client_output_buffer_limit_disconnections: 0,
            total_error_replies: 0,
            error_stats: HashMap::new(),
            latency_histos: HashMap::new(),
            lazyfree_pending_objects: 0,
            lazyfreed_objects: 0,
            io_threaded_total_prefetch_batches: 0,
            io_threaded_total_prefetch_entries: 0,
            active_defrag_hits: 0,
            active_defrag_misses: 0,
            active_defrag_key_hits: 0,
            active_defrag_key_misses: 0,
            total_active_defrag_time: 0,
            current_active_defrag_time: 0,
            active_defrag_running: false,
            total_commands_processed: 0,
            total_net_input_bytes: 0,
            total_net_output_bytes: 0,
            total_net_repl_input_bytes: 0,
            total_net_repl_output_bytes: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            total_forks: 0,
            latest_fork_usec: 0,
            instantaneous_ops_per_sec: 0,
            instantaneous_input_kbps: 0.0,
            instantaneous_output_kbps: 0.0,
            eventloop_cycles: 0,
            eventloop_duration_sum: 0,
            eventloop_duration_cmd_sum: 0,
            eventloop_priority_cycles: 0,
            eventloop_priority_duration_sum: 0,
            eventloop_priority_duration_cmd_sum: 0,
            eventloop_priority_duration_max: 0,
            eventloop_priority_cmd_per_cycle_max: 0,
            priority_commands_processed: 0,
            reset_generation: 0,
            last_reset_at: Instant::now(),
            is_priority_command: false,
            eventloop_duration_aof_sum: 0,
            eventloop_duration_cron_sum: 0,
            eventloop_events_collected: 0,
            eventloop_events_handled: 0,
            eventloop_events_deferred: 0,
            eventloop_events_requeued: 0,
            eventloop_events_stale: 0,
            eventloop_events_owner_closed: 0,
            eventloop_events_dispatch_latency_sum: 0,
            eventloop_duration_max: 0,
            eventloop_cmd_per_cycle_max: 0,
            instantaneous_eventloop_cycles_per_sec: 0,
            instantaneous_eventloop_duration_usec: 0,
            run_id: generate_run_id(),
            acl_access_denied_auth: 0,
            acl_access_denied_tls_cert: 0,
            acl_access_denied_cmd: 0,
            acl_access_denied_key: 0,
            acl_access_denied_channel: 0,
            acl_access_denied_db: 0,
            last_cmd_start: None,
            pending_latency_name: None,
            last_sample_time_ms: None,
            ops_metric: InstantaneousMetric::default(),
            input_metric: InstantaneousMetric::default(),
            output_metric: InstantaneousMetric::default(),
            eventloop_cycle_metric: InstantaneousMetric::default(),
            eventloop_duration_metric: InstantaneousMetric::default(),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn incr_connection(&mut self) {
        self.connections_received += 1;
        self.connected_clients += 1;
    }

    pub fn decr_connection(&mut self) {
        if self.connected_clients > 0 {
            self.connected_clients -= 1;
        }
    }

    pub fn incr_expired_keys(&mut self, n: u64) {
        self.expired_keys += n;
    }

    pub fn incr_expired_fields(&mut self, n: u64) {
        self.expired_fields += n;
    }

    pub fn incr_evicted_keys(&mut self, n: u64) {
        self.evicted_keys += n;
    }

    pub fn incr_evicted_clients(&mut self, n: u64) {
        self.evicted_clients += n;
    }

    pub fn incr_lazyfreed_objects(&mut self, n: u64) {
        self.lazyfreed_objects = self.lazyfreed_objects.saturating_add(n);
    }

    pub fn add_lazyfree_pending_objects(&mut self, n: u64) {
        self.lazyfree_pending_objects = self.lazyfree_pending_objects.saturating_add(n);
    }

    pub fn complete_lazyfree_objects(&mut self, n: u64) {
        self.lazyfree_pending_objects = self.lazyfree_pending_objects.saturating_sub(n);
        self.lazyfreed_objects = self.lazyfreed_objects.saturating_add(n);
    }

    pub fn complete_lazyfree_work(&mut self, pending_effort: u64, freed_objects: u64) {
        self.lazyfree_pending_objects =
            self.lazyfree_pending_objects.saturating_sub(pending_effort);
        self.lazyfreed_objects = self.lazyfreed_objects.saturating_add(freed_objects);
    }

    pub fn reset_lazyfree_stats(&mut self) {
        self.lazyfree_pending_objects = 0;
        self.lazyfreed_objects = 0;
    }

    pub fn incr_sync_full(&mut self) {
        self.sync_full += 1;
    }

    pub fn incr_sync_partial_ok(&mut self) {
        self.sync_partial_ok += 1;
    }

    pub fn incr_sync_partial_err(&mut self) {
        self.sync_partial_err += 1;
    }

    pub fn incr_total_commands(&mut self) {
        self.total_commands_processed += 1;
    }

    pub fn add_net_input_bytes(&mut self, bytes: u64) {
        self.total_net_input_bytes += bytes;
    }

    pub fn add_net_output_bytes(&mut self, bytes: u64) {
        self.total_net_output_bytes += bytes;
    }

    pub fn add_net_repl_output_bytes(&mut self, bytes: u64) {
        self.total_net_repl_output_bytes += bytes;
    }

    pub fn add_net_repl_input_bytes(&mut self, bytes: u64) {
        self.total_net_repl_input_bytes += bytes;
    }

    pub fn incr_keyspace_hits(&mut self) {
        self.keyspace_hits += 1;
    }

    pub fn incr_keyspace_misses(&mut self) {
        self.keyspace_misses += 1;
    }

    pub fn incr_rejected_connections(&mut self) {
        self.rejected_connections += 1;
    }

    pub fn record_fork(&mut self, duration_usec: u64) {
        self.total_forks += 1;
        self.latest_fork_usec = duration_usec;
    }

    pub fn sample_instantaneous_metrics(&mut self, now_ms: i64) {
        if self
            .last_sample_time_ms
            .is_some_and(|last_ms| now_ms.saturating_sub(last_ms) < INSTANTANEOUS_METRIC_PERIOD_MS)
        {
            return;
        }
        self.last_sample_time_ms = Some(now_ms);
        let now_ms = now_ms.max(0) as u64;

        // Match server.c trackInstantaneousMetric(): sample at 100ms cadence
        // and expose the mean of the last sixteen samples, including the
        // zero-filled warm-up slots after CONFIG RESETSTAT.
        self.ops_metric
            .track(self.total_commands_processed, now_ms, 1_000);
        self.input_metric
            .track(self.total_net_input_bytes, now_ms, 1_000);
        self.output_metric
            .track(self.total_net_output_bytes, now_ms, 1_000);
        self.eventloop_cycle_metric
            .track(self.eventloop_cycles, now_ms, 1_000);
        self.eventloop_duration_metric
            .track(self.eventloop_duration_sum, self.eventloop_cycles, 1);

        self.instantaneous_ops_per_sec = self.ops_metric.average();
        self.instantaneous_input_kbps = self.input_metric.average() as f64 / 1024.0;
        self.instantaneous_output_kbps = self.output_metric.average() as f64 / 1024.0;
        self.instantaneous_eventloop_cycles_per_sec = self.eventloop_cycle_metric.average();
        self.instantaneous_eventloop_duration_usec = self.eventloop_duration_metric.average();
    }

    pub fn record_eventloop_cycle(
        &mut self,
        duration_usec: u64,
        cmd_duration_usec: u64,
        cmd_count: u64,
    ) {
        self.eventloop_cycles = self.eventloop_cycles.saturating_add(1);
        self.eventloop_duration_sum = self.eventloop_duration_sum.saturating_add(duration_usec);
        self.eventloop_duration_cmd_sum = self
            .eventloop_duration_cmd_sum
            .saturating_add(cmd_duration_usec);
        self.eventloop_duration_max = self.eventloop_duration_max.max(duration_usec);
        self.eventloop_cmd_per_cycle_max = self.eventloop_cmd_per_cycle_max.max(cmd_count);
    }

    pub fn record_eventloop_cron(&mut self, duration_usec: u64) {
        self.eventloop_duration_cron_sum = self
            .eventloop_duration_cron_sum
            .saturating_add(duration_usec);
    }

    pub fn record_eventloop_priority_cycle(&mut self, duration_usec: u64, cmd_count: u64) {
        self.eventloop_priority_cycles = self.eventloop_priority_cycles.saturating_add(1);
        self.eventloop_priority_duration_sum = self
            .eventloop_priority_duration_sum
            .saturating_add(duration_usec);
        self.eventloop_priority_duration_max =
            self.eventloop_priority_duration_max.max(duration_usec);
        self.eventloop_priority_cmd_per_cycle_max =
            self.eventloop_priority_cmd_per_cycle_max.max(cmd_count);
    }

    pub(crate) fn set_priority_command(&mut self, is_priority: bool) -> bool {
        std::mem::replace(&mut self.is_priority_command, is_priority)
    }

    pub(crate) fn record_priority_command(&mut self, duration_usec: u64) {
        self.priority_commands_processed = self.priority_commands_processed.saturating_add(1);
        self.eventloop_priority_duration_cmd_sum = self
            .eventloop_priority_duration_cmd_sum
            .saturating_add(duration_usec);
    }

    pub fn record_eventloop_aof(&mut self, duration_usec: u64) {
        self.eventloop_duration_aof_sum = self
            .eventloop_duration_aof_sum
            .saturating_add(duration_usec);
    }

    pub fn record_eventloop_event_collected(&mut self, count: usize) {
        self.eventloop_events_collected =
            self.eventloop_events_collected.saturating_add(count as u64);
    }

    pub fn record_eventloop_event_dispatch(
        &mut self,
        outcome: EventLoopEventOutcome,
        duration_usec: u64,
    ) {
        self.eventloop_events_dispatch_latency_sum = self
            .eventloop_events_dispatch_latency_sum
            .saturating_add(duration_usec);
        match outcome {
            EventLoopEventOutcome::Handled => {
                self.eventloop_events_handled = self.eventloop_events_handled.saturating_add(1);
            }
            EventLoopEventOutcome::Deferred => {
                self.eventloop_events_deferred = self.eventloop_events_deferred.saturating_add(1);
            }
            EventLoopEventOutcome::Requeued => {
                self.eventloop_events_requeued = self.eventloop_events_requeued.saturating_add(1);
            }
            EventLoopEventOutcome::Stale => {
                self.eventloop_events_stale = self.eventloop_events_stale.saturating_add(1);
            }
            EventLoopEventOutcome::OwnerClosed => {
                self.eventloop_events_owner_closed =
                    self.eventloop_events_owner_closed.saturating_add(1);
            }
        }
    }

    pub fn begin_cmd(&mut self) {
        self.last_cmd_start = Some(Instant::now());
    }

    pub fn set_cmd_fullname(&mut self, fullname: &str) {
        self.pending_latency_name = Some(fullname.to_string());
    }

    pub fn end_cmd(&mut self, name: &str, rejected: bool, track_latency: bool) -> u64 {
        let duration_us = self
            .last_cmd_start
            .take()
            .map(|start| start.elapsed().as_micros() as u64);
        self.record_cmd_completion(name, rejected, track_latency, duration_us)
    }

    pub fn end_cmd_with_duration(
        &mut self,
        name: &str,
        rejected: bool,
        track_latency: bool,
        elapsed_us: u64,
    ) -> u64 {
        self.record_cmd_completion(name, rejected, track_latency, Some(elapsed_us))
    }

    fn record_cmd_completion(
        &mut self,
        name: &str,
        rejected: bool,
        track_latency: bool,
        elapsed_us: Option<u64>,
    ) -> u64 {
        let stat = self.cmd_stats.entry(name.to_ascii_lowercase()).or_default();
        let mut duration_us = 0u64;
        if rejected {
            stat.rejected_calls += 1;
            self.total_commands_processed += 1;
        } else {
            stat.calls += 1;
            self.total_commands_processed += 1;
            if let Some(elapsed_us) = elapsed_us {
                stat.usec += elapsed_us;
                self.active_time_usec += elapsed_us;
                duration_us = elapsed_us;
                if track_latency {
                    stat.record_latency_usec(elapsed_us);
                }
            }
            let lat_name = self
                .pending_latency_name
                .take()
                .unwrap_or_else(|| name.to_ascii_lowercase());
            if track_latency {
                *self.latency_histos.entry(lat_name).or_insert(0) += 1;
            }
        }
        if self.pending_latency_name.is_some() {
            self.pending_latency_name = None;
        }
        if !rejected && self.is_priority_command {
            self.record_priority_command(duration_us);
        }
        duration_us
    }

    pub fn latency_histos_reset(&mut self) {
        self.latency_histos.clear();
        for stat in self.cmd_stats.values_mut() {
            stat.latency_histogram = None;
        }
    }

    pub fn record_command_latency(&mut self, name: &str, duration_usec: u64) {
        self.cmd_stats
            .entry(name.to_ascii_lowercase())
            .or_default()
            .record_latency_usec(duration_usec);
        *self
            .latency_histos
            .entry(name.to_ascii_lowercase())
            .or_insert(0) += 1;
    }

    pub fn reset(&mut self) {
        self.cluster_io_threaded_reads_processed = 0;
        self.cluster_io_threaded_writes_processed = 0;
        self.cluster_io_main_thread_fallbacks = 0;
        self.reset_generation = self.reset_generation.wrapping_add(1);
        self.last_reset_at = Instant::now();
        self.priority_commands_processed = 0;
        self.cmd_stats.clear();
        self.latency_histos.clear();
        self.expired_keys = 0;
        self.expired_fields = 0;
        self.evicted_keys = 0;
        self.evicted_clients = 0;
        self.client_query_buffer_limit_disconnections = 0;
        self.client_output_buffer_limit_disconnections = 0;
        self.connections_received = 0;
        self.connected_clients = 0;
        self.rejected_connections = 0;
        self.rejected_priority_connections = 0;
        self.sync_full = 0;
        self.sync_partial_ok = 0;
        self.sync_partial_err = 0;
        self.active_time_usec = 0;
        self.total_error_replies = 0;
        self.error_stats.clear();
        self.reset_lazyfree_stats();
        self.io_threaded_total_prefetch_batches = 0;
        self.io_threaded_total_prefetch_entries = 0;
        self.active_defrag_hits = 0;
        self.active_defrag_misses = 0;
        self.active_defrag_key_hits = 0;
        self.active_defrag_key_misses = 0;
        self.total_active_defrag_time = 0;
        self.current_active_defrag_time = 0;
        self.active_defrag_running = false;
        self.total_commands_processed = 0;
        self.total_net_input_bytes = 0;
        self.total_net_output_bytes = 0;
        self.total_net_repl_input_bytes = 0;
        self.total_net_repl_output_bytes = 0;
        self.keyspace_hits = 0;
        self.keyspace_misses = 0;
        self.total_forks = 0;
        self.latest_fork_usec = 0;
        self.instantaneous_ops_per_sec = 0;
        self.instantaneous_input_kbps = 0.0;
        self.instantaneous_output_kbps = 0.0;
        self.eventloop_cycles = 0;
        self.eventloop_duration_sum = 0;
        self.eventloop_duration_cmd_sum = 0;
        self.eventloop_priority_cycles = 0;
        self.eventloop_priority_duration_sum = 0;
        self.eventloop_priority_duration_cmd_sum = 0;
        self.eventloop_priority_duration_max = 0;
        self.eventloop_priority_cmd_per_cycle_max = 0;
        self.eventloop_duration_aof_sum = 0;
        self.eventloop_duration_cron_sum = 0;
        self.eventloop_events_collected = 0;
        self.eventloop_events_handled = 0;
        self.eventloop_events_deferred = 0;
        self.eventloop_events_requeued = 0;
        self.eventloop_events_stale = 0;
        self.eventloop_events_owner_closed = 0;
        self.eventloop_events_dispatch_latency_sum = 0;
        self.eventloop_duration_max = 0;
        self.eventloop_cmd_per_cycle_max = 0;
        self.instantaneous_eventloop_cycles_per_sec = 0;
        self.instantaneous_eventloop_duration_usec = 0;
        self.last_sample_time_ms = None;
        self.ops_metric = InstantaneousMetric::default();
        self.input_metric = InstantaneousMetric::default();
        self.output_metric = InstantaneousMetric::default();
        self.eventloop_cycle_metric = InstantaneousMetric::default();
        self.eventloop_duration_metric = InstantaneousMetric::default();
    }

    pub fn incr_error_reply(&mut self, err_name: &str) {
        self.total_error_replies += 1;
        let err_name = err_name.to_ascii_uppercase();
        let is_standard = STANDARD_ERROR_CODES.contains(&err_name.as_str());
        let should_overflow = !is_standard
            && self.error_stats.len() >= ERRORSTATS_LIMIT
            && !self.error_stats.contains_key(&err_name);
        let err_name = if should_overflow {
            ERRORSTATS_OVERFLOW
        } else {
            err_name.as_str()
        };
        *self.error_stats.entry(err_name.to_string()).or_insert(0) += 1;
    }

    pub fn incr_cmd_call(&mut self, name: &str) {
        if let Some(stat) = self.cmd_stats.get_mut(name) {
            stat.calls += 1;
            return;
        }
        let stat = self.cmd_stats.entry(name.to_ascii_lowercase()).or_default();
        stat.calls += 1;
    }

    pub fn incr_cmd_failed(&mut self, name: &str) {
        if let Some(stat) = self.cmd_stats.get_mut(name) {
            stat.failed_calls += 1;
            return;
        }
        let stat = self.cmd_stats.entry(name.to_ascii_lowercase()).or_default();
        stat.failed_calls += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn connection_and_key_counters_update_and_saturate() {
        let mut stats = ServerStats::new();

        stats.incr_connection();
        stats.incr_connection();
        stats.decr_connection();
        stats.incr_expired_keys(3);
        stats.incr_evicted_keys(4);
        stats.incr_sync_full();

        assert_eq!(stats.connections_received, 2);
        assert_eq!(stats.connected_clients, 1);
        stats.decr_connection();
        stats.decr_connection();
        assert_eq!(stats.connected_clients, 0);
        assert_eq!(stats.expired_keys, 3);
        assert_eq!(stats.evicted_keys, 4);
        assert_eq!(stats.sync_full, 1);
    }

    #[test]
    fn lazyfree_accounting_saturates_and_clamps_pending_underflow() {
        let mut stats = ServerStats::new();
        stats.lazyfree_pending_objects = u64::MAX - 1;

        stats.add_lazyfree_pending_objects(2);

        assert_eq!(stats.lazyfree_pending_objects, u64::MAX);

        stats.lazyfree_pending_objects = 3;
        stats.lazyfreed_objects = u64::MAX - 1;
        stats.complete_lazyfree_objects(5);

        assert_eq!(stats.lazyfree_pending_objects, 0);
        assert_eq!(stats.lazyfreed_objects, u64::MAX);
    }

    #[test]
    fn command_stats_record_success_rejection_failure_and_latency_name() {
        let mut stats = ServerStats::new();

        stats.begin_cmd();
        stats.set_cmd_fullname("config|get");
        thread::sleep(Duration::from_micros(1));
        let duration = stats.end_cmd("CONFIG", false, true);
        stats.end_cmd("GET", true, true);
        stats.incr_cmd_failed("GET");
        stats.incr_error_reply("wrongtype");

        assert!(duration > 0);
        let config = stats.cmd_stats.get("config").unwrap();
        assert_eq!(config.calls, 1);
        assert!(config.usec > 0);
        let get = stats.cmd_stats.get("get").unwrap();
        assert_eq!(get.rejected_calls, 1);
        assert_eq!(get.failed_calls, 1);
        assert_eq!(stats.latency_histos.get("config|get"), Some(&1));
        assert_eq!(stats.total_error_replies, 1);
        assert_eq!(stats.error_stats.get("WRONGTYPE"), Some(&1));
    }

    #[test]
    fn command_latency_percentiles_come_from_recorded_samples() {
        let mut stat = CmdStat::default();
        for duration_usec in [10, 20, 100] {
            stat.record_latency_usec(duration_usec);
        }

        assert_eq!(stat.percentile_usec(50.0), Some(20));
        assert_eq!(stat.percentile_usec(99.0), Some(100));
    }

    #[test]
    fn command_latency_histogram_keeps_tens_of_milliseconds_distinct() {
        let mut stat = CmdStat::default();
        stat.record_latency_usec(50_000);

        assert!(stat.percentile_usec(50.0).unwrap() >= 50_000);
    }

    #[test]
    fn reset_clears_command_latency_error_and_counter_state() {
        let mut stats = ServerStats::new();
        stats.incr_connection();
        stats.incr_expired_keys(1);
        stats.incr_evicted_keys(1);
        stats.incr_sync_full();
        stats.begin_cmd();
        stats.end_cmd("set", false, true);
        stats.incr_error_reply("ERR");
        stats.io_threaded_total_prefetch_batches = 1;
        stats.io_threaded_total_prefetch_entries = 2;
        stats.latency_histos_reset();
        assert!(stats.latency_histos.is_empty());
        stats.begin_cmd();
        stats.end_cmd("get", false, true);

        stats.reset();

        assert!(stats.cmd_stats.is_empty());
        assert!(stats.latency_histos.is_empty());
        assert!(stats.error_stats.is_empty());
        assert_eq!(stats.connections_received, 0);
        assert_eq!(stats.connected_clients, 0);
        assert_eq!(stats.expired_keys, 0);
        assert_eq!(stats.expired_fields, 0);
        assert_eq!(stats.evicted_keys, 0);
        assert_eq!(stats.sync_full, 0);
        assert_eq!(stats.active_time_usec, 0);
        assert_eq!(stats.total_error_replies, 0);
        assert_eq!(stats.io_threaded_total_prefetch_batches, 0);
        assert_eq!(stats.io_threaded_total_prefetch_entries, 0);
        assert!(stats.uptime_seconds() <= 1);
    }

    #[test]
    fn instantaneous_metrics_wait_for_the_c_sampling_period() {
        let mut stats = ServerStats::new();
        stats.sample_instantaneous_metrics(1_000);

        stats.total_commands_processed = 1;
        stats.eventloop_cycles = 1;
        stats.sample_instantaneous_metrics(1_010);

        assert_eq!(stats.instantaneous_ops_per_sec, 0);
        assert_eq!(stats.instantaneous_eventloop_cycles_per_sec, 0);
    }

    #[test]
    fn instantaneous_metrics_average_the_c_sixteen_sample_window() {
        let mut stats = ServerStats::new();
        stats.sample_instantaneous_metrics(1_000);

        for sample in 1..=16 {
            stats.total_commands_processed += 10;
            stats.total_net_input_bytes += 1_024;
            stats.total_net_output_bytes += 1_024;
            stats.eventloop_cycles += 10;
            stats.eventloop_duration_sum += 1_000;
            stats.sample_instantaneous_metrics(1_000 + sample * 100);
        }

        stats.total_commands_processed += 20;
        stats.total_net_input_bytes += 2_048;
        stats.total_net_output_bytes += 2_048;
        stats.eventloop_cycles += 20;
        stats.eventloop_duration_sum += 4_000;
        stats.sample_instantaneous_metrics(2_700);

        assert_eq!(stats.instantaneous_ops_per_sec, 106);
        assert_eq!(stats.instantaneous_input_kbps, 10.625);
        assert_eq!(stats.instantaneous_output_kbps, 10.625);
        assert_eq!(stats.instantaneous_eventloop_cycles_per_sec, 106);
        assert_eq!(stats.instantaneous_eventloop_duration_usec, 106);
    }

    #[test]
    fn eventloop_event_outcomes_and_latency_accumulate() {
        let mut stats = ServerStats::new();

        stats.record_eventloop_event_collected(3);
        stats.record_eventloop_event_dispatch(EventLoopEventOutcome::Handled, 4);
        stats.record_eventloop_event_dispatch(EventLoopEventOutcome::Deferred, 5);
        stats.record_eventloop_event_dispatch(EventLoopEventOutcome::OwnerClosed, 6);

        assert_eq!(stats.eventloop_events_collected, 3);
        assert_eq!(stats.eventloop_events_handled, 1);
        assert_eq!(stats.eventloop_events_deferred, 1);
        assert_eq!(stats.eventloop_events_owner_closed, 1);
        assert_eq!(stats.eventloop_events_dispatch_latency_sum, 15);
    }
}
