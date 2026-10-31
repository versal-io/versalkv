use super::{IoThreadInfo, LatencyState, ServerContext};
use crate::foundation::memory::metrics::memory_stats::MemoryCategoryTracker;
use crate::foundation::memory::metrics::stats::{EventLoopEventOutcome, ServerStats};
use crate::framework::context::RuntimeConfig;
use crate::framework::observability::commandlog::CommandLog;
use crate::framework::observability::slot_metrics::SlotMetrics;
use crate::framework::observability::slowlog::SlowLog;

impl ServerContext {
    // ── Configuration ──
    pub fn config(&self) -> &RuntimeConfig {
        &self.rconfig
    }
    pub(crate) fn config_mut(&mut self) -> &mut RuntimeConfig {
        &mut self.rconfig
    }

    // ── Statistics and observability ──
    pub fn stats(&self) -> &ServerStats {
        &self.stats
    }
    pub(crate) fn stats_mut(&mut self) -> &mut ServerStats {
        &mut self.stats
    }

    #[cfg(test)]
    pub fn incr_lazyfreed_objects(&mut self, n: u64) {
        self.stats.incr_lazyfreed_objects(n);
    }
    pub fn begin_cmd(&mut self) {
        self.stats.begin_cmd();
    }
    pub fn end_cmd(&mut self, name: &str, rejected: bool) -> u64 {
        self.stats
            .end_cmd(name, rejected, self.rconfig.server.latency_tracking)
    }
    pub fn end_cmd_with_duration(&mut self, name: &str, rejected: bool, elapsed_us: u64) -> u64 {
        self.stats.end_cmd_with_duration(
            name,
            rejected,
            self.rconfig.server.latency_tracking,
            elapsed_us,
        )
    }
    pub fn set_cmd_fullname(&mut self, fullname: &str) {
        self.stats.set_cmd_fullname(fullname);
    }
    pub fn incr_error_reply(&mut self, err_name: &str) {
        self.stats.incr_error_reply(err_name);
    }
    pub fn incr_cmd_failed(&mut self, name: &str) {
        self.stats.incr_cmd_failed(name);
    }
    pub fn incr_cmd_call(&mut self, name: &str) {
        self.stats.incr_cmd_call(name);
    }
    pub fn record_command_latency(&mut self, name: &str, duration_usec: u64) {
        if self.rconfig.server.latency_tracking {
            self.stats.record_command_latency(name, duration_usec);
        }
    }
    pub fn incr_expired_keys(&mut self, n: u64) {
        self.stats.incr_expired_keys(n);
    }
    pub fn incr_expired_fields(&mut self, n: u64) {
        self.stats.incr_expired_fields(n);
    }
    pub fn incr_evicted_keys(&mut self, n: u64) {
        self.stats.incr_evicted_keys(n);
    }

    pub fn incr_evicted_clients(&mut self, n: u64) {
        self.stats.incr_evicted_clients(n);
    }
    pub fn add_lazyfree_pending_objects(&mut self, n: u64) {
        self.stats.add_lazyfree_pending_objects(n);
    }
    pub fn complete_lazyfree_objects(&mut self, n: u64) {
        self.stats.complete_lazyfree_objects(n);
    }
    pub fn incr_total_commands(&mut self, n: u64) {
        self.stats.total_commands_processed = self.stats.total_commands_processed.saturating_add(n);
    }
    pub fn decr_total_commands(&mut self, n: u64) {
        self.stats.total_commands_processed = self.stats.total_commands_processed.saturating_sub(n);
    }
    pub fn add_active_time_usec(&mut self, us: u64) {
        self.stats.active_time_usec = self.stats.active_time_usec.saturating_add(us);
    }
    pub fn reset_lazyfree_stats(&mut self) {
        self.stats.reset_lazyfree_stats();
    }
    pub fn incr_connection(&mut self) {
        self.stats.incr_connection();
    }
    pub fn decr_connection(&mut self) {
        self.stats.decr_connection();
    }
    pub fn incr_rejected_connections(&mut self) {
        self.stats.incr_rejected_connections();
    }
    pub fn incr_sync_full(&mut self) {
        self.stats.incr_sync_full();
    }
    pub fn incr_sync_partial_ok(&mut self) {
        self.stats.incr_sync_partial_ok();
    }
    pub fn incr_sync_partial_err(&mut self) {
        self.stats.incr_sync_partial_err();
    }
    pub fn add_net_input_bytes(&mut self, bytes: u64) {
        self.stats.add_net_input_bytes(bytes);
    }
    pub fn add_net_output_bytes(&mut self, bytes: u64) {
        self.stats.add_net_output_bytes(bytes);
    }
    pub fn set_tcp_port(&mut self, port: u16) {
        self.stats.tcp_port = port;
    }
    pub fn sample_instantaneous_metrics(&mut self, now_ms: i64) {
        self.stats.sample_instantaneous_metrics(now_ms);
    }
    pub fn record_eventloop_cycle(
        &mut self,
        duration_usec: u64,
        cmd_duration_usec: u64,
        cmd_count: u64,
    ) {
        self.stats
            .record_eventloop_cycle(duration_usec, cmd_duration_usec, cmd_count);
    }
    pub fn record_eventloop_cron(&mut self, duration_usec: u64) {
        self.stats.record_eventloop_cron(duration_usec);
    }
    pub fn record_eventloop_aof(&mut self, duration_usec: u64) {
        self.stats.record_eventloop_aof(duration_usec);
    }
    pub fn record_eventloop_event_collected(&mut self, count: usize) {
        self.stats.record_eventloop_event_collected(count);
    }
    pub fn record_eventloop_event_dispatch(
        &mut self,
        outcome: EventLoopEventOutcome,
        duration_usec: u64,
    ) {
        self.stats
            .record_eventloop_event_dispatch(outcome, duration_usec);
    }
    pub fn mem_stats(&self) -> &'static MemoryCategoryTracker {
        self.mem_stats
    }
    pub fn latency(&self) -> &LatencyState {
        &self.latency
    }
    pub(crate) fn latency_mut(&mut self) -> &mut LatencyState {
        &mut self.latency
    }
    pub fn slowlog(&self) -> &SlowLog {
        &self.slowlog
    }
    pub(crate) fn slowlog_mut(&mut self) -> &mut SlowLog {
        &mut self.slowlog
    }
    pub fn commandlog(&self) -> &CommandLog {
        &self.commandlog
    }
    pub(crate) fn commandlog_mut(&mut self) -> &mut CommandLog {
        &mut self.commandlog
    }
    pub fn slot_metrics(&self) -> &SlotMetrics {
        &self.slot_metrics
    }
    pub(crate) fn slot_metrics_mut(&mut self) -> &mut SlotMetrics {
        &mut self.slot_metrics
    }
    pub fn io_thread_info(&self) -> &IoThreadInfo {
        &self.io_thread_info
    }
    pub fn tls_info(&self) -> &crate::foundation::memory::tls::TlsInfo {
        &self.tls_info
    }
    pub fn set_tls_info(&mut self, info: crate::foundation::memory::tls::TlsInfo) {
        self.tls_info = info;
    }
    pub fn set_io_thread_info(&mut self, info: IoThreadInfo) {
        self.io_thread_info = info;
    }
    pub fn reset_io_thread_counters(&mut self) {
        self.io_thread_info.reset_counters();
    }
    pub fn normal_client_memory(&self) -> usize {
        self.normal_client_memory
    }
    pub(crate) fn adjust_normal_client_memory(&mut self, previous: usize, current: usize) {
        debug_assert!(self.normal_client_memory >= previous);
        self.normal_client_memory = self
            .normal_client_memory
            .saturating_sub(previous)
            .saturating_add(current);
    }
}
