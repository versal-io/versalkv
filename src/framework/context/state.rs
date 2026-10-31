use crate::engine::extension::function::FunctionRegistry;
use crate::engine::extension::lua::LUA_ENGINE;
use crate::engine::extension::script::ScriptControlHandle;
use crate::framework::commands::scripting::script_cache::ScriptCache;
use crate::framework::CommandRegistry;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_MONITOR_EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoThreadInfo {
    pub configured_threads: usize,
    pub active_threads: usize,
    pub active_time_us: Vec<u64>,
    pub reads_processed: u64,
    pub writes_processed: u64,
    pub polls_processed: u64,
    pub reads_pending: u64,
    pub writes_pending: u64,
    pub polls_pending: u64,
    pub main_thread_utilization_pct: u64,
    pub queue_depth_average: u64,
    pub(crate) counter_reset_generation: u64,
}

impl Default for IoThreadInfo {
    fn default() -> Self {
        Self {
            configured_threads: 1,
            active_threads: 1,
            active_time_us: vec![0],
            reads_processed: 0,
            writes_processed: 0,
            polls_processed: 0,
            reads_pending: 0,
            writes_pending: 0,
            polls_pending: 0,
            main_thread_utilization_pct: 0,
            queue_depth_average: 0,
            counter_reset_generation: 0,
        }
    }
}

impl IoThreadInfo {
    pub(crate) fn reset_counters(&mut self) {
        self.reads_processed = 0;
        self.writes_processed = 0;
        self.polls_processed = 0;
        self.reads_pending = 0;
        self.writes_pending = 0;
        self.polls_pending = 0;
        self.counter_reset_generation = self.counter_reset_generation.saturating_add(1);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorEvent {
    pub(crate) seconds: u64,
    pub(crate) microseconds: u32,
    pub(crate) sequence: u64,
    pub(crate) db: u32,
    pub(crate) source: String,
    pub(crate) argv: Vec<Bytes>,
    pub(crate) recipients: Vec<u64>,
}

impl MonitorEvent {
    pub(crate) fn capture(
        db: u32,
        source: impl Into<String>,
        argv: Vec<Bytes>,
        recipients: Vec<u64>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            seconds: now.as_secs(),
            microseconds: now.subsec_micros(),
            sequence: NEXT_MONITOR_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            db,
            source: source.into(),
            argv,
            recipients,
        }
    }

    #[cfg(test)]
    pub(crate) fn at(
        seconds: u64,
        microseconds: u32,
        sequence: u64,
        db: u32,
        source: impl Into<String>,
        argv: Vec<Bytes>,
    ) -> Self {
        Self {
            seconds,
            microseconds,
            sequence,
            db,
            source: source.into(),
            argv,
            recipients: Vec::new(),
        }
    }
}

const LATENCY_HISTORY_LEN: usize = 160;

struct LatencyTimeSeries {
    samples: [(i64, i64); LATENCY_HISTORY_LEN],
    next_index: usize,
    len: usize,
    max_ms: i64,
    sum_ms: i64,
    sample_count: i64,
}

impl Default for LatencyTimeSeries {
    fn default() -> Self {
        Self {
            samples: [(0, 0); LATENCY_HISTORY_LEN],
            next_index: 0,
            len: 0,
            max_ms: 0,
            sum_ms: 0,
            sample_count: 0,
        }
    }
}

impl LatencyTimeSeries {
    fn record_sample(&mut self, timestamp_sec: i64, latency_ms: i64) {
        self.max_ms = self.max_ms.max(latency_ms);
        self.sum_ms += latency_ms;
        self.sample_count += 1;

        let last_index = (self.next_index + LATENCY_HISTORY_LEN - 1) % LATENCY_HISTORY_LEN;
        // Coalesce the latest second without dropping lifetime totals.
        if self.len > 0 && self.samples[last_index].0 == timestamp_sec {
            self.samples[last_index].1 = self.samples[last_index].1.max(latency_ms);
            return;
        }

        self.samples[self.next_index] = (timestamp_sec, latency_ms);
        self.next_index = (self.next_index + 1) % LATENCY_HISTORY_LEN;
        self.len = (self.len + 1).min(LATENCY_HISTORY_LEN);
    }

    fn latest(&self) -> Option<(i64, i64)> {
        if self.len == 0 {
            None
        } else {
            Some(self.samples[(self.next_index + LATENCY_HISTORY_LEN - 1) % LATENCY_HISTORY_LEN])
        }
    }

    fn iter(&self) -> impl Iterator<Item = &(i64, i64)> {
        let start_index = (self.next_index + LATENCY_HISTORY_LEN - self.len) % LATENCY_HISTORY_LEN;
        (0..self.len).map(move |offset| &self.samples[(start_index + offset) % LATENCY_HISTORY_LEN])
    }
}

pub struct LatencyState {
    events: HashMap<String, LatencyTimeSeries>,
    threshold_ms: i64,
}

impl LatencyState {
    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
            threshold_ms: 0,
        }
    }

    pub fn add_sample(&mut self, event: &str, latency_ms: i64) {
        if self.threshold_ms <= 0 || latency_ms < self.threshold_ms {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.add_sample_at(event, latency_ms, now);
    }

    pub(super) fn add_sample_at(&mut self, event: &str, latency_ms: i64, timestamp_sec: i64) {
        if self.threshold_ms <= 0 || latency_ms < self.threshold_ms {
            return;
        }
        self.events
            .entry(event.to_string())
            .or_default()
            .record_sample(timestamp_sec, latency_ms);
    }

    pub fn reset(&mut self, event: Option<&str>) -> usize {
        match event {
            Some(name) => {
                if self.events.remove(name).is_some() {
                    1
                } else {
                    0
                }
            }
            None => {
                let count = self.events.len();
                self.events.clear();
                count
            }
        }
    }

    pub fn set_threshold(&mut self, threshold: i64) {
        self.threshold_ms = threshold;
    }

    pub fn latest(&self) -> Vec<(String, i64, i64, i64, i64, i64)> {
        let mut result = Vec::new();
        for (name, series) in &self.events {
            if let Some(last) = series.latest() {
                result.push((
                    name.clone(),
                    last.0,
                    last.1,
                    series.max_ms,
                    series.sum_ms,
                    series.sample_count,
                ));
            }
        }
        result
    }

    pub fn history(&self, event: &str) -> Vec<(i64, i64)> {
        match self.events.get(event) {
            Some(series) => series.iter().copied().collect(),
            None => Vec::new(),
        }
    }

    pub fn graph(&self, event: &str) -> String {
        match self.events.get(event) {
            Some(series) if series.len > 0 => {
                let max = series.iter().map(|s| s.1).max().unwrap_or(0);
                let min = series.iter().map(|s| s.1).min().unwrap_or(0);
                let width = 40i64;
                let mut graph = String::new();
                graph.push_str(&format!(
                    "{} - high {} ms, low {} ms (all time high {} ms)\n",
                    event, max, min, series.max_ms
                ));
                for _ in 0..width {
                    graph.push('-');
                }
                graph.push('\n');
                let range = if max > min { max - min } else { 1 };
                let n = series.len.min(width as usize);
                for &(_, v) in series.iter().skip(series.len - n) {
                    let col = ((v - min) * (width - 1) / range).max(0).min(width - 1);
                    for j in 0..width {
                        if j == col {
                            graph.push('█');
                        } else {
                            graph.push(' ');
                        }
                    }
                    graph.push('\n');
                }
                graph
            }
            _ => "No latency data for this event".to_string(),
        }
    }

    pub fn doctor(&self) -> String {
        "I'm sorry, Dave, I'm afraid I can't do that.\n\nNo latency data available.".to_string()
    }
}

#[cfg(test)]
mod latency_tests {
    use super::LatencyState;

    #[test]
    fn latency_storage_capacity_stays_fixed_after_warmup() {
        let mut lat = LatencyState::new();
        lat.set_threshold(1);
        for timestamp_sec in 1..=160 {
            lat.add_sample_at("command", 1, timestamp_sec);
        }

        let series = lat.events.get("command").unwrap();
        let storage_capacity = series.samples.len();
        let storage_bytes = std::mem::size_of_val(&series.samples);
        let storage_ptr = series.samples.as_ptr();
        let event_capacity = lat.events.capacity();
        for timestamp_sec in 161..=160 + 65_536 {
            lat.add_sample_at("command", 1, timestamp_sec);
        }

        let series = lat.events.get("command").unwrap();
        assert_eq!(series.samples.len(), storage_capacity);
        assert_eq!(std::mem::size_of_val(&series.samples), storage_bytes);
        assert_eq!(series.samples.as_ptr(), storage_ptr);
        assert_eq!(lat.events.capacity(), event_capacity);
        assert_eq!(storage_capacity, 160);
        assert_eq!(storage_bytes, 160 * std::mem::size_of::<(i64, i64)>());
        assert_eq!(lat.history("command").len(), 160);
        assert_eq!(
            lat.latest(),
            vec![(
                "command".to_string(),
                160 + 65_536,
                1,
                1,
                160 + 65_536,
                160 + 65_536
            )]
        );
    }
}

#[derive(Default)]
pub struct TrackingState {
    pub keys: HashMap<Bytes, HashSet<usize>>,
    pub clients: HashSet<usize>,
    pub prefix_counts: HashMap<usize, usize>,
    pub max_keys: usize,
}

impl TrackingState {
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
            clients: HashSet::new(),
            prefix_counts: HashMap::new(),
            max_keys: 1_000_000,
        }
    }

    pub fn remember_key(&mut self, client_id: usize, key: &Bytes) {
        self.keys.entry(key.clone()).or_default().insert(client_id);
    }

    pub fn take_clients_for_key(&mut self, key: &Bytes) -> Vec<usize> {
        self.keys
            .remove(key)
            .map(|clients| clients.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn clear_client(&mut self, client_id: usize) {
        self.clients.remove(&client_id);
        self.prefix_counts.remove(&client_id);
        self.keys.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
    }

    pub fn sweep_disconnected_clients(&mut self) {
        self.keys.retain(|_, clients| {
            clients.retain(|client_id| self.clients.contains(client_id));
            !clients.is_empty()
        });
    }

    pub fn total_keys(&self) -> usize {
        self.keys.len()
    }

    pub fn total_items(&self) -> usize {
        self.keys.values().map(|clients| clients.len()).sum()
    }

    pub fn total_prefixes(&self) -> usize {
        self.prefix_counts.values().sum()
    }

    pub fn invalidate_all_keys(&mut self) -> Vec<usize> {
        let mut clients = HashSet::new();
        for owners in self.keys.values() {
            clients.extend(owners.iter().copied());
        }
        self.keys.clear();
        clients.into_iter().collect()
    }

    pub fn evict_over_limit(&mut self) -> Vec<(Bytes, Vec<usize>)> {
        let started = std::time::Instant::now();
        let mut evicted = Vec::new();
        while self.keys.len() > self.max_keys {
            let Some(key) = self.keys.keys().next().cloned() else {
                break;
            };
            if let Some(clients) = self.keys.remove(&key) {
                evicted.push((key, clients.into_iter().collect()));
            }
            if started.elapsed() >= std::time::Duration::from_micros(500) {
                break;
            }
        }
        evicted
    }
}

pub struct ScriptExecutionState {
    pub script_cache: ScriptCache,
    pub function_registry: FunctionRegistry,
    pub running_script: Option<ScriptControlHandle>,
    pub(crate) scripts_eviction_pending: bool,
    pub debug_session: Option<crate::engine::command::SessionId>,
    pub script_busy: bool,
    pub script_busy_is_function: bool,
    pub script_busy_write_dirty: bool,
    pub timedout_script_clients: Vec<usize>,
    pub pending_script_kill: bool,
    /// DEBUG-only override for ordinary CMD_NOSCRIPT checks in scripts.
    script_disable_deny_scripts: bool,
    pub command_registry: CommandRegistry,
    pub monitor_client_ids: HashSet<u64>,
    pub pending_internal_monitor: Vec<MonitorEvent>,
}

impl ScriptExecutionState {
    pub fn new() -> Self {
        Self {
            script_cache: ScriptCache::new(),
            function_registry: FunctionRegistry::new(&LUA_ENGINE),
            running_script: None,
            scripts_eviction_pending: false,
            debug_session: None,
            script_busy: false,
            script_busy_is_function: false,
            script_busy_write_dirty: false,
            timedout_script_clients: Vec::new(),
            pending_script_kill: false,
            script_disable_deny_scripts: false,
            command_registry: CommandRegistry::new(),
            monitor_client_ids: HashSet::new(),
            pending_internal_monitor: Vec::new(),
        }
    }
    pub fn is_busy(&self) -> bool {
        self.script_busy
    }
    pub fn is_busy_function(&self) -> bool {
        self.script_busy_is_function
    }
    pub fn is_busy_write_dirty(&self) -> bool {
        self.script_busy_write_dirty
    }
    pub fn set_busy(&mut self, is_function: bool, write_dirty: bool) {
        self.script_busy = true;
        self.script_busy_is_function = is_function;
        self.script_busy_write_dirty = write_dirty;
    }
    pub fn clear_busy(&mut self) {
        self.script_busy = false;
        self.script_busy_is_function = false;
        self.script_busy_write_dirty = false;
    }
    pub fn disable_deny_scripts(&self) -> bool {
        self.script_disable_deny_scripts
    }
    pub fn set_disable_deny_scripts(&mut self, disabled: bool) {
        self.script_disable_deny_scripts = disabled;
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PauseAction {
    None,
    Write,
    All,
}

#[derive(Debug, Clone)]
pub struct ClientPauseState {
    pub action: PauseAction,
    pub end_time_ms: i64,
    pub reason: &'static str,
}

impl ClientPauseState {
    pub fn is_paused(&self, now_ms: i64) -> PauseAction {
        if self.end_time_ms == 0 {
            PauseAction::None
        } else if self.end_time_ms > 0 && now_ms < self.end_time_ms {
            self.action
        } else {
            PauseAction::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IoThreadInfo, TrackingState};
    use bytes::Bytes;

    #[test]
    fn tracking_limit_evicts_each_key_with_all_registered_owners() {
        let mut tracking = TrackingState::new();
        tracking.max_keys = 1;
        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        tracking.remember_key(7, &first);
        tracking.remember_key(8, &first);
        tracking.remember_key(7, &second);
        tracking.remember_key(8, &second);

        let evicted = tracking.evict_over_limit();

        assert_eq!(tracking.total_keys(), 1);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].1.len(), 2);
        assert!(evicted[0].1.contains(&7));
        assert!(evicted[0].1.contains(&8));
    }

    #[test]
    fn io_thread_counter_reset_includes_poll_counters_and_advances_generation() {
        let mut info = IoThreadInfo {
            reads_processed: 1,
            writes_processed: 2,
            polls_processed: 3,
            reads_pending: 4,
            writes_pending: 5,
            polls_pending: 6,
            ..IoThreadInfo::default()
        };

        info.reset_counters();

        assert_eq!(info.reads_processed, 0);
        assert_eq!(info.writes_processed, 0);
        assert_eq!(info.polls_processed, 0);
        assert_eq!(info.reads_pending, 0);
        assert_eq!(info.writes_pending, 0);
        assert_eq!(info.polls_pending, 0);
        assert_eq!(info.counter_reset_generation, 1);
    }
}
