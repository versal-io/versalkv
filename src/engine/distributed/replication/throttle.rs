//! Replication pressure policy and monotonic token accounting.

use std::collections::HashMap;
use std::time::{Duration, Instant};

const UNLIMITED_RATE: f64 = 10_000_000.0;
const EPSILON: f64 = 0.0001;

struct BufferTrend {
    updated: Instant,
    total: u128,
    samples: u64,
    previous: Option<usize>,
    change_per_sec: f64,
}

impl BufferTrend {
    fn new(now: Instant) -> Self {
        Self {
            updated: now,
            total: 0,
            samples: 0,
            previous: None,
            change_per_sec: 0.0,
        }
    }

    fn record(&mut self, now: Instant, bytes: usize) {
        self.total += bytes as u128;
        self.samples += 1;
        if now.duration_since(self.updated) < Duration::from_millis(200) {
            return;
        }
        let average = (self.total / u128::from(self.samples)) as usize;
        self.change_per_sec = self
            .previous
            .map_or(0.0, |previous| (average as f64 - previous as f64) / 0.2);
        self.previous = Some(average);
        self.total = 0;
        self.samples = 0;
        self.updated = now;
    }
}

pub struct ReplicationThrottle {
    active: bool,
    rate: f64,
    tokens: f64,
    refilled: Instant,
    adjusted: Option<Instant>,
    trends: HashMap<usize, BufferTrend>,
    input_updated: Instant,
    input_pending: u64,
    input_window: f64,
    input_is_new: bool,
    below_guardrail_since: Option<Instant>,
    pub activation_events: u64,
    pub more_events: u64,
    pub less_events: u64,
    pub total_commands: u64,
}

impl Default for ReplicationThrottle {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            active: false,
            rate: UNLIMITED_RATE,
            tokens: 0.0,
            refilled: now,
            adjusted: None,
            trends: HashMap::new(),
            input_updated: now,
            input_pending: 0,
            input_window: 0.0,
            input_is_new: true,
            below_guardrail_since: None,
            activation_events: 0,
            more_events: 0,
            less_events: 0,
            total_commands: 0,
        }
    }
}

impl ReplicationThrottle {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn rate(&self) -> f64 {
        if self.active {
            self.rate
        } else {
            -1.0
        }
    }

    pub fn guardrail_secs(&self) -> u64 {
        self.below_guardrail_since
            .map_or(0, |since| since.elapsed().as_secs())
    }

    pub fn target(soft: usize, hard: usize) -> usize {
        if soft > 0 {
            soft / 2
        } else {
            hard / 2
        }
    }

    pub fn adjust(
        &mut self,
        is_enabled_primary: bool,
        target: usize,
        has_replicas: bool,
        online_buffers: &[(usize, usize)],
    ) {
        self.adjust_at(
            Instant::now(),
            is_enabled_primary,
            target,
            has_replicas,
            online_buffers,
        );
    }

    fn adjust_at(
        &mut self,
        now: Instant,
        is_enabled_primary: bool,
        target: usize,
        has_replicas: bool,
        online_buffers: &[(usize, usize)],
    ) {
        if !is_enabled_primary || target == 0 || !has_replicas {
            self.active = false;
            self.below_guardrail_since = None;
            self.trends.clear();
            return;
        }
        if self
            .adjusted
            .is_some_and(|last| now.duration_since(last) < Duration::from_millis(100))
        {
            return;
        }
        self.adjusted = Some(now);
        self.trends
            .retain(|id, _| online_buffers.iter().any(|(online, _)| id == online));
        let mut largest = None;
        for &(id, bytes) in online_buffers {
            let trend = self
                .trends
                .entry(id)
                .or_insert_with(|| BufferTrend::new(now));
            trend.record(now, bytes);
            let bytes = if bytes <= 16 * 1024 { 0 } else { bytes };
            if largest.is_none_or(|(largest_bytes, _)| bytes > largest_bytes) {
                largest = Some((bytes, trend.change_per_sec));
            }
        }
        let should_reduce = largest.is_some_and(|(bytes, trend)| {
            bytes >= target / 2 && bytes as f64 + trend * 30.0 > target as f64
        });
        if !self.active {
            if should_reduce {
                if self.activation_events == 0 {
                    self.input_updated = now;
                }
                self.active = true;
                self.rate = UNLIMITED_RATE;
                self.tokens = self.capacity();
                self.refilled = now;
                self.activation_events += 1;
            }
            return;
        }
        self.refill(now);
        if should_reduce {
            self.update_input(now, 0);
            let incoming = self.input_window / 5.0;
            self.rate *= 0.95;
            if incoming > EPSILON {
                self.rate = self.rate.min(incoming);
            }
            self.more_events += 1;
        } else {
            self.rate = if self.rate < EPSILON {
                100.0
            } else {
                self.rate + (self.rate * 0.05).max(1.0)
            };
            self.less_events += 1;
        }
        if self.rate < EPSILON {
            self.rate = 0.0;
        }
        if self.rate >= UNLIMITED_RATE {
            self.rate = UNLIMITED_RATE;
            self.active = false;
        }
        self.tokens = self.tokens.min(self.capacity());
        if self.active && self.rate <= 0.1 {
            self.below_guardrail_since.get_or_insert(now);
        } else {
            self.below_guardrail_since = None;
        }
    }

    fn capacity(&self) -> f64 {
        if self.rate < EPSILON {
            0.0
        } else {
            2.0 + self.rate * 0.1
        }
    }

    fn refill(&mut self, now: Instant) {
        self.tokens = (self.tokens + now.duration_since(self.refilled).as_secs_f64() * self.rate)
            .min(self.capacity());
        self.refilled = now;
    }

    fn update_input(&mut self, now: Instant, commands: u64) {
        self.input_pending += commands;
        let elapsed = now.duration_since(self.input_updated).as_secs_f64();
        if elapsed < 0.25 {
            return;
        }
        let count = self.input_pending as f64;
        self.input_window = if self.input_is_new || elapsed >= 5.0 {
            count * 5.0 / elapsed
        } else {
            self.input_window * (5.0 - elapsed) / 5.0 + count
        };
        self.input_is_new = false;
        self.input_pending = 0;
        self.input_updated = now;
    }

    /// Charge a matching command once, preserving FIFO admission.
    pub fn should_postpone(&mut self, has_queued_clients: bool) -> bool {
        if !self.active {
            return false;
        }
        let now = Instant::now();
        self.update_input(now, 1);
        if !has_queued_clients && self.try_release() {
            return false;
        }
        self.total_commands += 1;
        true
    }

    pub fn try_release(&mut self) -> bool {
        if !self.active {
            return true;
        }
        self.refill(Instant::now());
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    pub fn release_delay(&mut self) -> Duration {
        if !self.active {
            return Duration::ZERO;
        }
        self.refill(Instant::now());
        if self.rate < EPSILON {
            return Duration::from_millis(100);
        }
        Duration::from_secs_f64(((1.0 - self.tokens).max(0.0) / self.rate).min(0.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trend_uses_averages_of_adjacent_two_hundred_ms_intervals() {
        let now = Instant::now();
        let mut trend = BufferTrend::new(now);
        trend.record(now + Duration::from_millis(100), 20_000);
        trend.record(now + Duration::from_millis(200), 40_000);
        assert_eq!(trend.change_per_sec, 0.0);
        trend.record(now + Duration::from_millis(300), 40_000);
        trend.record(now + Duration::from_millis(400), 60_000);
        assert_eq!(trend.change_per_sec, 100_000.0);
    }

    #[test]
    fn first_installation_starts_input_measurement_after_an_idle_server() {
        let mut throttle = ReplicationThrottle::default();
        let now = throttle.input_updated + Duration::from_secs(3600);
        throttle.adjust_at(now, true, 32_768, true, &[(1, 65_536)]);
        throttle.update_input(now + Duration::from_millis(250), 250);
        assert_eq!(throttle.input_window / 5.0, 1_000.0);
    }

    #[test]
    fn pressure_activation_caps_reduction_to_measured_input() {
        let mut throttle = ReplicationThrottle::default();
        let now = throttle.input_updated;
        throttle.adjust_at(now, true, 32_768, true, &[(1, 65_536)]);
        assert!(throttle.is_active());
        assert_eq!(throttle.activation_events, 1);
        throttle.update_input(now + Duration::from_millis(250), 250);
        throttle.adjust_at(
            now + Duration::from_millis(300),
            true,
            32_768,
            true,
            &[(1, 65_536)],
        );
        assert_eq!(throttle.rate(), 1_000.0);
        assert_eq!(throttle.more_events, 1);
    }

    #[test]
    fn queued_commands_cannot_be_bypassed_even_with_available_tokens() {
        let mut throttle = ReplicationThrottle::default();
        throttle.adjust_at(Instant::now(), true, 32_768, true, &[(1, 65_536)]);
        assert!(throttle.should_postpone(true));
        assert_eq!(throttle.total_commands, 1);
        assert!(throttle.try_release());
        assert_eq!(throttle.total_commands, 1);
    }

    #[test]
    fn token_refill_is_bounded_and_a_halted_rate_recovers() {
        let mut throttle = ReplicationThrottle::default();
        let now = Instant::now();
        throttle.active = true;
        throttle.rate = 10.0;
        throttle.tokens = 0.0;
        throttle.refilled = now;
        throttle.refill(now + Duration::from_secs(10));
        assert_eq!(throttle.tokens, 3.0);
        throttle.rate = 0.0;
        throttle.refill(now + Duration::from_secs(11));
        assert_eq!(throttle.tokens, 0.0);
        throttle.adjust_at(now + Duration::from_secs(12), true, 32_768, true, &[]);
        assert_eq!(throttle.rate(), 100.0);
    }

    #[test]
    fn teardown_is_immediate_and_preserves_cumulative_counters() {
        for (enabled, target, has_replicas) in [
            (false, 32_768, true),
            (true, 0, true),
            (true, 32_768, false),
        ] {
            let mut throttle = ReplicationThrottle::default();
            let now = Instant::now();
            throttle.adjust_at(now, true, 32_768, true, &[(1, 65_536)]);
            assert!(throttle.should_postpone(true));
            throttle.adjust_at(now, enabled, target, has_replicas, &[(1, 65_536)]);
            assert_eq!(throttle.rate(), -1.0);
            assert!(throttle.try_release());
            assert_eq!(throttle.activation_events, 1);
            assert_eq!(throttle.total_commands, 1);
        }
    }
}
