use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeClock {
    monotonic_us: u64,
    unix_ms: i64,
}

impl RuntimeClock {
    pub const fn new(monotonic_ms: u64, unix_ms: i64) -> Self {
        Self {
            monotonic_us: monotonic_ms.saturating_mul(1_000),
            unix_ms,
        }
    }

    pub const fn from_micros(monotonic_us: u64, unix_ms: i64) -> Self {
        Self {
            monotonic_us,
            unix_ms,
        }
    }

    pub const fn monotonic(self) -> RuntimeInstant {
        RuntimeInstant(self.monotonic_us)
    }

    pub const fn unix_ms(self) -> i64 {
        self.unix_ms
    }

    pub fn deadline_after(self, delay: Duration) -> RuntimeInstant {
        let delay_us = u64::try_from(delay.as_micros()).unwrap_or(u64::MAX);
        RuntimeInstant(self.monotonic_us.saturating_add(delay_us))
    }

    pub fn deadline_for_unix_ms(self, unix_deadline_ms: i64) -> RuntimeInstant {
        let remaining_ms = unix_deadline_ms.saturating_sub(self.unix_ms).max(0) as u64;
        RuntimeInstant(
            self.monotonic_us
                .saturating_add(remaining_ms.saturating_mul(1_000)),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RuntimeInstant(u64);

impl RuntimeInstant {
    pub const fn from_millis(milliseconds: u64) -> Self {
        Self(milliseconds.saturating_mul(1_000))
    }

    pub const fn from_micros(microseconds: u64) -> Self {
        Self(microseconds)
    }

    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000
    }

    pub const fn as_micros(self) -> u64 {
        self.0
    }

    pub fn as_millis_f64(self) -> f64 {
        self.0 as f64 / 1_000.0
    }

    pub fn duration_until(self, now: RuntimeInstant) -> Duration {
        Duration::from_micros(self.0.saturating_sub(now.0))
    }
}
