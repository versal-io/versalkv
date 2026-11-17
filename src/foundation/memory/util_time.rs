//! Monotonic microsecond timestamp shared across foundation layers.
//!
//! `MonotonicInstant` lives in the `memory` layer so that in-memory metrics
//! (e.g. command latency bookkeeping) can use it without creating a
//! `memory -> io` dependency. The hot-path clock source is injected at
//! startup by `io::sys::time` via [`install_monotonic_clock`]; until then a
//! portable fallback based on `std::time::Instant` (or an atomic counter on
//! `wasm32`) is used.

use std::sync::OnceLock;
use std::time::Duration;

/// A monotonic microsecond timestamp.
///
/// Mirrors the public API of Valkey C's `monotime`; the backing clock is
/// selected once and stored in a function pointer so the hot path only pays
/// for an indirect call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicInstant(u64);

static MONOTONIC_US_FN: OnceLock<fn() -> u64> = OnceLock::new();

/// Install a hardware-accelerated monotonic clock implementation.
///
/// Must be invoked before the first [`MonotonicInstant::now`] to take
/// effect. Returns `false` if a clock was already installed or the fallback
/// has already been initialised. Intended to be called once at process
/// startup by the `io` layer.
pub fn install_monotonic_clock(f: fn() -> u64) -> bool {
    MONOTONIC_US_FN.set(f).is_ok()
}

/// Portable fallback used when no hardware counter has been installed.
///
/// Uses `SystemTime` (UNIX epoch microseconds) so the returned value is a
/// large absolute timestamp; this keeps `saturating_sub` semantics working in
/// tests and early-startup code paths that predate the hardware clock install.
/// `SystemTime` is not strictly monotonic (NTP can step it), but the fallback
/// only runs before `install_monotonic_clock` takes effect.
#[cfg(not(target_arch = "wasm32"))]
fn fallback_monotonic_us() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// `wasm32` has no working monotonic clock (`Instant::now` panics); use an
/// atomic counter that advances on every read so ordering is preserved.
#[cfg(target_arch = "wasm32")]
fn fallback_monotonic_us() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

impl MonotonicInstant {
    pub fn now() -> Self {
        let f = MONOTONIC_US_FN.get_or_init(|| fallback_monotonic_us);
        Self(f())
    }

    pub const fn from_millis(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    pub const fn from_micros(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000
    }

    pub const fn as_micros(self) -> u64 {
        self.0
    }

    pub fn saturating_add(self, delay: Duration) -> Self {
        Self(
            self.0
                .saturating_add(delay.as_micros().min(u64::MAX as u128) as u64),
        )
    }

    /// Returns the `Duration` elapsed since an earlier instant.
    ///
    /// Saturates to zero when `self` predates `earlier` (which can happen for
    /// synthetic test timestamps or clock readjustments).
    pub fn duration_since(self, earlier: Self) -> Duration {
        Duration::from_micros(self.0.saturating_sub(earlier.0))
    }

    /// Returns the `Duration` elapsed since this instant using the currently
    /// installed monotonic clock.
    pub fn elapsed(self) -> Duration {
        Self::now().duration_since(self)
    }

    /// Returns an instant `delay` before `self`, saturating at zero.
    pub fn saturating_sub(self, delay: Duration) -> Self {
        Self(
            self.0
                .saturating_sub(delay.as_micros().min(u64::MAX as u128) as u64),
        )
    }

    pub fn duration_until(self, now: Self) -> Duration {
        Duration::from_micros(self.0.saturating_sub(now.0))
    }
}
