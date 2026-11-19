//! Monotonic microsecond timestamp shared across foundation layers.
//!
//! `MonotonicInstant` lives in the `memory` layer so that in-memory metrics
//! (e.g. command latency bookkeeping) can use it without creating a
//! `memory -> io` dependency. The hot-path clock source is injected at
//! startup by `io::sys::time` via [`install_monotonic_clock`]; until then a
//! portable fallback based on `clock_gettime(CLOCK_MONOTONIC)` (or an atomic
//! counter on `wasm32`) is used.

use std::sync::atomic::{AtomicPtr, Ordering};
use std::time::Duration;

/// A monotonic microsecond timestamp.
///
/// Mirrors the public API of Valkey C's `monotime`; the backing clock is
/// selected once and stored in a function pointer so the hot path only pays
/// for an indirect call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicInstant(u64);

/// Installed clock function, stored as a raw pointer so the hot path avoids
/// the `OnceLock` state-machine overhead. A null pointer means "not
/// installed, use the portable fallback". Set once at process startup via
/// [`install_monotonic_clock`] before any [`MonotonicInstant::now`] call.
static MONOTONIC_US_FN: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Install a hardware-accelerated monotonic clock implementation.
///
/// Must be invoked before the first [`MonotonicInstant::now`] to take
/// effect. Returns `false` if a clock was already installed. Intended to be
/// called once at process startup by the `io` layer.
pub fn install_monotonic_clock(f: fn() -> u64) -> bool {
    let ptr = f as *mut ();
    MONOTONIC_US_FN
        .compare_exchange(
            std::ptr::null_mut(),
            ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
}

/// Portable fallback used when no hardware counter has been installed.
///
/// Calls `clock_gettime(CLOCK_MONOTONIC)` directly via libc, matching C
/// Valkey's `getMonotonicUs_posix`. This avoids the `SystemTime` abstraction
/// (epoch subtraction, `as_micros` u128 conversion) that adds measurable
/// overhead on the `latency_tracking` hot path when TSC/CNTVCT is unavailable.
#[cfg(not(target_arch = "wasm32"))]
fn fallback_monotonic_us() -> u64 {
    // SAFETY: `timespec` is plain-old-data with no Drop invariants, so a
    // zero-initialized value is a valid bit pattern.
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: `ts` is a valid stack-allocated timespec pointer.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1_000
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
        let ptr = MONOTONIC_US_FN.load(Ordering::Acquire);
        let f = if ptr.is_null() {
            fallback_monotonic_us
        } else {
            // SAFETY: `ptr` was stored from a valid `fn() -> u64` by
            // `install_monotonic_clock`, which is called once at startup
            // before any `now` invocation. Function pointers and `*mut ()`
            // have the same representation on all supported 64-bit targets.
            unsafe { std::mem::transmute::<*mut (), fn() -> u64>(ptr) }
        };
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
