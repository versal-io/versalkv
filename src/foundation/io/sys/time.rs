use std::cell::Cell;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub use crate::foundation::memory::util_time::{install_monotonic_clock, MonotonicInstant};

/// Select and initialise the fastest available hardware monotonic clock.
///
/// Returns `Some(fn)` when a TSC (x86_64 Linux) or CNTVCT (aarch64) counter
/// is available and calibrated, or `None` when the portable fallback in
/// `memory::util_time` should be used.
fn init_monotonic_clock() -> Option<fn() -> u64> {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    if let Some(f) = monotonic_x86_tsc::init() {
        return Some(f);
    }
    #[cfg(target_arch = "aarch64")]
    if let Some(f) = monotonic_aarch64_cntvct::init() {
        return Some(f);
    }
    None
}

/// Install the hardware-accelerated monotonic clock into the shared
/// `memory::util_time` function pointer. Call once at process startup,
/// before any `MonotonicInstant::now`, so latency bookkeeping benefits from
/// TSC/CNTVCT instead of the `Instant` fallback. A no-op when no hardware
/// counter is available or a clock is already installed.
pub fn init_hardware_clock() {
    if let Some(f) = init_monotonic_clock() {
        let _ = install_monotonic_clock(f);
    }
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
mod monotonic_x86_tsc {
    use super::*;

    /// Fixed-point shift for `ticks_per_us` reciprocal.
    const FPMULT_SHIFT: u32 = 24;
    /// Calibration iterations; the minimum multiplier is kept for accuracy.
    const CALIBRATION_ITERATIONS: usize = 3;
    /// Sleep duration between TSC samples, in microseconds.
    const CALIBRATION_SLEEP_US: u64 = 10_000;

    /// `(1 << FPMULT_SHIFT) / ticks_per_us`, stored so that
    /// `(rdtsc() * MULT) >> FPMULT_SHIFT` yields microseconds.
    static TICKS_PER_US_MULT: AtomicU64 = AtomicU64::new(0);

    #[inline]
    fn rdtsc() -> u64 {
        // SAFETY: `_rdtsc` reads the time-stamp counter, a baseline x86_64
        // instruction with no side effects.
        unsafe { core::arch::x86_64::_rdtsc() }
    }

    pub fn monotonic_us() -> u64 {
        let mult = TICKS_PER_US_MULT.load(Ordering::Relaxed);
        ((rdtsc() as u128 * mult as u128) >> FPMULT_SHIFT) as u64
    }

    /// Returns `true` if the kernel clocksource is `tsc`.
    fn kernel_clocksource_is_tsc() -> bool {
        let Ok(name) = std::fs::read_to_string(
            "/sys/devices/system/clocksource/clocksource0/current_clocksource",
        ) else {
            return false;
        };
        name.trim() == "tsc"
    }

    /// Returns `true` if `/proc/cpuinfo` advertises the `constant_tsc` flag.
    fn cpu_has_constant_tsc() -> bool {
        let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") else {
            return false;
        };
        cpuinfo
            .lines()
            .filter(|line| line.starts_with("flags"))
            .any(|line| line.split_whitespace().any(|tok| tok == "constant_tsc"))
    }

    /// Calibrate the TSC-to-microsecond multiplier against `CLOCK_MONOTONIC`.
    fn calibrate() -> Option<u64> {
        let mut best_mult: Option<u64> = None;
        for _ in 0..CALIBRATION_ITERATIONS {
            // SAFETY: `timespec` is a plain-old-data struct with no Drop
            // invariants, so a zero-initialized value is a valid bit pattern.
            let mut start_ts: libc::timespec = unsafe { std::mem::zeroed() };
            let mut end_ts: libc::timespec = unsafe { std::mem::zeroed() };
            // SAFETY: both timespec pointers are valid stack allocations.
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut start_ts) };
            let tsc_start = rdtsc();
            // SAFETY: usleep only suspends the current thread.
            unsafe { libc::usleep(CALIBRATION_SLEEP_US as libc::useconds_t) };
            let tsc_end = rdtsc();
            // SAFETY: both timespec pointers are valid stack allocations.
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut end_ts) };

            let elapsed_us = (end_ts.tv_sec - start_ts.tv_sec) as u64 * 1_000_000
                + (end_ts.tv_nsec - start_ts.tv_nsec) as u64 / 1_000;
            if tsc_end <= tsc_start || elapsed_us == 0 {
                continue;
            }
            let ticks_per_us = (tsc_end - tsc_start) as f64 / elapsed_us as f64;
            let sample_mult = ((1u128 << FPMULT_SHIFT) as f64 / ticks_per_us) as u64;
            if sample_mult == 0 {
                continue;
            }
            best_mult = Some(match best_mult {
                Some(cur) => cur.min(sample_mult),
                None => sample_mult,
            });
        }
        best_mult
    }

    pub fn init() -> Option<fn() -> u64> {
        if !kernel_clocksource_is_tsc() {
            return None;
        }
        if !cpu_has_constant_tsc() {
            return None;
        }
        let mult = calibrate()?;
        TICKS_PER_US_MULT.store(mult, Ordering::Relaxed);
        Some(monotonic_us)
    }
}

#[cfg(target_arch = "aarch64")]
mod monotonic_aarch64_cntvct {
    use super::*;

    /// Counter ticks per microsecond, derived from `cntfrq_el0`.
    static TICKS_PER_US: AtomicU64 = AtomicU64::new(0);

    #[inline]
    fn cntvct() -> u64 {
        let val: u64;
        // SAFETY: `cntvct_el0` is a user-space readable system register on
        // aarch64; the `mrs` instruction has no side effects.
        unsafe {
            core::arch::asm!("mrs {}, cntvct_el0", out(reg) val, options(nomem, nostack, preserves_flags));
        }
        val
    }

    #[inline]
    fn cntfrq() -> u64 {
        let val: u64;
        // SAFETY: `cntfrq_el0` is a user-space readable system register on
        // aarch64; the `mrs` instruction has no side effects.
        unsafe {
            core::arch::asm!("mrs {}, cntfrq_el0", out(reg) val, options(nomem, nostack, preserves_flags));
        }
        val
    }

    pub fn monotonic_us() -> u64 {
        let ticks_per_us = TICKS_PER_US.load(Ordering::Relaxed);
        cntvct() / ticks_per_us
    }

    pub fn init() -> Option<fn() -> u64> {
        let ticks_per_us = cntfrq() / 1_000_000;
        if ticks_per_us == 0 {
            return None;
        }
        TICKS_PER_US.store(ticks_per_us, Ordering::Relaxed);
        Some(monotonic_us)
    }
}

thread_local! {
    static COMMAND_TIME_MS: Cell<Option<i64>> = const { Cell::new(None) };
    static WALL_TIME_US: Cell<Option<i64>> = const { Cell::new(None) };
}

pub struct CommandTimeSnapshotGuard {
    previous: Option<i64>,
    previous_wall_time: Option<i64>,
    should_restore_wall_time: bool,
}

impl Drop for CommandTimeSnapshotGuard {
    fn drop(&mut self) {
        COMMAND_TIME_MS.with(|cell| cell.set(self.previous));
        if self.should_restore_wall_time {
            WALL_TIME_US.with(|cell| cell.set(self.previous_wall_time));
        }
    }
}

pub fn enter_command_time_snapshot(ms: i64) -> CommandTimeSnapshotGuard {
    COMMAND_TIME_MS.with(|cell| {
        let previous = cell.get();
        let previous_wall_time = WALL_TIME_US.with(Cell::get);
        if previous.is_none() {
            cell.set(Some(ms));
            WALL_TIME_US.with(|cell| cell.set(Some(real_ustime())));
        }
        CommandTimeSnapshotGuard {
            previous,
            previous_wall_time,
            should_restore_wall_time: previous.is_none(),
        }
    })
}

/// Enter a command-time snapshot, reading the clock only at the outermost entry.
/// The historical lazy entry point now caches TIME eagerly to preserve unit-entry
/// time even if the first TIME follows a delay. Expiry uses the same sample in
/// milliseconds, and nested commands keep both outer snapshots unchanged.
pub fn enter_command_time_snapshot_lazy() -> CommandTimeSnapshotGuard {
    COMMAND_TIME_MS.with(|cell| {
        let previous = cell.get();
        let previous_wall_time = WALL_TIME_US.with(Cell::get);
        if previous.is_none() {
            let wall_time_us = real_ustime();
            cell.set(Some(wall_time_us / 1_000));
            WALL_TIME_US.with(|cell| cell.set(Some(wall_time_us)));
        }
        CommandTimeSnapshotGuard {
            previous,
            previous_wall_time,
            should_restore_wall_time: previous.is_none(),
        }
    })
}

/// Install a fresh wall-clock snapshot even when a command is already active.
/// Nested blocked event-loop servicing uses this to give admitted commands the
/// current time while restoring the outer command's frozen snapshot on return.
/// TIME retains the refreshed wall-clock cache after nested servicing returns.
pub fn enter_fresh_command_time_snapshot() -> CommandTimeSnapshotGuard {
    COMMAND_TIME_MS.with(|cell| {
        let wall_time_us = real_ustime();
        let previous = cell.replace(Some(wall_time_us / 1_000));
        let previous_wall_time = WALL_TIME_US.with(|wall| wall.replace(Some(wall_time_us)));
        // Blocked servicing refreshes TIME, but expiry restores the outer snapshot.
        CommandTimeSnapshotGuard {
            previous,
            previous_wall_time,
            should_restore_wall_time: previous.is_none(),
        }
    })
}

fn command_time_ms() -> Option<i64> {
    COMMAND_TIME_MS.with(Cell::get)
}

fn real_mstime() -> i64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    d.as_secs() as i64 * 1000 + d.subsec_millis() as i64
}

pub fn mstime() -> i64 {
    command_time_ms().unwrap_or_else(real_mstime)
}

fn real_ustime() -> i64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    d.as_secs() as i64 * 1_000_000 + d.subsec_micros() as i64
}

pub fn ustime() -> i64 {
    command_time_ms()
        .map(|ms| ms.saturating_mul(1000))
        .unwrap_or_else(real_ustime)
}

/// TIME uses a separate cache that nested event servicing may advance.
pub(crate) fn cached_wall_ustime() -> i64 {
    if command_time_ms().is_none() {
        return real_ustime();
    }
    WALL_TIME_US.with(|cell| match cell.get() {
        Some(us) => us,
        None => {
            let us = real_ustime();
            cell.set(Some(us));
            us
        }
    })
}

pub fn unixtime() -> i64 {
    command_time_ms().map(|ms| ms / 1000).unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    })
}

fn rusage() -> (u64, u64) {
    // SAFETY: zeroed() is safe for a plain-old-data struct (rusage has no Drop invariants).
    // getrusage() is a POSIX syscall; usage is a valid stack-allocated rusage struct.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let utime = usage.ru_utime.tv_sec as u64 * 1_000_000 + usage.ru_utime.tv_usec as u64;
        let stime = usage.ru_stime.tv_sec as u64 * 1_000_000 + usage.ru_stime.tv_usec as u64;
        (utime, stime)
    }
}

pub fn used_cpu_user_sec_frac() -> (u64, u64) {
    let (utime, _) = rusage();
    (utime / 1_000_000, utime % 1_000_000)
}

pub fn used_cpu_sys_sec_frac() -> (u64, u64) {
    let (_, stime) = rusage();
    (stime / 1_000_000, stime % 1_000_000)
}

pub fn sleep_interruptible(usec: u64, cancelled: impl Fn() -> bool) {
    let mut remaining = usec;
    while remaining > 0 && !cancelled() {
        let chunk = remaining.min(1_000_000);
        // SAFETY: usleep only suspends the current thread and accepts this bounded value.
        let result = unsafe { libc::usleep(chunk as libc::useconds_t) };
        if result != 0 {
            break;
        }
        remaining -= chunk;
    }
}

/// C Valkey's `debugDelay`: negative values mean a one-microsecond sleep is
/// sampled once per absolute-value calls, rather than an unsigned long sleep.
/// The draw is explicit so the probability rule remains independently testable.
fn debug_delay_sleep_us(delay_us: i64, draw: usize) -> u64 {
    if delay_us >= 0 {
        return delay_us as u64;
    }
    let denominator = usize::try_from(delay_us.unsigned_abs()).unwrap_or(usize::MAX);
    u64::from(draw.is_multiple_of(denominator))
}

pub fn sleep_debug_delay_interruptible(delay_us: i64, cancelled: impl Fn() -> bool) {
    let sleep_us = if delay_us < 0 {
        let denominator = usize::try_from(delay_us.unsigned_abs()).unwrap_or(usize::MAX);
        debug_delay_sleep_us(delay_us, super::rand::random_usize(denominator))
    } else {
        debug_delay_sleep_us(delay_us, 0)
    };
    #[cfg(not(target_family = "wasm"))]
    if (1..=50).contains(&sleep_us) {
        // Debug key delays need microsecond precision. Kernel wakeup latency
        // can amplify tiny sleeps by orders of magnitude, turning a bounded
        // RDB pause into minutes. Spin only for these short, explicit delays;
        // zero-delay workloads and longer interruptible sleeps keep their path.
        let started = std::time::Instant::now();
        let delay = std::time::Duration::from_micros(sleep_us);
        while !cancelled() && started.elapsed() < delay {
            std::hint::spin_loop();
        }
        return;
    }
    sleep_interruptible(sleep_us, cancelled);
}

pub fn set_watchdog_timer(period_ms: u64) -> io::Result<()> {
    let timer = libc::itimerval {
        it_interval: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        it_value: libc::timeval {
            tv_sec: (period_ms / 1000) as libc::time_t,
            tv_usec: ((period_ms % 1000) * 1000) as libc::suseconds_t,
        },
    };
    // SAFETY: timer is fully initialized and the old value is intentionally discarded.
    if unsafe { libc::setitimer(libc::ITIMER_REAL, &timer, std::ptr::null_mut()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_time_snapshot_freezes_wall_clock_helpers() {
        let _guard = enter_command_time_snapshot(12345);
        assert_eq!(mstime(), 12345);
        assert_eq!(ustime(), 12345000);
        assert_eq!(unixtime(), 12);
    }

    #[test]
    fn nested_command_time_snapshot_keeps_outer_time() {
        let _outer = enter_command_time_snapshot(1000);
        {
            let _inner = enter_command_time_snapshot(2000);
            assert_eq!(mstime(), 1000);
        }
        assert_eq!(mstime(), 1000);
    }

    #[test]
    fn fresh_command_time_snapshot_temporarily_replaces_outer_time() {
        let _outer = enter_command_time_snapshot(1000);
        {
            let _inner = enter_fresh_command_time_snapshot();
            assert_ne!(mstime(), 1000);
        }
        assert_eq!(mstime(), 1000);
    }

    #[test]
    fn cached_wall_time_first_read_after_sleep_keeps_execution_unit_entry() {
        for is_lazy in [false, true] {
            let before_us = real_ustime();
            let _outer = if is_lazy {
                enter_command_time_snapshot_lazy()
            } else {
                enter_command_time_snapshot(1_000)
            };
            let after_us = real_ustime();
            let expiry_ms = mstime();
            std::thread::sleep(std::time::Duration::from_millis(50));

            let _nested = enter_command_time_snapshot(9_000);
            let _nested_lazy = enter_command_time_snapshot_lazy();
            let first_us = cached_wall_ustime();
            assert!(
                (before_us..=after_us).contains(&first_us),
                "first TIME must use execution unit entry before sleep: \
                 is_lazy={is_lazy}, entry={before_us}..={after_us}, first={first_us}"
            );
            assert_eq!(cached_wall_ustime(), first_us);
            assert_eq!(mstime(), expiry_ms);
            assert_eq!(ustime(), expiry_ms * 1_000);
            if is_lazy {
                assert_eq!(expiry_ms, first_us / 1_000);
            }
        }
    }

    #[test]
    fn cached_wall_time_advances_through_nested_service_and_clears_at_unit_exit() {
        for is_lazy in [false, true] {
            assert!(command_time_ms().is_none());
            assert_eq!(WALL_TIME_US.with(Cell::get), None);
            let outer = if is_lazy {
                enter_command_time_snapshot_lazy()
            } else {
                enter_command_time_snapshot(1_000)
            };
            let expiry_ms = mstime();
            WALL_TIME_US.with(|cell| cell.set(Some(12_345)));
            {
                let _nested = enter_command_time_snapshot(9_000);
                let _nested_lazy = enter_command_time_snapshot_lazy();
                assert_eq!(cached_wall_ustime(), 12_345);
                assert_eq!(mstime(), expiry_ms);
            }
            assert_eq!(cached_wall_ustime(), 12_345);

            let refreshed_us;
            {
                let _service = enter_fresh_command_time_snapshot();
                {
                    let _nested_service = enter_fresh_command_time_snapshot();
                    refreshed_us = cached_wall_ustime();
                    assert_eq!(mstime(), refreshed_us / 1_000);
                }
                assert_eq!(cached_wall_ustime(), refreshed_us);
            }
            assert_ne!(refreshed_us, 12_345);
            assert_eq!(cached_wall_ustime(), refreshed_us);
            assert_eq!(mstime(), expiry_ms);
            assert_eq!(ustime(), expiry_ms * 1_000);
            drop(outer);
            assert!(command_time_ms().is_none());
            assert_eq!(WALL_TIME_US.with(Cell::get), None);
        }
        {
            let _standalone_service = enter_fresh_command_time_snapshot();
            assert_eq!(mstime(), cached_wall_ustime() / 1_000);
        }
        assert!(command_time_ms().is_none());
        assert_eq!(WALL_TIME_US.with(Cell::get), None);
    }

    #[test]
    fn lazy_snapshot_caches_entry_time_and_freezes() {
        let _guard = enter_command_time_snapshot_lazy();
        let first = mstime();
        assert!(first > 0);
        assert_eq!(first, cached_wall_ustime() / 1_000);
        // Subsequent reads within the same snapshot return the frozen value.
        assert_eq!(mstime(), first);
        assert_eq!(ustime(), first.saturating_mul(1000));
    }

    #[test]
    fn lazy_snapshot_nested_keeps_outer_value() {
        let _outer = enter_command_time_snapshot_lazy();
        let outer_ms = mstime();
        {
            let _inner = enter_command_time_snapshot_lazy();
            // The inner lazy entry must not overwrite the outer snapshot.
            assert_eq!(mstime(), outer_ms);
        }
        assert_eq!(mstime(), outer_ms);
    }

    #[test]
    fn real_time_functions_return_positive_values() {
        let ms = mstime();
        assert!(ms > 0);
        let us = ustime();
        assert!(us > 0);
        assert!(us >= ms * 1000);
        let unix = unixtime();
        assert!(unix > 0);
        assert_eq!(unix, ms / 1000);
    }

    #[test]
    fn cpu_time_functions_return_non_negative() {
        let (user_sec, user_usec) = used_cpu_user_sec_frac();
        assert!(user_usec < 1_000_000);
        let (sys_sec, sys_usec) = used_cpu_sys_sec_frac();
        assert!(sys_usec < 1_000_000);
        let _ = (user_sec, sys_sec);
    }

    #[test]
    fn guard_drop_restores_previous_state() {
        assert!(command_time_ms().is_none());
        {
            let _guard = enter_command_time_snapshot(5000);
            assert_eq!(command_time_ms(), Some(5000));
        }
        assert!(command_time_ms().is_none());
    }

    #[test]
    fn debug_delay_keeps_positive_delays_and_samples_negative_delays() {
        assert_eq!(debug_delay_sleep_us(0, 0), 0);
        assert_eq!(debug_delay_sleep_us(25, 17), 25);
        assert_eq!(debug_delay_sleep_us(-10, 0), 1);
        assert_eq!(debug_delay_sleep_us(-10, 1), 0);
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn short_debug_delays_do_not_accumulate_kernel_wakeup_latency() {
        let started = std::time::Instant::now();
        for _ in 0..10_000 {
            sleep_debug_delay_interruptible(10, || false);
        }
        let elapsed = started.elapsed();
        eprintln!("ten thousand short debug delays elapsed: {elapsed:?}");
        assert!(elapsed >= std::time::Duration::from_millis(100));
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "short debug delays accumulated excessive wakeup latency: {elapsed:?}"
        );
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn short_debug_delay_observes_cancellation_before_waiting() {
        let calls = Cell::new(0);
        sleep_debug_delay_interruptible(50, || {
            calls.set(calls.get() + 1);
            true
        });
        assert_eq!(calls.get(), 1);
        sleep_debug_delay_interruptible(0, || panic!("zero delay must not wait"));
    }
}
