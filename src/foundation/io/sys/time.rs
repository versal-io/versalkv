use std::cell::Cell;
use std::io;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicInstant(u64);

impl MonotonicInstant {
    pub fn now() -> Self {
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        let elapsed = EPOCH.get_or_init(Instant::now).elapsed().as_micros();
        Self(elapsed.min(u64::MAX as u128) as u64)
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

    pub fn duration_until(self, now: Self) -> Duration {
        Duration::from_micros(self.0.saturating_sub(now.0))
    }
}

thread_local! {
    static COMMAND_TIME_MS: Cell<Option<i64>> = const { Cell::new(None) };
}

pub struct CommandTimeSnapshotGuard {
    previous: Option<i64>,
}

impl Drop for CommandTimeSnapshotGuard {
    fn drop(&mut self) {
        COMMAND_TIME_MS.with(|cell| cell.set(self.previous));
    }
}

pub fn enter_command_time_snapshot(ms: i64) -> CommandTimeSnapshotGuard {
    COMMAND_TIME_MS.with(|cell| {
        let previous = cell.get();
        if previous.is_none() {
            cell.set(Some(ms));
        }
        CommandTimeSnapshotGuard { previous }
    })
}

/// Install a fresh wall-clock snapshot even when a command is already active.
/// Nested blocked event-loop servicing uses this to give admitted commands the
/// current time while restoring the outer command's frozen snapshot on return.
pub fn enter_fresh_command_time_snapshot() -> CommandTimeSnapshotGuard {
    COMMAND_TIME_MS.with(|cell| {
        let previous = cell.replace(Some(real_mstime()));
        CommandTimeSnapshotGuard { previous }
    })
}

fn command_time_ms() -> Option<i64> {
    COMMAND_TIME_MS.with(|cell| cell.get())
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
}
