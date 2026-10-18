//! Async-signal-safe shutdown-signal machinery.
//! The signal handler is restricted to atomic stores and `libc::write` with
//! static byte strings — nothing else is guaranteed safe from a POSIX signal
//! handler. The event-loop cron and hosted loading checkpoints consume the
//! pending-signal state from safe Rust execution contexts.

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Mutex;

static LAST_SIG: AtomicI32 = AtomicI32::new(0);
static SIGINT_COUNT: AtomicUsize = AtomicUsize::new(0);
static SECOND_SIGINT: AtomicBool = AtomicBool::new(false);
static CHILD_SIGNAL_PENDING: AtomicBool = AtomicBool::new(false);
static CHILD_SIGNAL_WAKE_FD: AtomicI32 = AtomicI32::new(-1);
/// PID slot for the current BGSAVE/BGREWRITEAOF child, published by the parent
/// so the SIGINT ripcord path can `unlink(2)` the temp file without a heap
/// allocation. `AtomicI32` reads/writes are lock-free on all supported targets.
static RDB_CHILD_PID_SLOT: AtomicI32 = AtomicI32::new(-1);
static AOF_CHILD_PID_SLOT: AtomicI32 = AtomicI32::new(-1);
#[cfg(test)]
static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Called by the parent whenever `rdb_child_pid` is (re)assigned so the signal
/// handler can locate the temp file. Also called with `-1` to clear.
pub fn set_rdb_child_pid(pid: i32) {
    RDB_CHILD_PID_SLOT.store(pid, Ordering::Relaxed);
}
pub fn set_aof_child_pid(pid: i32) {
    AOF_CHILD_PID_SLOT.store(pid, Ordering::Relaxed);
}

/// Arm the process-wide one-shot software watchdog, or disarm it with zero.
/// The event-loop cron refreshes this before each responsive cycle; leaving it
/// armed while a blocking load is in progress lets SIGALRM interrupt syscalls
/// and expose stalls, matching Valkey's watchdog contract.
pub fn schedule_watchdog(period_ms: u64) {
    let timer = libc::itimerval {
        it_interval: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        it_value: libc::timeval {
            tv_sec: (period_ms / 1_000) as libc::time_t,
            tv_usec: ((period_ms % 1_000) * 1_000) as libc::suseconds_t,
        },
    };
    // SAFETY: timer is fully initialized and ITIMER_REAL is process-local.
    unsafe {
        libc::setitimer(libc::ITIMER_REAL, &timer, std::ptr::null_mut());
    }
}

/// Async-signal-safe format-and-unlink of `temp-{pid}.rdb` / `temp-rewriteaof-bg-{pid}.aof`.
/// Uses `libc::unlink` (async-signal-safe) with a hand-rolled decimal formatter
/// into a stack buffer — no heap allocation, no snprintf (snprintf is not on
/// the POSIX async-signal-safe list). Path is relative to CWD which we
/// chdir'd into at startup.
fn ripcord_unlink_temp_files() {
    // The parent's own synchronous save writes temp-{parent_pid}.rdb, so we
    // unlink both parent- and child-pid variants. In the ripcord path we may
    // be racing a synchronous save; that's fine — unlink after the fact still
    // wins as long as the file remains at "temp-{pid}.rdb" until rename.
    // SAFETY: getpid() is async-signal-safe and always succeeds per POSIX.
    let parent_pid = unsafe { libc::getpid() };
    let rdb_child_pid = RDB_CHILD_PID_SLOT.load(Ordering::Relaxed);
    let aof_child_pid = AOF_CHILD_PID_SLOT.load(Ordering::Relaxed);
    let mut buf = [0u8; 64];
    if parent_pid > 0 {
        format_temp_path(&mut buf, b"temp-", parent_pid, b".rdb");
        // SAFETY: unlink is async-signal-safe; buf is NUL-terminated.
        unsafe { libc::unlink(buf.as_ptr() as *const libc::c_char) };
    }
    if rdb_child_pid > 0 {
        format_temp_path(&mut buf, b"temp-", rdb_child_pid, b".rdb");
        // SAFETY: unlink is async-signal-safe; buf is a NUL-terminated stack-allocated C string.
        unsafe { libc::unlink(buf.as_ptr() as *const libc::c_char) };
    }
    if aof_child_pid > 0 {
        format_temp_path(&mut buf, b"temp-rewriteaof-bg-", aof_child_pid, b".aof");
        // SAFETY: unlink is async-signal-safe; buf is a NUL-terminated stack-allocated C string.
        unsafe { libc::unlink(buf.as_ptr() as *const libc::c_char) };
    }
}

/// Writes `<prefix><pid><suffix>\0` into `buf` and returns bytes written
/// (excluding NUL). Async-signal-safe — no allocation, no libc calls.
fn format_temp_path(buf: &mut [u8], prefix: &[u8], pid: i32, suffix: &[u8]) -> usize {
    let mut n = 0;
    for &b in prefix {
        if n + 1 >= buf.len() {
            buf[buf.len() - 1] = 0;
            return n;
        }
        buf[n] = b;
        n += 1;
    }
    // Format pid (positive) as decimal.
    let mut digits = [0u8; 10];
    let mut d = 0;
    let mut v = pid as u32;
    if v == 0 {
        digits[0] = b'0';
        d = 1;
    } else {
        while v > 0 {
            digits[d] = b'0' + (v % 10) as u8;
            v /= 10;
            d += 1;
        }
    }
    while d > 0 && n + 1 < buf.len() {
        d -= 1;
        buf[n] = digits[d];
        n += 1;
    }
    for &b in suffix {
        if n + 1 >= buf.len() {
            break;
        }
        buf[n] = b;
        n += 1;
    }
    buf[n] = 0;
    n
}

extern "C" fn sig_shutdown_handler(sig: i32) {
    LAST_SIG.store(sig, Ordering::Relaxed);
    if sig == libc::SIGINT {
        let prev = SIGINT_COUNT.fetch_add(1, Ordering::Relaxed);
        if prev >= 1 {
            SECOND_SIGINT.store(true, Ordering::Relaxed);
            // Yank the ripcord: 2nd SIGINT means the user won't wait for graceful
            // shutdown (e.g. cron blocked in kill_children). Match Valkey C's
            // sigShutdownHandler: clean up temp files then _exit(1).
            let msg: &[u8] = b"You insist... exiting now.\n";
            // SAFETY: write(2), unlink(2), snprintf(3), _exit(2) are all
            // async-signal-safe. RDB/AOF pid slots are read via atomic loads.
            unsafe {
                libc::write(
                    libc::STDERR_FILENO,
                    msg.as_ptr() as *const libc::c_void,
                    msg.len(),
                );
            }
            ripcord_unlink_temp_files();
            // SAFETY: _exit(2) is async-signal-safe and terminates the process immediately.
            unsafe {
                libc::_exit(1);
            }
        }
    }
    let msg: &[u8] = match sig {
        libc::SIGTERM => b"Received SIGTERM scheduling shutdown...\n",
        libc::SIGINT => b"Received SIGINT scheduling shutdown...\n",
        _ => b"Received unknown signal, scheduling shutdown...\n",
    };
    // SAFETY: write(2) is async-signal-safe; static byte slice is 'static.
    unsafe {
        libc::write(
            libc::STDERR_FILENO,
            msg.as_ptr() as *const libc::c_void,
            msg.len(),
        );
    }
}

extern "C" fn sig_diagnostic_handler(sig: i32) {
    let message: &[u8] = match sig {
        libc::SIGALRM => b"WATCHDOG TIMER EXPIRED\nReceived SIGALRM\n------ STACK TRACE ------\n------ STACK TRACE DONE ------\n",
        _ => b"crashed by signal\n------ STACK TRACE ------\n------ STACK TRACE DONE ------\n",
    };
    // SAFETY: stdout is a process-owned fd and libc::write with a static byte
    // slice is async-signal-safe. Fatal diagnostics must remain visible to the
    // Tcl harness even when the logger mutex is unavailable.
    unsafe {
        libc::write(
            libc::STDOUT_FILENO,
            message.as_ptr() as *const libc::c_void,
            message.len(),
        );
    }
    if sig != libc::SIGALRM {
        // SAFETY: these functions are async-signal-safe. The handler restores
        // the default disposition, unblocks the signal, and re-delivers it.
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            let mut unblock = std::mem::zeroed::<libc::sigset_t>();
            libc::sigemptyset(&mut unblock);
            libc::sigaddset(&mut unblock, sig);
            libc::pthread_sigmask(libc::SIG_UNBLOCK, &unblock, std::ptr::null_mut());
            libc::kill(libc::getpid(), sig);
        }
    }
}

extern "C" fn sigchld_handler(_sig: i32) {
    CHILD_SIGNAL_PENDING.store(true, Ordering::Release);
    let fd = CHILD_SIGNAL_WAKE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = [1u8; 1];
        // SAFETY: write(2) is async-signal-safe. The fd is installed by the
        // runtime as a nonblocking self-pipe writer; EAGAIN/EBADF are ignored
        // because the pending bit makes collection level-triggered in Rust.
        unsafe {
            libc::write(fd, byte.as_ptr().cast(), byte.len());
        }
    }
}

/// Install SIGTERM/SIGINT handlers. Idempotent — safe to call once at startup.
pub fn install_shutdown_signals() {
    // SAFETY: sigaction/sigemptyset receive a locally-owned sigaction struct and
    // constant signal numbers; handler is extern "C" and has static lifetime.
    unsafe {
        let mut act: libc::sigaction = std::mem::zeroed();
        act.sa_sigaction = sig_shutdown_handler as *const () as usize;
        libc::sigemptyset(&mut act.sa_mask);
        act.sa_flags = 0; // no SA_RESTART: poll returns EINTR faster
        libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
        let mut diagnostic = act;
        diagnostic.sa_sigaction = sig_diagnostic_handler as *const () as usize;
        libc::sigaction(libc::SIGABRT, &diagnostic, std::ptr::null_mut());
        libc::sigaction(libc::SIGSEGV, &diagnostic, std::ptr::null_mut());
        libc::sigaction(libc::SIGBUS, &diagnostic, std::ptr::null_mut());
        libc::sigaction(libc::SIGFPE, &diagnostic, std::ptr::null_mut());
        libc::sigaction(libc::SIGILL, &diagnostic, std::ptr::null_mut());
        libc::sigaction(libc::SIGALRM, &diagnostic, std::ptr::null_mut());
    }
}

pub fn install_child_signal_wake(write_fd: RawFd) {
    CHILD_SIGNAL_WAKE_FD.store(write_fd, Ordering::Release);
    // SAFETY: sigaction receives an initialized struct and installs a static
    // C ABI handler. The handler only touches atomics and write(2).
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = sigchld_handler as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_flags = libc::SA_NOCLDSTOP;
        libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut());
    }
}

pub fn clear_child_signal_wake(write_fd: RawFd) {
    let _ =
        CHILD_SIGNAL_WAKE_FD.compare_exchange(write_fd, -1, Ordering::AcqRel, Ordering::Acquire);
}

pub fn take_child_signal_pending() -> bool {
    CHILD_SIGNAL_PENDING.swap(false, Ordering::AcqRel)
}

/// Terminate a software panic after the panic hook has handled diagnostics.
/// An external SIGABRT still uses the diagnostic handler installed at startup.
/// This path also applies when crash logging is disabled: abort must not create
/// a diagnostic report that the panic hook intentionally suppressed.
pub(super) fn abort_after_panic_report() -> ! {
    // SAFETY: sigaction receives an initialized action for the constant SIGABRT
    // signal. Restoring its default disposition preserves abort/core semantics
    // without re-entering our diagnostic handler after a software panic.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = libc::SIG_DFL;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGABRT, &action, std::ptr::null_mut());
    }
    std::process::abort();
}

pub fn crash_via_readonly_page() {
    // SAFETY: mmap returns either MAP_FAILED or a page-sized valid mapping.
    // Writing the read-only mapping intentionally raises SIGSEGV and does not return.
    unsafe {
        let page = libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if page == libc::MAP_FAILED {
            let _ = libc::raise(libc::SIGSEGV);
            return;
        }
        std::ptr::write_bytes(page as *mut u8, b'x', 1);
    }
}

/// Consume the pending signal number, if any. Clears the atomic slot.
pub fn take_pending_signal() -> Option<i32> {
    match LAST_SIG.swap(0, Ordering::Relaxed) {
        0 => None,
        s => Some(s),
    }
}

/// Whether SIGINT has fired at least twice since startup (and cleared).
pub fn take_second_sigint() -> bool {
    SECOND_SIGINT.swap(false, Ordering::Relaxed)
}

/// Test-only reset. Never call from production paths.
#[cfg(test)]
pub(crate) fn reset_for_test() {
    LAST_SIG.store(0, Ordering::Relaxed);
    SIGINT_COUNT.store(0, Ordering::Relaxed);
    SECOND_SIGINT.store(false, Ordering::Relaxed);
    CHILD_SIGNAL_PENDING.store(false, Ordering::Relaxed);
    CHILD_SIGNAL_WAKE_FD.store(-1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_pending_signal_roundtrip() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        reset_for_test();
        assert_eq!(take_pending_signal(), None);
        LAST_SIG.store(libc::SIGTERM, Ordering::Relaxed);
        assert_eq!(take_pending_signal(), Some(libc::SIGTERM));
        assert_eq!(take_pending_signal(), None);
    }

    #[test]
    fn second_sigint_flag_transitions() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        reset_for_test();
        assert!(!take_second_sigint());
        SECOND_SIGINT.store(true, Ordering::Relaxed);
        assert!(take_second_sigint());
        assert!(!take_second_sigint());
    }

    #[test]
    fn watchdog_can_be_armed_and_disarmed_without_changing_shutdown_state() {
        let _guard = SIGNAL_TEST_LOCK.lock().expect("signal test lock");
        reset_for_test();
        schedule_watchdog(60_000);
        schedule_watchdog(0);

        assert_eq!(take_pending_signal(), None);
        assert!(!take_second_sigint());
    }
}
