use crate::foundation::io::sys::time;

pub extern "C" fn ValkeyModule_Milliseconds() -> i64 {
    time::mstime()
}

pub extern "C" fn ValkeyModule_Microseconds() -> i64 {
    time::ustime()
}

pub extern "C" fn ValkeyModule_CachedMicroseconds() -> i64 {
    time::ustime()
}

pub extern "C" fn ValkeyModule_MonotonicMicroseconds() -> u64 {
    // SAFETY: clock_gettime writes a timespec to the valid stack pointer.
    unsafe {
        let mut timestamp: libc::timespec = std::mem::zeroed();
        if libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) != 0 {
            return 0;
        }
        (timestamp.tv_sec as u64)
            .saturating_mul(1_000_000)
            .saturating_add(timestamp.tv_nsec as u64 / 1_000)
    }
}
