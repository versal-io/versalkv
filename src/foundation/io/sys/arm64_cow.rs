//! ARM64 Linux `MADV_FREE` + fork safety probe from Valkey `syscheck.c`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arm64CowCheck {
    NotApplicable,
    Safe,
    Affected,
    Inconclusive(&'static str),
}

pub fn check_madv_free_fork_safety() -> Arm64CowCheck {
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        check_platform()
    }
    #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
    {
        Arm64CowCheck::NotApplicable
    }
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn check_platform() -> Arm64CowCheck {
    const MADV_FREE: libc::c_int = 8;
    // SAFETY: sysconf reads the process page-size constant without memory side effects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Arm64CowCheck::Inconclusive("could not determine page size");
    }
    let map_size = (page_size as usize).saturating_mul(3);
    // SAFETY: mmap receives a null hint, a positive map size, and anonymous private flags.
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            map_size,
            libc::PROT_READ,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if mapping == libc::MAP_FAILED {
        return Arm64CowCheck::Inconclusive("could not allocate probe mapping");
    }
    // SAFETY: the mapping owns exactly three pages, so advancing one page remains in bounds.
    let middle = unsafe { (mapping as *mut u8).add(page_size as usize) };
    let result = probe_mapping(middle, page_size as usize, MADV_FREE);
    // SAFETY: mapping was returned by mmap with this exact size and has not been unmapped.
    unsafe {
        libc::munmap(mapping, map_size);
    }
    result
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn probe_mapping(middle: *mut u8, page_size: usize, madv_free: libc::c_int) -> Arm64CowCheck {
    // SAFETY: middle points to the mapped middle page and page_size is the kernel page size.
    let protected =
        unsafe { libc::mprotect(middle.cast(), page_size, libc::PROT_READ | libc::PROT_WRITE) };
    if protected != 0 {
        return Arm64CowCheck::Inconclusive("could not protect probe page");
    }
    // SAFETY: mprotect made the middle mapped page writable.
    unsafe {
        std::ptr::write_volatile(middle, 0);
    }
    // SAFETY: middle points to a valid mapped page for the requested range.
    let advised = unsafe { libc::madvise(middle.cast(), page_size, madv_free) };
    if advised != 0 {
        return if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL) {
            Arm64CowCheck::Safe
        } else {
            Arm64CowCheck::Inconclusive("MADV_FREE probe failed")
        };
    }
    // SAFETY: the page remains writable after MADV_FREE and this reclaims ownership.
    unsafe {
        std::ptr::write_volatile(middle, 0);
    }
    let mut pipe_fds = [-1, -1];
    // SAFETY: pipe_fds provides writable storage for exactly two descriptors.
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Arm64CowCheck::Inconclusive("could not create probe pipe");
    }
    // SAFETY: this probe runs before the server creates runtime worker threads.
    let child = unsafe { libc::fork() };
    if child < 0 {
        close_probe_fds(pipe_fds);
        return Arm64CowCheck::Inconclusive("could not fork probe child");
    }
    if child == 0 {
        // SAFETY: child owns its inherited read descriptor and does not use it.
        unsafe {
            libc::close(pipe_fds[0]);
        }
        let result =
            shared_dirty_kb(middle as usize).map_or(0i32, |value| if value == 0 { -1 } else { 1 });
        // SAFETY: the result pointer is valid for exactly one i32 write to the child-owned pipe.
        unsafe {
            libc::write(
                pipe_fds[1],
                (&result as *const i32).cast(),
                std::mem::size_of::<i32>(),
            );
            libc::close(pipe_fds[1]);
            libc::_exit(0);
        }
    }
    // SAFETY: parent owns its inherited write descriptor and does not use it.
    unsafe {
        libc::close(pipe_fds[1]);
    }
    let mut child_result = 0i32;
    // SAFETY: child_result provides valid writable storage for one i32.
    let read = unsafe {
        libc::read(
            pipe_fds[0],
            (&mut child_result as *mut i32).cast(),
            std::mem::size_of::<i32>(),
        )
    };
    // SAFETY: parent owns the read descriptor and waits for exactly its fork child.
    unsafe {
        libc::close(pipe_fds[0]);
        libc::waitpid(child, std::ptr::null_mut(), 0);
    }
    if read as usize != std::mem::size_of::<i32>() {
        return Arm64CowCheck::Inconclusive("could not read probe child result");
    }
    match child_result {
        1 => Arm64CowCheck::Safe,
        -1 => Arm64CowCheck::Affected,
        _ => Arm64CowCheck::Inconclusive("could not inspect probe page dirtiness"),
    }
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn close_probe_fds(fds: [libc::c_int; 2]) {
    for fd in fds {
        if fd >= 0 {
            // SAFETY: each descriptor came from the local probe pipe and has not been closed.
            unsafe {
                libc::close(fd);
            }
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn shared_dirty_kb(address: usize) -> Option<u64> {
    let smaps = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut contains_address = false;
    for line in smaps.lines() {
        if let Some((range, _)) = line.split_once(' ') {
            if let Some((start, end)) = range.split_once('-') {
                if let (Ok(start), Ok(end)) = (
                    usize::from_str_radix(start, 16),
                    usize::from_str_radix(end, 16),
                ) {
                    contains_address = (start..end).contains(&address);
                    continue;
                }
            }
        }
        if contains_address {
            if let Some(value) = line.strip_prefix("Shared_Dirty:") {
                return value.split_whitespace().next()?.parse().ok();
            }
        }
    }
    None
}

#[cfg(all(test, not(all(target_os = "linux", target_arch = "aarch64"))))]
mod tests {
    use super::{check_madv_free_fork_safety, Arm64CowCheck};

    #[test]
    fn non_arm64_platforms_do_not_run_the_probe() {
        assert_eq!(check_madv_free_fork_safety(), Arm64CowCheck::NotApplicable);
    }
}
