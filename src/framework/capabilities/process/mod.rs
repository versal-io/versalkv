//! Owned process capabilities used by persistence and replication orchestration.

use crate::foundation::io::sys::process::{exit_immediately, send_signal, ProcessSignal};
#[cfg(test)]
use crate::foundation::io::sys::process::{wait_blocking, wait_nonblocking, WaitOutcome};
use std::fs::File;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

#[derive(Debug)]
pub(crate) struct ForkChild {
    pid: i32,
}

impl ForkChild {
    #[cfg(test)]
    pub(crate) fn pid(&self) -> i32 {
        self.pid
    }

    pub(crate) fn into_pid(self) -> i32 {
        self.pid
    }

    #[cfg(test)]
    pub(crate) fn signal(&self, signal: ProcessSignal) -> io::Result<()> {
        send_signal(self.pid, signal)
    }

    #[cfg(test)]
    pub(crate) fn kill(&self) -> io::Result<()> {
        self.signal(ProcessSignal::Kill)
    }

    #[cfg(test)]
    pub(crate) fn try_wait(&mut self) -> io::Result<WaitOutcome> {
        wait_nonblocking(self.pid)
    }

    #[cfg(test)]
    pub(crate) fn wait(&mut self) -> io::Result<WaitOutcome> {
        wait_blocking(self.pid)
    }

    #[cfg(test)]
    pub(crate) fn wait_timeout(&mut self, timeout: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            if !matches!(self.try_wait()?, WaitOutcome::Running) {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

pub(crate) enum ForkBranch {
    Parent(ForkChild),
    Child,
}

pub(crate) fn fork_process() -> io::Result<ForkBranch> {
    // SAFETY: callers invoke this capability from the single owner thread and
    // child branches terminate through exit_immediately without unwinding.
    let pid = unsafe { libc::fork() };
    match pid {
        value if value < 0 => Err(io::Error::last_os_error()),
        0 => Ok(ForkBranch::Child),
        value => Ok(ForkBranch::Parent(ForkChild { pid: value })),
    }
}

#[cfg(test)]
pub(crate) fn fork_owned_child(child: impl FnOnce() -> i32 + 'static) -> io::Result<ForkChild> {
    match fork_process()? {
        ForkBranch::Parent(child_process) => Ok(child_process),
        ForkBranch::Child => exit_immediately(child()),
    }
}

extern "C" fn persistence_child_sigusr1(_signal: i32) {
    write_all_fd(
        libc::STDERR_FILENO,
        b"Received SIGUSR1 in child, exiting now.\n",
    );
    exit_immediately(1);
}

extern "C" fn persistence_child_sigusr2(_signal: i32) {
    crate::foundation::io::persistence::serialize::interrupt_key_save_delay();
}

pub(crate) fn install_persistence_child_signal_handlers() {
    // SAFETY: both handlers have static C ABI addresses; sigaction only reads
    // the initialized local structures and installs process-local handlers.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = persistence_child_sigusr1 as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_flags = 0;
        libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut());

        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = persistence_child_sigusr2 as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_flags = 0;
        libc::sigaction(libc::SIGUSR2, &action, std::ptr::null_mut());
    }
}

pub(crate) fn close_fd(fd: RawFd) -> bool {
    if fd < 0 {
        return false;
    }
    // SAFETY: callers transfer their current-process descriptor copy here.
    unsafe { libc::close(fd) == 0 }
}

pub(crate) fn write_all_fd(fd: RawFd, bytes: &[u8]) -> bool {
    let mut offset = 0;
    while offset < bytes.len() {
        // SAFETY: bytes exposes a live region for the duration of write(2).
        let written = unsafe {
            libc::write(
                fd,
                bytes[offset..].as_ptr().cast::<libc::c_void>(),
                bytes.len() - offset,
            )
        };
        if written > 0 {
            offset += written as usize;
            continue;
        }
        if written < 0 && crate::foundation::io::sys::os::errno() == libc::EINTR {
            continue;
        }
        return false;
    }
    true
}

pub(crate) fn wait_for_pipe_eof(read_end: File) {
    use std::os::fd::AsRawFd;

    let fd = read_end.as_raw_fd();
    let mut byte = 0_u8;
    loop {
        // SAFETY: byte is writable for the one-byte read from the owned descriptor.
        let result = unsafe { libc::read(fd, (&mut byte as *mut u8).cast::<libc::c_void>(), 1) };
        if result == 0 {
            break;
        }
        if result < 0 && crate::foundation::io::sys::os::errno() == libc::EINTR {
            continue;
        }
        break;
    }
    drop(read_end);
}

#[cfg(test)]
pub(crate) fn send_all(fd: RawFd, bytes: &[u8], timeout: Duration) -> bool {
    send_all_counted(fd, bytes, timeout).0
}

pub(crate) fn send_all_counted(fd: RawFd, bytes: &[u8], timeout: Duration) -> (bool, usize) {
    let mut offset = 0;
    while offset < bytes.len() {
        // SAFETY: bytes exposes a live region for the duration of send(2).
        let written = unsafe {
            libc::send(
                fd,
                bytes[offset..].as_ptr().cast::<libc::c_void>(),
                bytes.len() - offset,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if written > 0 {
            offset += written as usize;
            continue;
        }
        let error = crate::foundation::io::sys::os::errno();
        if written == 0
            || (error != libc::EINTR && error != libc::EAGAIN && error != libc::EWOULDBLOCK)
        {
            return (false, offset);
        }
        if error == libc::EINTR {
            continue;
        }
        let mut descriptor = crate::foundation::io::sys::os::PollDescriptor::new(fd, libc::POLLOUT);
        match crate::foundation::io::sys::os::poll_descriptors(
            std::slice::from_mut(&mut descriptor),
            timeout,
        ) {
            Ok(ready) if ready > 0 => {}
            _ => return (false, offset),
        }
    }
    (true, offset)
}

pub(crate) fn set_send_timeout(fd: RawFd, timeout: Duration) -> io::Result<()> {
    let timeout = libc::timeval {
        tv_sec: timeout.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
        tv_usec: timeout.subsec_micros() as libc::suseconds_t,
    };
    // SAFETY: timeout is initialized for the exact SO_SNDTIMEO size.
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDTIMEO,
            (&timeout as *const libc::timeval).cast::<libc::c_void>(),
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) fn stop_current_process() -> io::Result<()> {
    send_signal(std::process::id() as i32, ProcessSignal::Stop)
}

#[cfg(test)]
pub(crate) fn pause_current_process() -> io::Result<()> {
    // SAFETY: pause has no pointer arguments and only waits for signal delivery.
    if unsafe { libc::pause() } == -1 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn arm_parent_death_signal(expected_parent: i32) -> bool {
    // SAFETY: prctl takes scalar arguments and getppid has no pointer arguments.
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == 0
            && libc::getppid() == expected_parent
    }
}

#[cfg(test)]
mod tests;
