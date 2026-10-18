//! Process signal and wait adapters.

use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::OnceLock;

static PERSISTENCE_CHILD_PARENT: AtomicI32 = AtomicI32::new(0);

pub fn parent_process_id() -> i32 {
    // SAFETY: getppid has no arguments and reads the current process's parent PID.
    unsafe { libc::getppid() }
}

/// Install only in a post-fork child, before serialization starts.
pub fn monitor_persistence_child_parent() {
    let parent = parent_process_id();
    PERSISTENCE_CHILD_PARENT.store(parent, Ordering::Relaxed);
}

pub fn exit_if_persistence_child_parent_changed() {
    let expected = PERSISTENCE_CHILD_PARENT.load(Ordering::Relaxed);
    if expected == 0 {
        return;
    }
    if parent_process_id() != expected || expected <= 1 {
        exit_immediately(1);
    }
}

static SYSTEMD_SUPERVISED: AtomicBool = AtomicBool::new(false);
static SYSTEMD_NOTIFY_SOCKET: OnceLock<Option<String>> = OnceLock::new();

pub fn set_systemd_supervised(active: bool) {
    SYSTEMD_SUPERVISED.store(active, Ordering::Relaxed);
}

pub fn is_systemd_supervised() -> bool {
    SYSTEMD_SUPERVISED.load(Ordering::Relaxed)
}

pub fn activate_upstart_supervision() -> io::Result<bool> {
    if std::env::var_os("UPSTART_JOB").is_none() {
        return Ok(false);
    }
    // SAFETY: SIGSTOP affects only this process and is the documented Upstart
    // readiness handshake; the supervising parent resumes it with SIGCONT.
    if unsafe { libc::raise(libc::SIGSTOP) } != 0 {
        return Err(io::Error::last_os_error());
    }
    const UPSTART_JOB: &[u8] = b"UPSTART_JOB\0";
    // SAFETY: the constant is a NUL-terminated environment key; this happens
    // before runtime worker threads are created, matching C's post-resume cleanup.
    unsafe { libc::unsetenv(UPSTART_JOB.as_ptr().cast()) };
    Ok(true)
}

pub fn systemd_notify_available() -> bool {
    SYSTEMD_NOTIFY_SOCKET
        .get()
        .and_then(|path| path.as_deref())
        .is_some()
}

#[derive(Clone)]
pub struct PreparedCpuAffinity {
    #[cfg(target_os = "linux")]
    mask: libc::cpu_set_t,
}

pub fn prepare_cpu_affinity(cpulist: &str) -> io::Result<Option<PreparedCpuAffinity>> {
    if cpulist.trim().is_empty() {
        return Ok(None);
    }
    #[cfg(target_os = "linux")]
    {
        let cpus = parse_cpu_list(cpulist)?;
        // SAFETY: cpu_set_t is a plain bitset whose all-zero representation
        // denotes an empty affinity mask before selected bits are set below.
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of_val(&set);
        for cpu in cpus {
            let byte = cpu / 8;
            if byte >= size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CPU index exceeds cpu_set_t capacity",
                ));
            }
            // SAFETY: byte was checked against the complete cpu_set_t storage.
            unsafe {
                *((&mut set as *mut libc::cpu_set_t).cast::<u8>().add(byte)) |= 1 << (cpu % 8)
            };
        }
        Ok(Some(PreparedCpuAffinity { mask: set }))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpulist;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "CPU affinity requires Linux",
        ))
    }
}

pub fn apply_prepared_cpu_affinity(affinity: &PreparedCpuAffinity) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the mask was initialized by prepare_cpu_affinity with the exact kernel ABI size.
        if unsafe {
            libc::sched_setaffinity(0, std::mem::size_of_val(&affinity.mask), &affinity.mask)
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = affinity;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "CPU affinity requires Linux",
        ))
    }
}

pub fn apply_cpu_affinity(cpulist: &str) -> io::Result<()> {
    if let Some(affinity) = prepare_cpu_affinity(cpulist)? {
        apply_prepared_cpu_affinity(&affinity)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn parse_cpu_list(cpulist: &str) -> io::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for token in cpulist.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty CPU list token",
            ));
        }
        let (range, step) = token.split_once(':').map_or((token, 1), |(range, step)| {
            (range, step.parse::<usize>().unwrap_or(0))
        });
        if step == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CPU step must be positive",
            ));
        }
        if let Some((start, end)) = range.split_once('-') {
            let start = start
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid CPU range"))?;
            let end = end
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid CPU range"))?;
            if end < start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CPU range is descending",
                ));
            }
            cpus.extend((start..=end).step_by(step));
        } else {
            if step != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CPU step requires a range",
                ));
            }
            cpus.push(
                range.parse::<usize>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid CPU index")
                })?,
            );
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

pub fn notify_systemd(message: &str) -> io::Result<bool> {
    let Some(socket_path) = SYSTEMD_NOTIFY_SOCKET
        .get_or_init(|| std::env::var("NOTIFY_SOCKET").ok())
        .as_deref()
    else {
        return Ok(false);
    };
    if socket_path.is_empty() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        if let Some(abstract_name) = socket_path.strip_prefix('@') {
            return notify_systemd_abstract(abstract_name.as_bytes(), message.as_bytes())
                .map(|_| true);
        }
        let socket = UnixDatagram::unbound()?;
        socket.send_to(message.as_bytes(), socket_path)?;
        Ok(true)
    }
    #[cfg(not(unix))]
    {
        let _ = message;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "systemd notification requires Unix sockets",
        ))
    }
}

#[cfg(target_os = "linux")]
fn notify_systemd_abstract(name: &[u8], message: &[u8]) -> io::Result<()> {
    if name.is_empty() || name.len() + 1 > std::mem::size_of::<[libc::c_char; 108]>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid abstract systemd socket name",
        ));
    }
    // SAFETY: creates an owned Unix datagram descriptor used only by this call.
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    address.sun_path[0] = 0;
    for (slot, byte) in address.sun_path[1..].iter_mut().zip(name) {
        *slot = *byte as libc::c_char;
    }
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len()) as libc::socklen_t;
    // SAFETY: address is initialized for exactly `length` bytes and message is live for the send.
    let result = unsafe {
        libc::sendto(
            fd,
            message.as_ptr().cast(),
            message.len(),
            0,
            (&address as *const libc::sockaddr_un).cast(),
            length,
        )
    };
    let error = if result < 0 {
        Some(io::Error::last_os_error())
    } else {
        None
    };
    // SAFETY: fd is owned by this helper and no longer used.
    unsafe { libc::close(fd) };
    error.map_or(Ok(()), Err)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn notify_systemd_abstract(_name: &[u8], _message: &[u8]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract systemd sockets require Linux",
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessSignal {
    Terminate,
    Interrupt,
    Kill,
    Stop,
    Continue,
    User1,
    User2,
}

impl ProcessSignal {
    const fn raw(self) -> libc::c_int {
        match self {
            Self::Terminate => libc::SIGTERM,
            Self::Interrupt => libc::SIGINT,
            Self::Kill => libc::SIGKILL,
            Self::Stop => libc::SIGSTOP,
            Self::Continue => libc::SIGCONT,
            Self::User1 => libc::SIGUSR1,
            Self::User2 => libc::SIGUSR2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    Running,
    Exited(i32),
    Signaled(i32),
    Other(i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DaemonizeOutcome {
    Parent,
    Child,
}

pub fn daemonize() -> io::Result<DaemonizeOutcome> {
    #[cfg(unix)]
    {
        // SAFETY: fork duplicates only the current process; callers branch on
        // the returned outcome before initializing server-owned resources.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid > 0 {
            return Ok(DaemonizeOutcome::Parent);
        }
        // SAFETY: the freshly forked child has no controlling session yet.
        if unsafe { libc::setsid() } < 0 {
            return Err(io::Error::last_os_error());
        }
        const DEV_NULL: &[u8] = b"/dev/null\0";
        // SAFETY: DEV_NULL is NUL terminated and the returned descriptor is
        // immediately duplicated into the three standard descriptor slots.
        let fd = unsafe { libc::open(DEV_NULL.as_ptr().cast(), libc::O_RDWR) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        for target in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
            // SAFETY: fd is open and target is one of the standard descriptors.
            if unsafe { libc::dup2(fd, target) } < 0 {
                let error = io::Error::last_os_error();
                // SAFETY: fd is still owned by this child on this error path.
                unsafe { libc::close(fd) };
                return Err(error);
            }
        }
        if fd > libc::STDERR_FILENO {
            // SAFETY: every standard descriptor now owns its duplicated handle.
            unsafe { libc::close(fd) };
        }
        Ok(DaemonizeOutcome::Child)
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "daemonize requires Unix process primitives",
        ))
    }
}

pub fn send_signal(pid: i32, signal: ProcessSignal) -> io::Result<()> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process pid must be positive",
        ));
    }
    // SAFETY: pid is positive and signal is selected from known process signals.
    if unsafe { libc::kill(pid, signal.raw()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub fn process_exists(pid: i32) -> io::Result<bool> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process pid must be positive",
        ));
    }
    // SAFETY: signal 0 does not modify the target process and only probes pid
    // visibility and permission.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error),
    }
}

fn decode_wait_status(status: libc::c_int) -> WaitOutcome {
    if libc::WIFEXITED(status) {
        WaitOutcome::Exited(libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        WaitOutcome::Signaled(libc::WTERMSIG(status))
    } else {
        WaitOutcome::Other(status)
    }
}

fn wait_pid(pid: i32, options: libc::c_int) -> io::Result<WaitOutcome> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process pid must be positive",
        ));
    }
    loop {
        let mut status = 0;
        // SAFETY: status is writable storage and pid names one owned child.
        let result = unsafe { libc::waitpid(pid, &mut status, options) };
        if result == pid {
            return Ok(decode_wait_status(status));
        }
        if result == 0 {
            return Ok(WaitOutcome::Running);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn wait_nonblocking(pid: i32) -> io::Result<WaitOutcome> {
    wait_pid(pid, libc::WNOHANG)
}

pub fn wait_blocking(pid: i32) -> io::Result<WaitOutcome> {
    wait_pid(pid, 0)
}

#[cfg(target_os = "linux")]
pub fn open_pidfd(pid: i32) -> io::Result<OwnedFd> {
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process pid must be positive",
        ));
    }
    // SAFETY: pidfd_open is called with a positive pid and flags=0. On success
    // the returned descriptor is uniquely owned by the caller.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: fd is a fresh owned descriptor returned by pidfd_open.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) })
    }
}

#[cfg(not(target_os = "linux"))]
pub fn open_pidfd(pid: i32) -> io::Result<OwnedFd> {
    let _ = pid;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "pidfd requires Linux",
    ))
}

pub fn exit_immediately(code: i32) -> ! {
    // SAFETY: _exit terminates only the current process and never returns.
    unsafe { libc::_exit(code) }
}
