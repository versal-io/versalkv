use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::time::Duration;

#[repr(transparent)]
pub struct PollDescriptor {
    raw: libc::pollfd,
}

impl PollDescriptor {
    pub fn new(fd: RawFd, events: libc::c_short) -> Self {
        Self {
            raw: libc::pollfd {
                fd,
                events,
                revents: 0,
            },
        }
    }

    pub fn revents(&self) -> libc::c_short {
        self.raw.revents
    }
}

pub fn is_tty(fd: BorrowedFd<'_>) -> bool {
    // SAFETY: fd is borrowed from a live descriptor for this call.
    unsafe { libc::isatty(fd.as_raw_fd()) != 0 }
}

pub fn poll_descriptors(
    descriptors: &mut [PollDescriptor],
    timeout: Duration,
) -> io::Result<usize> {
    let timeout_ms = timeout.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;
    // SAFETY: PollDescriptor is a transparent owner of pollfd values and the
    // slice remains valid and writable for the duration of poll.
    let result = unsafe {
        libc::poll(
            descriptors.as_mut_ptr().cast::<libc::pollfd>(),
            descriptors.len() as libc::nfds_t,
            timeout_ms,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

pub fn total_system_memory() -> Option<usize> {
    // SAFETY: sysconf reads process-global constants and has no memory effects.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    // SAFETY: sysconf reads process-global constants and has no memory effects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (pages > 0 && page_size > 0).then(|| (pages as usize).saturating_mul(page_size as usize))
}

pub fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

pub fn set_errno(value: i32) {
    #[cfg(target_vendor = "apple")]
    // SAFETY: libc returns the current thread's writable errno slot.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(target_vendor = "apple"))]
    // SAFETY: libc returns the current thread's writable errno slot.
    unsafe {
        *libc::__errno_location() = value;
    }
}

pub fn pipe2(fds: &mut [libc::c_int; 2], flags: libc::c_int) -> libc::c_int {
    #[cfg(target_os = "linux")]
    // SAFETY: fds points to writable storage for exactly two pipe descriptors.
    unsafe {
        libc::pipe2(fds.as_mut_ptr(), flags)
    }

    #[cfg(not(target_os = "linux"))]
    // SAFETY: fds points to writable storage for exactly two pipe descriptors;
    // all descriptor operations use the descriptors returned by pipe.
    unsafe {
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return -1;
        }
        if flags & libc::O_NONBLOCK != 0
            && (libc::fcntl(fds[0], libc::F_SETFL, libc::O_NONBLOCK) != 0
                || libc::fcntl(fds[1], libc::F_SETFL, libc::O_NONBLOCK) != 0)
        {
            let error = errno();
            libc::close(fds[0]);
            libc::close(fds[1]);
            set_errno(error);
            return -1;
        }
        if flags & libc::O_CLOEXEC != 0
            && (libc::fcntl(fds[0], libc::F_SETFD, libc::FD_CLOEXEC) != 0
                || libc::fcntl(fds[1], libc::F_SETFD, libc::FD_CLOEXEC) != 0)
        {
            let error = errno();
            libc::close(fds[0]);
            libc::close(fds[1]);
            set_errno(error);
            return -1;
        }
        0
    }
}

pub fn retry_on_interrupt<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

/// Drain a nonblocking reader until EOF or `WouldBlock`. Returns `true` only
/// for EOF; all bytes read before either terminal condition stay in `output`.
pub fn drain_nonblocking(
    scratch: &mut [u8],
    output: &mut Vec<u8>,
    mut read: impl FnMut(&mut [u8]) -> io::Result<usize>,
) -> io::Result<bool> {
    loop {
        match retry_on_interrupt(|| read(scratch)) {
            Ok(0) => return Ok(true),
            Ok(n) => output.extend_from_slice(&scratch[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error),
        }
    }
}

/// Return bytes currently retained in a socket's kernel send queue.
pub fn socket_pending_write_bytes_fd(fd: RawFd) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        let mut pending: libc::c_int = 0;
        // SAFETY: `fd` is borrowed from a live socket and `pending` is valid
        // writable storage for Linux's TIOCOUTQ integer result.
        if unsafe { libc::ioctl(fd, libc::TIOCOUTQ, &mut pending) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(pending.max(0) as usize)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = fd;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "socket send-queue accounting requires Linux TIOCOUTQ",
        ))
    }
}

/// Return kernel send-queue bytes only after the socket has stopped accepting
/// writes. Bytes already handed to a still-writable loopback socket are not
/// back-pressure memory yet and must not make shutdown's fill probe finish
/// before the following write is blocked behind them.
pub fn socket_backpressured_write_bytes_fd(fd: RawFd) -> io::Result<usize> {
    let pending = socket_pending_write_bytes_fd(fd)?;
    if pending == 0 {
        return Ok(0);
    }
    let mut poll_fd = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: poll_fd is valid writable storage for one descriptor and the
    // zero timeout performs a nonblocking readiness query.
    let result = unsafe { libc::poll(&mut poll_fd, 1, 0) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if poll_fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0
        || poll_fd.revents & libc::POLLOUT != 0
    {
        return Ok(0);
    }
    Ok(pending)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        drain_nonblocking, retry_on_interrupt, socket_backpressured_write_bytes_fd,
        socket_pending_write_bytes_fd,
    };
    use std::collections::VecDeque;
    use std::io::{ErrorKind, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;

    #[test]
    fn interrupted_operations_retry_until_data_or_would_block() {
        let mut data = VecDeque::from([
            Err(std::io::Error::from(ErrorKind::Interrupted)),
            Ok(7usize),
        ]);
        assert_eq!(retry_on_interrupt(|| data.pop_front().unwrap()).unwrap(), 7);

        let mut blocked: VecDeque<std::io::Result<usize>> = VecDeque::from([
            Err(std::io::Error::from(ErrorKind::Interrupted)),
            Err(std::io::Error::from(ErrorKind::WouldBlock)),
        ]);
        assert_eq!(
            retry_on_interrupt(|| blocked.pop_front().unwrap())
                .unwrap_err()
                .kind(),
            ErrorKind::WouldBlock
        );
    }

    #[test]
    fn nonblocking_drain_keeps_data_before_would_block_or_eof() {
        let mut scratch = [0u8; 8];
        let mut output = Vec::new();
        let mut steps = VecDeque::from([
            Err(std::io::Error::from(ErrorKind::Interrupted)),
            Ok(b"data".to_vec()),
            Err(std::io::Error::from(ErrorKind::WouldBlock)),
        ]);
        let eof = drain_nonblocking(&mut scratch, &mut output, |buf| {
            match steps.pop_front().unwrap() {
                Ok(bytes) => {
                    buf[..bytes.len()].copy_from_slice(&bytes);
                    Ok(bytes.len())
                }
                Err(error) => Err(error),
            }
        })
        .unwrap();
        assert!(!eof);
        assert_eq!(output, b"data");

        let mut eof_steps = VecDeque::from([
            Err(std::io::Error::from(ErrorKind::Interrupted)),
            Ok(b"tail".to_vec()),
            Ok(Vec::new()),
        ]);
        output.clear();
        let eof = drain_nonblocking(&mut scratch, &mut output, |buf| {
            match eof_steps.pop_front().unwrap() {
                Ok(bytes) => {
                    buf[..bytes.len()].copy_from_slice(&bytes);
                    Ok(bytes.len())
                }
                Err(error) => Err(error),
            }
        })
        .unwrap();
        assert!(eof);
        assert_eq!(output, b"tail");
    }

    #[test]
    fn reports_bytes_buffered_in_tcp_send_queue() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_reader, _) = listener.accept().unwrap();
        writer.set_nonblocking(true).unwrap();

        let chunk = [0x5a; 64 * 1024];
        loop {
            match writer.write(&chunk) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => panic!("write failed before socket backpressure: {error}"),
            }
        }

        assert!(
            socket_pending_write_bytes_fd(writer.as_raw_fd()).unwrap() > 0,
            "a back-pressured TCP socket must report pending kernel bytes"
        );
        assert!(
            socket_backpressured_write_bytes_fd(writer.as_raw_fd()).unwrap() > 0,
            "a full TCP send queue must count as back-pressured memory"
        );
    }

    #[test]
    fn writable_socket_does_not_count_kernel_send_queue_as_backpressure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_reader, _) = listener.accept().unwrap();
        writer.set_nonblocking(true).unwrap();
        writer.write_all(b"small").unwrap();

        assert_eq!(
            socket_backpressured_write_bytes_fd(writer.as_raw_fd()).unwrap(),
            0,
            "kernel outq must not be counted until the socket stops being writable"
        );
    }
}
