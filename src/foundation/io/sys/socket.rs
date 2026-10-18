//! Socket metadata and filesystem ownership adapters.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SocketBuffer {
    Send,
    Receive,
}

pub fn socket_port(fd: BorrowedFd<'_>, peer: bool) -> io::Result<u16> {
    let mut storage = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: storage and len are writable for the selected socket address query.
    let result = unsafe {
        if peer {
            libc::getpeername(
                fd.as_raw_fd(),
                storage.as_mut_ptr().cast::<libc::sockaddr>(),
                &mut len,
            )
        } else {
            libc::getsockname(
                fd.as_raw_fd(),
                storage.as_mut_ptr().cast::<libc::sockaddr>(),
                &mut len,
            )
        }
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful syscall initialized storage and its family field.
    let storage = unsafe { storage.assume_init() };
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            // SAFETY: ss_family confirms the sockaddr_in representation.
            let address = unsafe { &*(&storage as *const _ as *const libc::sockaddr_in) };
            Ok(u16::from_be(address.sin_port))
        }
        libc::AF_INET6 => {
            // SAFETY: ss_family confirms the sockaddr_in6 representation.
            let address = unsafe { &*(&storage as *const _ as *const libc::sockaddr_in6) };
            Ok(u16::from_be(address.sin6_port))
        }
        _ => Err(io::Error::from(io::ErrorKind::InvalidInput)),
    }
}

pub fn socket_port_raw(fd: std::os::fd::RawFd, peer: bool) -> io::Result<u16> {
    if fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket descriptor must be non-negative",
        ));
    }
    // SAFETY: the descriptor is borrowed only for the duration of the metadata
    // query; callers retain ownership and socket_port does not close it.
    socket_port(unsafe { BorrowedFd::borrow_raw(fd) }, peer)
}

/// Check whether a nonblocking stream has reached an orderly peer shutdown.
///
/// The portable polling crate does not preserve kqueue's EOF flags. Probe the
/// descriptor's hangup status before peeking so buffered application bytes do
/// not hide a half-close from the runtime's bounded read-HUP drain.
pub fn socket_read_closed_raw(fd: std::os::fd::RawFd) -> io::Result<bool> {
    if fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket descriptor must be non-negative",
        ));
    }
    #[cfg(target_os = "linux")]
    let read_events = libc::POLLIN | libc::POLLRDHUP;
    #[cfg(not(target_os = "linux"))]
    let read_events = libc::POLLIN;
    let mut descriptor = super::os::PollDescriptor::new(fd, read_events);
    super::os::poll_descriptors(std::slice::from_mut(&mut descriptor), Duration::ZERO)?;
    #[cfg(target_os = "linux")]
    let closed_events = libc::POLLHUP | libc::POLLRDHUP;
    #[cfg(not(target_os = "linux"))]
    let closed_events = libc::POLLHUP;
    if descriptor.revents() & closed_events != 0 {
        return Ok(true);
    }
    let mut byte = [0u8; 1];
    // SAFETY: fd is borrowed for this nonblocking peek, and byte is a valid
    // writable buffer for one byte. MSG_PEEK leaves application data queued.
    let result = unsafe {
        libc::recv(
            fd,
            byte.as_mut_ptr().cast::<libc::c_void>(),
            byte.len(),
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    if result > 0 {
        return Ok(false);
    }
    let error = io::Error::last_os_error();
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => Ok(false),
        _ => Err(error),
    }
}

pub fn send_timeout(fd: BorrowedFd<'_>) -> io::Result<Duration> {
    let mut timeout = std::mem::MaybeUninit::<libc::timeval>::zeroed();
    let mut len = std::mem::size_of::<libc::timeval>() as libc::socklen_t;
    // SAFETY: timeout and len are writable for the exact SO_SNDTIMEO value.
    if unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDTIMEO,
            timeout.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if len as usize != std::mem::size_of::<libc::timeval>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SO_SNDTIMEO returned an unexpected value size",
        ));
    }
    // SAFETY: the successful getsockopt initialized the complete timeval.
    let timeout = unsafe { timeout.assume_init() };
    if timeout.tv_sec < 0 || timeout.tv_usec < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SO_SNDTIMEO returned a negative duration",
        ));
    }
    Ok(Duration::from_secs(timeout.tv_sec as u64)
        .saturating_add(Duration::from_micros(timeout.tv_usec as u64)))
}

pub fn set_buffer_size_raw(
    fd: std::os::fd::RawFd,
    buffer: SocketBuffer,
    size: libc::c_int,
) -> io::Result<()> {
    if fd < 0 || size <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket descriptor and buffer size must be positive",
        ));
    }
    let option = match buffer {
        SocketBuffer::Send => libc::SO_SNDBUF,
        SocketBuffer::Receive => libc::SO_RCVBUF,
    };
    // SAFETY: fd is used only for this scalar socket option update and size is
    // passed through a correctly sized, initialized integer.
    if unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            (&size as *const libc::c_int).cast::<libc::c_void>(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    } == 0
    {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub fn chown_group(path: &Path, group: &str) -> io::Result<()> {
    let group = CString::new(group)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid group name"))?;
    // SAFETY: group is NUL-terminated and the returned entry is read immediately.
    let gid = unsafe {
        let entry = libc::getgrnam(group.as_ptr());
        if entry.is_null() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "unknown group"));
        }
        (*entry).gr_gid
    };
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid socket path"))?;
    // SAFETY: path is NUL-terminated; uid -1 preserves the existing owner.
    if unsafe { libc::chown(path.as_ptr(), u32::MAX, gid) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
