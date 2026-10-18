//! Owned file-descriptor adapters.

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, RawFd};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipeFlags(libc::c_int);

impl PipeFlags {
    pub const NONE: Self = Self(0);
    pub const CLOEXEC: Self = Self(libc::O_CLOEXEC);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK);
    pub const CLOEXEC_NONBLOCK: Self = Self(libc::O_CLOEXEC | libc::O_NONBLOCK);

    pub const fn bits(self) -> libc::c_int {
        self.0
    }
}

pub struct OwnedPipe {
    pub read: File,
    pub write: File,
}

impl OwnedPipe {
    pub fn into_parts(self) -> (File, File) {
        (self.read, self.write)
    }
}

pub fn pipe(flags: PipeFlags) -> io::Result<OwnedPipe> {
    let mut fds = [-1; 2];
    if super::os::pipe2(&mut fds, flags.bits()) != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful pipe2 call returns two distinct owned descriptors.
    let read = unsafe { File::from_raw_fd(fds[0]) };
    // SAFETY: ownership of the second descriptor is transferred independently.
    let write = unsafe { File::from_raw_fd(fds[1]) };
    Ok(OwnedPipe { read, write })
}

#[cfg(test)]
pub(crate) fn pipe_with_background_payload(
    payload: Vec<u8>,
) -> io::Result<(File, std::thread::JoinHandle<io::Result<()>>)> {
    use std::io::Write;

    let (read, mut write) = pipe(PipeFlags::NONE)?.into_parts();
    let writer = std::thread::spawn(move || write.write_all(&payload));
    Ok((read, writer))
}

#[cfg(test)]
pub(crate) fn join_background_pipe_writer(writer: std::thread::JoinHandle<io::Result<()>>) {
    writer
        .join()
        .expect("background pipe writer panicked")
        .expect("background pipe writer failed");
}

#[cfg(test)]
pub(crate) fn join_background_pipe_writer_after_reader_close(
    writer: std::thread::JoinHandle<io::Result<()>>,
) {
    match writer.join().expect("background pipe writer panicked") {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
        Err(error) => panic!("background pipe writer failed: {error}"),
    }
}

pub fn descriptor_is_open(fd: RawFd) -> bool {
    if fd < 0 {
        return false;
    }
    // SAFETY: F_GETFD only queries descriptor metadata and accepts invalid
    // descriptor numbers, reporting them through EBADF.
    unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
}

#[cfg(test)]
pub(super) fn descriptor_identity(fd: RawFd) -> Option<(libc::dev_t, libc::ino_t)> {
    if fd < 0 {
        return None;
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: stat points to writable storage and fstat initializes it on success.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: successful fstat initialized every field of the stat structure.
    let stat = unsafe { stat.assume_init() };
    Some((stat.st_dev, stat.st_ino))
}

pub fn is_nonblocking(fd: BorrowedFd<'_>) -> io::Result<bool> {
    // SAFETY: fd is borrowed from a live owner and F_GETFL only reads flags.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(flags & libc::O_NONBLOCK != 0)
    }
}

pub fn set_nonblocking(fd: BorrowedFd<'_>, enabled: bool) -> io::Result<()> {
    // SAFETY: fd is borrowed from a live owner and F_GETFL only reads flags.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let updated = if enabled {
        flags | libc::O_NONBLOCK
    } else {
        flags & !libc::O_NONBLOCK
    };
    // SAFETY: fd remains live and the updated value preserves all flags except
    // the explicitly selected O_NONBLOCK bit.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, updated) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
