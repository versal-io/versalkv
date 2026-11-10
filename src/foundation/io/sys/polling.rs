use polling::{Event, PollMode, Poller};
use std::io;
use std::os::fd::RawFd;

pub fn add_with_mode(
    poller: &Poller,
    fd: RawFd,
    interest: Event,
    mode: PollMode,
) -> io::Result<()> {
    // SAFETY: callers register descriptors owned by the transport slab. The
    // transport deletes the descriptor from this poller before removing or
    // dropping the slab entry, so the raw fd remains valid while registered.
    unsafe { poller.add_with_mode(fd, interest, mode) }
}

pub fn delete_fd(poller: &Poller, fd: RawFd) -> io::Result<()> {
    // SAFETY: callers ensure fd was previously registered via add_with_mode
    // and is still valid (the owning transport holds the descriptor).
    unsafe { poller.delete(std::os::fd::BorrowedFd::borrow_raw(fd)) }
}
