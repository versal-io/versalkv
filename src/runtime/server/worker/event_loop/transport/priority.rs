use crate::framework::capabilities::io::Token;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::time::Duration;

/// Dedicated epoll instance for priority connections (cluster bus, replication,
/// slot migration, and priority-subnet clients).
///
/// The main event loop wakes via a shared eventfd pair: the reader fd is
/// registered in both this poller and the main transport. When a priority fd
/// becomes ready, the poller writes to the eventfd, which interrupts the main
/// epoll and triggers an immediate priority drain.
pub(crate) struct PriorityPoller {
    poller: polling::Poller,
    events_buf: polling::Events,
    /// token → raw fd (for delete)
    fd_registry: HashMap<Token, RawFd>,
    /// Wake pair: writer is used internally to interrupt epoll_wait.
    wake_reader: std::os::unix::net::UnixStream,
    wake_writer: std::os::unix::net::UnixStream,
}

/// Readiness event from the priority epoll.
pub(crate) struct PriorityEvent {
    pub(crate) token: Token,
    pub(crate) readable: bool,
    pub(crate) writable: bool,
}

const WAKE_KEY: usize = 0;
const PRIORITY_FDS_CAPACITY: usize = 1024;

impl PriorityPoller {
    pub(crate) fn new() -> io::Result<Self> {
        let poller = polling::Poller::new()?;
        let (reader, writer) = std::os::unix::net::UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;

        // Register the wake reader fd.
        let wake_fd = reader.as_raw_fd();
        crate::foundation::io::sys::polling::add_with_mode(
            &poller,
            wake_fd,
            polling::Event::readable(WAKE_KEY),
            polling::PollMode::Level,
        )?;
        let events_buf = polling::Events::with_capacity(
            std::num::NonZeroUsize::new(PRIORITY_FDS_CAPACITY).unwrap(),
        );

        Ok(Self {
            poller,
            events_buf,
            fd_registry: HashMap::new(),
            wake_reader: reader,
            wake_writer: writer,
        })
    }

    pub(crate) fn wake_reader_fd(&self) -> RawFd {
        self.wake_reader.as_raw_fd()
    }

    /// Return a dup'd `File` from the wake reader for registration in the
    /// main transport. The duplicate is owned by the caller; the original
    /// stream stays alive within PriorityPoller.
    pub(crate) fn wake_reader_file(&self) -> io::Result<std::fs::File> {
        crate::foundation::io::sys::fd::dup_fd_to_file(self.wake_reader.as_fd())
    }

    /// Register a priority connection fd.
    pub(crate) fn register_fd(&mut self, token: Token, fd: RawFd) -> io::Result<()> {
        let key = priority_key(token);
        crate::foundation::io::sys::polling::add_with_mode(
            &self.poller,
            fd,
            polling::Event::all(key),
            polling::PollMode::Level,
        )?;
        self.fd_registry.insert(token, fd);
        let _ = self.wake_writer.write(&[1]);
        Ok(())
    }

    /// Delete a priority connection fd.
    pub(crate) fn delete_fd(&mut self, token: Token) -> io::Result<()> {
        if let Some(fd) = self.fd_registry.remove(&token) {
            crate::foundation::io::sys::polling::delete_fd(&self.poller, fd)?;
        }
        Ok(())
    }

    /// Modify the interest set for an already-registered priority fd.
    /// Returns `Ok(true)` if the token was registered, `Ok(false)` if it
    /// was not found (caller may need to register instead).
    pub(crate) fn modify_fd(
        &mut self,
        token: Token,
        readable: bool,
        writable: bool,
    ) -> io::Result<()> {
        if let Some(&fd) = self.fd_registry.get(&token) {
            let mut interest = polling::Event::none(priority_key(token));
            interest.readable = readable;
            interest.writable = writable;
            crate::foundation::io::sys::polling::add_with_mode(
                &self.poller,
                fd,
                interest,
                polling::PollMode::Level,
            )?;
        }
        Ok(())
    }

    /// Poll priority epoll. Returns readiness events.
    pub(crate) fn poll_events(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<Vec<PriorityEvent>> {
        self.events_buf.clear();
        self.poller.wait(&mut self.events_buf, timeout)?;

        let mut events = Vec::new();
        for ev in self.events_buf.iter() {
            if ev.key == WAKE_KEY {
                let mut buf = [0u8; 64];
                let _ = self.wake_reader.read(&mut buf);
                continue;
            }
            events.push(PriorityEvent {
                token: priority_token(ev.key),
                readable: ev.readable,
                writable: ev.writable,
            });
        }
        Ok(events)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.fd_registry.is_empty()
    }
}

fn priority_key(token: Token) -> usize {
    token.0
}

fn priority_token(key: usize) -> Token {
    Token(key)
}
