use std::fs::File;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::os::fd::RawFd;
use std::path::Path;
use std::time::Duration;

use super::{ThreadIoHandle, TlsClientConfig, TlsConfig, TlsContextBatch, TransportWakeHandle};

#[derive(Debug)]
pub(crate) enum ReadOutcome {
    Quiescent,
    BudgetExhausted,
    PeerClosed,
    Failed(io::Error),
}

#[derive(Debug)]
pub(crate) enum WriteOutcome {
    Complete,
    BudgetExhausted,
    Backpressured,
    Failed(io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportId(NonZeroUsize);

impl TransportId {
    pub fn new(raw: usize) -> Option<Self> {
        NonZeroUsize::new(raw).map(Self)
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListenerHandle {
    pub id: TransportId,
    pub inherited_fd: RawFd,
}

#[derive(Debug)]
pub struct OwnedPollSource {
    pub file: File,
    pub mode: PollSourceMode,
}

impl OwnedPollSource {
    pub fn readable(file: File) -> Self {
        Self {
            file,
            mode: PollSourceMode::Readable,
        }
    }

    pub fn completion(file: File) -> Self {
        Self {
            file,
            mode: PollSourceMode::Completion,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollSourceMode {
    Readable,
    Completion,
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectedPeer {
    pub id: TransportId,
    pub peer_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interest {
    None,
    Readable,
    Writable,
    ReadWritable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportPollMode {
    #[cfg(test)]
    Edge,
    Level,
}

#[derive(Debug, Clone, Copy)]
pub struct TransportEvent {
    pub id: TransportId,
    pub readable: bool,
    pub writable: bool,
    pub read_closed: bool,
    pub write_closed: bool,
    pub error: bool,
}

pub(crate) trait TransportPoller: Send {
    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<TransportEvent>>;
}

/// Exclusive ownership of a backend poller and its reusable event buffer.
/// The transport cannot mutate registrations while this value is detached.
pub struct TransportPollState {
    poller: Box<dyn TransportPoller>,
}

impl TransportPollState {
    pub(crate) fn new(poller: impl TransportPoller + 'static) -> Self {
        Self {
            poller: Box::new(poller),
        }
    }

    pub(crate) fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<TransportEvent>> {
        self.poller.poll(timeout)
    }
}

impl TransportEvent {
    pub fn ready(
        id: TransportId,
        readable: bool,
        writable: bool,
        read_closed: bool,
        write_closed: bool,
        error: bool,
    ) -> Self {
        Self {
            id,
            readable,
            writable,
            read_closed,
            write_closed,
            error,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UnixBindOptions {
    pub perm: u32,
    pub group: Option<String>,
}

pub trait TransportRuntime {
    /// None means the TLS handshake has not completed or this is not TLS.
    fn tls_peer_names(&self, _id: TransportId, _use_uri: bool) -> Option<Vec<Vec<u8>>> {
        None
    }
    fn swap_tls_contexts(&mut self, _batch: TlsContextBatch) -> io::Result<TlsContextBatch> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS context batches are not supported by this transport",
        ))
    }
    /// Configure the backlog used for future TCP listener binds.
    fn set_tcp_backlog(&mut self, _backlog: i32) -> io::Result<()> {
        Ok(())
    }
    fn bind_tcp(&mut self, addr: SocketAddr) -> io::Result<ListenerHandle>;
    /// Install TLS material used by later TLS listener binds. Backends that do
    /// not implement TLS must reject the request instead of binding plaintext.
    fn configure_tls_server(&mut self, _config: TlsConfig) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS server contexts are not supported by this transport",
        ))
    }
    fn bind_tls(&mut self, _addr: SocketAddr) -> io::Result<ListenerHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS listeners are not supported by this transport",
        ))
    }
    fn configure_tls_client(&mut self, _config: TlsClientConfig) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS client contexts are not supported by this transport",
        ))
    }
    fn connect_tls(&mut self, addr: SocketAddr, server_name: &str) -> io::Result<ConnectedPeer> {
        self.connect_tls_bound(addr, None, server_name)
    }
    fn connect_tls_bound(
        &mut self,
        _addr: SocketAddr,
        _source_ip: Option<std::net::IpAddr>,
        _server_name: &str,
    ) -> io::Result<ConnectedPeer> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS connections are not supported by this transport",
        ))
    }
    /// Bind a Linux MPTCP listener. Backends without a native MPTCP socket
    /// capability must return Unsupported instead of silently falling back to
    /// TCP.
    fn bind_mptcp(&mut self, _addr: SocketAddr) -> io::Result<ListenerHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "MPTCP listeners are not supported by this transport",
        ))
    }
    fn bind_unix(&mut self, path: &Path, options: UnixBindOptions) -> io::Result<ListenerHandle>;
    /// Transfer ownership of a nonblocking pollable descriptor to the poller.
    /// The descriptor remains owned by the transport until `remove` succeeds.
    fn register_owned_source(&mut self, source: OwnedPollSource) -> io::Result<TransportId> {
        drop(source);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "owned poll sources are not supported by this transport",
        ))
    }
    fn set_source_enabled(&mut self, _id: TransportId, _enabled: bool) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "owned poll sources are not supported by this transport",
        ))
    }
    fn connect_tcp(&mut self, addr: SocketAddr) -> io::Result<ConnectedPeer>;
    fn connect_mptcp_bound(
        &mut self,
        _addr: SocketAddr,
        _source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<ConnectedPeer> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "MPTCP connections are not supported by this transport",
        ))
    }
    fn connect_tcp_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<ConnectedPeer> {
        if source_ip.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "source-bound TCP connections are not supported by this transport",
            ));
        }
        self.connect_tcp(addr)
    }
    fn accept(&mut self, listener: TransportId) -> io::Result<Option<ConnectedPeer>>;
    /// Whether an accepted stream completed its transport handshake.
    /// Plain transports are ready immediately; TLS must report certificate
    /// validation and handshake completion before link establishment.
    fn is_connection_ready(&self, _id: TransportId) -> bool {
        true
    }
    fn read(&mut self, id: TransportId, dst: &mut [u8]) -> io::Result<usize>;
    fn write(&mut self, id: TransportId, src: &[u8]) -> io::Result<usize>;
    fn write_vectored(&mut self, id: TransportId, src: &[IoSlice<'_>]) -> io::Result<usize>;
    fn apply_test_tcp_buffer_limits(&mut self, id: TransportId) -> io::Result<()>;
    /// Configure TCP keepalive on one registered stream. Non-TCP backends may
    /// leave this as a no-op; the concrete polling backend rejects non-TCP streams.
    fn apply_tcp_keepalive(&mut self, _id: TransportId, _idle: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
    /// Change TCP_NODELAY on a single registered TCP stream.
    fn set_tcp_nodelay(&mut self, _id: TransportId, _enabled: bool) -> io::Result<()> {
        Ok(())
    }
    fn set_interest(&mut self, id: TransportId, interest: Interest) -> io::Result<()>;
    /// Whether a transport has application bytes buffered above the kernel
    /// readiness layer (for example decrypted plaintext inside TLS).
    fn has_pending_data(&self) -> bool {
        false
    }
    /// Append synthetic readable events for transports with pending
    /// application data and return the number appended.
    fn process_pending_data(&mut self, _out: &mut Vec<TransportEvent>) -> usize {
        0
    }
    fn take_poll_state(&mut self) -> io::Result<TransportPollState> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "poll ownership transfer is not supported by this transport",
        ))
    }
    fn restore_poll_state(&mut self, _state: TransportPollState) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "poll ownership transfer is not supported by this transport",
        ))
    }
    /// Complete backend-specific processing for events returned by a detached
    /// poll. Backends may need to inspect registered descriptors or drain
    /// wake sources after poll ownership returns to the event-loop thread.
    fn finalize_polled_events(&mut self, _events: &mut [TransportEvent]) {}
    fn poll(
        &mut self,
        out: &mut Vec<TransportEvent>,
        timeout: Option<Duration>,
    ) -> io::Result<usize>;
    fn take_connect_error(&mut self, id: TransportId) -> io::Result<Option<io::Error>>;
    fn shutdown(&mut self, id: TransportId) -> io::Result<()>;
    fn remove(&mut self, id: TransportId) -> io::Result<()>;
    fn raw_fd(&self, id: TransportId) -> io::Result<RawFd>;
}

pub(crate) trait TransportWorkerBackend {
    fn duplicate_stream(&self, id: TransportId) -> io::Result<ThreadIoHandle>;

    fn register_worker_waker(&mut self) -> io::Result<(TransportId, TransportWakeHandle)>;
}

#[cfg(test)]
pub(crate) trait TransportTestBackend {
    fn deregister_for_test(&mut self, id: TransportId) -> io::Result<()>;
    fn reregister_for_test(&mut self, id: TransportId) -> io::Result<()>;
    fn source_enabled_for_test(&self, id: TransportId) -> io::Result<bool>;
    fn fail_register_source_for_test(&mut self);
    fn fail_set_source_enabled_for_test(&mut self, enabled: bool);
    fn fail_remove_for_test(&mut self);
    fn interest_requests(&self) -> &[(TransportId, Interest)];
    fn clear_interest_requests(&mut self);
}
