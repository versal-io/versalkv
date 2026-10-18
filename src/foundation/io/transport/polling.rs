use super::api::TransportPoller;
use super::thread_io::TransportWake;
use super::{
    ConnectedPeer, Interest, ListenerHandle, OwnedPollSource, PollSourceMode, ThreadIoHandle,
    TlsClientConfig, TlsConfig, TransportEvent, TransportId, TransportPollMode, TransportPollState,
    TransportRuntime, TransportWakeHandle, TransportWorkerBackend, UnixBindOptions,
};
use openssl::ssl::{ErrorCode, Ssl, SslAcceptor, SslConnector, SslStream};
use polling::{Event, Events, PollMode, Poller};
use slab::Slab;
use socket2::{Domain, Protocol, SockRef, Socket, TcpKeepalive, Type};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

enum Stream {
    Tcp(TcpStream),
    Tls(SslStream<TcpStream>),
    Unix(std::os::unix::net::UnixStream),
}

impl Read for Stream {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(dst),
            Self::Tls(stream) => stream.read(dst),
            Self::Unix(stream) => stream.read(dst),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(src),
            Self::Tls(stream) => stream.write(src),
            Self::Unix(stream) => stream.write(src),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_vectored(&mut self, src: &[IoSlice<'_>]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write_vectored(src),
            Self::Tls(stream) => stream.write_vectored(src),
            Self::Unix(stream) => stream.write_vectored(src),
        }
    }
}

enum Registered {
    TcpListener(TcpListener),
    TlsListener(TcpListener),
    UnixListener(std::os::unix::net::UnixListener),
    Stream(Stream),
    OwnedSource {
        file: File,
        mode: PollSourceMode,
        poll_registered: bool,
    },
    Waker(std::os::unix::net::UnixStream),
}

#[derive(Clone, Copy)]
struct TcpBufferLimits {
    send_bytes: usize,
    receive_bytes: usize,
}

struct PollingPoller {
    poller: Arc<Poller>,
    events: Events,
}

impl TransportPoller for PollingPoller {
    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<TransportEvent>> {
        self.events.clear();
        match self.poller.wait(&mut self.events, timeout) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(Vec::new()),
            Err(error) => return Err(error),
        }
        Ok(coalesce_ready_events(self.events.iter().map(|event| {
            let failed = event.is_err().unwrap_or(false);
            TransportEvent::ready(
                TransportId::new(event.key).expect("polling token is nonzero"),
                event.readable,
                event.writable,
                event.is_interrupt(),
                false,
                failed,
            )
        })))
    }
}

// kqueue emits separate read/write filters; the runtime dispatches one event
// per descriptor so reply barriers see both flags in the same readiness turn.
fn coalesce_ready_events(events: impl IntoIterator<Item = TransportEvent>) -> Vec<TransportEvent> {
    let mut positions = HashMap::new();
    let mut ready: Vec<TransportEvent> = Vec::new();
    for event in events {
        if let Some(&position) = positions.get(&event.id) {
            let previous: &mut TransportEvent = &mut ready[position];
            previous.readable |= event.readable;
            previous.writable |= event.writable;
            previous.read_closed |= event.read_closed;
            previous.write_closed |= event.write_closed;
            previous.error |= event.error;
        } else {
            positions.insert(event.id, ready.len());
            ready.push(event);
        }
    }
    ready
}

struct StreamWake {
    writer: Mutex<std::os::unix::net::UnixStream>,
}

impl TransportWake for StreamWake {
    fn wake(&self) -> io::Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("worker waker lock poisoned"))?;
        match writer.write(&[1]) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error),
        }
    }
}

pub(crate) struct PollingTransportRuntime {
    poll_state: Option<TransportPollState>,
    poller: Arc<Poller>,
    mode: TransportPollMode,
    registered: Slab<Registered>,
    tcp_buffer_limits: Option<TcpBufferLimits>,
    tcp_backlog: i32,
    tls_server: Option<SslAcceptor>,
    tls_client: Option<SslConnector>,
    #[cfg(test)]
    interest_requests: Vec<(TransportId, Interest)>,
    #[cfg(test)]
    register_owned_source_error: Option<io::ErrorKind>,
    #[cfg(test)]
    set_source_enabled_errors: HashMap<bool, io::ErrorKind>,
    #[cfg(test)]
    remove_error: Option<io::ErrorKind>,
}

#[cfg(target_os = "linux")]
fn mptcp_protocol() -> io::Result<Protocol> {
    Ok(Protocol::from(262))
}

#[cfg(not(target_os = "linux"))]
fn mptcp_protocol() -> io::Result<Protocol> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "MPTCP requires a Linux kernel socket capability",
    ))
}

impl PollingTransportRuntime {
    pub(crate) fn new() -> io::Result<Self> {
        Self::with_mode(TransportPollMode::Level)
    }

    pub(crate) fn with_mode(mode: TransportPollMode) -> io::Result<Self> {
        Self::with_mode_and_tcp_buffer_limits(mode, test_tcp_buffer_limits_from_env()?)
    }

    fn with_mode_and_tcp_buffer_limits(
        mode: TransportPollMode,
        tcp_buffer_limits: Option<TcpBufferLimits>,
    ) -> io::Result<Self> {
        let poller = Arc::new(Poller::new()?);
        validate_mode(&poller, mode)?;
        Ok(Self {
            poll_state: Some(TransportPollState::new(PollingPoller {
                poller: Arc::clone(&poller),
                events: Events::with_capacity(NonZeroUsize::new(1024).unwrap()),
            })),
            poller,
            mode,
            registered: Slab::new(),
            tcp_buffer_limits,
            tcp_backlog: 511,
            tls_server: None,
            tls_client: None,
            #[cfg(test)]
            interest_requests: Vec::new(),
            #[cfg(test)]
            register_owned_source_error: None,
            #[cfg(test)]
            set_source_enabled_errors: HashMap::new(),
            #[cfg(test)]
            remove_error: None,
        })
    }

    fn ensure_poll_attached(&self) -> io::Result<()> {
        if self.poll_state.is_some() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "poll state is owned by an IO worker",
            ))
        }
    }

    fn poll_mode(&self) -> PollMode {
        match self.mode {
            #[cfg(test)]
            TransportPollMode::Edge => PollMode::Edge,
            TransportPollMode::Level => PollMode::Level,
        }
    }

    fn polling_event(id: TransportId, interest: Interest) -> Event {
        let event = match interest {
            Interest::Readable => Event::readable(id.get()),
            Interest::Writable => Event::writable(id.get()),
            Interest::ReadWritable => Event::all(id.get()),
        };
        event.with_interrupt()
    }

    fn add_source_fd(
        &self,
        fd: std::os::fd::RawFd,
        id: TransportId,
        interest: Interest,
    ) -> io::Result<()> {
        crate::foundation::io::sys::polling::add_with_mode(
            &self.poller,
            fd,
            Self::polling_event(id, interest),
            self.poll_mode(),
        )
    }

    fn reregister(&mut self, id: TransportId, interest: Interest) -> io::Result<()> {
        self.ensure_poll_attached()?;
        let poller = Arc::clone(&self.poller);
        let mode = self.poll_mode();
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::TcpListener(listener)) => {
                poller.modify_with_mode(listener, Self::polling_event(id, interest), mode)
            }
            Some(Registered::TlsListener(listener)) => {
                poller.modify_with_mode(listener, Self::polling_event(id, interest), mode)
            }
            Some(Registered::UnixListener(listener)) => {
                poller.modify_with_mode(listener, Self::polling_event(id, interest), mode)
            }
            Some(Registered::Stream(Stream::Tcp(stream))) => {
                poller.modify_with_mode(stream, Self::polling_event(id, interest), mode)
            }
            Some(Registered::Stream(Stream::Tls(stream))) => {
                let interest = if stream.ssl().is_init_finished() {
                    interest
                } else {
                    Interest::ReadWritable
                };
                poller.modify_with_mode(stream.get_ref(), Self::polling_event(id, interest), mode)
            }
            Some(Registered::Stream(Stream::Unix(stream))) => {
                poller.modify_with_mode(stream, Self::polling_event(id, interest), mode)
            }
            Some(Registered::OwnedSource { .. }) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "use set_source_enabled for owned poll sources",
            )),
            Some(Registered::Waker(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot change waker interest",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn bind_tcp_listener(&mut self, addr: SocketAddr, tls: bool) -> io::Result<ListenerHandle> {
        self.ensure_poll_attached()?;
        let socket = tcp_socket(addr, Protocol::TCP)?;
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(self.tcp_backlog)?;
        let listener = TcpListener::from(socket);
        let raw_fd = listener.as_raw_fd();
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(listener.as_raw_fd(), id, Interest::Readable)?;
        let inserted = if tls {
            self.registered.insert(Registered::TlsListener(listener))
        } else {
            self.registered.insert(Registered::TcpListener(listener))
        };
        debug_assert_eq!(inserted, key);
        Ok(ListenerHandle {
            id,
            inherited_fd: raw_fd,
        })
    }

    fn drain_waker(&mut self, id: TransportId) {
        let Some(Registered::Waker(reader)) = self.registered.get_mut(id.get() - 1) else {
            return;
        };
        let mut buf = [0u8; 64];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(_) => return,
            }
        }
    }

    fn finalize_polled_events_impl(&mut self, events: &mut [TransportEvent]) {
        for event in events {
            // Do not probe a failed socket before the runtime consumes
            // SO_ERROR. On Linux, probing a refused nonblocking connect with
            // recv(MSG_PEEK) can clear the pending connect error and leave
            // the higher-level failure path with only a generic message.
            if event.readable && !event.error && self.stream_read_closed(event.id) {
                event.read_closed = true;
            }
            self.drain_waker(event.id);
        }
    }
}

impl TransportWorkerBackend for PollingTransportRuntime {
    fn duplicate_stream(&self, id: TransportId) -> io::Result<ThreadIoHandle> {
        match self.registered.get(id.get() - 1) {
            Some(Registered::Stream(Stream::Tcp(stream))) => {
                let stream: std::net::TcpStream = SockRef::from(stream).try_clone()?.into();
                stream.set_nonblocking(true)?;
                Ok(ThreadIoHandle::Tcp(stream))
            }
            Some(Registered::Stream(Stream::Unix(stream))) => {
                let stream: std::os::unix::net::UnixStream =
                    SockRef::from(stream).try_clone()?.into();
                stream.set_nonblocking(true)?;
                Ok(ThreadIoHandle::Unix(stream))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport is not a stream",
            )),
        }
    }

    fn register_worker_waker(&mut self) -> io::Result<(TransportId, TransportWakeHandle)> {
        self.ensure_poll_attached()?;
        let (reader, writer) = std::os::unix::net::UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(reader.as_raw_fd(), id, Interest::Readable)?;
        let inserted = self.registered.insert(Registered::Waker(reader));
        debug_assert_eq!(inserted, key);
        Ok((
            id,
            TransportWakeHandle::new(Arc::new(StreamWake {
                writer: Mutex::new(writer),
            })),
        ))
    }
}

impl PollingTransportRuntime {
    fn stream_read_closed(&self, id: TransportId) -> bool {
        let fd = match self.registered.get(id.get() - 1) {
            Some(Registered::Stream(Stream::Tcp(stream))) => stream.as_raw_fd(),
            Some(Registered::Stream(Stream::Tls(stream))) => stream.get_ref().as_raw_fd(),
            Some(Registered::Stream(Stream::Unix(stream))) => stream.as_raw_fd(),
            Some(Registered::OwnedSource { file, .. }) => {
                let mut descriptor = crate::foundation::io::sys::os::PollDescriptor::new(
                    file.as_raw_fd(),
                    libc::POLLIN,
                );
                return crate::foundation::io::sys::os::poll_descriptors(
                    std::slice::from_mut(&mut descriptor),
                    Duration::ZERO,
                )
                .is_ok_and(|_| descriptor.revents() & libc::POLLHUP != 0);
            }
            _ => return false,
        };
        crate::foundation::io::sys::socket::socket_read_closed_raw(fd).unwrap_or(false)
    }
}

impl TransportRuntime for PollingTransportRuntime {
    fn set_tcp_backlog(&mut self, backlog: i32) -> io::Result<()> {
        if backlog < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP backlog must be non-negative",
            ));
        }
        self.tcp_backlog = backlog;
        Ok(())
    }

    fn bind_tcp(&mut self, addr: SocketAddr) -> io::Result<ListenerHandle> {
        self.bind_tcp_listener(addr, false)
    }

    fn configure_tls_server(&mut self, config: TlsConfig) -> io::Result<()> {
        self.tls_server = config.server_acceptor()?;
        Ok(())
    }

    fn bind_tls(&mut self, addr: SocketAddr) -> io::Result<ListenerHandle> {
        if self.tls_server.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS server context must be configured before binding a TLS listener",
            ));
        }
        self.bind_tcp_listener(addr, true)
    }

    fn configure_tls_client(&mut self, config: TlsClientConfig) -> io::Result<()> {
        self.tls_client = Some(config.connector()?);
        Ok(())
    }

    fn bind_mptcp(&mut self, addr: SocketAddr) -> io::Result<ListenerHandle> {
        self.ensure_poll_attached()?;
        let socket = tcp_socket(addr, mptcp_protocol()?)?;
        socket.set_nonblocking(true).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("configure MPTCP nonblocking: {error}"),
            )
        })?;
        socket
            .bind(&addr.into())
            .map_err(|error| io::Error::new(error.kind(), format!("bind MPTCP socket: {error}")))?;
        socket.listen(self.tcp_backlog).map_err(|error| {
            io::Error::new(error.kind(), format!("listen on MPTCP socket: {error}"))
        })?;
        let listener = TcpListener::from(socket);
        let raw_fd = listener.as_raw_fd();
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(listener.as_raw_fd(), id, Interest::Readable)?;
        let inserted = self.registered.insert(Registered::TcpListener(listener));
        debug_assert_eq!(inserted, key);
        Ok(ListenerHandle {
            id,
            inherited_fd: raw_fd,
        })
    }

    fn bind_unix(&mut self, path: &Path, options: UnixBindOptions) -> io::Result<ListenerHandle> {
        self.ensure_poll_attached()?;
        let _ = std::fs::remove_file(path);
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        if options.perm != 0 {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(options.perm))?;
        }
        if let Some(group) = options.group.as_deref() {
            crate::foundation::io::sys::socket::chown_group(path, group)?;
        }
        let raw_fd = listener.as_raw_fd();
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(listener.as_raw_fd(), id, Interest::Readable)?;
        let inserted = self.registered.insert(Registered::UnixListener(listener));
        debug_assert_eq!(inserted, key);
        Ok(ListenerHandle {
            id,
            inherited_fd: raw_fd,
        })
    }

    fn register_owned_source(&mut self, source: OwnedPollSource) -> io::Result<TransportId> {
        self.ensure_poll_attached()?;
        #[cfg(test)]
        if let Some(kind) = self.register_owned_source_error.take() {
            return Err(kind.into());
        }
        if source.file.metadata()?.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "regular files cannot be registered as owned poll sources",
            ));
        }
        let OwnedPollSource { file, mode } = source;
        crate::foundation::io::sys::fd::set_nonblocking(file.as_fd(), true)?;
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(file.as_raw_fd(), id, Interest::Readable)?;
        let inserted = self.registered.insert(Registered::OwnedSource {
            file,
            mode,
            poll_registered: true,
        });
        debug_assert_eq!(inserted, key);
        Ok(id)
    }

    fn set_source_enabled(&mut self, id: TransportId, enabled: bool) -> io::Result<()> {
        self.ensure_poll_attached()?;
        #[cfg(test)]
        if let Some(kind) = self.set_source_enabled_errors.remove(&enabled) {
            return Err(kind.into());
        }
        let mode = self.poll_mode();
        let Some(Registered::OwnedSource {
            file,
            mode: _,
            poll_registered,
        }) = self.registered.get_mut(id.get() - 1)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport is not an owned poll source",
            ));
        };
        if *poll_registered == enabled {
            return Ok(());
        }
        if enabled {
            crate::foundation::io::sys::polling::add_with_mode(
                &self.poller,
                file.as_raw_fd(),
                Self::polling_event(id, Interest::Readable),
                mode,
            )?;
        } else {
            self.poller.delete(&*file)?;
        }
        *poll_registered = enabled;
        Ok(())
    }

    fn connect_tcp(&mut self, addr: SocketAddr) -> io::Result<ConnectedPeer> {
        self.connect_tcp_bound(addr, None)
    }

    fn connect_tls_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
        server_name: &str,
    ) -> io::Result<ConnectedPeer> {
        self.ensure_poll_attached()?;
        let connector = self.tls_client.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS client context must be configured before connecting",
            )
        })?;
        let tcp = self.open_tcp_stream(addr, source_ip, Protocol::TCP)?;
        let mut ssl = connector
            .configure()
            .map_err(io::Error::other)?
            .into_ssl(server_name)
            .map_err(io::Error::other)?;
        ssl.set_connect_state();
        let mut stream = SslStream::new(ssl, tcp).map_err(io::Error::other)?;
        match stream.connect() {
            Ok(()) => {}
            Err(error) if matches!(error.code(), ErrorCode::WANT_READ | ErrorCode::WANT_WRITE) => {}
            Err(error) => return Err(io::Error::other(error)),
        }
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(stream.get_ref().as_raw_fd(), id, Interest::ReadWritable)?;
        let inserted = self
            .registered
            .insert(Registered::Stream(Stream::Tls(stream)));
        debug_assert_eq!(inserted, key);
        Ok(ConnectedPeer {
            id,
            peer_addr: addr,
        })
    }

    fn connect_tcp_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<ConnectedPeer> {
        self.ensure_poll_attached()?;
        let stream = self.open_tcp_stream(addr, source_ip, Protocol::TCP)?;
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(stream.as_raw_fd(), id, Interest::ReadWritable)?;
        let inserted = self
            .registered
            .insert(Registered::Stream(Stream::Tcp(stream)));
        debug_assert_eq!(inserted, key);
        Ok(ConnectedPeer {
            id,
            peer_addr: addr,
        })
    }

    fn connect_mptcp_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<ConnectedPeer> {
        self.ensure_poll_attached()?;
        let stream = self.open_tcp_stream(addr, source_ip, mptcp_protocol()?)?;
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        self.add_source_fd(stream.as_raw_fd(), id, Interest::ReadWritable)?;
        let inserted = self
            .registered
            .insert(Registered::Stream(Stream::Tcp(stream)));
        debug_assert_eq!(inserted, key);
        Ok(ConnectedPeer {
            id,
            peer_addr: addr,
        })
    }

    fn accept(&mut self, listener: TransportId) -> io::Result<Option<ConnectedPeer>> {
        self.ensure_poll_attached()?;
        enum AcceptedStream {
            Tcp(TcpStream),
            Tls(TcpStream),
            Unix(std::os::unix::net::UnixStream),
        }

        let accepted = match self.registered.get_mut(listener.get() - 1) {
            Some(Registered::TcpListener(listener)) => listener
                .accept()
                .map(|(stream, addr)| (AcceptedStream::Tcp(stream), addr)),
            Some(Registered::TlsListener(listener)) => listener
                .accept()
                .map(|(stream, addr)| (AcceptedStream::Tls(stream), addr)),
            Some(Registered::UnixListener(listener)) => listener.accept().map(|(stream, _)| {
                (
                    AcceptedStream::Unix(stream),
                    SocketAddr::from(([127, 0, 0, 1], 0)),
                )
            }),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "not a listener",
                ))
            }
        };
        let (accepted, peer_addr) = match accepted {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error),
        };
        let stream = match accepted {
            AcceptedStream::Tcp(stream) => Stream::Tcp(stream),
            AcceptedStream::Tls(stream) => {
                let acceptor = self.tls_server.as_ref().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "TLS listener has no configured server context",
                    )
                })?;
                let mut ssl = Ssl::new(acceptor.context()).map_err(io::Error::other)?;
                ssl.set_accept_state();
                Stream::Tls(SslStream::new(ssl, stream).map_err(io::Error::other)?)
            }
            AcceptedStream::Unix(stream) => Stream::Unix(stream),
        };
        apply_accepted_stream_options(&stream, self.tcp_buffer_limits)?;
        let key = self.registered.vacant_key();
        let id = TransportId::new(key + 1).expect("slab index plus one is nonzero");
        match &stream {
            Stream::Tcp(stream) => {
                self.add_source_fd(stream.as_raw_fd(), id, Interest::Readable)?
            }
            Stream::Tls(stream) => {
                self.add_source_fd(stream.get_ref().as_raw_fd(), id, Interest::ReadWritable)?
            }
            Stream::Unix(stream) => {
                self.add_source_fd(stream.as_raw_fd(), id, Interest::Readable)?
            }
        }
        let inserted = self.registered.insert(Registered::Stream(stream));
        debug_assert_eq!(inserted, key);
        Ok(Some(ConnectedPeer { id, peer_addr }))
    }

    fn read(&mut self, id: TransportId, dst: &mut [u8]) -> io::Result<usize> {
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::Stream(stream)) => stream.read(dst),
            Some(Registered::OwnedSource {
                file,
                mode: PollSourceMode::Readable,
                ..
            }) => file.read(dst),
            Some(Registered::OwnedSource {
                mode: PollSourceMode::Completion,
                ..
            }) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "completion sources do not support payload reads",
            )),
            Some(Registered::Waker(reader)) => reader.read(dst),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn write(&mut self, id: TransportId, src: &[u8]) -> io::Result<usize> {
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::Stream(stream)) => stream.write(src),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn write_vectored(&mut self, id: TransportId, src: &[IoSlice<'_>]) -> io::Result<usize> {
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::Stream(stream)) => stream.write_vectored(src),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn apply_test_tcp_buffer_limits(&mut self, id: TransportId) -> io::Result<()> {
        let Some(limits) = self.tcp_buffer_limits else {
            return Ok(());
        };
        let stream = tcp_stream_ref(self.registered.get(id.get() - 1))?;
        let socket = SockRef::from(stream);
        socket.set_send_buffer_size(limits.send_bytes)?;
        socket.set_recv_buffer_size(limits.receive_bytes)
    }

    fn apply_tcp_keepalive(&mut self, id: TransportId, idle: Option<Duration>) -> io::Result<()> {
        let stream = tcp_stream_ref(self.registered.get(id.get() - 1))?;
        let socket = SockRef::from(stream);
        match idle {
            Some(idle) => socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(idle)),
            None => socket.set_keepalive(false),
        }
    }

    fn set_tcp_nodelay(&mut self, id: TransportId, enabled: bool) -> io::Result<()> {
        tcp_stream_ref(self.registered.get(id.get() - 1))?.set_nodelay(enabled)
    }

    fn set_interest(&mut self, id: TransportId, interest: Interest) -> io::Result<()> {
        #[cfg(test)]
        self.interest_requests.push((id, interest));
        self.reregister(id, interest)
    }

    fn has_pending_data(&self) -> bool {
        self.registered.iter().any(|(_, registered)| {
            matches!(registered, Registered::Stream(Stream::Tls(stream)) if stream.ssl().pending() > 0)
        })
    }

    fn process_pending_data(&mut self, out: &mut Vec<TransportEvent>) -> usize {
        let before = out.len();
        out.extend(
            self.registered
                .iter()
                .filter(|(_, registered)| {
                    matches!(*registered, Registered::Stream(Stream::Tls(stream)) if stream.ssl().pending() > 0)
                })
                .map(|(index, _)| {
                    TransportEvent::ready(
                        TransportId::new(index + 1).expect("slab index plus one is nonzero"),
                        true,
                        false,
                        false,
                        false,
                        false,
                    )
                }),
        );
        out.len().saturating_sub(before)
    }

    fn take_poll_state(&mut self) -> io::Result<TransportPollState> {
        self.poll_state.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "poll state is already owned by an IO worker",
            )
        })
    }

    fn restore_poll_state(&mut self, state: TransportPollState) -> io::Result<()> {
        if self.poll_state.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "transport already owns a poll state",
            ));
        }
        self.poll_state = Some(state);
        Ok(())
    }

    fn finalize_polled_events(&mut self, events: &mut [TransportEvent]) {
        self.finalize_polled_events_impl(events);
    }

    fn poll(
        &mut self,
        out: &mut Vec<TransportEvent>,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let state = self.poll_state.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "poll state is owned by an IO worker",
            )
        })?;
        let mut events = state.poll(timeout)?;
        self.finalize_polled_events_impl(&mut events);
        out.extend(events);
        Ok(out.len())
    }

    fn take_connect_error(&mut self, id: TransportId) -> io::Result<Option<io::Error>> {
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::Stream(Stream::Tcp(stream))) => stream.take_error(),
            Some(Registered::Stream(Stream::Tls(stream))) => stream.get_ref().take_error(),
            Some(Registered::Stream(Stream::Unix(stream))) => stream.take_error(),
            _ => Ok(Some(io::Error::new(
                io::ErrorKind::NotFound,
                "not a stream",
            ))),
        }
    }

    fn shutdown(&mut self, id: TransportId) -> io::Result<()> {
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::Stream(Stream::Tcp(stream))) => {
                stream.shutdown(std::net::Shutdown::Both)
            }
            Some(Registered::Stream(Stream::Tls(stream))) => {
                stream.get_ref().shutdown(std::net::Shutdown::Both)
            }
            Some(Registered::Stream(Stream::Unix(stream))) => {
                stream.shutdown(std::net::Shutdown::Both)
            }
            Some(Registered::Waker(_)) => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "not a stream"))
            }
            _ => Err(io::Error::new(io::ErrorKind::NotFound, "not a stream")),
        }
    }

    fn remove(&mut self, id: TransportId) -> io::Result<()> {
        self.ensure_poll_attached()?;
        #[cfg(test)]
        if let Some(kind) = self.remove_error.take() {
            return Err(kind.into());
        }
        {
            match self.registered.get_mut(id.get() - 1) {
                Some(Registered::TcpListener(listener)) => self.poller.delete(&*listener)?,
                Some(Registered::TlsListener(listener)) => self.poller.delete(&*listener)?,
                Some(Registered::UnixListener(listener)) => self.poller.delete(&*listener)?,
                Some(Registered::Stream(Stream::Tcp(stream))) => self.poller.delete(&*stream)?,
                Some(Registered::Stream(Stream::Tls(stream))) => {
                    self.poller.delete(stream.get_ref())?
                }
                Some(Registered::Stream(Stream::Unix(stream))) => self.poller.delete(&*stream)?,
                Some(Registered::OwnedSource {
                    file,
                    mode: _,
                    poll_registered,
                }) => {
                    if *poll_registered {
                        self.poller.delete(&*file)?;
                    }
                }
                Some(Registered::Waker(reader)) => self.poller.delete(&*reader)?,
                None => return Ok(()),
            }
        }
        self.registered.try_remove(id.get() - 1);
        Ok(())
    }

    fn raw_fd(&self, id: TransportId) -> io::Result<std::os::fd::RawFd> {
        match self.registered.get(id.get() - 1) {
            Some(Registered::Stream(Stream::Tcp(stream))) => Ok(stream.as_raw_fd()),
            Some(Registered::Stream(Stream::Tls(stream))) => Ok(stream.get_ref().as_raw_fd()),
            Some(Registered::Stream(Stream::Unix(_))) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "direct RDB child requires TCP stream",
            )),
            Some(Registered::OwnedSource { file, .. }) => Ok(file.as_raw_fd()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport stream not found",
            )),
        }
    }
}

#[cfg(test)]
impl super::TransportTestBackend for PollingTransportRuntime {
    fn deregister_for_test(&mut self, id: TransportId) -> io::Result<()> {
        self.ensure_poll_attached()?;
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::TcpListener(listener)) => self.poller.delete(&*listener),
            Some(Registered::TlsListener(listener)) => self.poller.delete(&*listener),
            Some(Registered::UnixListener(listener)) => self.poller.delete(&*listener),
            Some(Registered::Stream(Stream::Tcp(stream))) => self.poller.delete(&*stream),
            Some(Registered::Stream(Stream::Tls(stream))) => self.poller.delete(stream.get_ref()),
            Some(Registered::Stream(Stream::Unix(stream))) => self.poller.delete(&*stream),
            Some(Registered::OwnedSource {
                file,
                mode: _,
                poll_registered,
            }) => {
                if !*poll_registered {
                    return Ok(());
                }
                self.poller.delete(&*file)?;
                *poll_registered = false;
                Ok(())
            }
            Some(Registered::Waker(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot deregister a waker",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn reregister_for_test(&mut self, id: TransportId) -> io::Result<()> {
        self.ensure_poll_attached()?;
        let mode = self.poll_mode();
        match self.registered.get_mut(id.get() - 1) {
            Some(Registered::TcpListener(listener)) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    listener.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::TlsListener(listener)) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    listener.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::UnixListener(listener)) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    listener.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::Stream(Stream::Tcp(stream))) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    stream.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::Stream(Stream::Tls(stream))) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    stream.get_ref().as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::Stream(Stream::Unix(stream))) => {
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    stream.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )
            }
            Some(Registered::OwnedSource {
                file,
                mode: _,
                poll_registered,
            }) => {
                if *poll_registered {
                    return Ok(());
                }
                crate::foundation::io::sys::polling::add_with_mode(
                    &self.poller,
                    file.as_raw_fd(),
                    Self::polling_event(id, Interest::Readable),
                    mode,
                )?;
                *poll_registered = true;
                Ok(())
            }
            Some(Registered::Waker(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot reregister a waker",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn source_enabled_for_test(&self, id: TransportId) -> io::Result<bool> {
        match self.registered.get(id.get() - 1) {
            Some(Registered::OwnedSource {
                poll_registered, ..
            }) => Ok(*poll_registered),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport is not an owned poll source",
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "transport not found",
            )),
        }
    }

    fn fail_register_source_for_test(&mut self) {
        self.fail_register_source();
    }

    fn fail_set_source_enabled_for_test(&mut self, enabled: bool) {
        self.fail_set_source_enabled(enabled);
    }

    fn fail_remove_for_test(&mut self) {
        self.fail_remove();
    }

    fn interest_requests(&self) -> &[(TransportId, Interest)] {
        &self.interest_requests
    }

    fn clear_interest_requests(&mut self) {
        self.interest_requests.clear();
    }
}

impl PollingTransportRuntime {
    fn open_tcp_stream(
        &self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
        protocol: Protocol,
    ) -> io::Result<TcpStream> {
        let socket = tcp_socket(addr, protocol)?;
        socket.set_reuse_address(true)?;
        socket.set_nodelay(true)?;
        socket.set_nonblocking(true)?;
        if let Some(limits) = self.tcp_buffer_limits {
            socket.set_send_buffer_size(limits.send_bytes)?;
            socket.set_recv_buffer_size(limits.receive_bytes)?;
        }
        if let Some(source_ip) = source_ip {
            if source_ip.is_ipv4() != addr.is_ipv4() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bind-source-addr family does not match destination",
                ));
            }
            socket.bind(&SocketAddr::new(source_ip, 0).into())?;
        }
        match socket.connect(&addr.into()) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(error) => return Err(error),
        }
        Ok(TcpStream::from(socket))
    }

    #[cfg(test)]
    pub(crate) fn source_enabled_for_test(&self, id: TransportId) -> bool {
        matches!(
            self.registered.get(id.get() - 1),
            Some(Registered::OwnedSource {
                poll_registered: true,
                ..
            })
        )
    }

    #[cfg(test)]
    pub(crate) fn fail_register_source(&mut self) {
        self.register_owned_source_error = Some(io::ErrorKind::Other);
    }

    #[cfg(test)]
    pub(crate) fn fail_set_source_enabled(&mut self, enabled: bool) {
        self.set_source_enabled_errors
            .insert(enabled, io::ErrorKind::Other);
    }

    #[cfg(test)]
    pub(crate) fn fail_remove(&mut self) {
        self.remove_error = Some(io::ErrorKind::Other);
    }
}

fn tcp_socket(addr: SocketAddr, protocol: Protocol) -> io::Result<Socket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(protocol))?;
    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    Ok(socket)
}

fn validate_mode(poller: &Poller, mode: TransportPollMode) -> io::Result<()> {
    match mode {
        #[cfg(test)]
        TransportPollMode::Edge if !poller.supports_edge() => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "polling backend does not support edge-triggered mode",
        )),
        TransportPollMode::Level if !poller.supports_level() => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "polling backend does not support level-triggered mode",
        )),
        _ => Ok(()),
    }
}

fn apply_accepted_stream_options(
    stream: &Stream,
    limits: Option<TcpBufferLimits>,
) -> io::Result<()> {
    match stream {
        Stream::Tcp(stream) => stream.set_nonblocking(true)?,
        Stream::Tls(stream) => stream.get_ref().set_nonblocking(true)?,
        Stream::Unix(stream) => stream.set_nonblocking(true)?,
    }
    match stream {
        Stream::Tcp(stream) => {
            stream.set_nodelay(true)?;
            if let Some(limits) = limits {
                let socket = SockRef::from(stream);
                socket.set_send_buffer_size(limits.send_bytes)?;
                socket.set_recv_buffer_size(limits.receive_bytes)?;
            }
        }
        Stream::Tls(stream) => {
            stream.get_ref().set_nodelay(true)?;
            if let Some(limits) = limits {
                let socket = SockRef::from(stream.get_ref());
                socket.set_send_buffer_size(limits.send_bytes)?;
                socket.set_recv_buffer_size(limits.receive_bytes)?;
            }
        }
        Stream::Unix(_) => {}
    }
    Ok(())
}

fn tcp_stream_ref(registered: Option<&Registered>) -> io::Result<&TcpStream> {
    match registered {
        Some(Registered::Stream(Stream::Tcp(stream))) => Ok(stream),
        Some(Registered::Stream(Stream::Tls(stream))) => Ok(stream.get_ref()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transport is not a TCP stream",
        )),
    }
}

fn test_tcp_buffer_limits_from_env() -> io::Result<Option<TcpBufferLimits>> {
    let Some(send_bytes) = tcp_buffer_limit_from_env("VALKEY_TEST_TCP_SNDBUF_BYTES")? else {
        return Ok(None);
    };
    let Some(receive_bytes) = tcp_buffer_limit_from_env("VALKEY_TEST_TCP_RCVBUF_BYTES")? else {
        return Ok(None);
    };
    Ok(Some(TcpBufferLimits {
        send_bytes,
        receive_bytes,
    }))
}

fn tcp_buffer_limit_from_env(name: &str) -> io::Result<Option<usize>> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(None);
    };
    let raw = raw.to_string_lossy();
    let value = raw.parse::<usize>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a byte count: {error}"),
        )
    })?;
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::{PollingTransportRuntime, Registered};
    use crate::foundation::io::transport::{
        OwnedPollSource, PollSourceMode, TransportPollMode, TransportRuntime,
    };
    use std::fs::File;
    use std::io::Write;
    use std::net::{Shutdown, SocketAddr, TcpStream};
    use std::os::fd::OwnedFd;
    use std::time::Duration;

    fn bind_tcp_loopback(
        runtime: &mut PollingTransportRuntime,
    ) -> (super::ListenerHandle, SocketAddr) {
        let listener = runtime
            .bind_tcp("127.0.0.1:0".parse().expect("loopback wildcard address"))
            .expect("bind listener");
        let address = match runtime.registered.get(listener.id.get() - 1) {
            Some(Registered::TcpListener(listener)) => {
                listener.local_addr().expect("read listener address")
            }
            _ => panic!("listener handle must point at a TCP listener"),
        };
        (listener, address)
    }

    #[test]
    fn separate_read_and_write_filters_are_coalesced_in_first_seen_order() {
        let first = super::TransportId::new(1).expect("nonzero transport id");
        let second = super::TransportId::new(2).expect("nonzero transport id");
        let events = super::coalesce_ready_events([
            super::TransportEvent::ready(first, false, true, false, false, false),
            super::TransportEvent::ready(second, true, false, false, false, false),
            super::TransportEvent::ready(first, true, false, true, false, true),
        ]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, first);
        assert!(events[0].readable && events[0].writable);
        assert!(events[0].read_closed && events[0].error);
        assert_eq!(events[1].id, second);
    }

    #[test]
    fn level_mode_repeats_listener_readiness_until_queue_is_drained() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (listener, listener_address) = bind_tcp_loopback(&mut runtime);
        let mut first = TcpStream::connect(listener_address).expect("connect first client");
        let _second = TcpStream::connect(listener_address).expect("connect second client");
        first.write_all(b"ready").expect("write first client");

        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll first readiness");
        assert!(events
            .iter()
            .any(|event| event.id == listener.id && event.readable));
        runtime
            .accept(listener.id)
            .expect("accept first result")
            .expect("first client");

        events.clear();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll repeated readiness");
        assert!(events
            .iter()
            .any(|event| event.id == listener.id && event.readable));
    }

    #[test]
    fn edge_mode_keeps_mio_compatibility_when_readiness_is_not_rearmed() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Edge)
            .expect("create polling runtime");
        let (listener, address) = bind_tcp_loopback(&mut runtime);
        let _client = TcpStream::connect(address).expect("connect client");
        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll first readiness");
        assert!(events
            .iter()
            .any(|event| event.id == listener.id && event.readable));
        events.clear();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll edge readiness");
        assert!(events.is_empty());
    }

    #[test]
    fn level_mode_reports_tcp_half_close_as_read_closed() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (listener, address) = bind_tcp_loopback(&mut runtime);
        let client = TcpStream::connect(address).expect("connect client");
        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll initial readiness");
        let peer = runtime
            .accept(listener.id)
            .expect("accept client")
            .expect("accepted client");
        client
            .shutdown(Shutdown::Write)
            .expect("half-close client write side");
        events.clear();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll half-close readiness");
        assert!(events
            .iter()
            .any(|event| event.id == peer.id && event.read_closed));
    }

    #[test]
    fn level_mode_delivers_data_after_accepted_socket_interest_change() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (listener, address) = bind_tcp_loopback(&mut runtime);
        let mut client = TcpStream::connect(address).expect("connect client");
        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll listener readiness");
        let peer = runtime
            .accept(listener.id)
            .expect("accept client")
            .expect("accepted client");
        runtime
            .set_interest(
                peer.id,
                crate::foundation::io::transport::Interest::ReadWritable,
            )
            .expect("enable accepted read/write interest");
        events.clear();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll accepted writable readiness");
        client
            .write_all(b"cluster-handshake")
            .expect("write client data");
        events.clear();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll accepted readable data");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !events
            .iter()
            .any(|event| event.id == peer.id && event.readable)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "read readiness must arrive before the deadline"
            );
            events.clear();
            runtime
                .poll(&mut events, Some(Duration::from_millis(10)))
                .expect("poll readable data");
        }
        assert!(events
            .iter()
            .any(|event| event.id == peer.id && event.readable));
    }

    #[test]
    fn level_mode_delivers_data_that_arrived_before_accept_registration() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (listener, address) = bind_tcp_loopback(&mut runtime);
        let mut client = TcpStream::connect(address).expect("connect client");
        client
            .write_all(b"data-before-accept")
            .expect("write client data");
        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll listener readiness");
        let peer = runtime
            .accept(listener.id)
            .expect("accept client")
            .expect("accepted client");
        runtime
            .set_interest(
                peer.id,
                crate::foundation::io::transport::Interest::ReadWritable,
            )
            .expect("enable accepted read/write interest");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !events
            .iter()
            .any(|event| event.id == peer.id && event.readable)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "read readiness must arrive before the deadline"
            );
            events.clear();
            runtime
                .poll(&mut events, Some(Duration::from_millis(10)))
                .expect("poll accepted readable data");
        }
        assert!(events
            .iter()
            .any(|event| event.id == peer.id && event.readable));
    }

    fn pipe_file_pair() -> (File, std::io::PipeWriter) {
        let (reader, writer) = std::io::pipe().expect("create pipe");
        let owned_reader: OwnedFd = reader.into();
        (File::from(owned_reader), writer)
    }

    #[test]
    fn readable_source_repeats_level_ready_until_pipe_is_drained() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, mut writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::readable(reader))
            .expect("register readable source");
        writer.write_all(b"ping").expect("write pipe payload");
        let mut events = Vec::new();

        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll readable source");
        assert!(events
            .iter()
            .any(|event| event.id == source && event.readable));

        events.clear();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll repeated readable source");
        assert!(events
            .iter()
            .any(|event| event.id == source && event.readable));
    }

    #[test]
    fn set_source_enabled_suppresses_and_resumes_readiness() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, mut writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::readable(reader))
            .expect("register readable source");
        writer.write_all(b"enabled").expect("write initial payload");
        let mut events = Vec::new();

        runtime
            .set_source_enabled(source, false)
            .expect("disable source");
        assert!(!runtime.source_enabled_for_test(source));
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll disabled source");
        assert!(events.is_empty());

        runtime
            .set_source_enabled(source, true)
            .expect("re-enable source");
        assert!(runtime.source_enabled_for_test(source));
        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll re-enabled source");
        assert!(events
            .iter()
            .any(|event| event.id == source && event.readable));
    }

    #[test]
    fn readable_source_reads_payload_and_drains_pipe() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, mut writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::readable(reader))
            .expect("register readable source");
        writer.write_all(b"payload").expect("write pipe payload");
        let mut dst = [0_u8; 16];
        let read = runtime.read(source, &mut dst).expect("read source payload");

        assert_eq!(&dst[..read], b"payload");

        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll drained source");
        assert!(events.is_empty());
    }

    #[test]
    fn readable_source_reports_eof_as_read_closed() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::readable(reader))
            .expect("register readable source");
        drop(writer);
        let mut events = Vec::new();

        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll pipe eof");

        assert!(events
            .iter()
            .any(|event| event.id == source && event.read_closed));
    }

    #[test]
    fn removing_owned_source_discards_stale_events_and_closes_owner() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, mut writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::readable(reader))
            .expect("register readable source");
        writer.write_all(b"stale").expect("write pipe payload");

        runtime.remove(source).expect("remove source");

        let mut events = Vec::new();
        runtime
            .poll(&mut events, Some(Duration::ZERO))
            .expect("poll after source removal");
        assert!(events.is_empty());
        assert_eq!(
            writer
                .write(b"after-remove")
                .expect_err("closed owner must reject new writes")
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn regular_file_is_rejected_before_poll_registration() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let temp_path = std::env::temp_dir().join(format!(
            "versal-owned-source-{}-regular-file",
            std::process::id()
        ));
        std::fs::write(&temp_path, b"regular-file").expect("write regular file");
        let file = File::open(&temp_path).expect("open regular file");

        let error = runtime
            .register_owned_source(OwnedPollSource::readable(file))
            .expect_err("regular file must be rejected");

        let _ = std::fs::remove_file(&temp_path);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn completion_source_is_ready_but_cannot_be_payload_read() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, mut writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource::completion(reader))
            .expect("register completion source");
        writer.write_all(b"done").expect("signal completion");
        let mut events = Vec::new();

        runtime
            .poll(&mut events, Some(Duration::from_secs(1)))
            .expect("poll completion source");
        assert!(events
            .iter()
            .any(|event| event.id == source && event.readable));
        let mut dst = [0_u8; 8];
        let error = runtime
            .read(source, &mut dst)
            .expect_err("completion source must reject payload reads");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn completion_source_enable_state_is_visible_to_tests() {
        let mut runtime = PollingTransportRuntime::with_mode(TransportPollMode::Level)
            .expect("create polling runtime");
        let (reader, _writer) = pipe_file_pair();
        let source = runtime
            .register_owned_source(OwnedPollSource {
                file: reader,
                mode: PollSourceMode::Completion,
            })
            .expect("register completion source");

        assert!(runtime.source_enabled_for_test(source));
        runtime
            .set_source_enabled(source, false)
            .expect("disable completion source");
        assert!(!runtime.source_enabled_for_test(source));
    }
}
