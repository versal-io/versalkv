mod registry;
mod removal;

use self::registry::TransportRegistry;
use crate::engine::command::context::Token;
#[cfg(test)]
use crate::foundation::io::transport::PollSourceMode;
use crate::foundation::io::transport::TransportPollState;
use crate::foundation::io::transport::{
    ConnectedPeer, Interest, ListenerHandle, OwnedPollSource, ThreadIoHandle, TlsClientConfig,
    TlsConfig, TransportEvent, TransportId, TransportRuntime, TransportWakeHandle,
    TransportWorkerBackend, UnixBindOptions,
};
use crate::framework::distributed::sentinel::io::{IoInterest, SentinelIo};
use crate::runtime::server::worker::reactor::sources::{RuntimeSourceRegistry, SourceOwner};
use std::io::{self, IoSlice, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::RawFd;
use std::path::Path;
use std::time::Duration;

pub(crate) use self::removal::{
    deregister_or_defer, retire_route_for_teardown, retry_pending_transport_removals,
    PendingRemovalOwner, PendingTransportRemoval, SourceInstallRollback,
};

#[derive(Debug)]
pub(crate) struct RuntimeEvent {
    pub(crate) token: Token,
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) read_hup: bool,
    pub(crate) error: bool,
}

pub(crate) struct RuntimeTransport<T: TransportRuntime> {
    inner: T,
    registry: TransportRegistry,
    quarantined_tokens: std::collections::HashSet<Token>,
    #[cfg(test)]
    test_event_batches: std::collections::VecDeque<Vec<RuntimeEvent>>,
    #[cfg(test)]
    test_pending_data_events: Vec<RuntimeEvent>,
    #[cfg(test)]
    test_next_connect_error: Option<io::Error>,
    next_poll_generation: u64,
    poll_inflight: Option<u64>,
    shutdown_started: bool,
}

pub(crate) struct RuntimePollJob {
    generation: u64,
    state: TransportPollState,
    timeout: Option<Duration>,
    #[cfg(test)]
    execution_observer: Option<std::sync::mpsc::Sender<String>>,
}

pub(crate) struct RuntimePollCompletion {
    generation: u64,
    state: TransportPollState,
    events: io::Result<Vec<TransportEvent>>,
    cancelled: bool,
    #[cfg(test)]
    executor_thread_name: Option<String>,
}

impl RuntimePollJob {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn execute(mut self) -> RuntimePollCompletion {
        #[cfg(test)]
        if let Some(observer) = self.execution_observer.take() {
            let _ = observer.send(
                std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_owned(),
            );
        }
        let events = self.state.poll(self.timeout);
        RuntimePollCompletion {
            generation: self.generation,
            state: self.state,
            events,
            cancelled: false,
            #[cfg(test)]
            executor_thread_name: std::thread::current().name().map(str::to_owned),
        }
    }

    pub(crate) fn cancel(self) -> RuntimePollCompletion {
        RuntimePollCompletion {
            generation: self.generation,
            state: self.state,
            events: Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "poll cancelled while stopping IO workers",
            )),
            cancelled: true,
            #[cfg(test)]
            executor_thread_name: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn observe_execution_for_test(
        mut self,
        observer: std::sync::mpsc::Sender<String>,
    ) -> Self {
        self.execution_observer = Some(observer);
        self
    }
}

impl RuntimePollCompletion {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    #[cfg(test)]
    pub(crate) fn executor_thread_name_for_test(&self) -> Option<&str> {
        self.executor_thread_name.as_deref()
    }
}

pub(crate) struct RuntimeListenerHandle {
    pub(crate) token: Token,
    pub(crate) inherited_fd: std::os::fd::RawFd,
}

pub(crate) struct RuntimeConnectedPeer {
    pub(crate) token: Token,
    pub(crate) peer_addr: SocketAddr,
}

impl<T: TransportRuntime> RuntimeTransport<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner,
            registry: TransportRegistry::new(),
            quarantined_tokens: std::collections::HashSet::new(),
            #[cfg(test)]
            test_event_batches: std::collections::VecDeque::new(),
            #[cfg(test)]
            test_pending_data_events: Vec::new(),
            #[cfg(test)]
            test_next_connect_error: None,
            next_poll_generation: 0,
            poll_inflight: None,
            shutdown_started: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    #[cfg(test)]
    pub(crate) fn wait_readable_for_test(&mut self, token: Token) -> Vec<RuntimeEvent> {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for readable token {token:?}"
            );
            let events = self
                .poll_events(Some(remaining))
                .expect("poll while waiting for test socket readability");
            if events
                .iter()
                .any(|event| event.token == token && event.readable)
            {
                return events;
            }
            // Level-triggered writable sockets may fire before incoming bytes arrive.
            std::thread::yield_now();
        }
    }

    #[cfg(test)]
    pub(crate) fn register_test_transport(&mut self, id: TransportId) -> Token {
        self.registry.register(id)
    }

    pub(crate) fn bind_tcp(&mut self, addr: SocketAddr) -> io::Result<RuntimeListenerHandle> {
        self.ensure_poll_state_attached()?;
        let listener = self.inner.bind_tcp(addr)?;
        Ok(self.register_listener(listener))
    }

    pub(crate) fn configure_tls_server(&mut self, config: TlsConfig) -> io::Result<()> {
        self.inner.configure_tls_server(config)
    }

    pub(crate) fn bind_tls(&mut self, addr: SocketAddr) -> io::Result<RuntimeListenerHandle> {
        self.ensure_poll_state_attached()?;
        let listener = self.inner.bind_tls(addr)?;
        Ok(self.register_listener(listener))
    }

    pub(crate) fn bind_mptcp(&mut self, addr: SocketAddr) -> io::Result<RuntimeListenerHandle> {
        self.ensure_poll_state_attached()?;
        let listener = self.inner.bind_mptcp(addr)?;
        Ok(self.register_listener(listener))
    }

    pub(crate) fn set_tcp_backlog(&mut self, backlog: i32) -> io::Result<()> {
        self.inner.set_tcp_backlog(backlog)
    }

    pub(crate) fn bind_unix(
        &mut self,
        path: &Path,
        options: UnixBindOptions,
    ) -> io::Result<RuntimeListenerHandle> {
        self.ensure_poll_state_attached()?;
        let listener = self.inner.bind_unix(path, options)?;
        Ok(self.register_listener(listener))
    }

    // Tasks 6-11 migrate current internal readiness producers to this
    // business-neutral wrapper.
    // TODO(dead_code): remove after runtime owned sources are used in production.
    #[allow(dead_code)]
    pub(crate) fn register_source(&mut self, source: OwnedPollSource) -> io::Result<Token> {
        self.ensure_poll_state_attached()?;
        let id = self.inner.register_owned_source(source)?;
        Ok(self.registry.register(id))
    }

    // TODO(dead_code): remove after runtime owned sources are used in production.
    #[allow(dead_code)]
    pub(crate) fn set_source_enabled(&mut self, token: Token, enabled: bool) -> io::Result<()> {
        self.ensure_poll_state_attached()?;
        let id = self.transport_id(token)?;
        self.inner.set_source_enabled(id, enabled)
    }

    // TODO(dead_code): remove after runtime owned sources are used in production.
    #[allow(dead_code)]
    pub(crate) fn read_source(&mut self, token: Token, dst: &mut [u8]) -> io::Result<usize> {
        self.ensure_poll_state_attached()?;
        let id = self.transport_id(token)?;
        self.inner.read(id, dst)
    }

    pub(crate) fn connect_tcp(&mut self, addr: SocketAddr) -> io::Result<RuntimeConnectedPeer> {
        self.connect_tcp_bound(addr, None)
    }

    pub(crate) fn configure_tls_client(&mut self, config: TlsClientConfig) -> io::Result<()> {
        self.inner.configure_tls_client(config)
    }

    pub(crate) fn connect_tls_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
        server_name: &str,
    ) -> io::Result<RuntimeConnectedPeer> {
        self.ensure_poll_state_attached()?;
        #[cfg(test)]
        if let Some(error) = self.test_next_connect_error.take() {
            return Err(error);
        }
        let peer = self.inner.connect_tls_bound(addr, source_ip, server_name)?;
        Ok(self.register_peer(peer))
    }

    pub(crate) fn connect_tcp_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<RuntimeConnectedPeer> {
        self.ensure_poll_state_attached()?;
        #[cfg(test)]
        if let Some(error) = self.test_next_connect_error.take() {
            return Err(error);
        }
        let peer = self.inner.connect_tcp_bound(addr, source_ip)?;
        Ok(self.register_peer(peer))
    }

    pub(crate) fn connect_mptcp_bound(
        &mut self,
        addr: SocketAddr,
        source_ip: Option<std::net::IpAddr>,
    ) -> io::Result<RuntimeConnectedPeer> {
        self.ensure_poll_state_attached()?;
        #[cfg(test)]
        if let Some(error) = self.test_next_connect_error.take() {
            return Err(error);
        }
        let peer = self.inner.connect_mptcp_bound(addr, source_ip)?;
        Ok(self.register_peer(peer))
    }

    pub(crate) fn accept(&mut self, listener: Token) -> io::Result<Option<RuntimeConnectedPeer>> {
        self.ensure_poll_state_attached()?;
        let listener = self.transport_id(listener)?;
        Ok(self
            .inner
            .accept(listener)?
            .map(|peer| self.register_peer(peer)))
    }

    pub(crate) fn read(&mut self, token: Token, dst: &mut [u8]) -> io::Result<usize> {
        let id = self.transport_id(token)?;
        self.inner.read(id, dst)
    }

    pub(crate) fn write(&mut self, token: Token, src: &[u8]) -> io::Result<usize> {
        let id = self.transport_id(token)?;
        self.inner.write(id, src)
    }

    pub(crate) fn write_vectored(
        &mut self,
        token: Token,
        src: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        let id = self.transport_id(token)?;
        self.inner.write_vectored(id, src)
    }

    pub(crate) fn apply_test_tcp_buffer_limits(&mut self, token: Token) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.apply_test_tcp_buffer_limits(id)
    }

    pub(crate) fn apply_tcp_keepalive(
        &mut self,
        token: Token,
        idle: Option<Duration>,
    ) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.apply_tcp_keepalive(id, idle)
    }

    pub(crate) fn set_tcp_nodelay(&mut self, token: Token, enabled: bool) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.set_tcp_nodelay(id, enabled)
    }

    pub(crate) fn set_interest(&mut self, token: Token, interest: Interest) -> io::Result<()> {
        self.ensure_poll_state_attached()?;
        let id = self.transport_id(token)?;
        self.inner.set_interest(id, interest)
    }

    pub(crate) fn take_connect_error(&mut self, token: Token) -> io::Result<Option<io::Error>> {
        let id = self.transport_id(token)?;
        self.inner.take_connect_error(id)
    }

    pub(crate) fn shutdown(&mut self, token: Token) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.shutdown(id)
    }

    pub(crate) fn remove_registered(&mut self, token: Token) -> io::Result<()> {
        self.ensure_poll_state_attached()?;
        let id = self.transport_id(token)?;
        self.inner.remove(id)?;
        self.registry.remove(id);
        self.quarantined_tokens.remove(&token);
        Ok(())
    }

    /// Exclude a retained registration from every runtime event path while its
    /// L1 deregistration is retried. The token mapping stays owned to prevent
    /// ABA reuse until `remove_registered` succeeds.
    pub(crate) fn quarantine_registered(&mut self, token: Token) {
        self.quarantined_tokens.insert(token);
    }

    pub(crate) fn raw_fd(&self, token: Token) -> io::Result<std::os::fd::RawFd> {
        self.inner.raw_fd(self.transport_id(token)?)
    }

    pub(crate) fn tcp_socket_ports(&self, token: Token) -> io::Result<(RawFd, u16, u16)> {
        let fd = self.raw_fd(token)?;
        Ok((
            fd,
            crate::foundation::io::sys::socket::socket_port_raw(fd, false)?,
            crate::foundation::io::sys::socket::socket_port_raw(fd, true)?,
        ))
    }

    pub(crate) fn is_registered(&self, token: Token) -> bool {
        self.registry.transport_id(token).is_some()
    }

    pub(crate) fn has_pending_data(&self) -> bool {
        #[cfg(test)]
        if !self.test_pending_data_events.is_empty() {
            return true;
        }
        self.inner.has_pending_data()
    }

    pub(crate) fn process_pending_data(&mut self) -> Vec<RuntimeEvent> {
        #[cfg(test)]
        let mut runtime_events = std::mem::take(&mut self.test_pending_data_events);
        #[cfg(not(test))]
        let mut runtime_events = Vec::new();
        let mut events = Vec::new();
        self.inner.process_pending_data(&mut events);
        runtime_events.extend(
            events
                .into_iter()
                .filter_map(|event| self.translate_event(event)),
        );
        runtime_events
    }

    pub(crate) fn begin_poll_offload(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<RuntimePollJob> {
        if self.shutdown_started {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "transport shutdown has started",
            ));
        }
        if self.poll_inflight.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "transport poll is already in flight",
            ));
        }
        let state = self.inner.take_poll_state()?;
        self.next_poll_generation = self.next_poll_generation.saturating_add(1).max(1);
        let generation = self.next_poll_generation;
        self.poll_inflight = Some(generation);
        Ok(RuntimePollJob {
            generation,
            state,
            timeout,
            #[cfg(test)]
            execution_observer: None,
        })
    }

    pub(crate) fn finish_poll_offload(
        &mut self,
        completion: RuntimePollCompletion,
    ) -> io::Result<Vec<RuntimeEvent>> {
        let expected_generation = self.poll_inflight;
        let completion_generation = completion.generation;
        let events = completion.events;
        let restore_result = self.inner.restore_poll_state(completion.state);
        self.poll_inflight = None;
        restore_result?;
        if expected_generation != Some(completion_generation) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stale poll completion generation",
            ));
        }
        let mut events = events?;
        self.inner.finalize_polled_events(&mut events);
        Ok(events
            .into_iter()
            .filter_map(|event| self.translate_event(event))
            .collect())
    }

    pub(crate) fn poll_events(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<Vec<RuntimeEvent>> {
        #[cfg(test)]
        if let Some(events) = self.test_event_batches.pop_front() {
            return Ok(events
                .into_iter()
                .filter(|event| !self.quarantined_tokens.contains(&event.token))
                .collect());
        }
        let mut events = Vec::new();
        self.inner.poll(&mut events, timeout)?;
        Ok(events
            .into_iter()
            .filter_map(|event| self.translate_event(event))
            .collect())
    }

    pub(crate) fn start_shutdown(&mut self) {
        self.shutdown_started = true;
    }

    pub(crate) fn shutdown_started(&self) -> bool {
        self.shutdown_started
    }

    pub(crate) fn poll_inflight(&self) -> bool {
        self.poll_inflight.is_some()
    }

    #[cfg(test)]
    pub(crate) fn poll_generation_for_test(&self) -> u64 {
        self.next_poll_generation
    }

    fn ensure_poll_state_attached(&self) -> io::Result<()> {
        if self.poll_inflight.is_none() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "poll state is owned by an IO worker",
            ))
        }
    }

    fn register_listener(&mut self, listener: ListenerHandle) -> RuntimeListenerHandle {
        RuntimeListenerHandle {
            token: self.registry.register(listener.id),
            inherited_fd: listener.inherited_fd,
        }
    }

    fn register_peer(&mut self, peer: ConnectedPeer) -> RuntimeConnectedPeer {
        RuntimeConnectedPeer {
            token: self.registry.register(peer.id),
            peer_addr: peer.peer_addr,
        }
    }

    fn transport_id(&self, token: Token) -> io::Result<TransportId> {
        self.registry
            .transport_id(token)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    }

    fn translate_event(&self, event: TransportEvent) -> Option<RuntimeEvent> {
        self.registry
            .runtime_token(event.id)
            .filter(|token| !self.quarantined_tokens.contains(token))
            .map(|token| RuntimeEvent {
                token,
                readable: event.readable,
                writable: event.writable,
                read_hup: event.read_closed,
                // The polling backend's write-closed signal has no false positives: once the
                // local socket can no longer reply, preserving a read-HUP
                // pipeline only accumulates parser/output state for a dead
                // peer. Treat it as transport failure while retaining the
                // graceful read-half-close path.
                error: event.error || event.write_closed,
            })
    }

    pub(crate) fn stream(&mut self, token: Token) -> Option<TransportStream<'_, T>> {
        self.registry.transport_id(token).map(|_| TransportStream {
            transport: self,
            token,
        })
    }
}

impl<T> RuntimeTransport<T>
where
    T: TransportRuntime + TransportWorkerBackend,
{
    /// Size a test socket's receive buffer so a non-consuming peek barrier can
    /// hold `minimum_bytes` before anything is read. On Linux loopback an
    /// unread socket receives at most SO_RCVBUF minus skb overhead (the
    /// default 128 KiB buffer delivers about 124 KiB), and the kernel stores
    /// twice the requested value; macOS applies the value as-is. Requesting
    /// twice the payload therefore fits the full barrier on both platforms.
    #[cfg(test)]
    pub(crate) fn reserve_test_receive_buffer(
        &self,
        token: Token,
        minimum_bytes: usize,
    ) -> io::Result<()> {
        let size = std::cmp::max(
            i32::try_from(minimum_bytes.saturating_mul(2)).unwrap_or(i32::MAX),
            64 * 1024,
        );
        crate::foundation::io::sys::socket::set_buffer_size_raw(
            self.raw_fd(token)?,
            crate::foundation::io::sys::socket::SocketBuffer::Receive,
            size,
        )
    }

    #[cfg(test)]
    pub(crate) fn wait_pending_bytes_for_test(&self, token: Token, minimum_bytes: usize) {
        let ThreadIoHandle::Tcp(stream) = self
            .duplicate_stream(token)
            .expect("duplicate test socket for a nonconsuming peek")
        else {
            panic!("pending-byte barrier requires a TCP test socket");
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut buffer = vec![0_u8; minimum_bytes];
        loop {
            match stream.peek(&mut buffer) {
                Ok(bytes) if bytes >= minimum_bytes => return,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("peek test socket {token:?}: {error}"),
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {minimum_bytes} pending bytes on token {token:?}"
            );
            std::thread::yield_now();
        }
    }

    pub(crate) fn duplicate_stream(&self, token: Token) -> io::Result<ThreadIoHandle> {
        let id = self.transport_id(token)?;
        self.inner.duplicate_stream(id)
    }

    pub(crate) fn register_thread_waker(&mut self) -> io::Result<(Token, TransportWakeHandle)> {
        self.ensure_poll_state_attached()?;
        let (id, wake) = self.inner.register_worker_waker()?;
        Ok((self.registry.register(id), wake))
    }
}

#[cfg(test)]
impl<T> RuntimeTransport<T>
where
    T: TransportRuntime + crate::foundation::io::transport::TransportTestBackend,
{
    pub(crate) fn source_enabled_for_test(&self, token: Token) -> io::Result<bool> {
        let id = self.transport_id(token)?;
        self.inner.source_enabled_for_test(id)
    }

    pub(crate) fn queue_test_transport_event_batch(
        &mut self,
        events: &[(Token, bool, bool, bool, bool)],
    ) {
        self.test_event_batches.push_back(
            events
                .iter()
                .map(
                    |&(token, readable, writable, read_hup, error)| RuntimeEvent {
                        token,
                        readable,
                        writable,
                        read_hup,
                        error,
                    },
                )
                .collect(),
        );
    }

    pub(crate) fn queue_test_pending_data_event(&mut self, token: Token) {
        self.test_pending_data_events.push(RuntimeEvent {
            token,
            readable: true,
            writable: false,
            read_hup: false,
            error: false,
        });
    }

    pub(crate) fn set_next_connect_error_for_test(&mut self, error: io::Error) {
        self.test_next_connect_error = Some(error);
    }

    pub(crate) fn deregister_for_test(&mut self, token: Token) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.deregister_for_test(id)
    }

    pub(crate) fn reregister_for_test(&mut self, token: Token) -> io::Result<()> {
        let id = self.transport_id(token)?;
        self.inner.reregister_for_test(id)
    }

    pub(crate) fn interest_requests_for_test(
        &self,
    ) -> &[(TransportId, crate::foundation::io::transport::Interest)] {
        self.inner.interest_requests()
    }

    pub(crate) fn last_set_interest_calls_for_test(
        &self,
    ) -> Vec<(Token, crate::foundation::io::transport::Interest)> {
        self.inner
            .interest_requests()
            .iter()
            .filter_map(|(id, interest)| {
                self.registry
                    .runtime_token(*id)
                    .map(|token| (token, *interest))
            })
            .collect()
    }

    pub(crate) fn clear_interest_requests_for_test(&mut self) {
        self.inner.clear_interest_requests();
    }

    pub(crate) fn fail_register_source_for_test(&mut self) {
        self.inner.fail_register_source_for_test();
    }

    pub(crate) fn fail_set_source_enabled_for_test(&mut self, enabled: bool) {
        self.inner.fail_set_source_enabled_for_test(enabled);
    }

    pub(crate) fn fail_remove_for_test(&mut self) {
        self.inner.fail_remove_for_test();
    }
}

pub(crate) struct TransportStream<'a, T: TransportRuntime> {
    transport: &'a mut RuntimeTransport<T>,
    token: Token,
}

impl<T: TransportRuntime> Read for TransportStream<'_, T> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        self.transport.read(self.token, dst)
    }
}

impl<T: TransportRuntime> Write for TransportStream<'_, T> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.transport.write(self.token, src)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_vectored(&mut self, src: &[IoSlice<'_>]) -> io::Result<usize> {
        self.transport.write_vectored(self.token, src)
    }
}

pub(crate) struct RuntimeSentinelIo<'a, T: TransportRuntime> {
    transport: &'a mut RuntimeTransport<T>,
    sources: &'a mut RuntimeSourceRegistry,
    pending_transport_removals: &'a mut std::collections::HashMap<Token, PendingTransportRemoval>,
}

impl<'a, T: TransportRuntime> RuntimeSentinelIo<'a, T> {
    pub(crate) fn new(
        transport: &'a mut RuntimeTransport<T>,
        sources: &'a mut RuntimeSourceRegistry,
        pending_transport_removals: &'a mut std::collections::HashMap<
            Token,
            PendingTransportRemoval,
        >,
    ) -> Self {
        Self {
            transport,
            sources,
            pending_transport_removals,
        }
    }
}

impl<T: TransportRuntime> SentinelIo for RuntimeSentinelIo<'_, T> {
    fn connect_tcp(&mut self, addr: &str) -> Result<Token, String> {
        let mut addrs = addr.to_socket_addrs().map_err(|error| error.to_string())?;
        let mut fallback = None;
        let mut socket_addr = None;
        for resolved in &mut addrs {
            if resolved.is_ipv4() {
                socket_addr = Some(resolved);
                break;
            }
            fallback.get_or_insert(resolved);
        }
        let socket_addr = socket_addr
            .or(fallback)
            .ok_or_else(|| format!("no address resolved for {addr}"))?;
        let peer = self
            .transport
            .connect_tcp(socket_addr)
            .map_err(|error| error.to_string())?;
        match self.sources.install(peer.token, SourceOwner::Sentinel) {
            Ok(_) => Ok(peer.token),
            Err(error) => {
                let cleanup = SourceInstallRollback::new(
                    self.transport,
                    self.sources,
                    self.pending_transport_removals,
                )
                .quarantine_failed_registration(
                    peer.token,
                    SourceOwner::Sentinel,
                    &error,
                );
                let message = match cleanup {
                    Ok(()) => error.to_string(),
                    Err(cleanup_error) => {
                        format!("{error}; transport rollback also failed: {cleanup_error}")
                    }
                };
                Err(message)
            }
        }
    }

    fn set_interest(&mut self, token: Token, interest: IoInterest) -> Result<(), String> {
        let interest = match interest {
            IoInterest::Readable => Interest::Readable,
            IoInterest::ReadWritable => Interest::ReadWritable,
        };
        self.transport
            .set_interest(token, interest)
            .map_err(|error| error.to_string())
    }

    fn remove(&mut self, token: Token) -> Result<(), String> {
        retire_route_for_teardown(
            self.sources,
            self.pending_transport_removals,
            token,
            "SENTINEL route",
        );
        deregister_or_defer(
            self.transport,
            self.pending_transport_removals,
            token,
            PendingRemovalOwner::Sentinel,
        )
        .map_err(|error| error.to_string())
    }

    fn take_connect_error(&mut self, token: Token) -> Result<Option<String>, String> {
        self.transport
            .take_connect_error(token)
            .map(|error| error.map(|error| error.to_string()))
            .map_err(|error| error.to_string())
    }

    fn read_stream(&mut self, token: Token, buf: &mut [u8]) -> io::Result<usize> {
        self.transport.read(token, buf)
    }

    fn write_stream(&mut self, token: Token, buf: &[u8]) -> io::Result<usize> {
        self.transport.write(token, buf)
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct FakeTransportRuntime {
    listeners: std::collections::VecDeque<ListenerHandle>,
    peers: std::collections::VecDeque<ConnectedPeer>,
    owned_sources: std::collections::VecDeque<(TransportId, PollSourceMode)>,
    accepted: std::collections::HashMap<TransportId, std::collections::VecDeque<ConnectedPeer>>,
    events: std::collections::VecDeque<TransportEvent>,
    pending_events: std::collections::VecDeque<TransportEvent>,
    reads: std::collections::HashMap<TransportId, std::collections::VecDeque<Vec<u8>>>,
    writes: std::collections::HashMap<TransportId, Vec<u8>>,
    interests: Vec<(TransportId, Interest)>,
    source_modes: std::collections::HashMap<TransportId, PollSourceMode>,
    source_enabled: std::collections::HashMap<TransportId, bool>,
    shutdowns: Vec<TransportId>,
    removed: Vec<TransportId>,
    remove_error: Option<io::ErrorKind>,
    register_source_error: Option<io::ErrorKind>,
    set_source_enabled_errors: std::collections::HashMap<bool, io::ErrorKind>,
    poll_timeouts: Vec<Option<Duration>>,
}

#[cfg(test)]
impl FakeTransportRuntime {
    fn push_listener(&mut self, listener: ListenerHandle) {
        self.listeners.push_back(listener);
    }

    pub(crate) fn push_peer(&mut self, peer: ConnectedPeer) {
        self.peers.push_back(peer);
    }

    pub(crate) fn push_owned_source(&mut self, id: TransportId, mode: PollSourceMode) {
        self.owned_sources.push_back((id, mode));
    }

    fn push_accepted(&mut self, listener: TransportId, peer: ConnectedPeer) {
        self.accepted.entry(listener).or_default().push_back(peer);
    }

    fn push_event(&mut self, event: TransportEvent) {
        self.events.push_back(event);
    }

    fn push_pending_event(&mut self, event: TransportEvent) {
        self.pending_events.push_back(event);
    }

    fn push_read(&mut self, id: TransportId, data: impl Into<Vec<u8>>) {
        self.reads.entry(id).or_default().push_back(data.into());
    }

    fn writes_for(&self, id: TransportId) -> &[u8] {
        self.writes.get(&id).map(Vec::as_slice).unwrap_or_default()
    }

    pub(crate) fn removed(&self) -> &[TransportId] {
        &self.removed
    }

    fn interests(&self) -> &[(TransportId, Interest)] {
        &self.interests
    }

    fn shutdowns(&self) -> &[TransportId] {
        &self.shutdowns
    }

    pub(crate) fn fail_remove(&mut self) {
        self.remove_error = Some(io::ErrorKind::Other);
    }

    pub(crate) fn fail_register_source(&mut self) {
        self.register_source_error = Some(io::ErrorKind::Other);
    }

    pub(crate) fn fail_set_source_enabled(&mut self, enabled: bool) {
        self.set_source_enabled_errors
            .insert(enabled, io::ErrorKind::Other);
    }

    fn poll_timeouts(&self) -> &[Option<Duration>] {
        &self.poll_timeouts
    }
}

#[cfg(test)]
impl TransportRuntime for FakeTransportRuntime {
    fn bind_tcp(
        &mut self,
        _: std::net::SocketAddr,
    ) -> io::Result<crate::foundation::io::transport::ListenerHandle> {
        self.listeners
            .pop_front()
            .ok_or_else(|| io::ErrorKind::Unsupported.into())
    }
    fn bind_unix(
        &mut self,
        _: &std::path::Path,
        _: crate::foundation::io::transport::UnixBindOptions,
    ) -> io::Result<crate::foundation::io::transport::ListenerHandle> {
        self.listeners
            .pop_front()
            .ok_or_else(|| io::ErrorKind::Unsupported.into())
    }
    fn connect_tcp(
        &mut self,
        _: std::net::SocketAddr,
    ) -> io::Result<crate::foundation::io::transport::ConnectedPeer> {
        self.peers
            .pop_front()
            .ok_or_else(|| io::ErrorKind::Unsupported.into())
    }
    fn register_owned_source(
        &mut self,
        _source: OwnedPollSource,
    ) -> io::Result<crate::foundation::io::transport::TransportId> {
        if let Some(kind) = self.register_source_error.take() {
            return Err(kind.into());
        }
        let (id, mode) = self
            .owned_sources
            .pop_front()
            .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))?;
        self.source_modes.insert(id, mode);
        self.source_enabled.insert(id, true);
        Ok(id)
    }
    fn set_source_enabled(&mut self, id: TransportId, enabled: bool) -> io::Result<()> {
        if let Some(kind) = self.set_source_enabled_errors.remove(&enabled) {
            return Err(kind.into());
        }
        if !self.source_modes.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport is not an owned poll source",
            ));
        }
        self.source_enabled.insert(id, enabled);
        Ok(())
    }
    fn accept(
        &mut self,
        listener: TransportId,
    ) -> io::Result<Option<crate::foundation::io::transport::ConnectedPeer>> {
        Ok(self
            .accepted
            .get_mut(&listener)
            .and_then(|peers| peers.pop_front()))
    }
    fn read(&mut self, id: TransportId, dst: &mut [u8]) -> io::Result<usize> {
        if matches!(self.source_modes.get(&id), Some(PollSourceMode::Completion)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "completion sources do not support payload reads",
            ));
        }
        let Some(chunks) = self.reads.get_mut(&id) else {
            return Ok(0);
        };
        let Some(mut chunk) = chunks.pop_front() else {
            return Ok(0);
        };
        let read = dst.len().min(chunk.len());
        dst[..read].copy_from_slice(&chunk[..read]);
        if read < chunk.len() {
            chunk.drain(..read);
            chunks.push_front(chunk);
        }
        Ok(read)
    }
    fn write(&mut self, id: TransportId, src: &[u8]) -> io::Result<usize> {
        self.writes.entry(id).or_default().extend_from_slice(src);
        Ok(src.len())
    }
    fn write_vectored(&mut self, id: TransportId, src: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut written = 0;
        let output = self.writes.entry(id).or_default();
        for chunk in src {
            output.extend_from_slice(chunk);
            written += chunk.len();
        }
        Ok(written)
    }
    fn apply_test_tcp_buffer_limits(&mut self, _: TransportId) -> io::Result<()> {
        Ok(())
    }
    fn set_interest(
        &mut self,
        id: TransportId,
        interest: crate::foundation::io::transport::Interest,
    ) -> io::Result<()> {
        self.interests.push((id, interest));
        Ok(())
    }
    fn has_pending_data(&self) -> bool {
        !self.pending_events.is_empty()
    }
    fn process_pending_data(&mut self, out: &mut Vec<TransportEvent>) -> usize {
        let before = out.len();
        out.extend(
            self.pending_events
                .drain(..)
                .filter(|event| self.source_enabled.get(&event.id).copied().unwrap_or(true)),
        );
        out.len().saturating_sub(before)
    }
    fn poll(
        &mut self,
        out: &mut Vec<TransportEvent>,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        self.poll_timeouts.push(timeout);
        out.extend(
            self.events
                .drain(..)
                .filter(|event| self.source_enabled.get(&event.id).copied().unwrap_or(true)),
        );
        Ok(out.len())
    }
    fn take_connect_error(&mut self, _: TransportId) -> io::Result<Option<io::Error>> {
        Ok(None)
    }
    fn shutdown(&mut self, id: TransportId) -> io::Result<()> {
        self.shutdowns.push(id);
        Ok(())
    }
    fn remove(&mut self, id: TransportId) -> io::Result<()> {
        if let Some(kind) = self.remove_error.take() {
            return Err(kind.into());
        }
        self.source_modes.remove(&id);
        self.source_enabled.remove(&id);
        self.removed.push(id);
        Ok(())
    }
    fn raw_fd(&self, _: TransportId) -> io::Result<std::os::fd::RawFd> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(test)]
impl crate::foundation::io::transport::TransportTestBackend for FakeTransportRuntime {
    fn deregister_for_test(&mut self, id: TransportId) -> io::Result<()> {
        self.source_enabled.insert(id, false);
        Ok(())
    }

    fn reregister_for_test(&mut self, id: TransportId) -> io::Result<()> {
        if self.source_modes.contains_key(&id) {
            self.source_enabled.insert(id, true);
        }
        Ok(())
    }

    fn source_enabled_for_test(&self, id: TransportId) -> io::Result<bool> {
        self.source_enabled
            .get(&id)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "transport not found"))
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
        &self.interests
    }

    fn clear_interest_requests(&mut self) {
        self.interests.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{FakeTransportRuntime, RuntimeSentinelIo, RuntimeTransport};
    use crate::engine::command::context::Token;
    use crate::foundation::io::transport::{
        ConnectedPeer, Interest, ListenerHandle, OwnedPollSource, PollSourceMode, ThreadIoHandle,
        TransportEvent, TransportId, TransportWakeHandle, TransportWorkerBackend, UnixBindOptions,
    };
    use std::fs::File;
    use std::io::{self, IoSlice, Read, Write};
    use std::net::SocketAddr;
    use std::os::fd::OwnedFd;
    use std::path::Path;
    use std::time::Duration;

    #[derive(Debug, PartialEq, Eq)]
    enum RecordedTurnAction {
        Dispatch(Token),
        SetInterest(TransportId, Interest),
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedTransportEvent {
        transport_id: TransportId,
        readable: bool,
        writable: bool,
        read_closed: bool,
        write_closed: bool,
        error: bool,
    }

    #[derive(Default)]
    struct BackendNeutralEventRecorder {
        events: Vec<RecordedTransportEvent>,
        actions: Vec<RecordedTurnAction>,
    }

    impl BackendNeutralEventRecorder {
        fn record_event(&mut self, event: TransportEvent) {
            self.events.push(RecordedTransportEvent {
                transport_id: event.id,
                readable: event.readable,
                writable: event.writable,
                read_closed: event.read_closed,
                write_closed: event.write_closed,
                error: event.error,
            });
        }

        fn record_dispatch(&mut self, token: Token) {
            self.actions.push(RecordedTurnAction::Dispatch(token));
        }

        fn record_interest(&mut self, id: TransportId, interest: Interest) {
            self.actions
                .push(RecordedTurnAction::SetInterest(id, interest));
        }
    }

    fn pipe_source(mode: PollSourceMode) -> (OwnedPollSource, std::io::PipeWriter) {
        let (reader, writer) = std::io::pipe().expect("create pipe");
        let owned_reader: OwnedFd = reader.into();
        (
            OwnedPollSource {
                file: File::from(owned_reader),
                mode,
            },
            writer,
        )
    }

    #[test]
    fn poll_translates_transport_event_to_registered_runtime_token() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(7).unwrap();
        let token = transport.register_test_transport(id);
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, true, false, true, false, false));

        let event = transport
            .poll_events(Some(Duration::from_millis(1)))
            .unwrap()
            .remove(0);
        assert_eq!(event.token, token);
        assert!(event.readable);
        assert!(!event.writable);
        assert!(event.read_hup);
        assert!(!event.error);
        assert_eq!(
            transport.inner_mut().poll_timeouts(),
            &[Some(Duration::from_millis(1))]
        );
    }

    #[test]
    fn backend_neutral_event_recorder_tracks_events_actions_and_interest() {
        let id = TransportId::new(21).unwrap();
        let event = TransportEvent::ready(id, true, true, true, false, true);
        let mut recorder = BackendNeutralEventRecorder::default();
        recorder.record_event(event);
        recorder.record_dispatch(Token(3));
        recorder.record_interest(id, Interest::ReadWritable);

        assert_eq!(
            recorder.events,
            vec![RecordedTransportEvent {
                transport_id: id,
                readable: true,
                writable: true,
                read_closed: true,
                write_closed: false,
                error: true,
            }]
        );
        assert_eq!(
            recorder.actions,
            vec![
                RecordedTurnAction::Dispatch(Token(3)),
                RecordedTurnAction::SetInterest(id, Interest::ReadWritable),
            ]
        );
    }

    #[test]
    fn worker_capability_is_backend_neutral() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(11).unwrap();
        let token = transport.register_test_transport(id);

        let _stream = transport.duplicate_stream(token).unwrap();
        let (wake_token, wake) = transport.register_thread_waker().unwrap();
        wake.wake().unwrap();

        assert!(transport.is_registered(wake_token));
    }

    impl TransportWorkerBackend for FakeTransportRuntime {
        fn duplicate_stream(&self, _: TransportId) -> io::Result<ThreadIoHandle> {
            let (worker, _peer) = std::os::unix::net::UnixStream::pair()?;
            worker.set_nonblocking(true)?;
            Ok(ThreadIoHandle::Unix(worker))
        }

        fn register_worker_waker(&mut self) -> io::Result<(TransportId, TransportWakeHandle)> {
            struct TestWake;

            impl crate::foundation::io::transport::TransportWake for TestWake {
                fn wake(&self) -> io::Result<()> {
                    Ok(())
                }
            }

            Ok((
                TransportId::new(99).unwrap(),
                TransportWakeHandle::new(std::sync::Arc::new(TestWake)),
            ))
        }
    }

    #[test]
    fn poll_treats_write_closed_as_transport_failure() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(8).unwrap();
        let token = transport.register_test_transport(id);
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, true, false, true, true, false));

        let event = transport
            .poll_events(Some(Duration::from_millis(1)))
            .unwrap()
            .remove(0);

        assert_eq!(event.token, token);
        assert!(event.read_hup);
        assert!(event.error);
    }

    #[test]
    fn pending_data_translates_to_synthetic_runtime_readiness() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(19).unwrap();
        let token = transport.register_test_transport(id);
        transport
            .inner_mut()
            .push_pending_event(TransportEvent::ready(id, true, false, false, false, false));

        assert!(transport.has_pending_data());
        let events = transport.process_pending_data();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token, token);
        assert!(events[0].readable);
        assert!(!transport.has_pending_data());
    }

    #[test]
    fn pending_data_discards_stale_transport_ids() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(23).unwrap();
        let token = transport.register_test_transport(id);
        transport
            .inner_mut()
            .push_pending_event(TransportEvent::ready(id, true, false, false, false, false));
        transport.remove_registered(token).unwrap();

        assert!(transport.process_pending_data().is_empty());
    }

    #[test]
    fn source_registration_translates_runtime_tokens_and_reads_payload() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(24).unwrap();
        transport
            .inner_mut()
            .push_owned_source(id, PollSourceMode::Readable);
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        let token = transport
            .register_source(source)
            .expect("register runtime source");
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, true, false, false, false, false));
        transport.inner_mut().push_read(id, b"source-bytes");

        let events = transport.poll_events(Some(Duration::ZERO)).unwrap();
        let mut dst = [0_u8; 16];
        let read = transport
            .read_source(token, &mut dst)
            .expect("read source payload");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token, token);
        assert_eq!(&dst[..read], b"source-bytes");
    }

    #[test]
    fn source_removal_discards_stale_runtime_events() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(25).unwrap();
        transport
            .inner_mut()
            .push_owned_source(id, PollSourceMode::Readable);
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        let token = transport
            .register_source(source)
            .expect("register runtime source");

        transport.remove_registered(token).unwrap();
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, true, false, false, false, false));

        assert!(transport
            .poll_events(Some(Duration::ZERO))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn source_enabled_for_test_tracks_runtime_token_state() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(28).unwrap();
        transport
            .inner_mut()
            .push_owned_source(id, PollSourceMode::Readable);
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        let token = transport
            .register_source(source)
            .expect("register runtime source");

        assert!(transport
            .source_enabled_for_test(token)
            .expect("query initial source state"));

        transport
            .set_source_enabled(token, false)
            .expect("disable source");
        assert!(!transport
            .source_enabled_for_test(token)
            .expect("query disabled source state"));

        transport
            .set_source_enabled(token, true)
            .expect("re-enable source");
        assert!(transport
            .source_enabled_for_test(token)
            .expect("query re-enabled source state"));
    }

    #[test]
    fn completion_sources_cannot_be_payload_read_through_runtime() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(26).unwrap();
        transport
            .inner_mut()
            .push_owned_source(id, PollSourceMode::Completion);
        let (source, _writer) = pipe_source(PollSourceMode::Completion);
        let token = transport
            .register_source(source)
            .expect("register completion source");
        let mut dst = [0_u8; 4];

        let error = transport
            .read_source(token, &mut dst)
            .expect_err("completion source must reject payload reads");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn fake_backend_source_failures_propagate_through_runtime_transport() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        transport.inner_mut().fail_register_source();
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        assert_eq!(
            transport
                .register_source(source)
                .expect_err("register failure must propagate")
                .kind(),
            io::ErrorKind::Other
        );

        let id = TransportId::new(27).unwrap();
        transport
            .inner_mut()
            .push_owned_source(id, PollSourceMode::Readable);
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        let token = transport
            .register_source(source)
            .expect("register readable source");

        transport.inner_mut().fail_set_source_enabled(false);
        assert_eq!(
            transport
                .set_source_enabled(token, false)
                .expect_err("disable failure must propagate")
                .kind(),
            io::ErrorKind::Other
        );
        transport.inner_mut().fail_set_source_enabled(true);
        assert_eq!(
            transport
                .set_source_enabled(token, true)
                .expect_err("re-enable failure must propagate")
                .kind(),
            io::ErrorKind::Other
        );
        transport.inner_mut().fail_remove();
        assert_eq!(
            transport
                .remove_registered(token)
                .expect_err("remove failure must propagate")
                .kind(),
            io::ErrorKind::Other
        );
        assert!(transport.is_registered(token));
    }

    #[test]
    fn removing_transport_discards_queued_stale_event() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(11).unwrap();
        let token = transport.register_test_transport(id);

        transport.remove_registered(token).unwrap();
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, true, false, false, false, false));

        assert!(transport
            .poll_events(Some(Duration::ZERO))
            .unwrap()
            .is_empty());
        assert_eq!(transport.inner_mut().removed(), &[id]);
    }

    #[test]
    fn failed_removal_retains_registered_mapping() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(17).unwrap();
        let token = transport.register_test_transport(id);
        transport.inner_mut().fail_remove();

        assert_eq!(
            transport.remove_registered(token).unwrap_err().kind(),
            io::ErrorKind::Other
        );
        assert_eq!(transport.write(token, b"still registered").unwrap(), 16);
        assert_eq!(transport.inner_mut().writes_for(id), b"still registered");
    }

    #[test]
    fn lifecycle_operations_route_through_registered_runtime_tokens() {
        let listener_id = TransportId::new(12).unwrap();
        let unix_listener_id = TransportId::new(13).unwrap();
        let accepted_id = TransportId::new(14).unwrap();
        let connected_id = TransportId::new(15).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6380));
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        transport.inner_mut().push_listener(ListenerHandle {
            id: listener_id,
            inherited_fd: 100,
        });
        transport.inner_mut().push_listener(ListenerHandle {
            id: unix_listener_id,
            inherited_fd: 101,
        });
        transport.inner_mut().push_accepted(
            listener_id,
            ConnectedPeer {
                id: accepted_id,
                peer_addr,
            },
        );
        transport.inner_mut().push_peer(ConnectedPeer {
            id: connected_id,
            peer_addr,
        });

        let listener = transport
            .bind_tcp(SocketAddr::from(([127, 0, 0, 1], 6379)))
            .unwrap();
        let unix_listener = transport
            .bind_unix(
                Path::new("/tmp/versal-runtime-transport.sock"),
                UnixBindOptions {
                    perm: 0,
                    group: None,
                },
            )
            .unwrap();
        let accepted = transport.accept(listener.token).unwrap().unwrap();
        let connected = transport.connect_tcp(peer_addr).unwrap();

        assert_eq!(listener.inherited_fd, 100);
        assert_eq!(unix_listener.inherited_fd, 101);
        assert_eq!(accepted.peer_addr, peer_addr);
        assert_eq!(connected.peer_addr, peer_addr);
        assert_ne!(listener.token, accepted.token);
        assert_ne!(accepted.token, connected.token);

        transport
            .set_interest(accepted.token, Interest::ReadWritable)
            .unwrap();
        assert!(transport
            .take_connect_error(connected.token)
            .unwrap()
            .is_none());
        transport.shutdown(accepted.token).unwrap();
        transport.remove_registered(accepted.token).unwrap();

        assert_eq!(
            transport.inner_mut().interests(),
            &[(accepted_id, Interest::ReadWritable)]
        );
        assert_eq!(transport.inner_mut().shutdowns(), &[accepted_id]);
        assert_eq!(transport.inner_mut().removed(), &[accepted_id]);
    }

    #[test]
    fn transport_stream_writes_by_runtime_token() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(8).unwrap();
        let token = transport.register_test_transport(id);

        transport.stream(token).unwrap().write_all(b"abc").unwrap();

        assert_eq!(transport.inner_mut().writes_for(id), b"abc");
    }

    #[test]
    fn transport_stream_reads_by_runtime_token() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(9).unwrap();
        let token = transport.register_test_transport(id);
        transport.inner_mut().push_read(id, b"read");

        let mut dst = [0; 4];
        transport
            .stream(token)
            .unwrap()
            .read_exact(&mut dst)
            .unwrap();

        assert_eq!(&dst, b"read");
    }

    #[test]
    fn transport_stream_delegates_vectored_writes_by_runtime_token() {
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let id = TransportId::new(10).unwrap();
        let token = transport.register_test_transport(id);
        let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];

        assert_eq!(
            transport
                .stream(token)
                .unwrap()
                .write_vectored(&bufs)
                .unwrap(),
            4
        );

        assert_eq!(transport.inner_mut().writes_for(id), b"abcd");
    }

    #[test]
    fn sentinel_io_connects_through_runtime_transport() {
        use crate::framework::distributed::sentinel::io::SentinelIo;
        use crate::runtime::server::worker::reactor::sources::{
            RuntimeSourceRegistry, SourceOwner,
        };

        let id = TransportId::new(16).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 26379));
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        transport
            .inner_mut()
            .push_peer(ConnectedPeer { id, peer_addr });
        let mut sources = RuntimeSourceRegistry::default();

        let mut pending = std::collections::HashMap::new();
        let token = SentinelIo::connect_tcp(
            &mut RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending),
            "127.0.0.1:26379",
        )
        .unwrap();

        assert_eq!(token.0, 1);
        assert_eq!(
            sources
                .resolve_token(token)
                .map(|route| route.owner.clone()),
            Some(SourceOwner::Sentinel)
        );
    }

    #[test]
    fn blocked_poll_translates_registered_events_without_waiting() {
        let id = TransportId::new(18).unwrap();
        let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
        let token = transport.register_test_transport(id);
        transport
            .inner_mut()
            .push_event(TransportEvent::ready(id, false, true, false, false, true));

        let events = transport.poll_events(Some(Duration::ZERO)).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token, token);
        assert!(!events[0].readable);
        assert!(events[0].writable);
        assert!(!events[0].read_hup);
        assert!(events[0].error);
        assert_eq!(
            transport.inner_mut().poll_timeouts(),
            &[Some(Duration::ZERO)]
        );
    }

    #[test]
    fn concrete_runtime_duplicates_a_stream_by_runtime_token() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let connected = transport
            .connect_tcp(listener.local_addr().unwrap())
            .unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let mut worker = transport.duplicate_stream(connected.token).unwrap();

        peer.write_all(b"read").unwrap();
        let mut dst = [0_u8; 4];
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            match worker.read(&mut dst) {
                Ok(4) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
                result => panic!("unexpected duplicated-stream read result: {result:?}"),
            }
        }
        assert_eq!(&dst, b"read");
    }

    #[test]
    fn concrete_runtime_translates_worker_wake_to_a_runtime_token() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let (token, wake) = transport.register_thread_waker().unwrap();

        wake.wake().unwrap();
        let events = transport.poll_events(Some(Duration::from_secs(1))).unwrap();

        assert!(events.iter().any(|event| event.token == token));
    }

    #[test]
    fn concrete_poll_transfer_restores_events_to_the_same_runtime_generation() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let (token, wake) = transport.register_thread_waker().unwrap();
        let job = transport
            .begin_poll_offload(Some(Duration::from_secs(1)))
            .expect("detach poll job");

        assert_eq!(
            transport
                .poll_events(Some(Duration::ZERO))
                .expect_err("main runtime cannot poll while job owns the poller")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        wake.wake().expect("wake offloaded poll");
        let completion = job.execute();
        let events = transport
            .finish_poll_offload(completion)
            .expect("restore matching poll completion");

        assert!(events.iter().any(|event| event.token == token));
        assert!(transport.poll_events(Some(Duration::ZERO)).is_ok());
        transport
            .remove_registered(token)
            .expect("remove succeeds after poll ownership returns");
        assert!(!transport.is_registered(token));
    }

    #[test]
    fn second_poll_and_registration_are_rejected_until_poll_state_returns() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let connected = transport
            .connect_tcp(listener.local_addr().unwrap())
            .unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let job = transport
            .begin_poll_offload(Some(Duration::ZERO))
            .expect("detach poll job");

        assert_eq!(
            transport
                .begin_poll_offload(Some(Duration::ZERO))
                .err()
                .expect("a second poll must be rejected")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            transport
                .set_interest(connected.token, Interest::Writable)
                .expect_err("interest mutation must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );

        transport
            .finish_poll_offload(job.execute())
            .expect("restore poll state");
        transport
            .set_interest(connected.token, Interest::Writable)
            .expect("interest mutation succeeds after ownership returns");
    }

    #[test]
    fn owned_source_operations_are_rejected_while_poll_state_is_worker_owned() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let (source, _writer) = pipe_source(PollSourceMode::Readable);
        let token = transport
            .register_source(source)
            .expect("register owned source");
        let job = transport
            .begin_poll_offload(Some(Duration::ZERO))
            .expect("detach poll job");

        let (new_source, _new_writer) = pipe_source(PollSourceMode::Readable);
        assert_eq!(
            transport
                .register_source(new_source)
                .expect_err("register must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            transport
                .set_source_enabled(token, false)
                .expect_err("disable must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            transport
                .set_source_enabled(token, true)
                .expect_err("re-enable must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut dst = [0_u8; 1];
        assert_eq!(
            transport
                .read_source(token, &mut dst)
                .expect_err("read must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            transport
                .remove_registered(token)
                .expect_err("remove must wait for poll ownership")
                .kind(),
            io::ErrorKind::WouldBlock
        );

        transport
            .finish_poll_offload(job.execute())
            .expect("restore poll state");
        transport
            .set_source_enabled(token, false)
            .expect("disable after restore");
        transport
            .remove_registered(token)
            .expect("remove after restore");
    }

    #[test]
    fn poll_error_restores_state_and_allows_another_synchronous_poll() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let mut completion = transport
            .begin_poll_offload(Some(Duration::ZERO))
            .expect("detach poll job")
            .execute();
        completion.events = Err(io::Error::other("scripted poll failure"));

        assert_eq!(
            transport
                .finish_poll_offload(completion)
                .expect_err("poll error must propagate")
                .kind(),
            io::ErrorKind::Other
        );
        assert!(transport.poll_events(Some(Duration::ZERO)).is_ok());
    }

    #[test]
    fn shutdown_barrier_rejects_new_poll_offload() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );

        transport.start_shutdown();

        assert_eq!(
            transport
                .begin_poll_offload(Some(Duration::ZERO))
                .err()
                .expect("shutdown must reject poll offload")
                .kind(),
            io::ErrorKind::Interrupted
        );
    }

    #[test]
    fn stale_poll_completion_is_rejected_by_generation() {
        let mut transport = RuntimeTransport::new(
            crate::foundation::io::transport::PollingTransportRuntime::new().unwrap(),
        );
        let mut completion = transport
            .begin_poll_offload(Some(Duration::ZERO))
            .expect("detach poll job")
            .execute();
        completion.generation = completion.generation.saturating_add(1);

        assert_eq!(
            transport
                .finish_poll_offload(completion)
                .expect_err("stale poll completion must be rejected")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(
            transport.poll_events(Some(Duration::ZERO)).is_ok(),
            "a stale generation must still restore the unique poll state"
        );
    }
}
