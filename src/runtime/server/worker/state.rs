use crate::foundation::io::sys::fd::{pipe, PipeFlags};
use crate::foundation::io::transport::{
    Interest, OwnedPollSource, PollingTransportRuntime, UnixBindOptions,
};
use crate::framework::capabilities::io::mt_driver::IoDriverMtConfig;
use crate::framework::capabilities::io::Token;
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::link::SentinelLinkManager;
use crate::framework::CommandRegistry;
use crate::runtime::server::worker::distributed::replication::pending_lazyfree::PendingReplicationLazyfree;
use crate::runtime::server::worker::distributed::replication::FullSyncCoordinator;
use crate::runtime::server::worker::flows::maintenance::lazyfree::DataLazyfree;
use crate::runtime::server::worker::reactor::blocked_events::{
    BlockedEventState, PendingReplayContinuation, RdbLoadProgress,
};
use crate::runtime::server::worker::reactor::io_workers::IoOperationId;
use crate::runtime::server::worker::reactor::io_workers::{IoThreadRuntime, WorkerCompletionQueue};
use crate::runtime::server::worker::reactor::queue::EventBatch;
use crate::runtime::server::worker::reactor::sources::{
    ChildRegistry, ChildSignalOwner, RuntimeSourceHandle, RuntimeSourceRegistry, SourceOwner,
};
use crate::runtime::server::worker::reactor::timers::{
    DueTimeEvent, TimeEventId, TimeEventRegistry,
};
use crate::runtime::server::worker::reactor::transport::{
    deregister_or_defer, retire_route_for_teardown, RuntimeEvent, RuntimeTransport,
    SourceInstallRollback,
};
use crate::runtime::server::worker::sessions::write_state::WriteSchedulers;
use crate::runtime::server::worker::sessions::{ConnectionRegistry, ListenerSet};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::time::Instant;

pub(in crate::runtime::server) type ProductionTransport = PollingTransportRuntime;

pub(in crate::runtime::server) const IO_BUF_SIZE: usize = 128 * 1024;
pub(in crate::runtime::server) const NORMAL_CLIENT_READ_BUDGET: usize = 64 * 1024;
pub(in crate::runtime::server) const HUP_CLIENT_READ_BUDGET: usize = NORMAL_CLIENT_READ_BUDGET;
pub(in crate::runtime::server) const HUP_CLIENT_COMMAND_BUDGET: usize = 128;

#[derive(Default)]
pub(in crate::runtime::server) struct ActiveDefragState {
    pub(in crate::runtime::server) db_index: usize,
    pub(in crate::runtime::server) keys: Vec<Vec<u8>>,
    pub(in crate::runtime::server) key_index: usize,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::runtime::server) struct EventLoopCycleStart {
    pub(in crate::runtime::server) started_at: Instant,
    pub(in crate::runtime::server) command_time_usec: u64,
    pub(in crate::runtime::server) commands_processed: u64,
}

pub(in crate::runtime::server) use crate::runtime::server::worker::reactor::transport::{
    PendingRemovalOwner, PendingTransportRemoval,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime::server) enum SocketIoOwner {
    MainThread,
    IoWorkerRead(IoOperationId),
    IoWorkerWrite(IoOperationId),
    IoWorkerReadWrite {
        read: IoOperationId,
        write: IoOperationId,
    },
}

pub(crate) struct ServerWorkerState {
    pub(in crate::runtime::server) transport: RuntimeTransport<ProductionTransport>,
    pub(in crate::runtime::server) runtime_sources: RuntimeSourceRegistry,
    pub(in crate::runtime::server) child_registry: ChildRegistry,
    #[cfg(test)]
    pub(in crate::runtime::server) child_signal_token: Token,
    child_signal_write: Option<File>,
    pub(in crate::runtime::server) listeners: ListenerSet,
    pub(super) tls_reload: super::tls::TlsReloadState,
    pub(in crate::runtime::server) connection_registry: ConnectionRegistry,
    /// Minimal retry registry: tokens whose owner already tore down all business
    /// state but whose L1 deregister failed. A retained entry keeps the
    /// `TransportId`/`Token` reserved (ABA-safe) until a later tick completes the
    /// deregister. Nothing else in the runtime needs to inspect this map.
    pub(in crate::runtime::server) pending_transport_removals:
        HashMap<Token, PendingTransportRemoval>,
    /// Tokens currently executing business-state teardown. This transient guard
    /// prevents nested failure callbacks from retiring one connection twice.
    pub(in crate::runtime::server) retiring_connections: HashSet<Token>,
    pub(in crate::runtime::server) socket_owners: HashMap<Token, SocketIoOwner>,
    pub(in crate::runtime::server) full_sync: FullSyncCoordinator,
    pub(in crate::runtime::server) client_hup_drains: HashSet<Token>,
    pub(in crate::runtime::server) pending_replication_replay: Option<PendingReplayContinuation>,
    pub(in crate::runtime::server) writes: WriteSchedulers,
    /// Downstream socket writes update per-connection queue bytes immediately.
    /// Coalesce the O(replicas) backlog-floor and INFO-cache refresh once per
    /// event-loop turn instead of rescanning every link after every write.
    pub(in crate::runtime::server) replication_buffer_accounting_dirty: bool,
    pub(in crate::runtime::server) blocked_events: BlockedEventState,
    pub(in crate::runtime::server) replication_lazyfree: PendingReplicationLazyfree,
    pub(in crate::runtime::server) data_lazyfree: DataLazyfree,
    /// The replica-side debug pause is a one-shot synchronization fixture.
    /// Keeping this separate from the config flag prevents a reconnect from
    /// stopping the replica again before the harness can resume it.
    pub(in crate::runtime::server) debug_pause_after_fork_consumed: bool,
    pub(in crate::runtime::server) classic_rdb_loading_progress: RdbLoadProgress,
    /// Authoritative phase-aware queue for collected readiness and due timers.
    pub(in crate::runtime::server) event_batch: EventBatch,
    pub(in crate::runtime::server) events_buf: Vec<RuntimeEvent>,
    pub(in crate::runtime::server) io_buf: Vec<u8>,
    pub registry: CommandRegistry,
    pub ctx: ServerContext,
    pub(in crate::runtime::server) sentinel_links: SentinelLinkManager,
    pub(in crate::runtime::server) cluster_bus:
        crate::engine::distributed::cluster::bus::ClusterBusManager,
    /// Nesting depth of event-loop batches draining Cluster Bus readable
    /// sockets. While positive, receive-path election results stay queued
    /// until the batch boundary, reproducing C's beforeSleep failover handling
    /// across every readable connection.
    pub(in crate::runtime::server) cluster_bus_receive_batch_depth: u8,
    pub(in crate::runtime::server) replication_runtime:
        crate::framework::distributed::replication::ReplicationRuntime,
    pub(in crate::runtime::server) slot_migration_runtime:
        crate::framework::distributed::migration::SlotMigrationRuntime,
    pub(in crate::runtime::server) slot_migration_snapshot:
        Option<crate::runtime::server::worker::distributed::migration::SlotSnapshotRelay>,
    pub(in crate::runtime::server) processing_blocked_clients: bool,
    pub(in crate::runtime::server) initial_rdb_replication_info_installed: bool,
    pub(in crate::runtime::server) cluster_config:
        crate::runtime::server::worker::distributed::cluster::ClusterConfigFile,
    pub(in crate::runtime::server) io_threads: IoThreadRuntime,
    pub(in crate::runtime::server) worker_completions: WorkerCompletionQueue,
    pub(in crate::runtime::server) time_events: TimeEventRegistry,
    pub(in crate::runtime::server) pending_due_time_events: HashMap<TimeEventId, DueTimeEvent>,
    pub(in crate::runtime::server) module_timer_event: Option<TimeEventId>,
    pub(in crate::runtime::server) event_loop_cycle_start: Option<EventLoopCycleStart>,
    pub(crate) priority_subnets_generation: u64,
    pub(crate) priority_command_token: Option<Token>,
    pub(crate) priority_command_is_nested: bool,
    pub(crate) command_execution_depth: usize,
    pub(crate) priority_cycle: Option<super::sessions::qos::PriorityCycle>,
    #[cfg(test)]
    pub(in crate::runtime::server) test_time_event_callbacks:
        Vec<Option<Box<dyn FnOnce(&mut ServerWorkerState) + 'static>>>,
    #[cfg(test)]
    pub(in crate::runtime::server) turn_trace:
        Vec<crate::runtime::server::worker::reactor::TurnTracePoint>,
    #[cfg(test)]
    pub(in crate::runtime::server) blocked_round_trace:
        Vec<crate::runtime::server::worker::reactor::BlockedRoundPhase>,
    #[cfg(test)]
    pub(in crate::runtime::server) blocked_cron_calls: usize,
    /// Stale-primary recovery guard. It compares full slot
    /// bitmaps and therefore runs on a bounded cadence rather than every cron
    /// tick; normal C-compatible ownership updates use the bus path directly.
    pub(in crate::runtime::server) last_stale_primary_guard_ms: Option<i64>,
    pub(in crate::runtime::server) active_defrag: ActiveDefragState,
}

impl ServerWorkerState {
    pub(in crate::runtime::server) fn consume_debug_pause_after_fork(&mut self) -> bool {
        if !self.ctx.config().server.debug_pause_after_fork {
            self.debug_pause_after_fork_consumed = false;
            return false;
        }
        if self.debug_pause_after_fork_consumed {
            return false;
        }
        self.debug_pause_after_fork_consumed = true;
        true
    }

    pub fn new(num_dbs: u32) -> std::io::Result<Self> {
        Self::new_with_context(ServerContext::new(num_dbs))
    }

    pub fn new_with_slot_router(
        num_dbs: u32,
        slot_bits: u32,
        slot_router: fn(&[u8]) -> usize,
    ) -> std::io::Result<Self> {
        Self::new_with_context(ServerContext::new_with_slot_router(
            num_dbs,
            slot_bits,
            slot_router,
        ))
    }

    fn new_with_context(ctx: ServerContext) -> std::io::Result<Self> {
        let mut transport = RuntimeTransport::new(ProductionTransport::new()?);
        let (io_wake_token, io_wake) = transport.register_thread_waker()?;
        let mut runtime_sources = RuntimeSourceRegistry::default();
        if let Err(error) = runtime_sources.install(io_wake_token, SourceOwner::IoWorkerWake) {
            let cleanup = transport.remove_registered(io_wake_token);
            return Err(source_install_error(error, cleanup.err()));
        }
        let worker_completions = WorkerCompletionQueue::new(io_wake.clone());
        let worker_completion_sender = worker_completions.sender();
        let io_threads = IoThreadRuntime::new_with_completion_sender(
            IoDriverMtConfig::new(1),
            io_wake_token,
            io_wake,
            worker_completion_sender.clone(),
        )?;
        let child_signal_pipe = pipe(PipeFlags::CLOEXEC_NONBLOCK)?;
        let (child_signal_read, child_signal_write) = child_signal_pipe.into_parts();
        let child_signal_token =
            transport.register_source(OwnedPollSource::readable(child_signal_read))?;
        if let Err(error) = runtime_sources.install(
            child_signal_token,
            SourceOwner::ChildSignal(ChildSignalOwner::SelfPipe),
        ) {
            let cleanup = transport.remove_registered(child_signal_token);
            return Err(source_install_error(error, cleanup.err()));
        }
        crate::foundation::io::sys::signal::install_child_signal_wake(
            child_signal_write.as_raw_fd(),
        );
        Ok(Self {
            transport,
            runtime_sources,
            child_registry: ChildRegistry::default(),
            #[cfg(test)]
            child_signal_token,
            child_signal_write: Some(child_signal_write),
            listeners: ListenerSet::default(),
            tls_reload: super::tls::TlsReloadState::default(),
            connection_registry: ConnectionRegistry::default(),
            pending_transport_removals: HashMap::new(),
            retiring_connections: HashSet::new(),
            socket_owners: HashMap::new(),
            full_sync: FullSyncCoordinator::default(),
            client_hup_drains: HashSet::new(),
            pending_replication_replay: None,
            writes: WriteSchedulers::default(),
            replication_buffer_accounting_dirty: false,
            blocked_events: BlockedEventState::default(),
            replication_lazyfree: PendingReplicationLazyfree::new_with_completion_sender(
                worker_completion_sender.clone(),
            ),
            data_lazyfree: DataLazyfree::new_with_completion_sender(worker_completion_sender),
            debug_pause_after_fork_consumed: false,
            classic_rdb_loading_progress: RdbLoadProgress::default(),
            event_batch: EventBatch::default(),
            events_buf: Vec::with_capacity(1024),
            io_buf: vec![0u8; IO_BUF_SIZE],
            registry: CommandRegistry::new(),
            ctx,
            sentinel_links: SentinelLinkManager::new(),
            cluster_bus: crate::engine::distributed::cluster::bus::ClusterBusManager::new(),
            cluster_bus_receive_batch_depth: 0,
            replication_runtime:
                crate::framework::distributed::replication::ReplicationRuntime::new(),
            slot_migration_runtime:
                crate::framework::distributed::migration::SlotMigrationRuntime::default(),
            slot_migration_snapshot: None,
            processing_blocked_clients: false,
            initial_rdb_replication_info_installed: false,
            cluster_config:
                crate::runtime::server::worker::distributed::cluster::ClusterConfigFile::default(),
            io_threads,
            worker_completions,
            time_events: TimeEventRegistry::default(),
            pending_due_time_events: HashMap::new(),
            module_timer_event: None,
            event_loop_cycle_start: None,
            priority_subnets_generation: 0,
            priority_command_token: None,
            priority_command_is_nested: false,
            command_execution_depth: 0,
            priority_cycle: None,
            #[cfg(test)]
            test_time_event_callbacks: Vec::new(),
            #[cfg(test)]
            turn_trace: Vec::new(),
            #[cfg(test)]
            blocked_round_trace: Vec::new(),
            #[cfg(test)]
            blocked_cron_calls: 0,
            last_stale_primary_guard_ms: None,
            active_defrag: ActiveDefragState::default(),
        })
    }

    /// The single centralized teardown predicate. Consulted only by the event
    /// dispatcher so stale readiness events for an already-isolated owner are
    /// dropped until its retained L1 registration is finally deregistered.
    pub(in crate::runtime::server) fn is_pending_transport_removal(&self, token: Token) -> bool {
        self.pending_transport_removals.contains_key(&token)
    }

    pub(in crate::runtime::server) fn socket_owner(&self, token: Token) -> SocketIoOwner {
        self.socket_owners
            .get(&token)
            .copied()
            .unwrap_or(SocketIoOwner::MainThread)
    }

    pub(in crate::runtime::server) fn socket_owned_by_main_thread(&self, token: Token) -> bool {
        matches!(self.socket_owner(token), SocketIoOwner::MainThread)
    }

    pub(in crate::runtime::server) fn socket_read_owned_by_main_thread(
        &self,
        token: Token,
    ) -> bool {
        matches!(
            self.socket_owner(token),
            SocketIoOwner::MainThread | SocketIoOwner::IoWorkerWrite(_)
        )
    }

    pub(in crate::runtime::server) fn socket_write_owned_by_main_thread(
        &self,
        token: Token,
    ) -> bool {
        matches!(
            self.socket_owner(token),
            SocketIoOwner::MainThread | SocketIoOwner::IoWorkerRead(_)
        )
    }

    pub(in crate::runtime::server) fn acquire_io_worker_read_socket_lease(
        &mut self,
        id: IoOperationId,
    ) {
        let owner = match self.socket_owner(id.token) {
            SocketIoOwner::MainThread => SocketIoOwner::IoWorkerRead(id),
            SocketIoOwner::IoWorkerWrite(write) => {
                SocketIoOwner::IoWorkerReadWrite { read: id, write }
            }
            SocketIoOwner::IoWorkerRead(_) | SocketIoOwner::IoWorkerReadWrite { .. } => return,
        };
        self.socket_owners.insert(id.token, owner);
    }

    pub(in crate::runtime::server) fn acquire_io_worker_write_socket_lease(
        &mut self,
        id: IoOperationId,
    ) {
        let owner = match self.socket_owner(id.token) {
            SocketIoOwner::MainThread => SocketIoOwner::IoWorkerWrite(id),
            SocketIoOwner::IoWorkerRead(read) => {
                SocketIoOwner::IoWorkerReadWrite { read, write: id }
            }
            SocketIoOwner::IoWorkerWrite(_) | SocketIoOwner::IoWorkerReadWrite { .. } => return,
        };
        self.socket_owners.insert(id.token, owner);
    }

    pub(in crate::runtime::server) fn release_io_worker_read_socket_lease(
        &mut self,
        id: IoOperationId,
    ) {
        match self.socket_owner(id.token) {
            SocketIoOwner::IoWorkerRead(read) if read == id => {
                self.socket_owners.remove(&id.token);
            }
            SocketIoOwner::IoWorkerReadWrite { read, write } if read == id => {
                self.socket_owners
                    .insert(id.token, SocketIoOwner::IoWorkerWrite(write));
            }
            _ => {}
        }
    }

    pub(in crate::runtime::server) fn release_io_worker_write_socket_lease(
        &mut self,
        id: IoOperationId,
    ) {
        let was_released = match self.socket_owner(id.token) {
            SocketIoOwner::IoWorkerWrite(write) if write == id => {
                self.socket_owners.remove(&id.token);
                true
            }
            SocketIoOwner::IoWorkerReadWrite { read, write } if write == id => {
                self.socket_owners
                    .insert(id.token, SocketIoOwner::IoWorkerRead(read));
                true
            }
            _ => false,
        };
        if was_released {
            self.queue_forked_debugger_for_reprocessing(id.token);
        }
    }

    pub(in crate::runtime::server) fn route_runtime_source(
        &mut self,
        token: Token,
        owner: SourceOwner,
    ) -> std::io::Result<RuntimeSourceHandle> {
        match self.runtime_sources.install(token, owner.clone()) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let cleanup = SourceInstallRollback::new(
                    &mut self.transport,
                    &mut self.runtime_sources,
                    &mut self.pending_transport_removals,
                )
                .quarantine_failed_registration(token, owner, &error);
                Err(source_install_error(error, cleanup.err()))
            }
        }
    }

    // Tasks 6-11 install non-socket readiness producers through this atomic
    // helper without exposing L1 transport identifiers.
    // TODO(dead_code): remove after runtime owned sources are used in production.
    #[allow(dead_code)]
    pub(in crate::runtime::server) fn install_runtime_source(
        &mut self,
        source: OwnedPollSource,
        owner: SourceOwner,
    ) -> std::io::Result<RuntimeSourceHandle> {
        let token = self.transport.register_source(source)?;
        match self.runtime_sources.install(token, owner.clone()) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let cleanup = SourceInstallRollback::new(
                    &mut self.transport,
                    &mut self.runtime_sources,
                    &mut self.pending_transport_removals,
                )
                .quarantine_failed_registration(token, owner, &error);
                Err(source_install_error(error, cleanup.err()))
            }
        }
    }

    pub(in crate::runtime::server) fn retire_runtime_source(&mut self, token: Token) {
        retire_route_for_teardown(
            &mut self.runtime_sources,
            &self.pending_transport_removals,
            token,
            "runtime source route",
        );
    }

    #[cfg(test)]
    pub(in crate::runtime::server) fn install_test_connection_source(
        &mut self,
        token: Token,
    ) -> RuntimeSourceHandle {
        self.runtime_sources
            .install(token, SourceOwner::Connection)
            .unwrap_or_else(|error| {
                panic!(
                    "test setup must install a unique runtime route for connection token {}: {}",
                    token.0, error
                )
            })
    }

    pub(in crate::runtime::server) fn route_listener_handle(
        &mut self,
        handle: crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle,
        owner: SourceOwner,
    ) -> std::io::Result<crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle>
    {
        self.route_runtime_source(handle.token, owner)?;
        Ok(handle)
    }

    pub(in crate::runtime::server) fn route_cluster_bus_listener_handle(
        &mut self,
        handle: crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle,
    ) -> std::io::Result<crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle>
    {
        self.route_listener_handle(handle, SourceOwner::ClusterBus)
    }

    /// Complete the L1 deregister for an owner that is already isolated from
    /// every business index, or retain a single retry entry if the deregister
    /// fails. Keeping the registration reserves the `TransportId`/`Token`
    /// (ABA-safe) until a later `retry_pending_transport_removals` succeeds.
    pub(in crate::runtime::server) fn deregister_or_defer(
        &mut self,
        token: Token,
        owner_kind: PendingRemovalOwner,
    ) {
        let _ = self.deregister_or_defer_with_result(token, owner_kind);
    }

    pub(in crate::runtime::server) fn deregister_or_defer_with_result(
        &mut self,
        token: Token,
        owner_kind: PendingRemovalOwner,
    ) -> std::io::Result<()> {
        if !self.transport.is_registered(token) {
            self.pending_transport_removals.remove(&token);
            return Ok(());
        }
        deregister_or_defer(
            &mut self.transport,
            &mut self.pending_transport_removals,
            token,
            owner_kind,
        )
    }

    /// Retry every retained teardown from a later background/cron tick. Owners
    /// are already isolated, so a successful deregister just drops the retry
    /// entry; the transport registry frees the `TransportId` only here.
    pub(in crate::runtime::server) fn retry_pending_transport_removals(&mut self) {
        crate::runtime::server::worker::reactor::transport::retry_pending_transport_removals(
            &mut self.transport,
            &mut self.pending_transport_removals,
        );
    }

    pub(in crate::runtime::server) fn queue_client_reply(
        &mut self,
        token: Token,
        frame: Vec<u8>,
    ) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)
        else {
            return false;
        };
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return false;
        }
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(frame);
        self.refresh_client_memory_usage(token, false);
        self.reconcile_slot_export_output_allocation(token.0);
        let is_slot_export = matches!(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .slot_migration_role,
            Some(
                crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                    _
                )
            )
        );
        if is_slot_export {
            return self.arm_slot_export_output(token.0);
        }
        if self
            .replication_runtime
            .role(crate::framework::distributed::replication::ConnectionId(
                token.0,
            ))
            .is_none()
        {
            return self.schedule_ordinary_write(token.0);
        }
        if self.enforce_client_output_buffer_limit_for(token.0) {
            return false;
        }
        let interest = if self.replication_runtime.replica_main_read_paused()
            && self.ctx.persistence().forkless_capture_in_progress()
            && self
                .replication_runtime
                .replica_link()
                .is_some_and(|link| link.connection_id().0 == token.0)
        {
            crate::foundation::io::transport::Interest::Writable
        } else {
            crate::foundation::io::transport::Interest::ReadWritable
        };
        self.transport.set_interest(token, interest).ok();
        true
    }

    pub(in crate::runtime::server) fn queue_replica_loading_newline(
        &mut self,
        main: crate::framework::distributed::replication::ConnectionId,
    ) -> bool {
        if self
            .replication_runtime
            .replica_link()
            .is_none_or(|link| link.connection_id() != main)
        {
            return false;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main.0))
            .then_some(main.0)
        else {
            return false;
        };
        let Some(connection) = self
            .connection_registry
            .get_mut(crate::framework::capabilities::io::Token(connection_token))
        else {
            return false;
        };
        connection.queue_reply(b"\n".to_vec());
        let interest = if self.replication_runtime.replica_main_read_paused() {
            Interest::Writable
        } else {
            Interest::ReadWritable
        };
        if let Err(error) = self.transport.set_interest(Token(main.0), interest) {
            crate::server_log!(
                warning,
                "Failed to queue replica loading newline: token={} interest={:?} error={}",
                main.0,
                interest,
                error
            );
            self.remove_connection(main.0);
            return false;
        }
        true
    }

    pub fn bind(&mut self, addr: SocketAddr) -> std::io::Result<()> {
        let handle = self.transport.bind_tcp(addr)?;
        let handle = self.route_listener_handle(handle, SourceOwner::Listener)?;
        self.register_tcp_listener(addr, handle);
        Ok(())
    }

    pub fn bind_mptcp(&mut self, addr: SocketAddr) -> std::io::Result<()> {
        let handle = self.transport.bind_mptcp(addr)?;
        let handle = self.route_listener_handle(handle, SourceOwner::Listener)?;
        self.register_tcp_listener(addr, handle);
        Ok(())
    }

    pub(in crate::runtime::server) fn configure_tls_client_from_config(
        &mut self,
    ) -> std::io::Result<()> {
        self.configure_tls_contexts_from_config()
    }

    pub(crate) fn bind_tls(&mut self, addr: SocketAddr) -> std::io::Result<()> {
        let handle = self.transport.bind_tls(addr)?;
        let handle = self.route_listener_handle(handle, SourceOwner::Listener)?;
        self.register_tls_listener(handle);
        Ok(())
    }

    fn register_tls_listener(
        &mut self,
        handle: crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle,
    ) {
        self.listeners.all.push(handle.token);
        self.listeners.tls.push(handle.token);
        self.listeners
            .inherited_fds
            .insert(handle.token.0, handle.inherited_fd);
        self.ctx.register_listener_fd(handle.inherited_fd);
    }

    fn register_tcp_listener(
        &mut self,
        addr: SocketAddr,
        handle: crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle,
    ) {
        self.listeners.all.push(handle.token);
        self.listeners.tcp.push(handle.token);
        self.listeners.bind_addrs.push(addr);
        self.listeners
            .inherited_fds
            .insert(handle.token.0, handle.inherited_fd);
        self.ctx.register_listener_fd(handle.inherited_fd);
    }

    pub(crate) fn configure_tcp_backlog_from_config(&mut self) -> std::io::Result<()> {
        self.transport
            .set_tcp_backlog(self.ctx.config().server.tcp_backlog as i32)
    }

    pub(crate) fn configure_bio_cpulist_from_config(&self) -> std::io::Result<()> {
        let affinity = crate::foundation::io::sys::process::prepare_cpu_affinity(
            &self.ctx.config().server.bio_cpulist,
        )?;
        self.data_lazyfree
            .configure_cpu_affinity(affinity.clone())?;
        self.replication_lazyfree.configure_cpu_affinity(affinity)
    }

    pub(crate) fn bind_cluster_bus_listener(
        &mut self,
        addr: SocketAddr,
        use_tls: bool,
    ) -> std::io::Result<()> {
        let handle = if use_tls {
            self.transport.bind_tls(addr)
        } else {
            self.transport.bind_tcp(addr)
        }?;
        let handle = self.route_cluster_bus_listener_handle(handle)?;
        self.cluster_bus.listener_tokens.push(handle.token);
        self.ctx.register_listener_fd(handle.inherited_fd);
        Ok(())
    }

    pub(crate) fn install_startup_replica_resume(
        &mut self,
        master: crate::framework::distributed::replication::MasterInfo,
        replid: String,
        offset: crate::engine::distributed::replication::ReplicationOffset,
        selected_db: u32,
    ) {
        self.replication_runtime
            .set_replica_resume(master, replid, offset, selected_db);
    }

    pub(crate) fn mark_initial_rdb_replication_info_installed(&mut self) {
        self.initial_rdb_replication_info_installed = true;
    }

    pub fn bind_unix(&mut self, path: &str, perm: u32, group: &str) -> std::io::Result<()> {
        let handle = self.transport.bind_unix(
            std::path::Path::new(path),
            UnixBindOptions {
                perm,
                group: (!group.is_empty()).then(|| group.to_string()),
            },
        )?;
        let handle = self.route_listener_handle(handle, SourceOwner::Listener)?;
        self.listeners.all.push(handle.token);
        self.listeners
            .inherited_fds
            .insert(handle.token.0, handle.inherited_fd);
        self.ctx.register_listener_fd(handle.inherited_fd);
        Ok(())
    }
}

fn source_install_error(
    error: crate::runtime::server::worker::reactor::sources::RuntimeSourceRegistryError,
    cleanup_error: Option<std::io::Error>,
) -> std::io::Error {
    let message = match cleanup_error {
        Some(cleanup_error) => {
            format!("{error}; transport rollback also failed: {cleanup_error}")
        }
        None => error.to_string(),
    };
    std::io::Error::new(std::io::ErrorKind::AlreadyExists, message)
}

impl Drop for ServerWorkerState {
    fn drop(&mut self) {
        if let Some(write) = self.child_signal_write.take() {
            crate::foundation::io::sys::signal::clear_child_signal_wake(write.as_raw_fd());
        }
        if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
            relay.release_child();
        }
        self.cancel_disk_sync_batch();
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;

    #[test]
    fn replica_debug_pause_is_consumed_once_across_reconnects() {
        let mut backend = ServerWorkerState::new(1).expect("event loop test runtime");
        backend.ctx.config_mut().server.debug_pause_after_fork = true;
        assert!(backend.consume_debug_pause_after_fork());
        assert!(!backend.consume_debug_pause_after_fork());
        backend.ctx.config_mut().server.debug_pause_after_fork = false;
        assert!(!backend.consume_debug_pause_after_fork());
        backend.ctx.config_mut().server.debug_pause_after_fork = true;
        assert!(backend.consume_debug_pause_after_fork());
    }

    #[test]
    fn tcp_listener_rebind_is_atomic() {
        use crate::foundation::io::sys::socket::socket_port_raw;
        use socket2::{Domain, Protocol, Socket, Type};
        use std::net::{SocketAddr, TcpListener, TcpStream};

        let mut backend = ServerWorkerState::new(1).expect("event loop test runtime");
        backend
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("bind old listener");
        let old_tokens = backend.listeners.tcp.clone();
        assert_eq!(old_tokens.len(), 1);
        let old_fd = backend.listeners.inherited_fds[&old_tokens[0].0];
        let old_port = socket_port_raw(old_fd, false).expect("read old listener port");
        assert_ne!(old_port, 0);
        backend.ctx.config_mut().server.listening_port = old_port;

        // Keep a non-listening wildcard reservation until the specific listener
        // is bound. A same-address reservation conflicts with it on macOS.
        let reservation =
            Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).expect("new port socket");
        reservation
            .set_reuse_address(true)
            .expect("allow listener address reuse");
        reservation
            .bind(&SocketAddr::from(([0, 0, 0, 0], 0)).into())
            .expect("reserve new port");
        let new_port = reservation
            .local_addr()
            .expect("read reserved address")
            .as_socket()
            .expect("reserved IP address")
            .port();
        assert_ne!(new_port, 0);
        assert_ne!(new_port, old_port);

        backend
            .rebind_tcp_listeners("127.0.0.1", new_port)
            .expect("rebind listener");
        drop(reservation);

        assert_eq!(backend.ctx.config().server.listening_port, new_port);
        assert!(old_tokens
            .iter()
            .all(|token| !backend.transport.is_registered(*token)));
        assert_eq!(backend.listeners.tcp.len(), old_tokens.len());
        assert!(backend
            .listeners
            .tcp
            .iter()
            .all(|token| backend.transport.is_registered(*token)));
        let occupied = TcpListener::bind("127.0.0.1:0").expect("reserve occupied port");
        let occupied_port = occupied.local_addr().unwrap().port();
        let previous_tokens = backend.listeners.tcp.clone();
        let previous_bind = backend.ctx.config().server.bind.clone();
        let previous_addresses = backend.listeners.bind_addrs.clone();
        let error = backend
            .rebind_tcp_listeners("127.0.0.1", occupied_port)
            .expect_err("occupied listener must prevent rebind");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(backend.ctx.config().server.listening_port, new_port);
        assert_eq!(backend.ctx.config().server.bind, previous_bind);
        assert_eq!(backend.listeners.bind_addrs, previous_addresses);
        // Failed replacement restores the endpoint with fresh runtime tokens.
        assert_eq!(backend.listeners.tcp.len(), previous_tokens.len());
        assert!(previous_tokens
            .iter()
            .all(|token| !backend.transport.is_registered(*token)));
        assert!(backend
            .listeners
            .tcp
            .iter()
            .all(|token| backend.transport.is_registered(*token)));
        assert!(TcpStream::connect(("127.0.0.1", new_port)).is_ok());
        drop(occupied);
    }
}
