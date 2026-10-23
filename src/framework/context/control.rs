use super::{
    ClientPauseState, PendingIoThreadsReconfigure, PendingTcpRebind, PendingTlsReconfigure,
    RestartIntent, ServerContext,
};
use crate::engine::command::SessionId;
use crate::engine::distributed::cluster::ClusterState;
use crate::engine::distributed::sentinel::SentinelState;
use crate::engine::distributed::shutdown::ShutdownState;
use crate::framework::capabilities::io::Token;

impl ServerContext {
    // ── Server control ──
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }
    pub fn set_shutdown_requested(&mut self, val: bool) {
        self.shutdown_requested = val;
    }
    pub fn is_loading(&self) -> bool {
        self.loading || self.host_task_loading
    }
    pub(crate) fn is_host_task_loading(&self) -> bool {
        self.host_task_loading
    }
    pub fn set_loading(&mut self, loading: bool) {
        self.loading = loading;
    }
    pub(crate) fn set_host_task_loading(&mut self, loading: bool) {
        self.host_task_loading = loading;
    }
    pub fn shutdown_state(&self) -> &ShutdownState {
        &self.shutdown_state
    }
    pub(crate) fn shutdown_state_mut(&mut self) -> &mut ShutdownState {
        &mut self.shutdown_state
    }
    pub fn client_pause(&self) -> &ClientPauseState {
        &self.client_pause
    }
    pub(crate) fn client_pause_mut(&mut self) -> &mut ClientPauseState {
        &mut self.client_pause
    }
    pub fn paused_clients(&self) -> &indexmap::IndexSet<Token> {
        &self.paused_clients
    }
    pub(crate) fn paused_clients_mut(&mut self) -> &mut indexmap::IndexSet<Token> {
        &mut self.paused_clients
    }
    pub fn pending_unpause(&self) -> bool {
        self.pending_unpause
    }
    pub fn set_pending_unpause(&mut self, val: bool) {
        self.pending_unpause = val;
    }
    pub fn set_needs_eviction(&mut self, val: bool) {
        self.needs_eviction = val;
    }

    pub(crate) fn needs_eviction(&self) -> bool {
        self.needs_eviction
    }
    pub(crate) fn pending_acl_client_kills_mut(&mut self) -> &mut Vec<String> {
        &mut self.pending_acl_client_kills
    }
    pub(crate) fn pending_acl_tokens_to_kill_mut(&mut self) -> &mut Vec<Token> {
        &mut self.pending_acl_tokens_to_kill
    }
    pub(crate) fn pending_blocked_to_reject_mut(&mut self) -> &mut Vec<SessionId> {
        &mut self.pending_blocked_to_reject
    }
    #[cfg(test)]
    pub fn pending_blocked_to_reject(&self) -> &[SessionId] {
        &self.pending_blocked_to_reject
    }
    pub fn pause_cron(&self) -> u32 {
        self.pause_cron
    }
    pub fn set_pause_cron(&mut self, val: u32) {
        self.pause_cron = val;
    }
    pub(crate) fn protected_clients_mut(&mut self) -> &mut Vec<u64> {
        &mut self.protected_clients
    }
    #[cfg(test)]
    pub fn protected_clients(&self) -> &[u64] {
        &self.protected_clients
    }
    pub(crate) fn request_tcp_rebind(
        &mut self,
        previous_config: crate::framework::context::RuntimeConfig,
        previous_oom_score_adj: Option<i32>,
        bind: String,
        port: u16,
        changed_parameter: &'static str,
    ) {
        self.pending_tcp_rebind = Some(PendingTcpRebind {
            previous_config,
            previous_oom_score_adj,
            bind,
            port,
            changed_parameter,
        });
    }
    pub(crate) fn take_tcp_rebind_request(&mut self) -> Option<PendingTcpRebind> {
        self.pending_tcp_rebind.take()
    }
    pub(crate) fn request_tls_reconfigure(
        &mut self,
        previous_config: crate::framework::context::RuntimeConfig,
        port_changed: bool,
        reconnect_replica: bool,
        rebind_cluster_bus: bool,
        changed_parameter: &'static str,
    ) {
        self.pending_tls_reconfigure = Some(PendingTlsReconfigure {
            previous_config,
            port_changed,
            reconnect_replica,
            rebind_cluster_bus,
            changed_parameter,
        });
    }
    pub(crate) fn take_tls_reconfigure_request(&mut self) -> Option<PendingTlsReconfigure> {
        self.pending_tls_reconfigure.take()
    }
    pub(crate) fn request_io_threads_reconfigure(
        &mut self,
        previous: crate::framework::context::RuntimeConfig,
        requested: usize,
    ) {
        self.pending_io_threads_reconfigure = Some(PendingIoThreadsReconfigure {
            previous,
            requested,
        });
    }
    pub(crate) fn take_io_threads_reconfigure_request(
        &mut self,
    ) -> Option<PendingIoThreadsReconfigure> {
        self.pending_io_threads_reconfigure.take()
    }
    pub(crate) fn request_server_restart(&mut self, intent: RestartIntent) {
        self.pending_server_restart = Some(intent);
    }
    pub(crate) fn take_server_restart_request(&mut self) -> Option<RestartIntent> {
        self.pending_server_restart.take()
    }
    pub(crate) fn set_startup_args(&mut self, args: Vec<std::ffi::OsString>) {
        self.startup_args = args;
    }
    pub(crate) fn startup_args(&self) -> &[std::ffi::OsString] {
        &self.startup_args
    }
    /// Record the raw fd of a listening socket bound at startup so fork
    /// children can close it (see `listener_fds` field docs).
    pub fn register_listener_fd(&mut self, fd: i32) {
        self.listener_fds.push(fd);
    }
    pub(crate) fn replace_listener_fds(&mut self, old: &[i32], new: &[i32]) {
        self.listener_fds.retain(|fd| !old.contains(fd));
        self.listener_fds.extend_from_slice(new);
    }
    /// All recorded listening socket fds, to be closed in fork children.
    pub fn listener_fds(&self) -> &[i32] {
        &self.listener_fds
    }
    pub fn sentinel_state(&self) -> &SentinelState {
        &self.sentinel_state
    }
    pub(crate) fn sentinel_state_mut(&mut self) -> &mut SentinelState {
        &mut self.sentinel_state
    }
    pub fn cluster_state(&self) -> &Option<Box<ClusterState>> {
        &self.cluster_state
    }
    pub(crate) fn cluster_state_mut(&mut self) -> &mut Option<Box<ClusterState>> {
        &mut self.cluster_state
    }
}
