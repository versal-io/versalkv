use crate::engine::command::context::CommandConn;
use crate::engine::command::{CommandArgs, CommandDescriptor, CommandKeyIndices, ResolvedCommand};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use std::net::SocketAddr;

pub trait ScriptRuntime {
    fn lookup_command(&self, name: &[u8]) -> Option<ResolvedCommand>;
    fn command_acl_categories(
        &self,
        command: CommandDescriptor,
        _args: &CommandArgs,
    ) -> &'static [&'static str] {
        command.acl_categories
    }
    /// Validate a nested script command against the runtime's cluster view.
    /// The engine keeps no topology state; the framework implementation mirrors
    /// Valkey's script-mode module call, which rejects non-local key access
    /// before invoking the command handler.
    fn script_cluster_error(
        &self,
        _command: CommandDescriptor,
        _conn: &dyn CommandConn,
        _args: &CommandArgs,
        _key_indices: CommandKeyIndices,
    ) -> Option<Value> {
        None
    }
    fn execute_command(
        &mut self,
        command: ResolvedCommand,
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
    ) -> Value;
    fn num_dbs(&self) -> usize;
    fn has_maxmemory_limit(&self) -> bool;
    fn has_enough_replicas_to_write(&self) -> bool;
    fn is_readonly_replica(&self) -> bool;
    fn is_stale_replica(&self) -> bool;
    fn has_persistence_write_error(&self) -> bool;
    fn script_disable_deny_scripts(&self) -> bool;
    fn end_command(&mut self, name: &str, rejected: bool);
    fn record_command_call(&mut self, name: &str);
    fn record_command_failure(&mut self, name: &str);
    fn command_stats_name(&self, command: CommandDescriptor, args: &CommandArgs) -> String {
        let _ = args;
        command.name.as_str().to_ascii_lowercase()
    }
    /// Observation gates captured once when script invocation starts, so
    /// nested commands never re-read slowlog/commandlog config or MONITOR
    /// state per call.
    fn script_log_gate(&self) -> super::ScriptLogGate {
        super::ScriptLogGate::default()
    }
    fn remember_tracked_key(&mut self, token: usize, key: &[u8]);
    fn record_slow_command(
        &mut self,
        argv: Vec<Bytes>,
        duration_us: u64,
        peer_addr: &SocketAddr,
        client_name: Option<Bytes>,
    );
    fn record_internal_monitor(&mut self, command: CommandDescriptor, db: u32, argv: Vec<Bytes>);
    fn pending_script_kill(&self) -> bool;
    fn set_script_busy(&mut self, is_function: bool, write_dirty: bool);
}
