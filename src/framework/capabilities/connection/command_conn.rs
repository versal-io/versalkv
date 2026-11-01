use super::{Connection, TransactionEnd};
use crate::engine::command::context::{
    BlockedCmd, BlockedState, CommandConn, ConnectionState, WatchedKey,
};
use crate::engine::command::CommandArgs;
use crate::engine::command::SessionId;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::foundation::io::sys::time::mstime;
use crate::framework::capabilities::io::Token;
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Instant;

impl CommandConn for Connection {
    fn token(&self) -> Token {
        self.token
    }

    fn client_id(&self) -> u64 {
        self.client_id
    }

    fn session_id(&self) -> SessionId {
        self.session_id()
    }

    fn command_seq(&self) -> u64 {
        self.command_seq
    }

    fn last_write_offset(&self) -> crate::engine::distributed::replication::ReplicationOffset {
        self.last_write_offset
    }

    fn set_last_write_offset(
        &mut self,
        offset: crate::engine::distributed::replication::ReplicationOffset,
    ) {
        self.last_write_offset = offset;
    }

    fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    fn resp_version(&self) -> u8 {
        self.resp_version
    }

    fn set_resp_version(&mut self, v: u8) {
        self.resp_version = v;
    }

    fn authenticated(&self) -> bool {
        self.authenticated
    }

    fn set_authenticated(&mut self, v: bool) {
        self.authenticated = v;
    }

    fn ever_authenticated(&self) -> bool {
        self.ever_authenticated
    }

    fn set_ever_authenticated(&mut self, v: bool) {
        self.ever_authenticated = v;
    }

    fn username(&self) -> &str {
        &self.username
    }

    fn set_username(&mut self, v: String) {
        self.username = v;
    }

    fn selected_db(&self) -> u32 {
        self.selected_db
    }

    fn set_selected_db(&mut self, db: u32) {
        self.selected_db = db;
    }

    fn last_interaction(&self) -> Instant {
        self.last_interaction
    }

    fn set_last_interaction(&mut self, t: Instant) {
        self.last_interaction = t;
    }

    fn create_time(&self) -> Instant {
        self.create_time
    }

    fn name(&self) -> Option<&Bytes> {
        self.name.as_ref()
    }

    fn set_name(&mut self, name: Option<Bytes>) {
        self.name = name;
    }

    fn lib_name(&self) -> &str {
        &self.lib_name
    }

    fn set_lib_name(&mut self, v: String) {
        self.lib_name = v;
    }

    fn lib_ver(&self) -> &str {
        &self.lib_ver
    }

    fn set_lib_ver(&mut self, v: String) {
        self.lib_ver = v;
    }

    fn capa_flags(&self) -> &[String] {
        &self.capa_flags
    }

    fn set_capa_flags(&mut self, flags: Vec<String>) {
        self.capa_flags = flags;
    }

    fn req_bytes_curr_cmd(&self) -> usize {
        self.request_bytes_current_command()
    }

    fn reply_bytes_curr_cmd(&self) -> usize {
        self.reply_bytes_current_command()
    }

    fn output_buffer_memory_usage(&self) -> usize {
        self.client_memory_usage().output
    }

    fn output_list_len(&self) -> usize {
        self.output.queue.len()
    }

    fn transaction_memory_usage(&self) -> usize {
        self.client_memory_usage().transaction
    }

    fn total_memory_usage(&self) -> usize {
        self.client_memory_usage().total
    }

    fn query_buffer_used(&self) -> usize {
        self.query_buffer_used()
    }

    fn query_buffer_free(&self) -> usize {
        self.query_buffer_free()
    }

    fn diagnostic_query_snapshot(&self) -> &[u8] {
        &self.last_query_snapshot
    }

    fn no_touch(&self) -> bool {
        self.no_touch
    }

    fn set_no_touch(&mut self, v: bool) {
        self.no_touch = v;
    }

    fn no_evict(&self) -> bool {
        self.no_evict
    }

    fn set_no_evict(&mut self, v: bool) {
        self.no_evict = v;
    }

    fn import_source(&self) -> bool {
        self.import_source
    }

    fn set_import_source(&mut self, v: bool) {
        self.import_source = v;
    }

    fn slot_migration_role(
        &self,
    ) -> Option<&crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole> {
        self.slot_migration_role.as_ref()
    }

    fn set_slot_migration_role(
        &mut self,
        role: Option<crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole>,
    ) {
        self.slot_migration_role = role;
    }

    fn readonly(&self) -> bool {
        self.readonly
    }

    fn set_readonly(&mut self, v: bool) {
        self.readonly = v;
    }

    fn asking(&self) -> bool {
        self.asking
    }

    fn set_asking(&mut self, v: bool) {
        self.asking = v;
    }

    fn deny_blocking(&self) -> bool {
        self.deny_blocking
    }

    fn set_deny_blocking(&mut self, v: bool) {
        self.deny_blocking = v;
    }

    fn tot_net_in(&self) -> u64 {
        self.tot_net_in
    }

    fn tot_net_out(&self) -> u64 {
        self.tot_net_out
    }

    fn out_before(&self) -> u64 {
        self.output_before_current_command()
    }

    fn cmd_count(&self) -> u64 {
        self.cmd_count
    }

    fn set_cmd_count(&mut self, v: u64) {
        self.cmd_count = v;
    }

    fn reply_off(&self) -> bool {
        self.reply.off
    }

    fn set_reply_off(&mut self, v: bool) {
        self.reply.off = v;
    }

    fn reply_skip(&self) -> bool {
        self.reply.skip
    }

    fn set_reply_skip(&mut self, v: bool) {
        self.reply.skip = v;
    }

    fn reply_skip_next(&self) -> bool {
        self.reply.skip_next
    }

    fn set_reply_skip_next(&mut self, v: bool) {
        self.reply.skip_next = v;
    }

    fn reply_suppressed(&self) -> bool {
        self.command_reply_suppressed()
    }

    fn suppress_reply(&mut self) {
        self.suppress_command_reply();
    }

    fn tracking(&self) -> bool {
        self.tracking.enabled
    }

    fn set_tracking(&mut self, v: bool) {
        self.tracking.enabled = v;
    }

    fn tracking_bcast(&self) -> bool {
        self.tracking.broadcast
    }

    fn set_tracking_bcast(&mut self, v: bool) {
        self.tracking.broadcast = v;
    }

    fn tracking_noloop(&self) -> bool {
        self.tracking.no_loop
    }

    fn set_tracking_noloop(&mut self, v: bool) {
        self.tracking.no_loop = v;
    }

    fn tracking_optin(&self) -> bool {
        self.tracking.opt_in
    }

    fn set_tracking_optin(&mut self, v: bool) {
        self.tracking.opt_in = v;
    }

    fn tracking_optout(&self) -> bool {
        self.tracking.opt_out
    }

    fn set_tracking_optout(&mut self, v: bool) {
        self.tracking.opt_out = v;
    }

    fn tracking_caching(&self) -> bool {
        self.tracking.caching
    }

    fn set_tracking_caching(&mut self, v: bool) {
        self.tracking.caching = v;
    }

    fn tracking_redirect(&self) -> Option<usize> {
        self.tracking.redirect
    }

    fn set_tracking_redirect(&mut self, v: Option<usize>) {
        self.tracking.redirect = v;
    }

    fn tracking_prefixes(&self) -> &[Bytes] {
        &self.tracking.prefixes
    }

    fn set_tracking_prefixes(&mut self, prefixes: Vec<Bytes>) {
        self.tracking.prefix_node_count = super::tracking_prefix_rax_node_count(&prefixes);
        self.tracking.prefixes = prefixes;
    }

    fn tracking_broken_redirect(&self) -> bool {
        self.tracking.broken_redirect
    }

    fn set_tracking_broken_redirect(&mut self, v: bool) {
        self.tracking.broken_redirect = v;
    }

    fn monitoring(&self) -> bool {
        self.monitoring
    }

    fn set_monitoring(&mut self, v: bool) {
        self.monitoring = v;
    }

    fn sub_channel_count(&self) -> usize {
        self.sub_channel_count
    }

    fn set_sub_channel_count(&mut self, v: usize) {
        self.sub_channel_count = v;
    }

    fn psub_channel_count(&self) -> usize {
        self.psub_channel_count
    }

    fn set_psub_channel_count(&mut self, v: usize) {
        self.psub_channel_count = v;
    }

    fn ssub_channel_count(&self) -> usize {
        self.ssub_channel_count
    }

    fn set_ssub_channel_count(&mut self, v: usize) {
        self.ssub_channel_count = v;
    }

    fn in_transaction(&self) -> bool {
        self.transaction.is_active()
    }

    fn begin_transaction(&mut self) -> bool {
        self.transaction.begin(
            i64::from(self.selected_db),
            self.request_bytes_current_command() as u64,
        )
    }

    fn discard_transaction(&mut self) {
        self.transaction.end(TransactionEnd::Discard);
    }

    fn transaction_db_id(&self) -> i64 {
        self.transaction
            .selected_db()
            .unwrap_or(i64::from(self.selected_db))
    }

    fn custom_reply(&self) -> bool {
        self.command_has_custom_reply()
    }

    fn set_custom_reply(&mut self, v: bool) {
        self.set_command_custom_reply(v);
    }

    fn request_cluster_config_save(&mut self) {
        self.request_cluster_config_save_intent(Value::ok());
    }

    fn request_cluster_config_save_with_reply(&mut self, reply: Value) {
        self.request_cluster_config_save_intent(reply);
    }

    fn skip_propagate(&self) -> bool {
        self.skip_propagation()
    }

    fn set_skip_propagate(&mut self, v: bool) {
        self.set_skip_propagation(v);
    }

    fn record_dirty_delta(&mut self, delta: i64) {
        Connection::record_dirty_delta(self, delta);
    }

    fn mark_transaction_queue_error(&mut self) {
        self.transaction.mark_queue_error();
    }

    fn clear_watched_keys(&mut self) {
        self.transaction.clear_watches();
    }

    fn watch_keys(&mut self, watched: Vec<WatchedKey>) -> usize {
        self.transaction.watch_many(watched)
    }

    fn watched_keys(&self) -> &[WatchedKey] {
        self.transaction.watched_keys()
    }

    fn record_custom_propagation(&mut self, frame: Vec<u8>) {
        Connection::record_custom_propagation(self, frame);
    }

    fn extend_custom_propagation(&mut self, frames: Vec<Vec<u8>>) {
        Connection::extend_custom_propagation(self, frames);
    }

    fn clear_custom_propagation(&mut self) {
        Connection::clear_custom_propagation(self);
    }

    fn custom_propagation_checkpoint(&self) -> usize {
        Connection::custom_propagation_checkpoint(self)
    }

    fn take_custom_propagation_since(&mut self, checkpoint: usize) -> Vec<Vec<u8>> {
        Connection::take_custom_propagation_since(self, checkpoint)
    }

    fn custom_propagation(&self) -> &[Vec<u8>] {
        Connection::custom_propagation(self)
    }

    fn record_script_propagation(&mut self, operation: PropagationOp) {
        Connection::record_script_propagation(self, operation);
    }

    fn extend_script_propagation(&mut self, operations: Vec<PropagationOp>) {
        Connection::extend_script_propagation(self, operations);
    }

    fn script_propagation(&self) -> &[PropagationOp] {
        Connection::script_propagation(self)
    }

    fn record_dirty_key(&mut self, dirty_key: (u32, Bytes)) {
        Connection::record_dirty_key(self, dirty_key);
    }

    fn replace_dirty_keys(&mut self, dirty_keys: Vec<(u32, Bytes)>) {
        Connection::replace_dirty_keys(self, dirty_keys);
    }

    fn dirty_keys(&self) -> &[(u32, Bytes)] {
        Connection::dirty_keys(self)
    }

    fn defer_lazyfree_object(&mut self, object: Robj) {
        self.defer_command_lazyfree_object(object);
    }

    fn record_notification(&mut self, notification: (Bytes, Bytes)) {
        Connection::record_notification(self, notification);
    }

    fn extend_notifications(&mut self, notifications: Vec<(Bytes, Bytes)>) {
        Connection::extend_notifications(self, notifications);
    }

    fn record_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        Connection::record_shard_notification(self, notification);
    }

    fn record_cluster_notification(&mut self, notification: (Bytes, Bytes)) {
        Connection::record_cluster_notification(self, notification);
    }

    fn record_cluster_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        Connection::record_cluster_shard_notification(self, notification);
    }

    fn swapdb_dbs(&self) -> Option<(u32, u32)> {
        self.swapdb_intent()
    }

    fn set_swapdb_dbs(&mut self, v: Option<(u32, u32)>) {
        self.set_swapdb_intent(v);
    }

    fn blocked(&self) -> Option<&BlockedState> {
        self.blocked.as_ref()
    }

    fn set_blocked(&mut self, state: Option<BlockedState>) {
        self.blocked = state;
    }

    fn reset_parser(&mut self) {
        self.parser.reset();
    }

    fn mark_closing(&mut self) {
        self.state = ConnectionState::Closing;
    }

    fn set_module_auth_callback(
        &mut self,
        callback: *mut libc::c_void,
        privdata: *mut libc::c_void,
    ) {
        if callback.is_null() {
            self.module_auth_callback = None;
        } else {
            // SAFETY: Module API supplies the documented user-changed callback ABI.
            let callback = unsafe {
                std::mem::transmute::<*mut libc::c_void, extern "C" fn(u64, *mut libc::c_void)>(
                    callback,
                )
            };
            self.module_auth_callback = Some((callback, privdata));
        }
    }

    fn notify_module_auth_changed(&mut self) {
        if let Some((callback, privdata)) = self.module_auth_callback.take() {
            callback(self.token.0 as u64, privdata);
        }
    }

    fn module_auth_callback(
        &self,
    ) -> Option<(extern "C" fn(u64, *mut libc::c_void), *mut libc::c_void)> {
        self.module_auth_callback
    }

    fn module_auth_user(&self) -> Option<&str> {
        self.module_auth_user.as_deref()
    }

    fn set_module_auth_user(&mut self, username: Option<String>) {
        self.module_auth_user = username;
    }

    fn redact_command_argument(&mut self, pos: usize) -> bool {
        if pos == 0 {
            return false;
        }
        self.redact_argument(pos)
    }

    fn is_redacted_command_argument(&self, pos: usize) -> bool {
        self.is_argument_redacted(pos)
    }

    fn queue_reply(&mut self, data: Vec<u8>) {
        Connection::queue_command_reply(self, data);
    }

    fn queue_command_push_reply(&mut self, data: Vec<u8>) {
        Connection::queue_command_push_reply(self, data);
    }

    fn enter_blocking(
        &mut self,
        cmd: BlockedCmd,
        keys: Vec<Bytes>,
        timeout_ms: u64,
        orig_args: &CommandArgs,
        req_bytes: usize,
    ) -> Value {
        if self.transaction.is_active() {
            return Value::NullArray;
        }
        self.blocked = Some(BlockedState {
            cmd,
            keys,
            timeout_ms,
            start_time_ms: mstime(),
            orig_args: orig_args.clone(),
            req_bytes,
            orig_wrapped: None,
        });
        self.set_skip_propagation(true);
        Value::Null
    }
}
