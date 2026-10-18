#![allow(non_snake_case)]

use bytes::Bytes;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::command::context::{BlockedState, CommandConn, Token};
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::capability::ModuleClientTracking;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;

static NEXT_MODULE_CALL_ID: AtomicU64 = AtomicU64::new(0x1000000);

pub(crate) struct ModuleCallConn {
    token: Token,
    selected_db: u32,
    resp_version: u8,
    username: String,
    tracking: Option<ModuleClientTracking>,
    raw_reply: Vec<u8>,
    has_custom_reply: bool,
    pending_notifications: Vec<(Bytes, Bytes)>,
    pending_shard_notifications: Vec<(Bytes, Bytes)>,
    pending_cluster_notifications: Vec<(Bytes, Bytes)>,
    pending_cluster_shard_notifications: Vec<(Bytes, Bytes)>,
    custom_propagation: Vec<Vec<u8>>,
    script_propagation_ops: Vec<PropagationOp>,
    dirty_keys: Vec<(u32, Bytes)>,
    allow_oom: bool,
    deny_blocking: bool,
    redacted_command_arguments: HashSet<usize>,
}

impl ModuleCallConn {
    #[cfg(test)]
    pub fn new(db_idx: u32) -> Self {
        Self::with_tracking(db_idx, None)
    }

    pub fn with_tracking(db_idx: u32, tracking: Option<ModuleClientTracking>) -> Self {
        let id = NEXT_MODULE_CALL_ID.fetch_add(1, Ordering::Relaxed);
        ModuleCallConn {
            token: Token(tracking.map_or(id as usize, |state| state.id)),
            selected_db: db_idx,
            resp_version: 2,
            username: "default".to_string(),
            tracking,
            raw_reply: Vec::new(),
            has_custom_reply: false,
            pending_notifications: Vec::new(),
            pending_shard_notifications: Vec::new(),
            pending_cluster_notifications: Vec::new(),
            pending_cluster_shard_notifications: Vec::new(),
            custom_propagation: Vec::new(),
            script_propagation_ops: Vec::new(),
            dirty_keys: Vec::new(),
            allow_oom: false,
            deny_blocking: true,
            redacted_command_arguments: HashSet::new(),
        }
    }

    pub fn take_raw_reply(&mut self) -> Option<Vec<u8>> {
        self.has_custom_reply
            .then(|| std::mem::take(&mut self.raw_reply))
    }

    pub fn take_pending_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.pending_notifications)
    }

    pub fn take_pending_shard_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.pending_shard_notifications)
    }
}

impl CommandConn for ModuleCallConn {
    fn token(&self) -> Token {
        self.token
    }
    fn file_descriptor(&self) -> i64 {
        -1
    }
    fn peer_addr(&self) -> std::net::SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }
    fn resp_version(&self) -> u8 {
        self.resp_version
    }
    fn set_resp_version(&mut self, v: u8) {
        self.resp_version = v;
    }
    fn authenticated(&self) -> bool {
        true
    }
    fn set_authenticated(&mut self, _v: bool) {}
    fn ever_authenticated(&self) -> bool {
        true
    }
    fn set_ever_authenticated(&mut self, _v: bool) {}
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
    fn last_interaction(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
    fn set_last_interaction(&mut self, _t: std::time::Instant) {}
    fn create_time(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
    fn name(&self) -> Option<&Bytes> {
        None
    }
    fn set_name(&mut self, _name: Option<Bytes>) {}
    fn req_bytes_curr_cmd(&self) -> usize {
        0
    }
    fn reply_bytes_curr_cmd(&self) -> usize {
        0
    }
    fn query_buffer_used(&self) -> usize {
        0
    }
    fn query_buffer_free(&self) -> usize {
        0
    }
    fn lib_name(&self) -> &str {
        ""
    }
    fn set_lib_name(&mut self, _v: String) {}
    fn lib_ver(&self) -> &str {
        ""
    }
    fn set_lib_ver(&mut self, _v: String) {}
    fn capa_flags(&self) -> &[String] {
        &[]
    }
    fn set_capa_flags(&mut self, _flags: Vec<String>) {}
    fn no_touch(&self) -> bool {
        false
    }
    fn set_no_touch(&mut self, _v: bool) {}
    fn no_evict(&self) -> bool {
        false
    }
    fn set_no_evict(&mut self, _v: bool) {}
    fn import_source(&self) -> bool {
        false
    }
    fn set_import_source(&mut self, _v: bool) {}
    fn readonly(&self) -> bool {
        false
    }
    fn set_readonly(&mut self, _v: bool) {}
    fn allow_oom(&self) -> bool {
        self.allow_oom
    }
    fn set_allow_oom(&mut self, value: bool) {
        self.allow_oom = value;
    }
    fn deny_blocking(&self) -> bool {
        self.deny_blocking
    }
    fn set_deny_blocking(&mut self, value: bool) {
        self.deny_blocking = value;
    }
    fn tot_net_in(&self) -> u64 {
        0
    }
    fn tot_net_out(&self) -> u64 {
        0
    }
    fn out_before(&self) -> u64 {
        0
    }
    fn cmd_count(&self) -> u64 {
        0
    }
    fn set_cmd_count(&mut self, _v: u64) {}
    fn reply_off(&self) -> bool {
        false
    }
    fn set_reply_off(&mut self, _v: bool) {}
    fn reply_skip(&self) -> bool {
        false
    }
    fn set_reply_skip(&mut self, _v: bool) {}
    fn reply_skip_next(&self) -> bool {
        false
    }
    fn set_reply_skip_next(&mut self, _v: bool) {}
    fn reply_suppressed(&self) -> bool {
        false
    }
    fn suppress_reply(&mut self) {}
    fn tracking(&self) -> bool {
        self.tracking.is_some_and(|state| state.is_tracking)
    }
    fn set_tracking(&mut self, v: bool) {
        if let Some(state) = &mut self.tracking {
            state.is_tracking = v;
        }
    }
    fn tracking_bcast(&self) -> bool {
        self.tracking.is_some_and(|state| state.is_broadcast)
    }
    fn set_tracking_bcast(&mut self, v: bool) {
        if let Some(state) = &mut self.tracking {
            state.is_broadcast = v;
        }
    }
    fn tracking_noloop(&self) -> bool {
        false
    }
    fn set_tracking_noloop(&mut self, _v: bool) {}
    fn tracking_optin(&self) -> bool {
        self.tracking.is_some_and(|state| state.is_optin)
    }
    fn set_tracking_optin(&mut self, v: bool) {
        if let Some(state) = &mut self.tracking {
            state.is_optin = v;
        }
    }
    fn tracking_optout(&self) -> bool {
        self.tracking.is_some_and(|state| state.is_optout)
    }
    fn set_tracking_optout(&mut self, v: bool) {
        if let Some(state) = &mut self.tracking {
            state.is_optout = v;
        }
    }
    fn tracking_caching(&self) -> bool {
        self.tracking.is_some_and(|state| state.is_caching)
    }
    fn set_tracking_caching(&mut self, v: bool) {
        if let Some(state) = &mut self.tracking {
            state.is_caching = v;
        }
    }
    fn tracking_redirect(&self) -> Option<usize> {
        None
    }
    fn set_tracking_redirect(&mut self, _v: Option<usize>) {}
    fn tracking_prefixes(&self) -> &[bytes::Bytes] {
        &[]
    }
    fn set_tracking_prefixes(&mut self, _prefixes: Vec<bytes::Bytes>) {}
    fn tracking_broken_redirect(&self) -> bool {
        false
    }
    fn set_tracking_broken_redirect(&mut self, _v: bool) {}
    fn monitoring(&self) -> bool {
        false
    }
    fn set_monitoring(&mut self, _v: bool) {}
    fn sub_channel_count(&self) -> usize {
        0
    }
    fn set_sub_channel_count(&mut self, _v: usize) {}
    fn psub_channel_count(&self) -> usize {
        0
    }
    fn set_psub_channel_count(&mut self, _v: usize) {}
    fn ssub_channel_count(&self) -> usize {
        0
    }
    fn set_ssub_channel_count(&mut self, _v: usize) {}
    fn in_transaction(&self) -> bool {
        false
    }
    fn custom_reply(&self) -> bool {
        self.has_custom_reply
    }
    fn set_custom_reply(&mut self, v: bool) {
        self.has_custom_reply = v;
    }
    fn skip_propagate(&self) -> bool {
        false
    }
    fn set_skip_propagate(&mut self, _v: bool) {}
    fn record_custom_propagation(&mut self, frame: Vec<u8>) {
        self.custom_propagation.push(frame);
    }
    fn clear_custom_propagation(&mut self) {
        self.custom_propagation.clear();
    }
    fn custom_propagation_checkpoint(&self) -> usize {
        self.custom_propagation.len()
    }
    fn take_custom_propagation_since(&mut self, checkpoint: usize) -> Vec<Vec<u8>> {
        self.custom_propagation
            .split_off(checkpoint.min(self.custom_propagation.len()))
    }
    fn custom_propagation(&self) -> &[Vec<u8>] {
        &self.custom_propagation
    }
    fn record_script_propagation(&mut self, operation: PropagationOp) {
        self.script_propagation_ops.push(operation);
    }
    fn script_propagation(&self) -> &[PropagationOp] {
        &self.script_propagation_ops
    }
    fn record_dirty_key(&mut self, dirty_key: (u32, Bytes)) {
        self.dirty_keys.push(dirty_key);
    }
    fn replace_dirty_keys(&mut self, dirty_keys: Vec<(u32, Bytes)>) {
        self.dirty_keys = dirty_keys;
    }
    fn dirty_keys(&self) -> &[(u32, Bytes)] {
        &self.dirty_keys
    }
    fn record_notification(&mut self, notification: (Bytes, Bytes)) {
        self.pending_notifications.push(notification);
    }
    fn record_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        self.pending_shard_notifications.push(notification);
    }
    fn record_cluster_notification(&mut self, notification: (Bytes, Bytes)) {
        self.pending_cluster_notifications.push(notification);
    }
    fn record_cluster_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        self.pending_cluster_shard_notifications.push(notification);
    }
    fn swapdb_dbs(&self) -> Option<(u32, u32)> {
        None
    }
    fn set_swapdb_dbs(&mut self, _v: Option<(u32, u32)>) {}
    fn blocked(&self) -> Option<&BlockedState> {
        None
    }
    fn set_blocked(&mut self, _state: Option<BlockedState>) {}
    fn reset_parser(&mut self) {}
    fn mark_closing(&mut self) {}
    fn set_module_auth_callback(
        &mut self,
        _callback: *mut libc::c_void,
        _privdata: *mut libc::c_void,
    ) {
    }
    fn notify_module_auth_changed(&mut self) {}
    fn module_auth_user(&self) -> Option<&str> {
        None
    }
    fn set_module_auth_user(&mut self, _username: Option<String>) {}
    fn redact_command_argument(&mut self, pos: usize) -> bool {
        pos > 0 && self.redacted_command_arguments.insert(pos)
    }
    fn is_redacted_command_argument(&self, pos: usize) -> bool {
        self.redacted_command_arguments.contains(&pos)
    }
    fn queue_reply(&mut self, data: Vec<u8>) {
        self.raw_reply.extend_from_slice(&data);
    }
    fn queue_command_push_reply(&mut self, data: Vec<u8>) {
        self.raw_reply.extend_from_slice(&data);
    }
    fn enter_blocking(
        &mut self,
        _cmd: crate::engine::command::context::BlockedCmd,
        _keys: Vec<Bytes>,
        _timeout_ms: u64,
        _orig_args: &CommandArgs,
        _req_bytes: usize,
    ) -> Value {
        Value::error("ERR blocking not supported in module call")
    }
}
