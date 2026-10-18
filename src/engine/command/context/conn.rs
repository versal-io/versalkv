use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Instant;

use crate::engine::command::{CommandArgs, SessionId};
use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;

use super::{BlockedCmd, BlockedState, Token};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedKey {
    pub db_id: u32,
    pub key: Bytes,
    pub expired: bool,
}

impl WatchedKey {
    pub fn new(db_id: u32, key: Bytes, expired: bool) -> Self {
        Self {
            db_id,
            key,
            expired,
        }
    }

    pub fn matches(&self, db_id: u32, key: &[u8]) -> bool {
        self.db_id == db_id && self.key.as_ref() == key
    }

    pub fn touched(&mut self, exists_after: bool) -> bool {
        if self.expired && !exists_after {
            self.expired = false;
            return false;
        }
        true
    }
}

#[allow(dead_code)]
// TODO(dead_code): CommandConn trait methods are used through dyn dispatch in command handlers.
// The dead_code lint on trait methods is imprecise — many of these are called via
// `&mut dyn CommandConn` in non-test code (e.g. client.rs, subscribe.rs, multi.rs).
pub trait CommandConn {
    fn token(&self) -> Token;
    fn client_id(&self) -> u64 {
        self.token().0 as u64
    }
    fn session_id(&self) -> SessionId {
        SessionId::from_u64(self.client_id())
    }
    fn command_seq(&self) -> u64 {
        0
    }
    fn file_descriptor(&self) -> i64 {
        i64::try_from(self.token().0).unwrap_or(-1)
    }
    fn peer_addr(&self) -> SocketAddr;
    fn is_local(&self) -> bool {
        self.peer_addr().ip().is_loopback()
    }
    fn resp_version(&self) -> u8;
    fn set_resp_version(&mut self, v: u8);
    fn authenticated(&self) -> bool;
    fn set_authenticated(&mut self, v: bool);
    fn ever_authenticated(&self) -> bool;
    fn set_ever_authenticated(&mut self, v: bool);
    fn username(&self) -> &str;
    fn set_username(&mut self, v: String);
    fn selected_db(&self) -> u32;
    fn set_selected_db(&mut self, db: u32);
    fn last_interaction(&self) -> Instant;
    fn set_last_interaction(&mut self, t: Instant);
    fn create_time(&self) -> Instant;
    fn name(&self) -> Option<&Bytes>;
    fn set_name(&mut self, name: Option<Bytes>);
    fn lib_name(&self) -> &str;
    fn set_lib_name(&mut self, v: String);
    fn lib_ver(&self) -> &str;
    fn set_lib_ver(&mut self, v: String);
    fn capa_flags(&self) -> &[String];
    fn set_capa_flags(&mut self, flags: Vec<String>);
    fn req_bytes_curr_cmd(&self) -> usize;
    fn reply_bytes_curr_cmd(&self) -> usize;
    fn output_buffer_memory_usage(&self) -> usize {
        0
    }
    fn output_list_len(&self) -> usize {
        0
    }
    fn transaction_memory_usage(&self) -> usize {
        0
    }
    fn total_memory_usage(&self) -> usize {
        self.query_buffer_used()
            .saturating_add(self.query_buffer_free())
            .saturating_add(self.output_buffer_memory_usage())
    }
    fn query_buffer_used(&self) -> usize;
    fn query_buffer_free(&self) -> usize;
    fn diagnostic_query_snapshot(&self) -> &[u8] {
        &[]
    }
    fn no_touch(&self) -> bool;
    fn set_no_touch(&mut self, v: bool);
    fn no_evict(&self) -> bool;
    fn set_no_evict(&mut self, v: bool);
    fn import_source(&self) -> bool;
    fn set_import_source(&mut self, v: bool);
    fn slot_migration_role(&self) -> Option<&SlotMigrationConnectionRole> {
        None
    }
    fn set_slot_migration_role(&mut self, _role: Option<SlotMigrationConnectionRole>) {}
    fn readonly(&self) -> bool;
    fn set_readonly(&mut self, v: bool);
    fn asking(&self) -> bool {
        false
    }
    fn set_asking(&mut self, _v: bool) {}
    fn allow_oom(&self) -> bool {
        false
    }
    fn set_allow_oom(&mut self, _v: bool) {}
    fn deny_blocking(&self) -> bool {
        false
    }
    fn set_deny_blocking(&mut self, _v: bool) {}
    fn tot_net_in(&self) -> u64;
    fn tot_net_out(&self) -> u64;
    fn out_before(&self) -> u64;
    fn cmd_count(&self) -> u64;
    fn set_cmd_count(&mut self, v: u64);
    fn reply_off(&self) -> bool;
    fn set_reply_off(&mut self, v: bool);
    fn reply_skip(&self) -> bool;
    fn set_reply_skip(&mut self, v: bool);
    fn reply_skip_next(&self) -> bool;
    fn set_reply_skip_next(&mut self, v: bool);
    fn reply_suppressed(&self) -> bool;
    fn suppress_reply(&mut self);
    fn tracking(&self) -> bool;
    fn set_tracking(&mut self, v: bool);
    fn tracking_bcast(&self) -> bool;
    fn set_tracking_bcast(&mut self, v: bool);
    fn tracking_noloop(&self) -> bool;
    fn set_tracking_noloop(&mut self, v: bool);
    fn tracking_optin(&self) -> bool;
    fn set_tracking_optin(&mut self, v: bool);
    fn tracking_optout(&self) -> bool;
    fn set_tracking_optout(&mut self, v: bool);
    fn tracking_caching(&self) -> bool;
    fn set_tracking_caching(&mut self, v: bool);
    fn tracking_redirect(&self) -> Option<usize>;
    fn set_tracking_redirect(&mut self, v: Option<usize>);
    fn tracking_prefixes(&self) -> &[Bytes];
    fn set_tracking_prefixes(&mut self, prefixes: Vec<Bytes>);
    fn tracking_broken_redirect(&self) -> bool;
    fn set_tracking_broken_redirect(&mut self, v: bool);
    fn monitoring(&self) -> bool;
    fn set_monitoring(&mut self, v: bool);
    fn sub_channel_count(&self) -> usize;
    fn set_sub_channel_count(&mut self, v: usize);
    fn psub_channel_count(&self) -> usize;
    fn set_psub_channel_count(&mut self, v: usize);
    fn ssub_channel_count(&self) -> usize;
    fn set_ssub_channel_count(&mut self, v: usize);
    fn in_transaction(&self) -> bool;
    fn begin_transaction(&mut self) -> bool {
        true
    }
    fn discard_transaction(&mut self) {}
    /// SELECTed DB tracked across a MULTI transaction for ACL checks of
    /// queued commands. Defaults to the connection DB outside transactions.
    fn transaction_db_id(&self) -> i64 {
        self.selected_db() as i64
    }
    fn custom_reply(&self) -> bool;
    fn set_custom_reply(&mut self, v: bool);
    fn request_cluster_config_save(&mut self) {
        self.request_cluster_config_save_with_reply(Value::ok());
    }
    fn request_cluster_config_save_with_reply(&mut self, _reply: Value) {}
    fn skip_propagate(&self) -> bool;
    fn set_skip_propagate(&mut self, v: bool);
    fn mark_transaction_queue_error(&mut self) {}
    fn clear_watched_keys(&mut self) {}
    fn watch_keys(&mut self, _watched: Vec<WatchedKey>) -> usize {
        0
    }
    fn watched_keys(&self) -> &[WatchedKey] {
        &[]
    }
    fn record_custom_propagation(&mut self, frame: Vec<u8>);
    fn extend_custom_propagation(&mut self, frames: Vec<Vec<u8>>) {
        for frame in frames {
            self.record_custom_propagation(frame);
        }
    }
    fn clear_custom_propagation(&mut self);
    fn custom_propagation_checkpoint(&self) -> usize;
    fn take_custom_propagation_since(&mut self, checkpoint: usize) -> Vec<Vec<u8>>;
    fn custom_propagation(&self) -> &[Vec<u8>];
    fn record_script_propagation(&mut self, operation: PropagationOp);
    fn extend_script_propagation(&mut self, operations: Vec<PropagationOp>) {
        for operation in operations {
            self.record_script_propagation(operation);
        }
    }
    fn script_propagation(&self) -> &[PropagationOp];
    fn record_dirty_key(&mut self, dirty_key: (u32, Bytes));
    fn replace_dirty_keys(&mut self, dirty_keys: Vec<(u32, Bytes)>);
    fn dirty_keys(&self) -> &[(u32, Bytes)];
    /// Defer a detached value for L5 lazyfree. Synthetic command contexts that
    /// have no L5 owner retain synchronous destruction as a safe fallback.
    fn defer_lazyfree_object(&mut self, object: Robj) {
        drop(object);
    }
    fn record_notification(&mut self, notification: (Bytes, Bytes));
    fn extend_notifications(&mut self, notifications: Vec<(Bytes, Bytes)>) {
        for notification in notifications {
            self.record_notification(notification);
        }
    }
    fn record_shard_notification(&mut self, notification: (Bytes, Bytes));
    fn record_cluster_notification(&mut self, notification: (Bytes, Bytes));
    fn record_cluster_shard_notification(&mut self, notification: (Bytes, Bytes));

    fn swapdb_dbs(&self) -> Option<(u32, u32)>;
    fn set_swapdb_dbs(&mut self, v: Option<(u32, u32)>);

    fn blocked(&self) -> Option<&BlockedState>;
    fn set_blocked(&mut self, state: Option<BlockedState>);

    fn reset_parser(&mut self);
    fn mark_closing(&mut self);
    fn set_module_auth_callback(
        &mut self,
        callback: *mut libc::c_void,
        privdata: *mut libc::c_void,
    );
    fn notify_module_auth_changed(&mut self);
    fn module_auth_user(&self) -> Option<&str>;
    fn set_module_auth_user(&mut self, username: Option<String>);
    fn redact_command_argument(&mut self, pos: usize) -> bool;
    fn is_redacted_command_argument(&self, pos: usize) -> bool;
    fn queue_reply(&mut self, data: Vec<u8>);
    fn queue_command_push_reply(&mut self, data: Vec<u8>);
    fn enter_blocking(
        &mut self,
        cmd: BlockedCmd,
        keys: Vec<Bytes>,
        timeout_ms: u64,
        orig_args: &CommandArgs,
        req_bytes: usize,
    ) -> Value;
}
