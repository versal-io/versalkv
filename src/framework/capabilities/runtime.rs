use bytes::Bytes;
use std::cell::Cell;

use crate::engine::command::context::CommandConn;
use crate::engine::command::{
    CmdFlags, CommandArgs, CommandDescriptor, CommandKeyIndices, DispatchGroup, ResolvedCommand,
    CMD_ALL_DBS, CMD_MODULE, CMD_WRITE,
};
use crate::engine::extension::module::capability::{
    ModuleClientInfo, ModuleClientTracking, ModuleRuntime,
};
use crate::engine::extension::script::ScriptRuntime;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandSpec;

#[derive(Clone, Copy)]
struct ModuleCommandConnPtr(*mut dyn CommandConn);

thread_local! {
    static MODULE_COMMAND_CONN: Cell<Option<ModuleCommandConnPtr>> = const { Cell::new(None) };
}

pub(crate) struct ModuleCommandConnScope {
    previous: Option<ModuleCommandConnPtr>,
}

pub(crate) fn with_current_module_conn<R>(
    callback: impl FnOnce(&mut dyn CommandConn) -> R,
) -> Option<R> {
    MODULE_COMMAND_CONN.with(|slot| {
        let conn = slot.get()?;
        // SAFETY: The scope owns this pointer for the synchronous callback.
        Some(unsafe { callback(&mut *conn.0) })
    })
}

impl ModuleCommandConnScope {
    pub(crate) fn enter(conn: &mut dyn CommandConn) -> Self {
        MODULE_COMMAND_CONN.with(|slot| {
            let previous = slot.get();
            slot.set(Some(ModuleCommandConnPtr(erase_conn_lifetime(conn))));
            Self { previous }
        })
    }
}

impl Drop for ModuleCommandConnScope {
    fn drop(&mut self) {
        MODULE_COMMAND_CONN.with(|slot| slot.set(self.previous));
    }
}

fn lookup_command(ctx: &ServerContext, name: &[u8]) -> Option<CommandDescriptor> {
    ctx.command_registry()
        .lookup(name)
        .map(CommandSpec::descriptor)
}

fn resolve_script_command(ctx: &ServerContext, name: &[u8]) -> Option<ResolvedCommand> {
    ctx.command_registry().resolve(name)
}

fn execute_script_command(
    ctx: &mut ServerContext,
    command: ResolvedCommand,
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
) -> Value {
    let Some(executor) = ctx.command_registry().executor(command) else {
        return Value::error("ERR command changed during script execution");
    };
    executor.execute(conn, args, ctx)
}

fn execute_command(
    ctx: &mut ServerContext,
    name: &[u8],
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
) -> Value {
    let executor = ctx
        .command_registry()
        .lookup(name)
        .expect("command was looked up before execution");
    executor.executor.execute(conn, args, ctx)
}

fn module_acl_error(
    ctx: &ServerContext,
    command: CommandDescriptor,
    args: &CommandArgs,
    conn: &dyn CommandConn,
) -> Option<String> {
    let keys = command.extract_keys(args).unwrap_or_default();
    let set_reads_old_value = command.name.as_str().eq_ignore_ascii_case("set")
        && args
            .args
            .iter()
            .skip(2)
            .any(|arg| arg.eq_ignore_ascii_case(b"GET"));
    let required = if set_reads_old_value {
        crate::engine::extension::acl::ACL_READ_PERMISSION
            | crate::engine::extension::acl::ACL_WRITE_PERMISSION
    } else if command.flags & CMD_WRITE != 0 {
        crate::engine::extension::acl::ACL_WRITE_PERMISSION
    } else {
        crate::engine::extension::acl::ACL_READ_PERMISSION
    };
    let key_requirements: Vec<(&[u8], u8)> =
        keys.iter().map(|key| (key.as_ref(), required)).collect();
    let acl_categories = crate::framework::spec::effective_acl_categories(
        command.name.as_str(),
        command.acl_categories,
        args.args.first().map(|arg| arg.as_ref()),
    );
    let should_restrict = crate::engine::extension::acl::should_restrict_cmd(
        command.name.as_str(),
        acl_categories,
        !keys.is_empty(),
    );
    let db_req = crate::engine::extension::acl::compute_db_requirement(
        command.name.as_str(),
        &args.args,
        command.flags & CMD_ALL_DBS != 0,
        should_restrict,
        conn.selected_db() as i64,
        ctx.num_dbs(),
    );
    let users = crate::engine::extension::acl::acl_users().lock().unwrap();
    let acl_command = crate::engine::extension::acl::AclCommand::new(
        command.name.as_str(),
        &args.args,
        acl_categories,
    );
    let denial = users.get(conn.username())?.check_command_perm(
        acl_command,
        &db_req,
        ctx.num_dbs(),
        &key_requirements,
    )?;
    let (reason, object, message) = module_acl_denial(
        command.name.as_str(),
        conn.username(),
        denial,
        &key_requirements,
        &args.args,
    );
    crate::engine::extension::acl::add_acl_log_entry(
        reason,
        crate::engine::extension::acl::ACL_LOG_CTX_MODULE,
        object,
        conn.username().to_string(),
        format!("addr={} cmd={}", conn.peer_addr(), command.name.as_str()),
    );
    Some(message)
}

fn module_acl_denial(
    command_name: &str,
    username: &str,
    denial: crate::engine::extension::acl::AclDenial,
    keys: &[(&[u8], u8)],
    args: &[Bytes],
) -> (i32, String, String) {
    use crate::engine::extension::acl::{AclDenial, ACL_DENIED_CMD, ACL_DENIED_DB, ACL_DENIED_KEY};
    match denial {
        AclDenial::Db { .. } => (
            ACL_DENIED_DB,
            command_name.to_string(),
            "NOPERM No permissions to access database".to_string(),
        ),
        AclDenial::Command => (
            ACL_DENIED_CMD,
            command_name.to_string(),
            format!(
                "NOPERM User {} has no permissions to run the '{}' command",
                username, command_name
            ),
        ),
        AclDenial::Key(index) => (
            ACL_DENIED_KEY,
            keys.get(index)
                .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
                .unwrap_or_else(|| command_name.to_string()),
            "NOPERM No permissions to access a key".to_string(),
        ),
        AclDenial::Channel(index) => (
            crate::engine::extension::acl::ACL_DENIED_CHANNEL,
            args.get(index)
                .map(|channel| String::from_utf8_lossy(channel).into_owned())
                .unwrap_or_else(|| command_name.to_string()),
            "NOPERM No permissions to access a channel".to_string(),
        ),
    }
}

impl ScriptRuntime for ServerContext {
    fn lookup_command(&self, name: &[u8]) -> Option<ResolvedCommand> {
        resolve_script_command(self, name)
    }

    fn command_acl_categories(
        &self,
        command: CommandDescriptor,
        args: &CommandArgs,
    ) -> &'static [&'static str] {
        crate::framework::spec::effective_acl_categories(
            command.name.as_str(),
            command.acl_categories,
            args.args.first().map(|arg| arg.as_ref()),
        )
    }

    fn script_cluster_error(
        &self,
        command: CommandDescriptor,
        conn: &dyn CommandConn,
        args: &CommandArgs,
        key_indices: CommandKeyIndices,
    ) -> Option<Value> {
        if !self.config().server.cluster_enabled {
            return None;
        }
        let keys = key_indices
            .map(|index| args.args[index].as_ref())
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return None;
        }
        let state = self.cluster_state().as_deref()?;
        if state.state != crate::engine::distributed::cluster::state::CLUSTER_OK
            && (!self.config().server.cluster_allow_reads_when_down
                || (command.flags & CMD_WRITE) != 0)
        {
            return Some(Value::error(format!(
                "ERR Can not execute a command '{}' while the cluster is down",
                command.name.as_str()
            )));
        }
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(keys[0]) as usize;
        let projection = state.projection();
        let is_open = projection.is_slot_open(slot as u16);
        let existing = is_open.then(|| {
            self.db(conn.selected_db())
                .map(|db| {
                    keys.iter()
                        .map(|key| db.get_no_expire(key).is_some())
                        .collect()
                })
                .unwrap_or_else(|| vec![false; keys.len()])
        });
        let decision = projection.decide_route(crate::engine::distributed::cluster::RouteRequest {
            keys: &keys,
            command_flags: command.flags,
            existing_keys: existing.as_deref(),
            allow_replica_read: conn.readonly()
                && (command.flags & crate::engine::command::CMD_READONLY) != 0,
            preferred_endpoint: crate::engine::distributed::cluster::PreferredEndpoint::Ip,
            transport: crate::engine::distributed::cluster::ClientTransport::Tcp,
            prefer_ipv6: conn.peer_addr().is_ipv6(),
            require_cluster_ok: false,
            route_sharded_pubsub: false,
            redirect_keyless: false,
            client_readonly: conn.readonly(),
            client_asking: conn.asking(),
            open_slot_policy: crate::engine::distributed::cluster::OpenSlotPolicy::Standard,
        });
        match decision {
            crate::engine::distributed::cluster::ClusterRouteDecision::ExecuteLocal => None,
            crate::engine::distributed::cluster::ClusterRouteDecision::ClusterDown => {
                Some(Value::error(format!(
                    "ERR Can not execute a command '{}' while the cluster is down",
                    command.name.as_str()
                )))
            }
            crate::engine::distributed::cluster::ClusterRouteDecision::ReadOnly => Some(
                Value::error("ERR Attempted to access a non local key in a cluster node"),
            ),
            _ => Some(Value::error(
                "ERR Attempted to access a non local key in a cluster node",
            )),
        }
    }

    fn execute_command(
        &mut self,
        command: ResolvedCommand,
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
    ) -> Value {
        execute_script_command(self, command, conn, args)
    }

    fn num_dbs(&self) -> usize {
        self.num_dbs()
    }

    fn has_maxmemory_limit(&self) -> bool {
        self.config().server.maxmemory > 0
    }

    fn has_enough_replicas_to_write(&self) -> bool {
        let required = self.config().replication.min_replicas_to_write;
        let max_lag_sec = self.config().replication.min_replicas_max_lag;
        if required == 0 || max_lag_sec == 0 {
            return true;
        }
        let max_lag_ms = max_lag_sec.saturating_mul(1_000);
        let good = self
            .replication()
            .view
            .replica_info
            .values()
            .filter(|info| info.state == "online")
            .filter(|info| max_lag_ms == 0 || info.lag_ms <= max_lag_ms)
            .count() as i64;
        good >= required
    }

    fn is_readonly_replica(&self) -> bool {
        // Mirror C `mustObeyClient()`: the AOF-loading client (CLIENT_ID_AOF)
        // and import clients obey their primary and may replay writes even on a
        // read-only replica. AOF startup replay marks this window with
        // `import_mode`, so a scripted `redis.call` write must not be rejected
        // as READONLY while it is set — otherwise loading an AOF whose commands
        // were produced by EVAL fails on a `replicaof`-configured node.
        self.replication().is_replica() && !self.config().server.import_mode
    }

    fn is_stale_replica(&self) -> bool {
        self.replication().is_replica()
            && self.config().replication.master_link_status != "up"
            && !self.config().replication.replica_serve_stale_data
    }

    fn has_persistence_write_error(&self) -> bool {
        self.config().persistence.stop_writes_on_bgsave_error
            && !self.config().persistence.saveparams.is_empty()
            && self.persistence().rdb().last_bgsave_status
                == crate::engine::persistence::OperationStatus::Err
    }

    fn script_disable_deny_scripts(&self) -> bool {
        self.script_disable_deny_scripts()
    }

    fn script_log_gate(&self) -> crate::engine::extension::script::ScriptLogGate {
        let slowlog = self.slowlog();
        let commandlog = &self.commandlog().slow;
        crate::engine::extension::script::ScriptLogGate {
            monitor_present: self.has_monitor_clients(),
            slowlog_enabled: slowlog.max_len != 0 && slowlog.threshold_us >= 0,
            slowlog_threshold_us: slowlog.threshold_us,
            commandlog_enabled: commandlog.max_len != 0 && commandlog.threshold_us >= 0,
            commandlog_threshold_us: commandlog.threshold_us,
        }
    }

    fn end_command(&mut self, name: &str, rejected: bool) {
        self.end_cmd(name, rejected);
    }

    fn record_command_call(&mut self, name: &str) {
        self.incr_cmd_call(name);
    }

    fn record_command_failure(&mut self, name: &str) {
        self.incr_cmd_failed(name);
    }

    fn command_stats_name(&self, command: CommandDescriptor, args: &CommandArgs) -> String {
        let name_lower = command.name.as_str().to_ascii_lowercase();
        if !crate::engine::command::is_subcommand_container(&name_lower) {
            return name_lower;
        }
        let Some(subcommand) = args.args.first() else {
            return name_lower;
        };
        let full_name = format!(
            "{}|{}",
            name_lower,
            String::from_utf8_lossy(subcommand).to_ascii_lowercase()
        );
        if crate::framework::generated::commands::SUBCOMMAND_NAMES
            .iter()
            .any(|known| known.eq_ignore_ascii_case(&full_name))
        {
            full_name
        } else {
            name_lower
        }
    }

    fn remember_tracked_key(&mut self, token: usize, key: &[u8]) {
        self.tracking_mut()
            .remember_key(token, &Bytes::copy_from_slice(key));
    }

    fn record_slow_command(
        &mut self,
        argv: Vec<Bytes>,
        duration_us: u64,
        peer_addr: &std::net::SocketAddr,
        client_name: Option<Bytes>,
    ) {
        self.push_cmd_log_slow_only(argv, duration_us, peer_addr, client_name);
    }

    fn record_internal_monitor(&mut self, command: CommandDescriptor, db: u32, argv: Vec<Bytes>) {
        self.record_internal_monitor(command, db, "lua", argv);
    }

    fn pending_script_kill(&self) -> bool {
        self.pending_script_kill()
    }

    fn set_script_busy(&mut self, is_function: bool, write_dirty: bool) {
        self.set_script_busy(is_function, write_dirty);
    }
}

impl ModuleRuntime for ServerContext {
    fn apply_module_call_effects(
        &mut self,
        mut effects: crate::engine::extension::module::api::call::pending::ModuleCallEffects,
    ) {
        if with_current_module_conn(|conn| {
            if let Some(delta) = effects.dirty_delta.take() {
                conn.record_dirty_delta(delta);
            }
            for key in effects.dirty_keys.drain(..) {
                conn.record_dirty_key(key);
            }
            conn.extend_script_propagation(std::mem::take(&mut effects.propagation));
            conn.extend_notifications(std::mem::take(&mut effects.notifications));
            for notification in effects.shard_notifications.drain(..) {
                conn.record_shard_notification(notification);
            }
            for notification in effects.cluster_notifications.drain(..) {
                conn.record_cluster_notification(notification);
            }
            for notification in effects.cluster_shard_notifications.drain(..) {
                conn.record_cluster_shard_notification(notification);
            }
        })
        .is_none()
        {
            self.module_registry_mut().pending_effects.push(effects);
        }
    }
    fn module_server_info(&mut self, section: Option<&str>) -> String {
        let args = CommandArgs {
            name: Bytes::from_static(b"INFO"),
            args: section
                .map(|section| vec![Bytes::copy_from_slice(section.as_bytes())])
                .unwrap_or_default(),
        };
        let mut conn =
            crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
                0, None,
            );
        match crate::framework::commands::server::info::info(&mut conn, &args, self) {
            Value::Bulk(text) => String::from_utf8_lossy(&text).into_owned(),
            _ => String::new(),
        }
    }
    fn retain_pending_module_call(
        &mut self,
        call: crate::engine::extension::module::api::call::pending::PendingModuleCall,
    ) -> Option<usize> {
        let blocked = call.conn.blocked()?;
        let session = crate::engine::command::SessionId(call.conn.token().0 as u64);
        if blocked.keys.is_empty() {
            self.blocking_registry_mut()
                .register_keyless_client(session);
        } else {
            self.blocking_registry_mut().register_client(
                call.conn.selected_db(),
                &blocked.keys,
                session,
            );
        }
        Some(self.module_registry_mut().retain_pending_call(call))
    }

    fn abort_pending_module_call(&mut self, handle: usize) -> bool {
        let Some(mut call) = self.module_registry_mut().pending_calls.remove(&handle) else {
            return false;
        };
        call.suppress_callbacks();
        unregister_pending_module_call(self, &call.conn);
        call.conn.set_blocked(None);
        true
    }
    fn num_dbs(&self) -> usize {
        self.num_dbs()
    }

    fn db_mut(&mut self, db_idx: u32) -> Option<&mut crate::foundation::io::store::kv_engine::Db> {
        self.db_mut(db_idx)
    }

    fn flush_all_dbs(&mut self) {
        self.flush_all_dbs();
    }

    fn uses_lfu(&self) -> bool {
        self.config().server.maxmemory_policy.ends_with("-lfu")
    }

    fn is_oom(&self) -> bool {
        self.maxmemory_state().is_over_limit()
    }

    fn has_paused_writes(&self) -> bool {
        self.client_pause()
            .is_paused(crate::foundation::io::sys::time::mstime())
            != crate::framework::context::PauseAction::None
    }

    fn has_enough_replicas_to_write(&self) -> bool {
        <ServerContext as ScriptRuntime>::has_enough_replicas_to_write(self)
    }

    fn is_readonly_replica(&self) -> bool {
        <ServerContext as ScriptRuntime>::is_readonly_replica(self)
    }

    fn is_stale_replica(&self) -> bool {
        <ServerContext as ScriptRuntime>::is_stale_replica(self)
    }

    fn has_persistence_write_error(&self) -> bool {
        <ServerContext as ScriptRuntime>::has_persistence_write_error(self)
    }

    fn context_flags(&self) -> libc::c_int {
        const PRIMARY: libc::c_int = 1 << 2;
        const REPLICA: libc::c_int = 1 << 3;
        const READONLY: libc::c_int = 1 << 4;
        const CLUSTER: libc::c_int = 1 << 5;
        const AOF: libc::c_int = 1 << 6;
        const RDB: libc::c_int = 1 << 7;
        const MAXMEMORY: libc::c_int = 1 << 8;
        const EVICT: libc::c_int = 1 << 9;

        let config = self.config();
        let mut flags = if self.replication().is_replica() {
            REPLICA | READONLY
        } else {
            PRIMARY
        };
        if config.server.cluster_enabled {
            flags |= CLUSTER;
        }
        if config.persistence.aof_enabled {
            flags |= AOF;
        }
        if !config.persistence.saveparams.is_empty() {
            flags |= RDB;
        }
        if config.server.maxmemory > 0 {
            flags |= MAXMEMORY;
            if !config
                .server
                .maxmemory_policy
                .eq_ignore_ascii_case("noeviction")
            {
                flags |= EVICT;
            }
        }
        flags
    }

    fn module_acl_error(&self, command: CommandDescriptor, args: &CommandArgs) -> Option<String> {
        MODULE_COMMAND_CONN.with(|slot| {
            let conn = slot.get()?;
            // SAFETY: the scope keeps the outer module connection live for this callback.
            module_acl_error(self, command, args, unsafe { &*conn.0 })
        })
    }

    fn module_client_id(&self) -> u64 {
        MODULE_COMMAND_CONN.with(|slot| {
            slot.get()
                .map(|conn| {
                    // SAFETY: the scope keeps the connection live for this callback.
                    unsafe { (&*conn.0).client_id() }
                })
                .unwrap_or(0)
        })
    }

    fn module_client_info(&self, client_id: u64) -> Option<ModuleClientInfo> {
        MODULE_COMMAND_CONN.with(|slot| {
            let conn = slot.get()?;
            // SAFETY: the scope keeps the connection live for this callback.
            let conn = unsafe { &*conn.0 };
            if conn.client_id() != client_id {
                return None;
            }
            Some(ModuleClientInfo {
                id: client_id,
                peer_addr: conn.peer_addr(),
                db: conn.selected_db(),
                is_pubsub: conn.sub_channel_count() > 0
                    || conn.psub_channel_count() > 0
                    || conn.ssub_channel_count() > 0,
                is_blocked: conn.blocked().is_some(),
                is_tracking: conn.tracking(),
                is_multi: conn.in_transaction(),
                is_readonly: conn.readonly(),
                is_replica: conn.monitoring(),
                is_monitor: conn.monitoring(),
                is_authenticated: conn.authenticated(),
                was_authenticated: conn.ever_authenticated(),
            })
        })
    }

    fn module_client_name(&self, client_id: u64) -> Option<Bytes> {
        MODULE_COMMAND_CONN.with(|slot| {
            let conn = slot.get()?;
            // SAFETY: the scope keeps the connection live for this callback.
            let conn = unsafe { &*conn.0 };
            (conn.client_id() == client_id)
                .then(|| conn.name().cloned())
                .flatten()
        })
    }

    fn set_module_client_name(&mut self, client_id: u64, name: Bytes) -> bool {
        MODULE_COMMAND_CONN.with(|slot| {
            let Some(conn) = slot.get() else {
                return false;
            };
            // SAFETY: the scope keeps the connection live and exclusive.
            let conn = unsafe { &mut *conn.0 };
            if conn.client_id() != client_id {
                return false;
            }
            conn.set_name(Some(name));
            true
        })
    }

    fn module_client_tracking(&self) -> Option<ModuleClientTracking> {
        MODULE_COMMAND_CONN.with(|slot| {
            let conn = slot.get()?;
            // SAFETY: the scope keeps the connection live for this callback.
            let conn = unsafe { &*conn.0 };
            Some(ModuleClientTracking {
                id: conn.tracking_token().0,
                is_tracking: conn.tracking(),
                is_broadcast: conn.tracking_bcast(),
                is_optin: conn.tracking_optin(),
                is_optout: conn.tracking_optout(),
                is_caching: conn.tracking_caching(),
            })
        })
    }

    fn remember_module_tracked_key(&mut self, client_id: usize, key: &[u8]) {
        self.tracking_mut()
            .remember_key(client_id, &Bytes::copy_from_slice(key));
    }

    fn reply_module_raw(&mut self, reply: Vec<u8>) -> bool {
        MODULE_COMMAND_CONN.with(|slot| {
            let Some(conn) = slot.get() else {
                return false;
            };
            // SAFETY: the scope keeps the connection live and exclusive.
            let conn = unsafe { &mut *conn.0 };
            conn.queue_reply(reply);
            conn.set_custom_reply(true);
            true
        })
    }

    fn list_max_listpack_size(&self) -> i64 {
        self.config().encoding.list_max_listpack_size
    }

    fn lookup_command(&self, name: &[u8]) -> Option<CommandDescriptor> {
        lookup_command(self, name)
    }

    fn command_acl_categories(
        &self,
        command: CommandDescriptor,
        args: &CommandArgs,
    ) -> &'static [&'static str] {
        crate::framework::spec::effective_acl_categories(
            command.name.as_str(),
            command.acl_categories,
            args.args.first().map(|arg| arg.as_ref()),
        )
    }

    fn execute_command(
        &mut self,
        name: &[u8],
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
    ) -> Value {
        execute_command(self, name, conn, args)
    }

    fn authenticate_client(
        &mut self,
        username: &str,
        callback: *mut libc::c_void,
        privdata: *mut libc::c_void,
    ) -> Option<u64> {
        with_current_module_conn(|conn| {
            conn.notify_module_auth_changed();
            conn.set_username(username.to_string());
            conn.set_authenticated(true);
            conn.set_ever_authenticated(true);
            conn.set_module_auth_callback(callback, privdata);
            conn.set_module_auth_user(Some(username.to_string()));
            conn.client_id()
        })
    }

    fn deauthenticate_client(&mut self, username: &str) {
        let _ = with_current_module_conn(|conn| {
            if conn.module_auth_user() == Some(username) {
                conn.notify_module_auth_changed();
                conn.mark_closing();
                conn.set_module_auth_user(None);
            }
        });
    }

    fn redact_client_command_argument(&mut self, pos: usize) -> bool {
        with_current_module_conn(|conn| conn.redact_command_argument(pos)).unwrap_or(false)
    }

    fn set_module_command_info(
        &mut self,
        name: &str,
        info: crate::engine::extension::module::registry::ModuleCommandInfo,
    ) -> bool {
        self.module_registry_mut().set_command_info(name, info)
    }

    fn module_scan_keys(&mut self, db_idx: u32) -> Vec<Vec<u8>> {
        self.dbs_mut()
            .get_mut(db_idx)
            .map(|db| db.keys())
            .unwrap_or_default()
    }

    fn module_scan_key(
        &mut self,
        db_idx: u32,
        key: &[u8],
    ) -> Result<crate::engine::extension::module::capability::ModuleKeyScan, ()> {
        let db = self.dbs_mut().get_mut(db_idx).ok_or(())?;
        let is_incremental = matches!(
            db.get_no_expire(key),
            Some(
                crate::foundation::io::store::Robj::HashHashtable(_)
                    | crate::foundation::io::store::Robj::SetHashtable(_)
                    | crate::foundation::io::store::Robj::ZSetSkiplist(_)
            )
        );
        let entries = db.module_scan_key(key)?;
        Ok(
            crate::engine::extension::module::capability::ModuleKeyScan {
                entries,
                is_incremental,
            },
        )
    }

    fn record_latency_sample(&mut self, event: &str, latency_ms: i64) {
        self.latency_mut().add_sample(event, latency_ms);
    }

    fn record_internal_monitor(
        &mut self,
        command: CommandDescriptor,
        db: u32,
        source: &str,
        argv: Vec<Bytes>,
    ) {
        self.record_internal_monitor(command, db, source, argv);
    }

    fn register_module_command(
        &mut self,
        name: &str,
        arity: i32,
        flags: CmdFlags,
        first_key: i32,
        last_key: i32,
        key_step: i32,
    ) -> bool {
        let registry = self.module_command_registry_ptr();
        if registry.is_null() {
            return false;
        }
        let spec = CommandSpec {
            name: crate::engine::command::CommandName::Shared(std::sync::Arc::from(name)),
            arity,
            flags: flags | CMD_MODULE,
            dispatch_group: DispatchGroup::Default,
            first_key,
            last_key,
            key_step,
            container: None,
            key_flags: &[],
            acl_categories: &[],
            executor: crate::framework::CommandExecutor::Hosted {
                fallback: crate::framework::capabilities::module::module_cmd_dispatcher,
                prepare: crate::framework::host_tasks::prepare_module_task,
            },
            get_keys: None,
        };
        // SAFETY: registration is synchronous on the event-loop thread and the
        // pointer was installed from backend.registry.
        unsafe { &mut *registry }.register(spec.clone());
        self.script_mut().command_registry.register(spec);
        true
    }

    fn update_module_command(
        &mut self,
        name: &str,
        arity: Option<i32>,
        first_key: i32,
        last_key: i32,
        key_step: i32,
    ) -> bool {
        let registry = self.module_command_registry_ptr();
        if registry.is_null() {
            return false;
        }
        // SAFETY: updates are synchronous on the event-loop thread and the
        // pointer was installed from backend.registry.
        let updated = unsafe { &mut *registry }
            .update_module_command(name, arity, first_key, last_key, key_step);
        self.script_mut()
            .command_registry
            .update_module_command(name, arity, first_key, last_key, key_step);
        updated
    }

    fn unregister_module_command(&mut self, name: &str) -> bool {
        let registry = self.module_command_registry_ptr();
        if registry.is_null() {
            return false;
        }
        // SAFETY: unregistering is synchronous on the event-loop thread and the
        // pointer was installed from the live backend registry.
        let removed = unsafe { &mut *registry }.unregister(name);
        self.script_mut().command_registry.unregister(name);
        removed
    }

    fn publish_module_message(
        &mut self,
        channel: Bytes,
        message: Bytes,
        is_shard: bool,
    ) -> Option<usize> {
        let receivers = if is_shard {
            self.pubsub_mut().shard_subscribers(&channel).len()
        } else {
            self.pubsub_mut().subscribers(&channel).len()
                + self.pubsub_mut().pattern_subscribers(&channel).len()
        };
        let queued = MODULE_COMMAND_CONN.with(|slot| {
            let Some(conn) = slot.get() else {
                return false;
            };
            // SAFETY: ModuleCommandConnScope installs this pointer only for the
            // synchronous duration of a module command callback on this thread.
            let conn = unsafe { &mut *conn.0 };
            if is_shard {
                conn.record_shard_notification((channel.clone(), message.clone()));
                conn.record_cluster_shard_notification((channel, message));
            } else {
                conn.record_notification((channel.clone(), message.clone()));
                conn.record_cluster_notification((channel, message));
            }
            true
        });
        queued.then_some(receivers)
    }

    fn propagate_module_command(&mut self, frame: Vec<u8>) -> bool {
        MODULE_COMMAND_CONN.with(|slot| {
            let Some(conn) = slot.get() else {
                return false;
            };
            // SAFETY: ModuleCommandConnScope installs this pointer only for the
            // synchronous duration of a module command callback on this thread.
            unsafe { &mut *conn.0 }.record_custom_propagation(frame);
            true
        })
    }
}

pub(crate) fn unregister_pending_module_call(ctx: &mut ServerContext, conn: &dyn CommandConn) {
    let Some(blocked) = conn.blocked() else {
        return;
    };
    let session = crate::engine::command::SessionId(conn.token().0 as u64);
    if blocked.keys.is_empty() {
        ctx.blocking_registry_mut()
            .unregister_keyless_client(&session);
    } else {
        ctx.blocking_registry_mut()
            .unregister_client(conn.selected_db(), &blocked.keys, &session);
    }
}

fn erase_conn_lifetime(conn: &mut dyn CommandConn) -> *mut dyn CommandConn {
    // SAFETY: ModuleCommandConnPtr is installed only inside
    // ModuleCommandConnScope and restored before the borrowed connection can be
    // released.
    unsafe { std::mem::transmute::<&mut dyn CommandConn, *mut dyn CommandConn>(conn) }
}

#[cfg(test)]
#[path = "runtime_scan_tests.rs"]
mod scan_tests;

#[cfg(test)]
#[path = "runtime_pause_tests.rs"]
mod pause_tests;

#[cfg(test)]
mod tests {
    use crate::engine::command::CommandName;
    use crate::engine::extension::module::capability::ModuleRuntime;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandRegistry;
    use std::sync::Arc;

    fn unregister_script_command(
        _conn: &mut dyn crate::engine::command::context::CommandConn,
        args: &crate::engine::command::CommandArgs,
        ctx: &mut ServerContext,
    ) -> crate::foundation::io::protocol::Value {
        let name = std::str::from_utf8(&args.name).unwrap();
        assert!(ctx.script_mut().command_registry.unregister(name));
        crate::foundation::io::protocol::Value::ok()
    }

    #[test]
    fn lua_statistics_and_monitor_survive_command_removal_during_execution() {
        use crate::engine::command::CommandArgs;
        use crate::foundation::io::protocol::Value;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::CommandExecutor;
        use bytes::Bytes;

        for (name, script, stats_name) in [
            (
                CommandName::Shared(Arc::from("Module.Script")),
                b"return redis.call('MODULE.SCRIPT')".as_slice(),
                "module.script",
            ),
            (
                CommandName::Static("ping"),
                b"return redis.call('PING')",
                "ping",
            ),
            (
                CommandName::Shared(Arc::from("object")),
                b"return redis.call('OBJECT','ENCODING','key')",
                "object|encoding",
            ),
        ] {
            let weak = match &name {
                CommandName::Shared(owner) => Some(Arc::downgrade(owner)),
                CommandName::Static(_) => None,
            };
            let expected_name = name.as_str().to_string();
            let mut spec = crate::framework::commands::string::get::GET_SPEC;
            spec.name = name;
            spec.arity = -1;
            spec.first_key = 0;
            spec.last_key = 0;
            spec.executor = CommandExecutor::Immediate(unregister_script_command);
            let mut ctx = ServerContext::new(1);
            ctx.script_mut().command_registry.register(spec);
            ctx.register_monitor_client(7);
            let mut conn = Connection::new_fake();
            let args = CommandArgs::from_parts(vec![
                Bytes::from_static(b"EVAL"),
                Bytes::from_static(script),
                Bytes::from_static(b"0"),
            ]);
            assert_eq!(
                crate::framework::commands::scripting::eval::eval(&mut conn, &args, &mut ctx),
                Value::ok()
            );
            assert!(weak.is_none_or(|weak| weak.upgrade().is_none()));
            assert!(ctx
                .command_registry()
                .lookup(expected_name.as_bytes())
                .is_none());
            assert_eq!(ctx.stats().cmd_stats[stats_name].calls, 1);
            let event = ctx.script().pending_internal_monitor.last().unwrap();
            assert!(event.argv[0].eq_ignore_ascii_case(expected_name.as_bytes()));
            assert_eq!(event.source, "lua");
        }
    }

    #[test]
    fn module_registration_shares_names_between_main_and_script_tables() {
        let mut registry = Box::new(CommandRegistry::new());
        let mut ctx = ServerContext::new(1);
        ctx.set_module_command_registry(&mut *registry);
        let text = String::from("Module.Shared");
        assert!(ModuleRuntime::register_module_command(
            &mut ctx, &text, 1, 0, 0, 0, 0
        ));
        drop(text);
        let weak = {
            let CommandName::Shared(main) = &registry.lookup(b"module.shared").unwrap().name else {
                panic!("module names must be shared");
            };
            let CommandName::Shared(script) = &ctx
                .command_registry()
                .lookup(b"module.shared")
                .unwrap()
                .name
            else {
                panic!("script module names must be shared");
            };
            assert!(Arc::ptr_eq(main, script));
            Arc::downgrade(main)
        };
        assert_eq!(weak.strong_count(), 4);
        assert!(registry.unregister("MODULE.SHARED"));
        assert_eq!(weak.strong_count(), 2);
        assert_eq!(
            ctx.command_registry()
                .lookup(b"module.shared")
                .unwrap()
                .name
                .as_str(),
            "Module.Shared"
        );
        assert!(ctx
            .script_mut()
            .command_registry
            .unregister("Module.Shared"));
        assert!(weak.upgrade().is_none());
        ctx.set_module_command_registry(std::ptr::null_mut());
    }

    #[test]
    fn unavailable_module_registration_leaves_script_table_unchanged() {
        let mut ctx = ServerContext::new(1);
        let text = String::from("Module.Unavailable");
        assert!(!ModuleRuntime::register_module_command(
            &mut ctx, &text, 1, 0, 0, 0, 0
        ));
        assert!(ctx.command_registry().lookup(text.as_bytes()).is_none());
        assert!(ctx.command_registry().lookup_original(&text).is_none());
    }

    #[test]
    fn module_oom_uses_logical_memory_after_exclusions() {
        let mut ctx = ServerContext::new(1);
        // total_allocated is process-global and other parallel tests may free
        // memory between two samples. A saturating exclusion gives this test a
        // stable logical zero without relying on a stale allocator snapshot.
        ctx.config_mut().server.maxmemory = 1;
        ctx.set_slot_export_output_memory(usize::MAX);

        assert!(!ModuleRuntime::is_oom(&ctx));

        ctx.set_slot_export_output_memory(0);
        assert!(ModuleRuntime::is_oom(&ctx));
    }
}
