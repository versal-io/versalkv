use super::side_effects::CommandEffects;
use super::ServerWorkerState;
use crate::engine::command::context::CommandConn;
use crate::engine::command::{CommandArgs, DispatchGroup, CMD_TOUCHES_ARBITRARY_KEYS, CMD_WRITE};
use crate::engine::extension::script::{PropagationOp, PropagationTargets};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{
    BlockedCmd, ConnectionCommandEffects, TransactionEnd,
};
use crate::framework::context::PauseAction;
use crate::framework::spec::CommandExecutor;
use crate::runtime::server::worker::distributed::propagation::{
    encode_command_args, sequence_propagation,
};
use bytes::Bytes;

pub(super) fn client_last_command_name(name_lower: &str, args: &CommandArgs) -> String {
    args.args
        .first()
        .filter(|_| {
            matches!(
                name_lower,
                "cluster" | "client" | "config" | "xinfo" | "slowlog"
            )
        })
        .map(|subcommand| {
            format!(
                "{}|{}",
                name_lower,
                String::from_utf8_lossy(subcommand).to_ascii_lowercase()
            )
        })
        .unwrap_or_else(|| name_lower.to_string())
}

struct CommandPropagationPlan<'a> {
    args: &'a CommandArgs,
    rejected: bool,
    is_write: bool,
    may_replicate: bool,
    touches_arbitrary_keys: bool,
    preserve_expired_replica_key: bool,
    metric_slots: Vec<u16>,
    streaming_replicas: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PropagationCaptureMode {
    Direct,
    TransactionItem,
}

pub(crate) struct CommandPropagationCapture {
    pub(crate) swapdb: Option<(u32, u32)>,
    pub(crate) operations: Vec<PropagationOp>,
}

pub(super) struct CommandPropagationInput<'a> {
    pub(super) connection_token: usize,
    pub(super) token_value: usize,
    pub(super) args: &'a CommandArgs,
    pub(super) name: &'a str,
    pub(super) command_flags: u64,
    pub(super) rejected: bool,
}

pub(crate) struct TransactionPropagationInput<'a> {
    pub(crate) token: crate::framework::capabilities::io::Token,
    pub(crate) args: &'a CommandArgs,
    pub(crate) name: &'a str,
    pub(crate) command_flags: u64,
    pub(crate) rejected: bool,
}

struct ConnectionPropagationState {
    swapdb: Option<(u32, u32)>,
    selected_db: u32,
    has_explicit_dirty_keys: bool,
    has_script_ops: bool,
    force_propagate: bool,
    skip_propagation: bool,
}

impl ConnectionPropagationState {
    fn should_propagate(&self, plan: &CommandPropagationPlan<'_>) -> bool {
        self.has_script_ops
            || self.force_propagate
            || (!plan.rejected && !self.skip_propagation && (plan.is_write || plan.may_replicate))
    }
}

impl ServerWorkerState {
    /// Install the C `BLOCKED_WAIT` continuation for `CLUSTER SETSLOT`.
    /// Mirrors `clusterCommandSetSlot` (cluster_legacy.c): force-propagate the
    /// command frame to replicas once, then suspend the client without running
    /// the command body or sending a reply. When enough replicas ACK the
    /// target offset the blocked continuation re-invokes the handler, and the
    /// retained command cycle suppresses a second propagation. Returns None
    /// when the command must run immediately (not a SETSLOT candidate or no
    /// eligible replica), or Some(reply) with the blocking sentinel / the
    /// validation error.
    pub(super) fn prepare_cluster_setslot_block(
        &mut self,
        token: crate::framework::capabilities::io::Token,
        args: &CommandArgs,
    ) -> Option<Value> {
        if !is_setslot_wait_candidate(self.connection_registry.get(token), args) {
            return None;
        }
        let prepared = match crate::framework::commands::cluster::slots::prepare_cluster_setslot(
            &args.args[1..],
            false,
            &self.ctx,
        ) {
            Ok(prepared) => prepared,
            Err(reply) => {
                self.connection_registry[token].set_skip_propagation(true);
                return Some(reply);
            }
        };
        let required = self.eligible_setslot_replica_count();
        if required == 0 {
            return None;
        }
        // The frame is appended before the target offset is read, matching C's
        // `server.primary_repl_offset + 1` target captured at block time.
        self.propagate_to_replicas_with_ack(&encode_command_args(args), true);
        let target_offset = self.ctx.replication().primary_repl_offset();
        let connection = self
            .connection_registry
            .get_mut(token)
            .expect("SETSLOT candidate connection exists");
        Some(connection.enter_blocking(
            BlockedCmd::SetSlot {
                required: required as i64,
                target_offset,
            },
            Vec::new(),
            prepared.timeout_ms,
            args,
            connection.request_bytes_current_command(),
        ))
    }

    fn eligible_setslot_replica_count(&self) -> usize {
        self.replication_runtime
            .primary_links()
            .values()
            .filter(|link| {
                link.state()
                    == crate::framework::distributed::replication::PrimaryLinkState::Streaming
                    && link.replica_version() > 0x0702ff
            })
            .count()
    }

    pub(super) fn execute_registered_handler(
        &mut self,
        dispatch_group: DispatchGroup,
        connection_token: usize,
        executor: CommandExecutor,
        args: &CommandArgs,
    ) -> Value {
        let should_skip_expire = self
            .ctx
            .client_pause()
            .is_paused(crate::foundation::io::sys::time::mstime())
            != crate::framework::context::PauseAction::None
            || (self.ctx.replication().is_replica()
                && self.ctx.config().replication.replica_read_only);
        self.ctx
            .dbs_mut()
            .set_pause_skip_expire_all(should_skip_expire);
        let replica_local_expire_only =
            self.ctx.replication().is_replica() && !self.ctx.config().replication.replica_read_only;
        for db_idx in 0..self.ctx.num_dbs() {
            if let Some(db) = self.ctx.db_mut(db_idx as u32) {
                db.set_replica_local_expire_only(replica_local_expire_only);
            }
        }
        if matches!(
            String::from_utf8_lossy(&args.name)
                .to_ascii_lowercase()
                .as_str(),
            "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro"
        ) {
            self.refresh_replication_view();
        }
        if dispatch_group == DispatchGroup::Shutdown {
            let reply = crate::runtime::server::supervisor::shutdown::execute(&mut self.ctx, args);
            let abort = args
                .args
                .iter()
                .any(|arg| arg.eq_ignore_ascii_case(b"abort"));
            if abort {
                crate::runtime::server::supervisor::shutdown::clear_shutdown_pause(&mut self.ctx);
                self.reply_to_shutdown_clients();
            }
            if self.ctx.is_shutdown_requested() {
                // C exits directly from shutdownCommand() after
                // finishShutdown(), whose final steps close every listener.
                // Close them before publishing this command's reply as well,
                // so a client cannot reconnect into the short window before
                // the outer Rust event-loop observes shutdown_requested.
                self.close_listeners_for_shutdown();
            }
            let accepted = !matches!(reply, Value::Error(_));
            let shutdown_in_progress = accepted
                && !abort
                && (self.ctx.is_shutdown_requested()
                    || self.ctx.shutdown_state().initiated
                    || self.ctx.shutdown_state().asap);
            if shutdown_in_progress {
                let conn = &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)];
                conn.enter_blocking(
                    BlockedCmd::Shutdown,
                    Vec::new(),
                    0,
                    args,
                    conn.request_bytes_current_command(),
                );
                self.ctx.shutdown_state_mut().blocked_clients += 1;
                if self.ctx.shutdown_state().mstime_deadline != 0 {
                    self.send_shutdown_getack_if_needed();
                }
            }
            reply
        } else {
            if let Some(reply) = self.prepare_cluster_setslot_block(
                crate::framework::capabilities::io::Token(connection_token),
                args,
            ) {
                return reply;
            }
            // C Valkey's commandTimeSnapshot() is stable for the lifetime of
            // one command. Keep wall-clock helpers on the same snapshot so a
            // short relative TTL cannot expire merely because the handler was
            // descheduled between parsing and applying it. Install this only
            // after synchronous pre-handler waits, whose deadlines must keep
            // observing real time.
            let _time_guard = crate::foundation::io::sys::time::enter_command_time_snapshot(
                crate::foundation::io::sys::time::mstime(),
            );
            self.invoke_command_executor(
                crate::framework::capabilities::io::Token(connection_token),
                executor,
                args,
            )
        }
    }

    fn command_propagation_plan<'a>(
        &self,
        dirty_keys: &[(u32, Bytes)],
        args: &'a CommandArgs,
        name: &'a str,
        command_flags: u64,
        rejected: bool,
    ) -> CommandPropagationPlan<'a> {
        let metric_slots = self.command_metric_slots(args, dirty_keys);
        let streaming_replicas = self
            .replication_runtime
            .primary_links()
            .values()
            .filter(|link| {
                link.state()
                    == crate::framework::distributed::replication::PrimaryLinkState::Streaming
            })
            .count() as u64;
        let is_write = (command_flags & CMD_WRITE) != 0;
        let is_cluster_publish =
            self.ctx.config().server.cluster_enabled && matches!(name, "publish" | "spublish");
        CommandPropagationPlan {
            args,
            rejected,
            is_write,
            may_replicate: (command_flags & crate::engine::command::CMD_MAY_REPLICATE) != 0
                && !is_cluster_publish,
            touches_arbitrary_keys: (command_flags & CMD_TOUCHES_ARBITRARY_KEYS) != 0,
            preserve_expired_replica_key: matches!(
                name,
                "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "decrbyfloat"
            ),
            metric_slots,
            streaming_replicas,
        }
    }

    fn take_connection_propagation_state(
        &mut self,
        connection_token: usize,
        rejected: bool,
        connection_effects: &mut ConnectionCommandEffects,
        fx: &mut CommandEffects,
    ) -> ConnectionPropagationState {
        let selected_db = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .selected_db;
        let state = ConnectionPropagationState {
            swapdb: connection_effects.take_swapdb(),
            selected_db,
            has_explicit_dirty_keys: !connection_effects.dirty_keys().is_empty(),
            has_script_ops: !connection_effects.script_propagation().is_empty(),
            force_propagate: !connection_effects.custom_propagation().is_empty(),
            skip_propagation: connection_effects.skip_propagation(),
        };
        if (!rejected || state.has_script_ops || state.force_propagate)
            && state.has_explicit_dirty_keys
        {
            if state.swapdb.is_some() {
                fx.wakeup_only.extend(connection_effects.take_dirty_keys());
            } else {
                fx.watch_dirty.extend(connection_effects.take_dirty_keys());
            }
        }
        state
    }

    fn take_command_propagation_ops(
        &mut self,
        plan: &CommandPropagationPlan<'_>,
        state: &ConnectionPropagationState,
        connection_effects: &mut ConnectionCommandEffects,
        fx: &mut CommandEffects,
        mode: PropagationCaptureMode,
    ) -> Vec<PropagationOp> {
        if !state.should_propagate(plan) {
            return Vec::new();
        }
        if plan.is_write && (!plan.rejected || state.has_script_ops || state.force_propagate) {
            self.ctx.persistence_mut().add_dirty(1);
        }
        if !plan.rejected && plan.is_write && !state.has_explicit_dirty_keys {
            self.collect_inferred_write_keys(state.selected_db, plan.args, fx);
        }
        let mut operations = connection_effects.take_script_propagation();
        let generated_command = operations.is_empty().then(|| {
            let (direct, generated) =
                Self::take_direct_propagation_ops(connection_effects, plan, state.selected_db);
            operations = direct;
            generated
        });
        if mode == PropagationCaptureMode::Direct && generated_command == Some(true) {
            self.record_propagation_network_bytes(plan, &operations[0]);
        }
        operations
    }

    fn take_direct_propagation_ops(
        effects: &mut ConnectionCommandEffects,
        plan: &CommandPropagationPlan<'_>,
        db_id: u32,
    ) -> (Vec<PropagationOp>, bool) {
        if !effects.custom_propagation().is_empty() {
            let operations = effects
                .take_custom_propagation()
                .into_iter()
                .map(|frame| PropagationOp::new(db_id, PropagationTargets::Both, frame))
                .collect();
            return (operations, false);
        }
        if !plan.rejected && (plan.is_write || plan.may_replicate) {
            return (
                vec![PropagationOp::both(db_id, encode_command_args(plan.args))],
                true,
            );
        }
        (Vec::new(), false)
    }

    fn record_propagation_network_bytes(
        &mut self,
        plan: &CommandPropagationPlan<'_>,
        operation: &PropagationOp,
    ) {
        if plan.is_write || plan.may_replicate {
            self.ctx.slot_metrics_mut().record(
                &plan.metric_slots,
                0,
                0,
                (operation.bytes.len() as u64).saturating_mul(plan.streaming_replicas),
            );
        }
    }

    fn collect_inferred_write_keys(
        &mut self,
        selected_db: u32,
        args: &CommandArgs,
        fx: &mut CommandEffects,
    ) {
        let Some(spec) = self.registry.lookup(&args.name) else {
            return;
        };
        if let Some(keys) = crate::runtime::server::worker::flows::request::extract_write_keys(
            spec,
            args,
            selected_db,
        ) {
            fx.watch_dirty.extend(keys);
        }
    }

    fn merge_connection_notifications(
        connection_effects: &mut ConnectionCommandEffects,
        fx: &mut CommandEffects,
    ) {
        fx.publish.extend(connection_effects.take_notifications());
        fx.publish_shard
            .extend(connection_effects.take_shard_notifications());
        fx.cluster_publish
            .extend(connection_effects.take_cluster_notifications());
        fx.cluster_publish_shard
            .extend(connection_effects.take_cluster_shard_notifications());
    }

    fn collect_expired_propagation(&mut self, fx: &mut CommandEffects) -> Vec<PropagationOp> {
        let now = crate::foundation::io::sys::time::mstime();
        if self.ctx.client_pause().is_paused(now) != PauseAction::None {
            return Vec::new();
        }
        let delete_command: &[u8] = if self.ctx.config().server.lazyfree_lazy_expire {
            b"UNLINK"
        } else {
            b"DEL"
        };
        let expired = self.ctx.drain_expired_deletions();
        self.ctx.incr_expired_keys(expired.len() as u64);
        let expired_hash_fields = self.ctx.drain_lazy_expired_hash_fields();
        if expired_hash_fields > 0 {
            self.ctx
                .stats_mut()
                .incr_expired_fields(expired_hash_fields);
        }
        let hash_fields = self.ctx.drain_pending_hexpired();
        let mut operations = Vec::new();
        let deleted_hashes = self.collect_expired_hash_fields(&hash_fields, fx, &mut operations);
        self.collect_expired_hash_deletes(deleted_hashes, delete_command, fx, &mut operations);
        self.ctx
            .persistence_mut()
            .add_dirty((expired.len() + hash_fields.len()) as i64);
        self.collect_expired_keys(expired, delete_command, fx, &mut operations);
        operations
    }

    fn collect_expired_hash_fields(
        &mut self,
        fields: &[(u32, Vec<u8>, Vec<u8>)],
        fx: &mut CommandEffects,
        operations: &mut Vec<PropagationOp>,
    ) -> std::collections::HashSet<(u32, Vec<u8>)> {
        let mut notified = std::collections::HashSet::new();
        let mut deleted = std::collections::HashSet::new();
        for (db_id, key, field) in fields {
            fx.watch_dirty.push((*db_id, Bytes::from(key.clone())));
            operations.push(PropagationOp::both(
                *db_id,
                crate::engine::distributed::replication::protocol::encode_command(&[
                    b"HDEL" as &[u8],
                    key.as_ref(),
                    field.as_ref(),
                ]),
            ));
            if notified.insert((*db_id, key.clone())) {
                self.enqueue_hash_expiry_notifications(*db_id, key, b"hexpired", fx);
                let key_deleted = self
                    .ctx
                    .db(*db_id)
                    .map(|db| db.get_allow_expired(key).is_none())
                    .unwrap_or(true);
                if key_deleted {
                    deleted.insert((*db_id, key.clone()));
                }
            }
        }
        deleted
    }

    fn collect_expired_hash_deletes(
        &mut self,
        deleted: std::collections::HashSet<(u32, Vec<u8>)>,
        delete_command: &[u8],
        fx: &mut CommandEffects,
        operations: &mut Vec<PropagationOp>,
    ) {
        for (db_id, key) in deleted {
            operations.push(PropagationOp::both(
                db_id,
                crate::engine::distributed::replication::protocol::encode_command(&[
                    delete_command,
                    key.as_ref(),
                ]),
            ));
            self.enqueue_hash_expiry_notifications(db_id, &key, b"del", fx);
        }
    }

    fn enqueue_hash_expiry_notifications(
        &mut self,
        db_id: u32,
        key: &[u8],
        event: &'static [u8],
        fx: &mut CommandEffects,
    ) {
        if self.ctx.config_mut().is_event_type_enabled('K') {
            let channel = format!("__keyspace@{}__:{}", db_id, String::from_utf8_lossy(key));
            fx.publish
                .push((Bytes::from(channel.into_bytes()), Bytes::from_static(event)));
        }
        if self.ctx.config_mut().is_event_type_enabled('E') {
            let channel = format!("__keyevent@{}__:{}", db_id, String::from_utf8_lossy(event));
            fx.publish.push((
                Bytes::from(channel.into_bytes()),
                Bytes::copy_from_slice(key),
            ));
        }
    }

    fn collect_expired_keys(
        &mut self,
        expired: Vec<(u32, Vec<u8>)>,
        delete_command: &[u8],
        fx: &mut CommandEffects,
        operations: &mut Vec<PropagationOp>,
    ) {
        for (db_id, key) in expired {
            fx.watch_dirty.push((db_id, Bytes::from(key.clone())));
            operations.push(PropagationOp::both(
                db_id,
                crate::engine::distributed::replication::protocol::encode_command(&[
                    delete_command,
                    key.as_ref(),
                ]),
            ));
            super::super::super::enqueue_expired_key_notifications(
                self.ctx.config(),
                &mut fx.publish,
                db_id,
                &key,
            );
        }
    }

    fn sequence_propagation_units(
        &mut self,
        units: Vec<Vec<PropagationOp>>,
        fx: &mut CommandEffects,
    ) {
        for unit in units {
            let sequenced = sequence_propagation(
                &unit,
                self.ctx.repl_seldb(),
                self.ctx.persistence().aof_selected_db(),
            );
            let replication_bytes = sequenced.replication_frames.iter().map(Vec::len).sum();
            let is_aof_enabled = self.ctx.config().persistence.aof_enabled;
            if self
                .ctx
                .replication_mut()
                .record_local_propagation(replication_bytes, is_aof_enabled)
                .is_err()
            {
                crate::server_log!(
                    warning,
                    "Primary replication offset overflow; requesting server shutdown"
                );
                self.ctx.set_shutdown_requested(true);
            }
            self.ctx.set_repl_seldb(sequenced.replication_selected_db);
            self.ctx
                .persistence_mut()
                .set_aof_selected_db(sequenced.aof_selected_db);
            fx.replication_only.extend(sequenced.replication_frames);
            fx.aof_only.extend(sequenced.aof_frames);
        }
    }

    fn reset_connection_after_command(
        &mut self,
        connection_token: usize,
        token_value: usize,
        name: &str,
        rejected: bool,
    ) {
        if rejected {
            return;
        }
        if name == "reset" {
            self.reset_client_connection(connection_token, token_value);
        }
    }

    fn reset_client_connection(&mut self, connection_token: usize, token_value: usize) {
        let conn = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        let session = conn.session_id();
        let blocked_keys = conn
            .blocked
            .as_ref()
            .map(|blocked| blocked.keys.clone())
            .unwrap_or_default();
        conn.authenticated = self.ctx.config_mut().security.requirepass.is_none();
        conn.selected_db = 0;
        conn.resp_version = 2;
        self.ctx.pubsub_mut().remove_client(conn.token().0);
        conn.sub_channel_count = 0;
        conn.psub_channel_count = 0;
        conn.ssub_channel_count = 0;
        conn.transaction.end(TransactionEnd::Reset);
        if let Some(info) = self.ctx.client_info_mut(token_value) {
            info.watched_keys = 0;
        }
        if !blocked_keys.is_empty() {
            let db_id = conn.selected_db;
            conn.blocked = None;
            self.ctx
                .blocking_registry_mut()
                .unregister_client(db_id, &blocked_keys, &session);
            if let Some(info) = self.ctx.client_info_mut(token_value) {
                info.flags.retain(|flag| flag != 'b');
                if info.flags.is_empty() {
                    info.flags.push('N');
                }
            }
        }
    }

    pub(super) fn build_command_propagation(
        &mut self,
        input: CommandPropagationInput<'_>,
        connection_effects: ConnectionCommandEffects,
        fx: &mut CommandEffects,
    ) -> Option<(u32, u32)> {
        let CommandPropagationInput {
            connection_token,
            token_value,
            args,
            name: name_lower,
            command_flags,
            rejected,
        } = input;
        if self.ctx.persistence_mut().take_rewrite_force_select() {
            self.ctx.persistence_mut().reset_aof_selected_db();
        }
        let plan = self.command_propagation_plan(
            connection_effects.dirty_keys(),
            args,
            name_lower,
            command_flags,
            rejected,
        );
        let offset_before = self.ctx.replication().primary_repl_offset();
        let capture = self.capture_command_propagation(
            connection_token,
            &plan,
            PropagationCaptureMode::Direct,
            connection_effects,
            fx,
        );
        let expired_ops = self.collect_expired_propagation(fx);
        let units = arrange_propagation_units(capture.operations, expired_ops, &plan);
        let migration_ops = units.iter().flatten().cloned().collect::<Vec<_>>();
        self.sequence_propagation_units(units, fx);
        let offset_after = self.ctx.replication().primary_repl_offset();
        if offset_after != offset_before {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .set_last_write_offset(offset_after);
        }
        self.reset_connection_after_command(connection_token, token_value, name_lower, rejected);
        self.queue_slot_migration_mutations(&migration_ops);
        capture.swapdb
    }

    pub(crate) fn capture_transaction_item_propagation(
        &mut self,
        input: TransactionPropagationInput<'_>,
        connection_effects: ConnectionCommandEffects,
        effects: &mut CommandEffects,
    ) -> CommandPropagationCapture {
        let plan = self.command_propagation_plan(
            connection_effects.dirty_keys(),
            input.args,
            input.name,
            input.command_flags,
            input.rejected,
        );
        self.capture_command_propagation(
            input.token.0,
            &plan,
            PropagationCaptureMode::TransactionItem,
            connection_effects,
            effects,
        )
    }

    fn capture_command_propagation(
        &mut self,
        connection_token: usize,
        plan: &CommandPropagationPlan<'_>,
        mode: PropagationCaptureMode,
        mut connection_effects: ConnectionCommandEffects,
        effects: &mut CommandEffects,
    ) -> CommandPropagationCapture {
        let state = self.take_connection_propagation_state(
            connection_token,
            plan.rejected,
            &mut connection_effects,
            effects,
        );
        let operations =
            self.take_command_propagation_ops(plan, &state, &mut connection_effects, effects, mode);
        Self::merge_connection_notifications(&mut connection_effects, effects);
        self.dispose_data_lazyfree_objects(connection_effects.take_lazyfree_objects());
        CommandPropagationCapture {
            swapdb: state.swapdb,
            operations,
        }
    }
}

fn is_setslot_wait_candidate(
    connection: Option<&crate::framework::capabilities::connection::Connection>,
    args: &CommandArgs,
) -> bool {
    args.name.eq_ignore_ascii_case(b"cluster")
        && args
            .args
            .first()
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"setslot"))
        && connection.is_some_and(|connection| !connection.import_source)
}

fn arrange_propagation_units(
    command_ops: Vec<PropagationOp>,
    mut expired_ops: Vec<PropagationOp>,
    plan: &CommandPropagationPlan<'_>,
) -> Vec<Vec<PropagationOp>> {
    if plan.preserve_expired_replica_key {
        // C replays increments on an expired replica key with its original TTL.
        expired_ops.clear();
    }
    if plan.touches_arbitrary_keys {
        let mut units = Vec::new();
        if !command_ops.is_empty() {
            units.push(command_ops);
        }
        units.extend(expired_ops.into_iter().map(|operation| vec![operation]));
        return units;
    }
    if command_ops.is_empty() {
        return (!expired_ops.is_empty())
            .then_some(vec![expired_ops])
            .unwrap_or_default();
    }
    expired_ops.extend(command_ops);
    vec![expired_ops]
}
