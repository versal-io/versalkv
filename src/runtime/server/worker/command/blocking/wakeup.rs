use super::wakeup_check_acl_keys;
use crate::engine::command::context::CommandConn;
use crate::engine::command::{
    CommandArgs, CommandError, CMD_MAY_REPLICATE, CMD_NO_AUTH, CMD_WRITE,
};
use crate::engine::extension::script::{PropagationOp, PropagationTargets};
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
#[cfg(test)]
use crate::framework::capabilities::connection::TransactionEnd;
use crate::framework::capabilities::connection::{
    BlockedCmd, CommandEffectDrain, Connection, ConnectionCommandEffects,
};
use crate::framework::capabilities::io::Token;
use crate::framework::context::PauseAction;
use crate::runtime::server::worker::cluster::propagation::{
    encode_command_args, sequence_propagation, SequencedPropagation,
};
use crate::runtime::server::worker::command::dispatch::{
    CmdPreconditions, DispatchAction, PostponeReason,
};
use crate::runtime::server::worker::command::dispatch::{
    CommandEffects, EffectQueue, ExecCollectedEffects, SpecialCommandCompletion,
};
use crate::runtime::server::worker::command::invocation::CommandOrigin;
use crate::runtime::server::worker::command::outcome::{CommandDisposition, SpecialCommandResult};
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;

#[derive(Clone, Copy)]
struct WakeupReplyContext<'a> {
    connection_token: usize,
    args: &'a CommandArgs,
    blocked_start_ms: i64,
    rejected: bool,
}

struct WakeupReplyOutput {
    propagation_rejected: bool,
    frame: Option<Vec<u8>>,
    arm_existing_output: bool,
}

struct WakeupPropagationPlan<'a> {
    selected_db: u32,
    args: &'a CommandArgs,
    is_write: bool,
    may_replicate: bool,
    rejected: bool,
}

pub(super) enum WakeupSpecialResult {
    Completed(Vec<(u32, Bytes)>),
    Reblocked,
    ConnectionGone(Vec<(u32, Bytes)>),
}

impl ServerWorkerState {
    pub(crate) fn commit_blocked_stream_read_effects(
        &mut self,
        connection_token: usize,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
        pending_effects: Option<&mut EffectQueue>,
    ) {
        let token = Token(connection_token);
        let Some(connection) = self.connection_registry.get_mut(token) else {
            return;
        };
        let Some(blocked) = connection
            .blocked
            .as_ref()
            .filter(|blocked| matches!(blocked.cmd, BlockedCmd::Xreadgroup))
        else {
            return;
        };
        if connection.dirty_keys().is_empty() && connection.custom_propagation().is_empty() {
            return;
        }
        let args = blocked.orig_args.clone();
        // Drain data effects without completing the blocked request's accounting.
        let Some(mut effects) = connection.take_command_effects(CommandEffectDrain::Nested) else {
            return;
        };
        connection.resume_command_effects();
        let (propagation_ops, dirty_keys) =
            self.wakeup_collect_propagation_and_dirty(connection_token, &args, false, &mut effects);
        if let Some(pending) = pending_effects {
            self.wakeup_apply_propagation(
                connection_token,
                propagation_ops,
                Some(&mut pending.replication_only),
                Some(&mut pending.aof_only),
            );
            pending
                .tracking
                .extend(self.wakeup_collect_watch_and_tracking(&dirty_keys, token));
            pending.publish.extend(effects.take_notifications());
            pending
                .publish_shard
                .extend(effects.take_shard_notifications());
            pending
                .cluster_publish
                .extend(effects.take_cluster_notifications());
            pending
                .cluster_publish_shard
                .extend(effects.take_cluster_shard_notifications());
        } else {
            self.wakeup_apply_propagation(
                connection_token,
                propagation_ops,
                pending_replication,
                pending_aof,
            );
            self.wakeup_handle_watch_and_tracking(&dirty_keys, token);
            self.wakeup_deliver_notifications(&mut effects);
        }
        self.dispose_data_lazyfree_objects(effects.take_lazyfree_objects());
    }

    pub(crate) fn wakeup_blocked_client(
        &mut self,
        token: Token,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) -> Vec<(u32, Bytes)> {
        self.wakeup_blocked_client_with_effect_queue(token, pending_replication, pending_aof, None)
    }

    pub(super) fn wakeup_blocked_client_with_effect_queue(
        &mut self,
        token: Token,
        mut pending_replication: Option<&mut Vec<Vec<u8>>>,
        mut pending_aof: Option<&mut Vec<Vec<u8>>>,
        mut pending_effects: Option<&mut EffectQueue>,
    ) -> Vec<(u32, Bytes)> {
        let is_key_wakeup = self
            .connection_registry
            .get(token)
            .is_some_and(|connection| {
                connection
                    .blocked
                    .as_ref()
                    .is_some_and(|blocked| !blocked.keys.is_empty())
            });
        let Some((connection_token, args, blocked_start_ms, blocked_req_bytes, was_paused)) =
            self.wakeup_prepare(token)
        else {
            return Vec::new();
        };
        self.resume_wakeup_command_effects(connection_token, was_paused);

        if was_paused
            && !self.should_defer_slot_import_write(connection_token, &args)
            && self.apply_slot_import_command(connection_token, args.clone())
        {
            if self.connection_registry.contains(token) {
                let is_closing = self.connection_registry[token].state
                    == crate::framework::capabilities::connection::ConnectionState::Closing;
                self.connection_registry[token].complete_command();
                if !is_closing {
                    self.queue_client_for_reprocessing(token);
                }
            }
            return Vec::new();
        }

        if was_paused
            && self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .transaction
                .is_active()
            && !args.name.eq_ignore_ascii_case(b"EXEC")
        {
            return self.finish_paused_transaction_queue_wakeup(
                connection_token,
                token,
                &args,
                blocked_start_ms,
            );
        }

        let (reply, rejected, is_exec_paused) = if was_paused {
            self.wakeup_handle_paused(connection_token, &args, blocked_req_bytes, token)
        } else {
            self.qos.command_depth += usize::from(is_key_wakeup);
            let (reply, rejected) = self.wakeup_execute_command(connection_token, &args);
            self.qos.command_depth -= usize::from(is_key_wakeup);
            (reply, rejected, false)
        };
        if !self.connection_registry.contains(token) {
            return Vec::new();
        }

        if is_exec_paused {
            if let Some(pending) = pending_effects {
                return self.finish_paused_exec_wakeup(
                    connection_token,
                    token,
                    Some(&mut pending.replication_only),
                    Some(&mut pending.aof_only),
                );
            }
            return self.finish_paused_exec_wakeup(
                connection_token,
                token,
                pending_replication,
                pending_aof,
            );
        }

        if self.wakeup_reblock_if_needed(
            connection_token,
            token,
            blocked_start_ms,
            pending_replication.as_deref_mut(),
            pending_aof.as_deref_mut(),
            pending_effects.as_deref_mut(),
        ) {
            return Vec::new();
        }

        let mut connection_effects = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("a terminal blocked command drains its effects once");
        let reply_output = self.wakeup_finalize_reply(
            WakeupReplyContext {
                connection_token,
                args: &args,
                blocked_start_ms,
                rejected,
            },
            reply,
            &mut connection_effects,
        );

        let (propagation_ops, dirty_keys) = self.wakeup_collect_propagation_and_dirty(
            connection_token,
            &args,
            reply_output.propagation_rejected,
            &mut connection_effects,
        );
        let swapdb = connection_effects.take_swapdb();

        if let Some(pending) = pending_effects {
            self.wakeup_apply_propagation(
                connection_token,
                propagation_ops,
                Some(&mut pending.replication_only),
                Some(&mut pending.aof_only),
            );
        } else {
            self.wakeup_apply_propagation(
                connection_token,
                propagation_ops,
                pending_replication,
                pending_aof,
            );
        }
        if !rejected {
            self.wakeup_cmd_log(
                connection_token,
                &args,
                blocked_start_ms,
                &connection_effects,
            );
        }
        self.deliver_wakeup_reply(token, reply_output);
        if let Some((db1, db2)) = swapdb {
            self.apply_swapdb_watch_state(db1, db2);
        } else {
            self.wakeup_handle_watch_and_tracking(&dirty_keys, token);
        }
        self.wakeup_deliver_notifications(&mut connection_effects);
        self.dispose_data_lazyfree_objects(connection_effects.take_lazyfree_objects());
        if self.connection_registry.contains(token) {
            self.connection_registry[token].complete_command();
            self.queue_client_for_reprocessing(token);
        }
        dirty_keys
    }

    fn finish_paused_transaction_queue_wakeup(
        &mut self,
        connection_token: usize,
        token: Token,
        args: &CommandArgs,
        blocked_start_ms: i64,
    ) -> Vec<(u32, Bytes)> {
        match self.wakeup_handle_paused_transaction_command(
            connection_token,
            token,
            args,
            blocked_start_ms,
        ) {
            WakeupSpecialResult::Completed(dirty_keys)
            | WakeupSpecialResult::ConnectionGone(dirty_keys) => dirty_keys,
            WakeupSpecialResult::Reblocked => Vec::new(),
        }
    }

    fn finish_paused_exec_wakeup(
        &mut self,
        connection_token: usize,
        token: Token,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) -> Vec<(u32, Bytes)> {
        let result = self.wakeup_handle_exec_paused(
            connection_token,
            token,
            pending_replication,
            pending_aof,
        );
        let (dirty_keys, connection_alive) = match result {
            WakeupSpecialResult::Completed(dirty_keys) => (dirty_keys, true),
            WakeupSpecialResult::ConnectionGone(dirty_keys) => (dirty_keys, false),
            WakeupSpecialResult::Reblocked => return Vec::new(),
        };
        self.wakeup_handle_watch_and_tracking(&dirty_keys, token);
        if connection_alive && self.connection_registry.contains(token) {
            self.connection_registry[token].complete_command();
            self.queue_client_for_reprocessing(token);
        }
        dirty_keys
    }

    fn resume_wakeup_command_effects(&mut self, connection_token: usize, was_paused: bool) {
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        if was_paused {
            connection.resume_postponed_command_effects();
        } else {
            connection.resume_command_effects();
        }
    }

    fn wakeup_handle_paused_transaction_command(
        &mut self,
        connection_token: usize,
        token: Token,
        args: &CommandArgs,
        blocked_start_ms: i64,
    ) -> WakeupSpecialResult {
        let name_lower = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        let resp3 = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .resp_version
            >= 3;
        let mut effects = CommandEffects::default();

        // The initial dispatch removed this command from the accounting when
        // it was postponed. Re-entering through the MULTI queue path mirrors a
        // fresh dispatch: a repeated postpone balances this increment again,
        // while a terminal queue/rejection retains exactly one command count.
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .cmd_count += 1;
        let handled =
            self.try_handle_multi_queue(token, args, &name_lower, resp3, &mut effects.replies);
        debug_assert!(handled.is_some());

        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked
            .is_some()
        {
            self.wakeup_reblock_if_needed(
                connection_token,
                token,
                blocked_start_ms,
                None,
                None,
                None,
            );
            return WakeupSpecialResult::Reblocked;
        }
        self.finish_resumed_multi_command(token, effects)
    }

    fn finish_resumed_multi_command(
        &mut self,
        token: Token,
        mut effects: CommandEffects,
    ) -> WakeupSpecialResult {
        let disposition =
            self.complete_wakeup_special(token, CommandDisposition::Completed, &mut effects);
        if disposition == CommandDisposition::ConnectionGone {
            return WakeupSpecialResult::ConnectionGone(Vec::new());
        }
        for frame in std::mem::take(&mut effects.replies) {
            self.queue_client_reply(token, frame);
        }
        if self.connection_registry.contains(token) {
            self.connection_registry[token].complete_command();
            self.queue_client_for_reprocessing(token);
        }
        WakeupSpecialResult::Completed(Vec::new())
    }

    fn complete_wakeup_special(
        &mut self,
        token: Token,
        disposition: CommandDisposition,
        effects: &mut CommandEffects,
    ) -> CommandDisposition {
        let reply_end = effects.replies.len();
        effects.attribute_replies(0, reply_end);
        self.complete_special_command(
            token,
            SpecialCommandCompletion {
                disposition,
                reply_start: 0,
                reply_end,
            },
            effects,
        )
    }

    fn wakeup_deliver_notifications(&mut self, effects: &mut ConnectionCommandEffects) {
        let cluster_notifications = effects.take_cluster_notifications();
        let cluster_shard_notifications = effects.take_cluster_shard_notifications();
        let notifications = effects.take_notifications();
        let shard_notifications = effects.take_shard_notifications();
        self.propagate_cluster_publishes(cluster_notifications, cluster_shard_notifications);
        self.deliver_published(notifications, shard_notifications);
    }

    pub(super) fn wakeup_prepare(
        &mut self,
        token: Token,
    ) -> Option<(usize, CommandArgs, i64, usize, bool)> {
        let connection_token = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)?;
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return None;
        }
        let blocked = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .take()?;
        let session = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .session_id();
        let (blocked, was_paused) = if matches!(blocked.cmd, BlockedCmd::Throttled) {
            self.ctx.throttled_clients_mut().shift_remove(&token);
            (blocked, true)
        } else if matches!(
            blocked.cmd,
            BlockedCmd::Paused | BlockedCmd::Forkless | BlockedCmd::SlotMigration(_)
        ) {
            self.ctx.paused_clients_mut().shift_remove(&token);
            self.ctx.forkless_clients_mut().shift_remove(&token);
            match blocked.orig_wrapped {
                Some(wrapped) => (*wrapped, false),
                None => (blocked, true),
            }
        } else {
            (blocked, false)
        };
        let db_id = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .selected_db;
        if !was_paused {
            if matches!(
                blocked.cmd,
                BlockedCmd::Wait { .. } | BlockedCmd::WaitAof { .. } | BlockedCmd::SetSlot { .. }
            ) {
                self.ctx
                    .blocking_registry_mut()
                    .unregister_keyless_client(&session);
            } else {
                self.ctx
                    .blocking_registry_mut()
                    .unregister_client(db_id, &blocked.keys, &session);
            }
        }
        if let Some(ci) = self.ctx.client_info_mut(token.0) {
            ci.flags.retain(|c| c != 'b' && c != 'h');
            if ci.flags.is_empty() {
                ci.flags.push('N');
            }
        }
        Some((
            connection_token,
            blocked.orig_args,
            blocked.start_time_ms,
            blocked.req_bytes,
            was_paused,
        ))
    }

    pub(super) fn wakeup_handle_paused(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        blocked_req_bytes: usize,
        token: Token,
    ) -> (Value, bool, bool) {
        if self.ctx.replication().is_replica()
            && mstime() < self.ctx.replication().disconnect_clients_until_ms
        {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
            return (Value::Null, false, false);
        }
        let now = mstime();
        let pause = self.ctx.client_pause().is_paused(now);
        let is_exec = args.name.as_ref() == b"EXEC";
        let (should_block, in_transaction) = {
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let sb = self.pause_should_block(conn, pause, is_exec, args);
            (sb, conn.transaction.is_active())
        };
        if should_block && (!in_transaction || is_exec) {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let reply = if is_exec && conn.transaction.is_active() {
                use crate::engine::command::context::BlockedState;
                conn.blocked = Some(BlockedState {
                    cmd: BlockedCmd::Paused,
                    keys: Vec::new(),
                    timeout_ms: 0,
                    start_time_ms: mstime(),
                    orig_args: args.clone(),
                    req_bytes: blocked_req_bytes,
                    orig_wrapped: None,
                });
                Value::Null
            } else {
                conn.enter_blocking(BlockedCmd::Paused, Vec::new(), 0, args, blocked_req_bytes)
            };
            self.ctx.paused_clients_mut().insert(token);
            (reply, false, false)
        } else if is_exec {
            (Value::Null, false, true)
        } else {
            let Some((executor, meta)) = self.registry.lookup(&args.name).map(|spec| {
                (
                    spec.executor,
                    CmdPreconditions {
                        name: spec.name.clone(),
                        arity: spec.arity,
                        flags: crate::runtime::server::worker::command::dispatch::subcmd_flags::effective_command_flags_for_call(
                            spec.name.as_str(),
                            spec.flags,
                            args,
                            &self.ctx,
                        ),
                        first_key: spec.first_key,
                        last_key: spec.last_key,
                        key_step: spec.key_step,
                        key_flags: spec.key_flags,
                        acl_categories:
                            crate::runtime::server::worker::command::dispatch::subcmd_flags::effective_acl_categories(
                                spec.name.as_str(),
                                spec.acl_categories,
                                args.args.first().map(|arg| arg.as_ref()),
                            ),
                        get_keys: spec.get_keys,
                    },
                )
            }) else {
                return (CommandError::unknown(&args.name), true, false);
            };
            let outcome = self.pre_dispatch_exec(connection_token, meta, args);
            match outcome.action {
                DispatchAction::Execute => {
                    let val = self.invoke_command_executor(token, executor, args);
                    (val, false, false)
                }
                DispatchAction::Rejected => {
                    (outcome.reject_reply.unwrap_or(Value::Null), true, false)
                }
                DispatchAction::Postpone(reason) => {
                    let is_throttled = reason == PostponeReason::ReplicationThrottle;
                    let is_forkless = reason == PostponeReason::ForklessKey;
                    let command = match reason {
                        PostponeReason::ClientPause => BlockedCmd::Paused,
                        PostponeReason::ForklessKey => BlockedCmd::Forkless,
                        PostponeReason::ReplicationThrottle => BlockedCmd::Throttled,
                        PostponeReason::SlotMigration(job_id) => BlockedCmd::SlotMigration(job_id),
                    };
                    let conn = &mut self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)];
                    conn.postpone_command(command, Vec::new(), 0, args, blocked_req_bytes);
                    if is_throttled {
                        self.ctx.throttled_clients_mut().insert(token);
                        self.schedule_replication_throttle();
                    } else {
                        self.ctx.paused_clients_mut().insert(token);
                        if is_forkless {
                            self.ctx.forkless_clients_mut().insert(token);
                        }
                    }
                    (Value::Null, false, false)
                }
            }
        }
    }

    pub(super) fn pause_should_block(
        &self,
        conn: &Connection,
        pause: PauseAction,
        is_exec: bool,
        args: &CommandArgs,
    ) -> bool {
        match pause {
            PauseAction::None => false,
            PauseAction::Write => {
                if is_exec {
                    conn.transaction.queued_commands().iter().any(|cmd| {
                        self.registry
                            .lookup(&cmd.name)
                            .is_some_and(|s| {
                                (crate::runtime::server::worker::command::dispatch::subcmd_flags::effective_command_flags_for_call(
                                    s.name.as_str(),
                                    s.flags,
                                    cmd,
                                    &self.ctx,
                                ) & (CMD_WRITE | CMD_MAY_REPLICATE)) != 0
                            })
                    })
                } else {
                    self.registry.lookup(&args.name).is_some_and(|s| {
                        (crate::runtime::server::worker::command::dispatch::subcmd_flags::effective_command_flags_for_call(
                            s.name.as_str(),
                            s.flags,
                            args,
                            &self.ctx,
                        ) & (CMD_WRITE | CMD_MAY_REPLICATE)) != 0
                    })
                }
            }
            PauseAction::All => true,
        }
    }

    pub(super) fn wakeup_execute_command(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
    ) -> (Value, bool) {
        match self.registry.lookup(&args.name).cloned() {
            Some(spec) => {
                // Re-check ACL key permissions for blocked commands (permissions may have changed
                // while the command was blocked). Command permission was already checked at
                // initial dispatch time, but key access may have been revoked.
                if (spec.flags & CMD_NO_AUTH) == 0 && spec.first_key != 0 {
                    let connection = &self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)];
                    let key_denied = wakeup_check_acl_keys(
                        &connection.username,
                        &spec,
                        args,
                        connection.selected_db as i64,
                    );
                    if key_denied {
                        (
                            Value::error("NOPERM No permissions to access a key".to_string()),
                            true,
                        )
                    } else {
                        let val = self.invoke_command_executor(
                            Token(connection_token),
                            spec.executor,
                            args,
                        );
                        (val, false)
                    }
                } else {
                    let val =
                        self.invoke_command_executor(Token(connection_token), spec.executor, args);
                    (val, false)
                }
            }
            None => (CommandError::unknown(&args.name), true),
        }
    }

    pub(super) fn wakeup_handle_exec_paused(
        &mut self,
        connection_token: usize,
        token: Token,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) -> WakeupSpecialResult {
        let resp3 = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .resp_version
            >= 3;
        let mut effects = CommandEffects::default();
        let result = self.try_handle_exec(
            crate::runtime::server::worker::command::invocation::CommandTarget::new(token),
            "exec",
            resp3,
            &mut effects,
        );
        let disposition = match result {
            SpecialCommandResult::Handled(disposition) => disposition,
            SpecialCommandResult::NotHandled => CommandDisposition::ConnectionGone,
        };
        let disposition = self.complete_wakeup_special(token, disposition, &mut effects);
        if disposition == CommandDisposition::Blocked {
            return WakeupSpecialResult::Reblocked;
        }
        let dirty_keys = self.apply_wakeup_exec_effects(
            token,
            effects.into_exec_effects(),
            pending_replication,
            pending_aof,
        );
        if disposition == CommandDisposition::Completed {
            WakeupSpecialResult::Completed(dirty_keys)
        } else {
            WakeupSpecialResult::ConnectionGone(dirty_keys)
        }
    }

    fn apply_wakeup_exec_effects(
        &mut self,
        token: Token,
        mut effects: ExecCollectedEffects,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) -> Vec<(u32, Bytes)> {
        let mut pending = EffectQueue::default();
        pending.replication_only = effects.replication_only;
        pending.aof_only = effects.aof_only;
        self.wakeup_blocked_on_keys_with_effect_queue(
            &effects.wakeup_only,
            None,
            None,
            Some(&mut pending),
        );
        self.apply_wakeup_exec_propagation(
            pending.replication_only,
            pending.aof_only,
            pending_replication,
            pending_aof,
        );
        for frame in effects.replies.drain(..) {
            self.queue_client_reply(token, frame);
        }
        for (target_id, frame) in pending.tracking {
            self.queue_client_reply(Token(target_id), frame);
        }
        self.propagate_cluster_publishes(pending.cluster_publish, pending.cluster_publish_shard);
        effects.publish.extend(pending.publish);
        effects.publish_shard.extend(pending.publish_shard);
        self.deliver_published(effects.publish, effects.publish_shard);
        effects.watch_dirty
    }

    fn apply_wakeup_exec_propagation(
        &mut self,
        replication: Vec<Vec<u8>>,
        aof: Vec<Vec<u8>>,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) {
        match pending_replication {
            Some(frames) => frames.extend(replication),
            None => {
                if !self.ctx.replication().is_replica() {
                    for frame in &replication {
                        self.propagate_to_replicas(frame);
                    }
                }
            }
        }
        match pending_aof {
            Some(frames) => frames.extend(aof),
            None => {
                for frame in &aof {
                    self.feed_aof(frame);
                }
                if !aof.is_empty() {
                    self.flush_aof_if_needed();
                }
            }
        }
    }

    pub(super) fn wakeup_reblock_if_needed(
        &mut self,
        connection_token: usize,
        token: Token,
        blocked_start_ms: i64,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
        pending_effects: Option<&mut EffectQueue>,
    ) -> bool {
        self.commit_blocked_stream_read_effects(
            connection_token,
            pending_replication,
            pending_aof,
            pending_effects,
        );
        let conn = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        if conn.blocked.is_none() {
            return false;
        }
        if let Some(blocked) = conn.blocked.as_mut() {
            blocked.start_time_ms = blocked_start_ms;
        }
        if let Some(ci) = self.ctx.client_info_mut(token.0) {
            let flag = if conn
                .blocked
                .as_ref()
                .is_some_and(|blocked| matches!(blocked.cmd, BlockedCmd::Throttled))
            {
                'h'
            } else {
                'b'
            };
            if !ci.flags.contains(flag) {
                ci.flags.retain(|c| c != 'N');
                ci.flags.push(flag);
            }
        }
        let token = conn.token();
        let session = conn.session_id();
        let db_id = conn.selected_db;
        if let Some(b) = &conn.blocked {
            if matches!(b.cmd, BlockedCmd::Throttled) {
                self.ctx.throttled_clients_mut().insert(token);
            } else if matches!(
                b.cmd,
                BlockedCmd::Paused | BlockedCmd::Forkless | BlockedCmd::SlotMigration(_)
            ) {
                self.ctx.paused_clients_mut().insert(token);
            } else {
                self.ctx
                    .blocking_registry_mut()
                    .register_client(db_id, &b.keys, session);
            }
        }
        conn.discard_command_effects();
        true
    }

    fn wakeup_finalize_reply(
        &mut self,
        completion: WakeupReplyContext<'_>,
        reply: Value,
        effects: &mut ConnectionCommandEffects,
    ) -> WakeupReplyOutput {
        let reply = self.apply_post_command_intents(
            CommandOrigin::Direct,
            completion.connection_token,
            Some(effects),
            reply,
        );
        let propagation_rejected = completion.rejected || matches!(reply, Value::Error(_));
        let duration_us = self.record_wakeup_completion(completion, &reply);
        let (frame, arm_existing_output) =
            self.prepare_wakeup_reply(completion, &reply, duration_us, effects);
        WakeupReplyOutput {
            propagation_rejected,
            frame,
            arm_existing_output,
        }
    }

    fn record_wakeup_completion(
        &mut self,
        completion: WakeupReplyContext<'_>,
        reply: &Value,
    ) -> u64 {
        let name_lower = String::from_utf8_lossy(&completion.args.name).to_ascii_lowercase();
        let duration_us = if !completion.rejected {
            let stat = self
                .ctx
                .stats_mut()
                .cmd_stats
                .entry(name_lower.clone())
                .or_default();
            stat.calls += 1;
            let duration_us =
                (mstime().saturating_sub(completion.blocked_start_ms)).max(0) as u64 * 1000;
            stat.usec += duration_us;
            self.ctx.incr_total_commands(1);
            self.ctx.add_active_time_usec(duration_us);
            self.ctx.record_command_latency(&name_lower, duration_us);
            duration_us
        } else {
            let stat = self
                .ctx
                .stats_mut()
                .cmd_stats
                .entry(name_lower.clone())
                .or_default();
            stat.rejected_calls += 1;
            self.ctx.incr_total_commands(1);
            0
        };
        self.record_wakeup_error(&name_lower, reply, completion.rejected);
        duration_us
    }

    fn record_wakeup_error(&mut self, name: &str, reply: &Value, rejected: bool) {
        let Value::Error(error) = reply else {
            return;
        };
        let error = String::from_utf8_lossy(error);
        self.ctx
            .incr_error_reply(error.split_whitespace().next().unwrap_or("ERR"));
        if !rejected {
            self.ctx.incr_cmd_failed(name);
        }
    }

    fn prepare_wakeup_reply(
        &mut self,
        completion: WakeupReplyContext<'_>,
        reply: &Value,
        duration_us: u64,
        effects: &mut ConnectionCommandEffects,
    ) -> (Option<Vec<u8>>, bool) {
        let (frame, visible_reply_bytes) =
            self.capture_wakeup_reply(completion.connection_token, reply, effects);
        self.record_wakeup_slot_metrics(completion, duration_us, visible_reply_bytes, effects);
        self.connection_registry
            [crate::framework::capabilities::io::Token(completion.connection_token)]
        .cmd_count += 1;
        let frame =
            (!effects.has_custom_reply() && !effects.should_suppress_reply()).then_some(frame);
        let arm_existing_output = effects.has_custom_reply() && !effects.should_suppress_reply();
        self.connection_registry
            [crate::framework::capabilities::io::Token(completion.connection_token)]
        .finish_reply_cycle();
        (frame, arm_existing_output)
    }

    fn record_wakeup_slot_metrics(
        &mut self,
        completion: WakeupReplyContext<'_>,
        duration_us: u64,
        visible_reply_bytes: usize,
        effects: &ConnectionCommandEffects,
    ) {
        if completion.rejected {
            return;
        }
        self.record_command_slot_metrics(
            completion.args,
            &[],
            crate::runtime::server::worker::cluster::migration::accounting::SlotMetricSample {
                cpu_usec: duration_us,
                network_bytes_in: effects.request_bytes() as u64,
                network_bytes_out: visible_reply_bytes as u64,
            },
        );
    }

    fn capture_wakeup_reply(
        &mut self,
        connection_token: usize,
        reply: &Value,
        effects: &mut ConnectionCommandEffects,
    ) -> (Vec<u8>, usize) {
        let resp3 = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .resp_version
            >= 3;
        let frame = if resp3 {
            serialize_to_vec_resp3(reply)
        } else {
            serialize_to_vec_resp2(reply)
        };
        let reply_bytes = if effects.has_custom_reply() {
            effects.reply_bytes()
        } else {
            effects.set_reply_bytes(frame.len());
            frame.len()
        };
        let visible_reply_bytes = if effects.should_suppress_reply() {
            0
        } else {
            reply_bytes
        };
        (frame, visible_reply_bytes)
    }

    fn deliver_wakeup_reply(&mut self, token: Token, output: WakeupReplyOutput) {
        if let Some(frame) = output.frame {
            self.queue_client_reply(token, frame);
        } else if output.arm_existing_output {
            let _ = self.queue_client_reply(token, Vec::new());
        }
    }

    pub(super) fn wakeup_collect_propagation_and_dirty(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        rejected: bool,
        effects: &mut ConnectionCommandEffects,
    ) -> (Vec<PropagationOp>, Vec<(u32, Bytes)>) {
        let dirty_delta = effects.take_dirty_delta();
        if let Some(delta) = dirty_delta {
            self.ctx.persistence_mut().add_dirty(delta);
        }
        let has_script_ops = !effects.script_propagation().is_empty();
        let force_propagate = !effects.custom_propagation().is_empty();
        if rejected && !has_script_ops && !force_propagate {
            return (Vec::new(), Vec::new());
        }
        let selected_db = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .selected_db;
        let command_flags = self.wakeup_command_flags(args);
        let is_write = (command_flags & CMD_WRITE) != 0;
        let may_replicate = (command_flags & CMD_MAY_REPLICATE) != 0;
        let dirty_keys = self.wakeup_dirty_keys(selected_db, args, is_write, rejected, effects);
        let propagation_ops = Self::wakeup_propagation_ops(
            WakeupPropagationPlan {
                selected_db,
                args,
                is_write,
                may_replicate,
                rejected,
            },
            effects,
        );
        if dirty_delta.is_none() && is_write && !propagation_ops.is_empty() {
            self.ctx.persistence_mut().add_dirty(1);
        }
        (propagation_ops, dirty_keys)
    }

    fn wakeup_dirty_keys(
        &self,
        selected_db: u32,
        args: &CommandArgs,
        is_write: bool,
        rejected: bool,
        effects: &mut ConnectionCommandEffects,
    ) -> Vec<(u32, Bytes)> {
        if !effects.dirty_keys().is_empty() {
            effects.take_dirty_keys()
        } else if is_write && !rejected && !effects.skip_propagation() {
            self.registry
                .lookup(&args.name)
                .and_then(|spec| {
                    crate::runtime::server::worker::command::extract_write_keys(
                        spec,
                        args,
                        selected_db,
                    )
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    fn wakeup_command_flags(&self, args: &CommandArgs) -> u64 {
        self.registry.lookup(&args.name).map_or(0, |spec| {
            crate::runtime::server::worker::command::dispatch::subcmd_flags::effective_command_flags_for_call(
                spec.name.as_str(), spec.flags, args, &self.ctx,
            )
        })
    }

    fn wakeup_propagation_ops(
        plan: WakeupPropagationPlan<'_>,
        effects: &mut ConnectionCommandEffects,
    ) -> Vec<PropagationOp> {
        let WakeupPropagationPlan {
            selected_db,
            args,
            is_write,
            may_replicate,
            rejected,
        } = plan;
        let has_script_ops = !effects.script_propagation().is_empty();
        let force_propagate = !effects.custom_propagation().is_empty();
        if has_script_ops
            || ((!rejected || force_propagate)
                && (is_write || may_replicate || force_propagate)
                && (!effects.skip_propagation() || force_propagate))
        {
            let mut propagation_ops = effects.take_script_propagation();
            if propagation_ops.is_empty() {
                if !effects.custom_propagation().is_empty() {
                    propagation_ops.extend(effects.take_custom_propagation().into_iter().map(
                        |frame| PropagationOp::new(selected_db, PropagationTargets::Both, frame),
                    ));
                } else {
                    propagation_ops
                        .push(PropagationOp::both(selected_db, encode_command_args(args)));
                }
            }
            return propagation_ops;
        }
        Vec::new()
    }

    pub(super) fn wakeup_apply_propagation(
        &mut self,
        connection_token: usize,
        propagation_ops: Vec<PropagationOp>,
        pending_replication: Option<&mut Vec<Vec<u8>>>,
        pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) {
        if propagation_ops.is_empty() {
            return;
        }
        let sequenced = self.sequence_wakeup_propagation(connection_token, &propagation_ops);
        match pending_replication {
            Some(frames) => frames.extend(sequenced.replication_frames),
            None => {
                if !self.ctx.replication().is_replica() {
                    for frame in &sequenced.replication_frames {
                        self.propagate_to_replicas(frame);
                    }
                }
            }
        }
        match pending_aof {
            Some(frames) => frames.extend(sequenced.aof_frames),
            None => {
                for frame in &sequenced.aof_frames {
                    self.feed_aof(frame);
                }
                if !sequenced.aof_frames.is_empty() {
                    self.flush_aof_if_needed();
                }
            }
        }
    }

    fn sequence_wakeup_propagation(
        &mut self,
        connection_token: usize,
        propagation_ops: &[PropagationOp],
    ) -> SequencedPropagation {
        if self.ctx.persistence_mut().take_rewrite_force_select() {
            self.ctx.persistence_mut().reset_aof_selected_db();
        }
        let sequenced = sequence_propagation(
            propagation_ops,
            self.ctx.repl_seldb(),
            self.ctx.persistence().aof_selected_db(),
        );
        let offset_before = self.ctx.replication().primary_repl_offset();
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
        let offset_after = self.ctx.replication().primary_repl_offset();
        if offset_after != offset_before {
            self.connection_registry[Token(connection_token)].set_last_write_offset(offset_after);
        }
        self.ctx.set_repl_seldb(sequenced.replication_selected_db);
        self.ctx
            .persistence_mut()
            .set_aof_selected_db(sequenced.aof_selected_db);
        sequenced
    }

    pub(super) fn wakeup_cmd_log(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        blocked_start_ms: i64,
        effects: &ConnectionCommandEffects,
    ) {
        let duration_us = ((mstime() - blocked_start_ms) as u64).saturating_mul(1000);
        let argv =
            crate::runtime::server::worker::command::dispatch::redacted_argv(Some(effects), args);
        let (peer_addr, client_name) = {
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            (conn.peer_addr, conn.name.clone())
        };
        let command_flags = self
            .registry
            .lookup(&args.name)
            .map(|spec| spec.flags)
            .unwrap_or(0);
        self.ctx.push_cmd_logs(
            argv,
            duration_us,
            &peer_addr,
            client_name,
            effects.request_bytes(),
            effects.reply_bytes(),
            command_flags,
        );
    }

    pub(super) fn wakeup_handle_watch_and_tracking(
        &mut self,
        dirty_keys: &[(u32, Bytes)],
        token: Token,
    ) {
        for (target_id, frame) in self.wakeup_collect_watch_and_tracking(dirty_keys, token) {
            self.queue_client_reply(Token(target_id), frame);
        }
    }

    fn wakeup_collect_watch_and_tracking(
        &mut self,
        dirty_keys: &[(u32, Bytes)],
        token: Token,
    ) -> Vec<(usize, Vec<u8>)> {
        if dirty_keys.is_empty() {
            return Vec::new();
        }
        for (db_id, key) in dirty_keys {
            let exists_after = self
                .ctx
                .db(*db_id)
                .is_some_and(|db| db.exists_no_expire(key));
            for (_, other_conn) in self.connection_registry.iter_mut() {
                if other_conn.transaction.watched_keys().is_empty() {
                    continue;
                }
                other_conn
                    .transaction
                    .touch_watched_key(*db_id, key, exists_after);
            }
        }
        let mut pending_tracking = Vec::new();
        self.collect_tracking_invalidations(dirty_keys, Some(token), &mut pending_tracking);
        pending_tracking
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::{BlockedState, CommandConn};
    use crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer;

    #[path = "stream_durability_tests.rs"]
    mod stream_durability_tests;

    #[test]
    fn stream_spec_repairs_paused_exec_swapdb_reply_precedes_reblock_notification() {
        use crate::foundation::io::store::structure::stream::StreamID;
        use crate::runtime::server::worker::command::dispatch::ReadBatchEffects;

        let mut backend = ServerWorkerState::new(2).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        for db_id in [0, 1] {
            backend
                .ctx
                .db_mut(db_id)
                .unwrap()
                .get_or_create_stream(b"s")
                .unwrap()
                .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
                .unwrap();
        }
        let mut add_connection = |id| {
            let mut conn = Connection::new_fake();
            conn.set_token_for_test(Token(id));
            conn.resp_version = 3;
            conn.set_custom_reply_enabled(false);
            conn.set_skip_propagation(false);
            Token(backend.connection_registry.insert(conn))
        };
        let reader = add_connection(42);
        let trigger = add_connection(43);
        let dispatch = |backend: &mut ServerWorkerState, token, parts: &[&[u8]]| {
            let args = CommandArgs::from_parts(
                parts
                    .iter()
                    .map(|part| Bytes::copy_from_slice(part))
                    .collect(),
            );
            let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
            let mut batch = ReadBatchEffects::default();
            backend.process_parsed_command(token, args, name, true, &mut batch);
            backend.complete_client_read(token.0, token.0, batch);
        };
        dispatch(
            &mut backend,
            reader,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"c",
                b"BLOCK",
                b"0",
                b"NOACK",
                b"STREAMS",
                b"s",
                b">",
            ],
        );
        assert!(backend.connection_registry[reader].blocked.is_some());
        dispatch(&mut backend, trigger, &[b"MULTI"]);
        dispatch(&mut backend, trigger, &[b"SWAPDB", b"0", b"1"]);
        backend.connection_registry[trigger].clear_output_queue();
        backend.ctx.config_mut().server.notify_keyspace_events = "Kt".to_owned();
        backend
            .ctx
            .pubsub_mut()
            .subscribe(trigger.0, Bytes::from_static(b"__keyspace@0__:s"));
        backend.ctx.client_pause_mut().action = PauseAction::Write;
        backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
        dispatch(&mut backend, trigger, &[b"EXEC"]);
        assert!(matches!(
            backend.connection_registry[trigger]
                .blocked
                .as_ref()
                .map(|state| &state.cmd),
            Some(BlockedCmd::Paused)
        ));
        assert!(backend.connection_registry[trigger].output.queue.is_empty());

        backend.connection_registry[trigger].set_tracking(true);
        backend
            .ctx
            .tracking_mut()
            .remember_key(trigger.0, &Bytes::from_static(b"s"));
        backend.ctx.client_pause_mut().action = PauseAction::None;
        backend.ctx.client_pause_mut().end_time_ms = 0;
        let prefix = b"existing-batch-frame".to_vec();
        let mut replication = vec![prefix.clone()];
        let mut aof = vec![prefix.clone()];
        backend.wakeup_blocked_client(trigger, Some(&mut replication), Some(&mut aof));
        let output = _delivered_bytes(&backend, trigger.0);
        assert!(
            output.starts_with(b"*1\r\n+OK\r\n>2\r\n"),
            "output: {}",
            String::from_utf8_lossy(&output)
        );
        let tracking = output
            .windows(10)
            .position(|part| part == b"invalidate")
            .unwrap();
        let notification = output
            .windows(21)
            .position(|part| part == b"xgroup-createconsumer")
            .unwrap();
        assert!(tracking < notification);
        assert!(backend.connection_registry[reader].blocked.is_some());
        assert!(backend.connection_registry[reader].output.queue.is_empty());
        assert_eq!(
            output
                .windows(21)
                .filter(|part| *part == b"xgroup-createconsumer")
                .count(),
            1
        );
        assert_eq!(replication, aof);
        assert_eq!(replication[0], prefix);
        let swap = replication
            .iter()
            .position(|frame| frame.windows(6).any(|part| part == b"SWAPDB"))
            .unwrap();
        let create = replication
            .iter()
            .position(|frame| frame.windows(14).any(|part| part == b"CREATECONSUMER"))
            .unwrap();
        assert!(swap < create);
        assert!(backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap()
            .consumers
            .contains_key(b"c".as_slice()));
    }

    #[test]
    fn stream_spec_repairs_reblock_queue_commits_watch_before_all_observer_delivery() {
        use crate::engine::command::context::WatchedKey;
        use crate::foundation::io::store::structure::stream::StreamID;

        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_or_create_stream(b"s")
            .unwrap()
            .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
            .unwrap();
        let mut observer = Connection::new_fake();
        observer.set_token_for_test(Token(42));
        observer.resp_version = 3;
        observer.set_tracking(true);
        observer
            .transaction
            .watch_many(vec![WatchedKey::new(0, Bytes::from_static(b"s"), false)]);
        let observer = Token(backend.connection_registry.insert(observer));
        backend
            .ctx
            .tracking_mut()
            .remember_key(observer.0, &Bytes::from_static(b"s"));
        let mut reader = Connection::new_fake();
        reader.set_token_for_test(Token(43));
        let reader = Token(backend.connection_registry.insert(reader));
        let args = CommandArgs::from_parts(
            [
                b"XREADGROUP".as_slice(),
                b"GROUP",
                b"g",
                b"c",
                b"BLOCK",
                b"0",
                b"NOACK",
                b"STREAMS",
                b"s",
                b">",
            ]
            .into_iter()
            .map(Bytes::copy_from_slice)
            .collect(),
        );
        crate::framework::commands::stream::xreadgroup::xreadgroup(
            &mut backend.connection_registry[reader],
            &args,
            &mut backend.ctx,
        );
        let notice = (
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
        );
        backend.connection_registry[reader].record_notification(notice.clone());
        backend.connection_registry[reader].record_shard_notification(notice.clone());
        backend.connection_registry[reader].record_cluster_notification(notice.clone());
        backend.connection_registry[reader].record_cluster_shard_notification(notice.clone());
        let mut pending = EffectQueue::default();
        backend.commit_blocked_stream_read_effects(reader.0, None, None, Some(&mut pending));
        assert!(backend.connection_registry[observer]
            .transaction
            .is_watch_dirty());
        assert!(backend.connection_registry[observer]
            .output
            .queue
            .is_empty());
        assert_eq!(pending.tracking.len(), 1);
        assert_eq!(pending.publish, vec![notice.clone()]);
        assert_eq!(pending.publish_shard, vec![notice.clone()]);
        assert_eq!(pending.cluster_publish, vec![notice.clone()]);
        assert_eq!(pending.cluster_publish_shard, vec![notice]);
        assert!(!pending.replication_only.is_empty());
        assert_eq!(pending.aof_only, pending.replication_only);
        let dirty = backend.ctx.persistence().dirty();
        backend.commit_blocked_stream_read_effects(reader.0, None, None, Some(&mut pending));
        assert_eq!(backend.ctx.persistence().dirty(), dirty);
        assert_eq!(pending.tracking.len(), 1);
    }

    #[test]
    fn blocked_stream_consumer_effects_commit_before_delivery_and_only_once() {
        for noack in [false, true] {
            let mut backend = ServerWorkerState::new(2).unwrap();
            crate::framework::commands::register_all(&mut backend.registry);
            let stream = backend
                .ctx
                .db_mut(1)
                .unwrap()
                .get_or_create_stream(b"s")
                .unwrap();
            stream
                .create_group(
                    Bytes::from_static(b"g"),
                    crate::foundation::io::store::structure::stream::StreamID::zero(),
                    None,
                )
                .unwrap();
            let mut watcher = Connection::new_fake();
            watcher.set_token_for_test(Token(42));
            watcher
                .transaction
                .watch_many(vec![crate::engine::command::context::WatchedKey::new(
                    1,
                    Bytes::from_static(b"s"),
                    false,
                )]);
            let watcher_token = Token(backend.connection_registry.insert(watcher));
            let mut reader = Connection::new_fake();
            reader.set_token_for_test(Token(43));
            reader.selected_db = 1;
            let reader_token = Token(backend.connection_registry.insert(reader));
            let mut parts = vec![
                b"XREADGROUP".as_slice(),
                b"GROUP",
                b"g",
                b"c",
                b"BLOCK",
                b"0",
            ];
            if noack {
                parts.push(b"NOACK");
            }
            parts.extend([b"STREAMS".as_slice(), b"s", b">"]);
            let args =
                CommandArgs::from_parts(parts.into_iter().map(Bytes::copy_from_slice).collect());
            crate::framework::commands::stream::xreadgroup::xreadgroup(
                &mut backend.connection_registry[reader_token],
                &args,
                &mut backend.ctx,
            );
            assert!(!backend.connection_registry[watcher_token]
                .transaction
                .is_watch_dirty());
            let dirty_before = backend.ctx.persistence().dirty();
            let request_bytes =
                backend.connection_registry[reader_token].request_bytes_current_command();
            backend.commit_blocked_stream_read_effects(reader_token.0, None, None, None);
            assert!(backend.connection_registry[watcher_token]
                .transaction
                .is_watch_dirty());
            assert_eq!(backend.ctx.persistence().dirty(), dirty_before + 1);
            let reader = &backend.connection_registry[reader_token];
            assert!(reader.blocked.is_some());
            assert!(reader.dirty_keys().is_empty());
            assert!(reader.custom_propagation().is_empty());
            assert_eq!(reader.request_bytes_current_command(), request_bytes);
            backend.commit_blocked_stream_read_effects(reader_token.0, None, None, None);
            assert_eq!(backend.ctx.persistence().dirty(), dirty_before + 1);
        }
    }

    fn redacting_write_handler(
        conn: &mut dyn CommandConn,
        _args: &CommandArgs,
        _ctx: &mut crate::framework::context::ServerContext,
    ) -> Value {
        assert!(conn.redact_command_argument(1));
        Value::ok()
    }

    fn custom_reply_write_handler(
        conn: &mut dyn CommandConn,
        _args: &CommandArgs,
        _ctx: &mut crate::framework::context::ServerContext,
    ) -> Value {
        conn.set_custom_reply(true);
        conn.queue_reply(b"+CUSTOM\r\n".to_vec());
        Value::ok()
    }

    #[path = "wakeup_pause_tests.rs"]
    mod pause_tests;

    #[path = "wakeup_slot_accounting_tests.rs"]
    mod slot_accounting_tests;

    #[path = "wakeup_teardown_tests.rs"]
    mod teardown_tests;

    fn _backend_with_connection() -> (ServerWorkerState, usize, Token) {
        let mut backend = ServerWorkerState::new(16).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let token = Token(42);
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(token);
        conn.selected_db = 7;
        conn.username = "session-user".to_string();
        conn.authenticated = false;
        assert!(conn.transaction.begin(7, 0));
        assert!(conn.transaction.queue(
            backend.ctx.num_dbs(),
            CommandArgs::from_parts(vec![Bytes::from_static(b"PING")])
        ));
        assert_eq!(
            conn.transaction
                .watch_many(vec![crate::engine::command::context::WatchedKey::new(
                    7,
                    Bytes::from_static(b"watched"),
                    false,
                )]),
            1
        );
        let connection_token = backend.connection_registry.insert(conn);
        (backend, connection_token, token)
    }

    fn _seed_command_transients(conn: &mut Connection) {
        conn.set_skip_propagation(true);
        conn.record_custom_propagation(b"custom-frame".to_vec());
        conn.record_script_propagation(PropagationOp::both(7, b"script-frame".to_vec()));
        conn.record_dirty_key((7, Bytes::from_static(b"dirty")));
        conn.record_notification((
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"message"),
        ));
        conn.record_shard_notification((
            Bytes::from_static(b"shard-channel"),
            Bytes::from_static(b"shard-message"),
        ));
        conn.set_swapdb_intent(Some((1, 2)));
    }

    fn _assert_command_transients_cleared(conn: &Connection) {
        assert!(_command_transients_are_clear(conn));
        assert_eq!(conn.selected_db, 7);
        assert_eq!(conn.username, "session-user");
        assert!(!conn.authenticated);
        assert!(conn.transaction.is_active());
        assert_eq!(conn.transaction.queued_commands().len(), 1);
        assert_eq!(conn.transaction.watched_key_count(), 1);
    }

    fn _command_transients_are_clear(conn: &Connection) -> bool {
        !conn.skip_propagate()
            && conn.custom_propagation().is_empty()
            && conn.script_propagation().is_empty()
            && conn.dirty_keys().is_empty()
            && conn.pending_notifications().is_empty()
            && conn.pending_shard_notifications().is_empty()
            && conn.swapdb_dbs().is_none()
    }

    #[test]
    fn timed_out_wakeup_clears_all_command_transients() {
        let (mut backend, connection_token, _token) = _backend_with_connection();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"0.001"),
        ]);
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked = Some(BlockedState {
            cmd: BlockedCmd::Blpop,
            keys: vec![Bytes::from_static(b"key")],
            timeout_ms: 1,
            start_time_ms: mstime().saturating_sub(10),
            orig_args: args,
            orig_wrapped: None,
            req_bytes: 32,
        });
        let session = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .session_id();
        backend.ctx.blocking_registry_mut().register_client(
            7,
            &[Bytes::from_static(b"key")],
            session,
        );
        _seed_command_transients(
            &mut backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)],
        );

        backend.check_blocked_timeouts();

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_none());
        _assert_command_transients_cleared(
            &backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)],
        );
    }

    #[test]
    fn reblocked_wakeup_clears_all_command_transients() {
        let (mut backend, connection_token, token) = _backend_with_connection();
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"BLPOP")]);
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked = Some(BlockedState {
            cmd: BlockedCmd::Blpop,
            keys: vec![Bytes::from_static(b"key")],
            timeout_ms: 0,
            start_time_ms: mstime(),
            orig_args: args,
            orig_wrapped: None,
            req_bytes: 32,
        });
        _seed_command_transients(
            &mut backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)],
        );

        assert!(backend.wakeup_reblock_if_needed(
            connection_token,
            token,
            mstime(),
            None,
            None,
            None
        ));

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_some());
        _assert_command_transients_cleared(
            &backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)],
        );
    }

    fn _add_subscriber(backend: &mut ServerWorkerState, channel: Bytes) -> usize {
        let mut subscriber = Connection::new_fake();
        subscriber.set_token_for_test(Token(43));
        let connection_token = backend.connection_registry.insert(subscriber);
        backend.ctx.pubsub_mut().subscribe(43, channel);
        connection_token
    }

    fn _delivered_bytes(backend: &ServerWorkerState, connection_token: usize) -> Vec<u8> {
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
            .iter()
            .flat_map(|frame| frame.iter().copied())
            .collect()
    }

    #[test]
    fn paused_exec_wakeup_consumes_notifications_before_clearing_transients() {
        let (mut backend, connection_token, token) = _backend_with_connection();
        let channel = Bytes::from_static(b"exec-channel");
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_skip_propagation(false);
        let transaction = &mut backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction;
        transaction.take_queued_commands();
        assert!(transaction.queue(
            backend.ctx.num_dbs(),
            CommandArgs::from_parts(vec![
                Bytes::from_static(b"PUBLISH"),
                channel.clone(),
                Bytes::from_static(b"exec-message"),
            ])
        ));
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .retire_command_attempt_for_postpone();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked = Some(BlockedState {
            cmd: BlockedCmd::Paused,
            keys: Vec::new(),
            timeout_ms: 0,
            start_time_ms: mstime(),
            orig_args: CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]),
            orig_wrapped: None,
            req_bytes: 32,
        });
        backend.ctx.paused_clients_mut().insert(token);
        let subscriber_key = _add_subscriber(&mut backend, channel);
        let (mut replication, mut aof) = (Vec::new(), Vec::new());

        backend.wakeup_blocked_client(token, Some(&mut replication), Some(&mut aof));

        assert!(_delivered_bytes(&backend, subscriber_key)
            .windows(12)
            .any(|window| window == b"exec-message"));
        assert!(!replication.is_empty() && !aof.is_empty());
        let conn = &backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        assert!(_command_transients_are_clear(conn));
        assert_eq!(
            (conn.selected_db, conn.username.as_str()),
            (7, "session-user")
        );
        assert!(!conn.authenticated);
    }

    #[test]
    fn successful_wakeup_consumes_effects_before_clearing_transients() {
        let (mut backend, connection_token, token) = _backend_with_connection();
        let channel = Bytes::from_static(b"wakeup-channel");
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"PUBLISH"),
            channel.clone(),
            Bytes::from_static(b"wakeup-message"),
        ]);
        let conn = &mut backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        conn.transaction.end(TransactionEnd::Reset);
        conn.postpone_command(BlockedCmd::Paused, Vec::new(), 0, &args, 32);
        backend.ctx.paused_clients_mut().insert(token);
        let subscriber_key = _add_subscriber(&mut backend, channel);
        let (mut replication, mut aof) = (Vec::new(), Vec::new());

        backend.wakeup_blocked_client(token, Some(&mut replication), Some(&mut aof));

        let delivered = _delivered_bytes(&backend, subscriber_key);
        assert!(
            delivered
                .windows(14)
                .any(|window| window == b"wakeup-message"),
            "subscriber output: {:?}; client output: {:?}",
            String::from_utf8_lossy(&delivered),
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
        );
        assert!(!replication.is_empty() && !aof.is_empty());
        let conn = &backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        assert!(_command_transients_are_clear(conn));
        assert_eq!(
            (conn.selected_db, conn.username.as_str()),
            (7, "session-user")
        );
        assert!(!conn.authenticated);
    }
}
