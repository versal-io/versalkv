use super::super::ServerWorkerState;
use crate::engine::command::{
    CommandArgs, CommandError, CMD_DENYOOM, CMD_MAY_REPLICATE, CMD_STALE, CMD_WRITE,
};
use crate::engine::distributed::replication::protocol as replication_svc;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::now_ms;
use crate::foundation::io::sys::time::mstime;
use crate::framework::capabilities::connection::CommandEffectDrain;
use crate::framework::capabilities::io::Token;
use crate::framework::context::PauseAction;
use crate::framework::CommandSpec;
use crate::runtime::server::worker::distributed::propagation::sequence_propagation;
use crate::runtime::server::worker::flows::request::dispatch::{
    CommandEffects, TransactionPropagationInput,
};
use crate::runtime::server::worker::flows::request::invocation::{CommandOrigin, CommandTarget};
use bytes::Bytes;

mod cluster_route;
mod exec_acl;
mod exec_runner;
mod multi_queue;
#[cfg(test)]
mod slot_accounting_tests;
mod tx_db_object;

use exec_acl::ExecAclRequest;

struct ExecPrechecks {
    noreplicas_reject: bool,
    oom_reject: bool,
    readonly_reject: bool,
    stale_reject: bool,
}

struct ExecRunState<'a> {
    target: CommandTarget,
    token: Token,
    username: String,
    client_info: String,
    results: Vec<Value>,
    dirty_keys: Vec<(u32, Bytes)>,
    propagation_ops: Vec<PropagationOp>,
    publish: Vec<(Bytes, Bytes)>,
    publish_shard: Vec<(Bytes, Bytes)>,
    wakeup_only: &'a mut Vec<(u32, Bytes)>,
    closed_publish_peers: std::collections::HashSet<String>,
}

#[derive(Default)]
struct ExecRunOutput {
    results: Vec<Value>,
    dirty_keys: Vec<(u32, Bytes)>,
    propagation_ops: Vec<PropagationOp>,
    publish: Vec<(Bytes, Bytes)>,
    publish_shard: Vec<(Bytes, Bytes)>,
}

struct ExecItemCompletion<'a> {
    token: Token,
    args: &'a CommandArgs,
    spec: CommandSpec,
    command_flags: u64,
    reply: Value,
    expired: Vec<(u32, Vec<u8>)>,
    was_blocked: bool,
    connection_effects:
        Option<crate::framework::capabilities::connection::ConnectionCommandEffects>,
}

impl ServerWorkerState {
    /// Apply Valkey touchAllWatchedKeysInDb semantics after the physical swap.
    pub(crate) fn apply_swapdb_watch_state(&mut self, db1: u32, db2: u32) {
        let ctx = &self.ctx;
        for (_, conn) in self.connection_registry.iter_mut() {
            conn.transaction
                .update_watches_after_swap(db1, db2, |db_id, key| {
                    let exists = ctx
                        .db(db_id)
                        .is_some_and(|db| db.get_allow_expired(key).is_some());
                    let expired = exists && ctx.db(db_id).is_some_and(|db| db.is_expired(key));
                    (exists, expired)
                });
        }
    }
    fn exec_expire_watched_keys(
        &mut self,
        connection_token: usize,
        pending_watch_dirty: &mut Vec<(u32, Bytes)>,
        pending_replication: &mut Vec<Vec<u8>>,
        pending_aof: &mut Vec<Vec<u8>>,
    ) {
        let watched = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .watched_keys()
        .to_vec();
        let now = now_ms();
        for watched in &watched {
            let is_expired = self
                .ctx
                .db(watched.db_id)
                .is_some_and(|db| db.is_expired_at(&watched.key, now));
            if is_expired {
                if let Some(db) = self.ctx.db_mut(watched.db_id) {
                    db.exists(&watched.key);
                }
            }
        }

        let expired = self.ctx.drain_expired_deletions();
        if expired.is_empty() {
            return;
        }
        self.ctx.incr_expired_keys(expired.len() as u64);
        self.ctx.persistence_mut().add_dirty(expired.len() as i64);
        let mut expired_ops = Vec::with_capacity(expired.len());
        let conn = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        for (db_id, key) in expired {
            let key = Bytes::from(key);
            conn.transaction.touch_watched_key(db_id, &key, false);
            pending_watch_dirty.push((db_id, key.clone()));
            expired_ops.push(PropagationOp::both(
                db_id,
                replication_svc::encode_command(&[
                    if self.ctx.config().server.lazyfree_lazy_expire {
                        b"UNLINK" as &[u8]
                    } else {
                        b"DEL"
                    },
                    key.as_ref(),
                ]),
            ));
        }
        self._append_exec_propagation(&expired_ops, pending_replication, pending_aof);
    }

    fn exec_compute_prechecks(&self, connection_token: usize) -> ExecPrechecks {
        let in_tx = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .is_active();
        let min_repl = self.ctx.config().replication.min_replicas_to_write;
        let min_repl_max_lag = self.ctx.config().replication.min_replicas_max_lag;
        let good_repl = self.good_replicas_count();
        let has_write_queued = in_tx
            && self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .iter()
            .any(|cmd| {
                self.registry.lookup(&cmd.name).is_some_and(|s| {
                    (super::dispatch::subcmd_flags::effective_command_flags_for_call(
                        s.name, s.flags, cmd, &self.ctx,
                    ) & CMD_WRITE)
                        != 0
                })
            });
        let noreplicas_reject = !self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .is_watch_dirty()
            && !self.ctx.is_script_busy()
            && min_repl > 0
            && min_repl_max_lag > 0
            && good_repl < min_repl
            && has_write_queued;
        let has_denyoom_queued = in_tx
            && self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .iter()
            .any(|cmd| {
                self.registry.lookup(&cmd.name).is_some_and(|s| {
                    (super::dispatch::subcmd_flags::effective_command_flags_for_call(
                        s.name, s.flags, cmd, &self.ctx,
                    ) & CMD_DENYOOM)
                        != 0
                })
            });
        let oom_reject = self.ctx.maxmemory_state().is_over_limit() && has_denyoom_queued;
        let has_deny_stale_queued = in_tx
            && self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .iter()
            .any(|cmd| {
                self.registry.lookup(&cmd.name).is_some_and(|s| {
                    (super::dispatch::subcmd_flags::effective_command_flags_for_call(
                        s.name, s.flags, cmd, &self.ctx,
                    ) & CMD_STALE)
                        == 0
                })
            });
        let stale_reject = self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_link_status != "up"
            && !self.ctx.config().replication.replica_serve_stale_data
            && has_deny_stale_queued;
        let readonly_reject = self.ctx.replication().is_replica()
            && self.ctx.config().replication.replica_read_only
            && in_tx
            && self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .iter()
            .any(|cmd| {
                if self.ctx.config().server.cluster_enabled
                    && cmd.name.eq_ignore_ascii_case(b"spublish")
                {
                    return false;
                }
                let Some(spec) = self.registry.lookup(&cmd.name) else {
                    return false;
                };
                let may_write = (spec.flags & (CMD_WRITE | CMD_MAY_REPLICATE)) != 0;
                may_write
                    && !super::dispatch::is_ro_script(
                        &String::from_utf8_lossy(&cmd.name).to_ascii_lowercase(),
                        cmd,
                        &self.ctx,
                    )
                    && !super::dispatch::is_eval_without_shebang(
                        &String::from_utf8_lossy(&cmd.name).to_ascii_lowercase(),
                        cmd,
                    )
            });
        ExecPrechecks {
            noreplicas_reject,
            oom_reject,
            readonly_reject,
            stale_reject,
        }
    }

    fn exec_should_pause(&mut self, connection_token: usize) -> bool {
        let now = mstime();
        let pause = self.ctx.client_pause().is_paused(now);
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        match pause {
            PauseAction::None => false,
            PauseAction::Write => conn.transaction.queued_commands().iter().any(|cmd| {
                let Some(spec) = self.registry.lookup(&cmd.name) else {
                    return false;
                };
                let flags = super::dispatch::subcmd_flags::effective_command_flags_for_call(
                    spec.name, spec.flags, cmd, &self.ctx,
                );
                let has_write_flag = (flags & (CMD_WRITE | CMD_MAY_REPLICATE)) != 0;
                if !has_write_flag {
                    return false;
                }
                let name_lower = String::from_utf8_lossy(&cmd.name).to_ascii_lowercase();
                let is_ro = super::dispatch::is_ro_script(&name_lower, cmd, &self.ctx);
                !is_ro
            }),
            PauseAction::All => true,
        }
    }

    fn exec_run_commands(
        &mut self,
        target: CommandTarget,
        pending_wakeup_only: &mut Vec<(u32, Bytes)>,
    ) -> ExecRunOutput {
        let Some(token) = target.resolve(self) else {
            return ExecRunOutput::default();
        };
        let cmds = self.connection_registry[token]
            .transaction
            .take_queued_commands();
        let mut state = ExecRunState {
            target,
            token,
            username: self.connection_registry[token].username.clone(),
            client_info: format!(
                "addr={} cmd=exec",
                self.connection_registry[token].peer_addr
            ),
            results: Vec::with_capacity(cmds.len()),
            dirty_keys: Vec::new(),
            propagation_ops: Vec::new(),
            publish: Vec::new(),
            publish_shard: Vec::new(),
            wakeup_only: pending_wakeup_only,
            closed_publish_peers: std::collections::HashSet::new(),
        };
        let _time_guard = crate::foundation::io::sys::time::enter_command_time_snapshot(mstime());
        for cmd_args in &cmds {
            if !self.exec_run_queued_item(&mut state, cmd_args) {
                break;
            }
        }
        ExecRunOutput {
            results: state.results,
            dirty_keys: state.dirty_keys,
            propagation_ops: state.propagation_ops,
            publish: state.publish,
            publish_shard: state.publish_shard,
        }
    }

    fn exec_run_queued_item(&mut self, state: &mut ExecRunState<'_>, args: &CommandArgs) -> bool {
        let Some((spec, command_flags)) = self.exec_admit_queued_item(state, args) else {
            return true;
        };
        let was_blocked = self.connection_registry[state.token].blocked.is_some();
        self.ctx.begin_cmd();
        self.connection_registry[state.token].begin_nested_command_effects();
        let reply = self.exec_invoke_queued_item(state.token, &spec, args);
        let Some(token) = state.target.resolve(self) else {
            return false;
        };
        state.token = token;
        let mut connection_effects = self.connection_registry[token]
            .take_command_effects(CommandEffectDrain::Nested)
            .expect("an EXEC item drains its connection effects exactly once");
        if let Some(cluster) = self.ctx.cluster_state().as_deref() {
            // Physical link teardown is immediate, but its outbound barrier
            // must survive until EXEC ends so later items cannot bypass it via
            // an inbound link before the event loop reconnects the peer.
            state.closed_publish_peers.extend(
                cluster
                    .pending_link_closes()
                    .iter()
                    .filter(|(_, direction)| {
                        matches!(
                            direction,
                            crate::engine::distributed::cluster::ClusterLinkDirection::To
                                | crate::engine::distributed::cluster::ClusterLinkDirection::All
                        )
                    })
                    .map(|(peer, _)| peer.clone()),
            );
        }
        let reply = self.apply_post_command_intents(
            CommandOrigin::TransactionItem,
            token.0,
            Some(&mut connection_effects),
            reply,
        );
        self.connection_registry[token].finish_nested_reply_state(&mut connection_effects);
        let mut completion = ExecItemCompletion {
            token,
            args,
            spec,
            command_flags,
            reply,
            expired: self.ctx.drain_expired_deletions(),
            was_blocked,
            connection_effects: Some(connection_effects),
        };
        self.exec_record_item_observability(&completion);
        self.exec_collect_item_effects(state, &mut completion);
        state.results.push(completion.reply);
        true
    }

    fn exec_admit_queued_item(
        &mut self,
        state: &mut ExecRunState<'_>,
        args: &CommandArgs,
    ) -> Option<(CommandSpec, u64)> {
        let Some(spec) = self.registry.lookup(&args.name).cloned() else {
            state.results.push(CommandError::unknown(&args.name));
            return None;
        };
        let flags = super::dispatch::subcmd_flags::effective_command_flags_for_call(
            spec.name, spec.flags, args, &self.ctx,
        );
        if let Some(reply) = self.exec_check_acl(ExecAclRequest {
            token: state.token,
            spec: &spec,
            args,
            username: &state.username,
            client_info: &state.client_info,
        }) {
            state.results.push(reply);
            return None;
        }
        if super::dispatch::monitor::is_explicit_script_monitor_command(spec.name) {
            self.exec_emit_item_monitor(state.token, args, flags, true);
        }
        Some((spec, flags))
    }

    fn exec_invoke_queued_item(
        &mut self,
        token: Token,
        spec: &CommandSpec,
        args: &CommandArgs,
    ) -> Value {
        if spec.dispatch_group == crate::engine::command::DispatchGroup::ClientAdmin {
            if let Some(reply) = self.exec_client_admin_value(token.0, args) {
                return reply;
            }
        }
        self.invoke_command_executor(token, spec.executor, args)
    }

    fn exec_emit_item_monitor(
        &mut self,
        token: Token,
        args: &CommandArgs,
        command_flags: u64,
        script: bool,
    ) {
        let conn = &self.connection_registry[token];
        let mut argv = Vec::with_capacity(args.args.len() + 1);
        argv.push(args.name.clone());
        argv.extend(args.args.iter().cloned());
        if let Some(event) = self.ctx.monitor_event(
            conn.selected_db,
            conn.peer_addr.to_string(),
            argv,
            command_flags,
            script,
        ) {
            self.ctx.script_mut().pending_internal_monitor.push(event);
        }
    }

    fn exec_record_item_observability(&mut self, item: &ExecItemCompletion<'_>) {
        self.exec_emit_item_monitor(item.token, item.args, item.command_flags, false);
        let name = String::from_utf8_lossy(&item.args.name).to_ascii_lowercase();
        let stats_name = super::dispatch::command_stats_name(&name, item.args);
        let duration_us = self.ctx.end_cmd(&stats_name, false);
        super::dispatch::record_error_reply(&mut self.ctx, &item.reply, Some(&stats_name));
        self.exec_update_blocking_flags(item.token.0, item.token.0, item.was_blocked);
        if item.was_blocked && self.connection_registry[item.token].blocked.is_some() {
            return;
        }
        let conn = &self.connection_registry[item.token];
        let peer_addr = conn.peer_addr;
        let conn_name = conn.name.clone();
        let mut argv = Vec::with_capacity(item.args.args.len() + 1);
        argv.push(item.args.name.clone());
        argv.extend(item.args.args.iter().cloned());
        self.ctx
            .push_cmd_log_slow_only(argv, duration_us, &peer_addr, conn_name);
    }

    fn exec_collect_item_effects(
        &mut self,
        state: &mut ExecRunState<'_>,
        item: &mut ExecItemCompletion<'_>,
    ) {
        state.dirty_keys.extend(
            item.expired
                .iter()
                .map(|(db_id, key)| (*db_id, Bytes::from(key.clone()))),
        );
        let mut effects = CommandEffects::default();
        let capture = self.capture_transaction_item_propagation(
            TransactionPropagationInput {
                token: item.token,
                args: item.args,
                name: item.spec.name,
                command_flags: item.spec.flags,
                rejected: matches!(item.reply, Value::Error(_)),
            },
            item.connection_effects
                .take()
                .expect("EXEC item effects are collected exactly once"),
            &mut effects,
        );
        if let Some((db1, db2)) = capture.swapdb {
            self.apply_swapdb_watch_state(db1, db2);
        }
        state.dirty_keys.extend(effects.take_watch_dirty());
        state.wakeup_only.extend(effects.take_wakeup_only());
        state.propagation_ops.extend(capture.operations);
        self.exec_collect_item_tail(state, item, &mut effects);
    }

    fn exec_collect_item_tail(
        &mut self,
        state: &mut ExecRunState<'_>,
        item: &ExecItemCompletion<'_>,
        effects: &mut CommandEffects,
    ) {
        let cluster_publish = std::mem::take(&mut effects.cluster_publish);
        let cluster_publish_shard = std::mem::take(&mut effects.cluster_publish_shard);
        self.propagate_cluster_publishes_excluding(
            cluster_publish,
            cluster_publish_shard,
            &state.closed_publish_peers,
        );
        state.publish.append(&mut effects.publish);
        state.publish_shard.append(&mut effects.publish_shard);
        self.ctx.incr_expired_keys(item.expired.len() as u64);
        self.ctx
            .persistence_mut()
            .add_dirty(item.expired.len() as i64);
        let expire_command = if self.ctx.config().server.lazyfree_lazy_expire {
            b"UNLINK" as &[u8]
        } else {
            b"DEL"
        };
        state
            .propagation_ops
            .extend(item.expired.iter().map(|(db_id, key)| {
                PropagationOp::both(
                    *db_id,
                    replication_svc::encode_command(&[expire_command, key.as_ref()]),
                )
            }));
    }

    fn exec_update_blocking_flags(
        &mut self,
        connection_token: usize,
        token_value: usize,
        was_blocked: bool,
    ) {
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        let is_blocked = conn.blocked.is_some();
        if !was_blocked && is_blocked {
            self.ctx.blocking_registry_mut().increment_count();
            if let Some(ci) = self.ctx.client_info_mut(token_value) {
                if !ci.flags.contains('b') {
                    ci.flags.retain(|c| c != 'N');
                    ci.flags.push('b');
                }
            }
        } else if was_blocked && !is_blocked {
            self.ctx.blocking_registry_mut().decrement_count();
            if let Some(ci) = self.ctx.client_info_mut(token_value) {
                ci.flags.retain(|c| c != 'b');
                if ci.flags.is_empty() {
                    ci.flags.push('N');
                }
            }
        }
    }

    fn _append_exec_propagation(
        &mut self,
        propagation_ops: &[PropagationOp],
        pending_replication: &mut Vec<Vec<u8>>,
        pending_aof: &mut Vec<Vec<u8>>,
    ) {
        if propagation_ops.is_empty() {
            return;
        }
        self.queue_slot_migration_mutations(propagation_ops);
        if self.ctx.persistence_mut().take_rewrite_force_select() {
            self.ctx.persistence_mut().reset_aof_selected_db();
        }
        let sequenced = sequence_propagation(
            propagation_ops,
            self.ctx.repl_seldb(),
            self.ctx.persistence().aof_selected_db(),
        );
        self.ctx.set_repl_seldb(sequenced.replication_selected_db);
        self.ctx
            .persistence_mut()
            .set_aof_selected_db(sequenced.aof_selected_db);
        pending_replication.extend(sequenced.replication_frames);
        pending_aof.extend(sequenced.aof_frames);
    }
}

#[cfg(test)]
mod cluster_publish_tests {
    use super::ServerWorkerState;
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::flows::request::invocation::CommandTarget;
    use bytes::Bytes;

    fn begin_with_commands(connection: &mut Connection, commands: Vec<CommandArgs>) {
        assert!(connection.transaction.begin(0, 0));
        for command in commands {
            assert!(connection.transaction.queue(command));
        }
    }

    #[test]
    fn replica_readonly_precheck_exempts_cluster_spublish_but_not_set() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.replication_mut().switch_to_replica();
        let mut connection = Connection::new_fake();
        begin_with_commands(
            &mut connection,
            vec![CommandArgs::from_parts(vec![
                Bytes::from_static(b"SPUBLISH"),
                Bytes::from_static(b"channel"),
                Bytes::from_static(b"payload"),
            ])],
        );
        let connection_token = backend.connection_registry.insert(connection);

        assert!(
            !backend
                .exec_compute_prechecks(connection_token)
                .readonly_reject
        );

        let transaction = &mut backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction;
        transaction.take_queued_commands();
        assert!(transaction.queue(CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ])));
        assert!(
            backend
                .exec_compute_prechecks(connection_token)
                .readonly_reject
        );
    }

    #[test]
    fn zero_max_lag_disables_exec_min_replicas_gate() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().replication.min_replicas_to_write = 1;
        backend.ctx.config_mut().replication.min_replicas_max_lag = 0;
        let mut connection = Connection::new_fake();
        begin_with_commands(
            &mut connection,
            vec![CommandArgs::from_parts(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"value"),
            ])],
        );
        let connection_token = backend.connection_registry.insert(connection);

        assert!(
            !backend
                .exec_compute_prechecks(connection_token)
                .noreplicas_reject,
            "C disables the EXEC write gate when min-replicas-max-lag is zero"
        );
    }

    #[test]
    fn exec_queues_cluster_publish_before_following_command() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.shard_id = state.myself.shard_id.clone();
        state.nodes.insert(peer_id.to_string(), peer);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            peer_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&41)
            .unwrap()
            .handshake_sent = true;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        begin_with_commands(
            &mut connection,
            vec![CommandArgs::from_parts(vec![
                Bytes::from_static(b"SPUBLISH"),
                Bytes::from_static(b"channel"),
                Bytes::from_static(b"payload"),
            ])],
        );
        let _ = backend.connection_registry.insert(connection);

        let output = backend.exec_run_commands(CommandTarget::new(Token(7)), &mut Vec::new());

        assert!(!backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(output.propagation_ops.is_empty());
    }

    #[test]
    fn exec_outbound_kill_prevents_inbound_publish_fallback_until_transaction_ends() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.enable_debug_command =
            crate::engine::extension::config::server::ProtectedAction::Yes;
        let killed = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let healthy = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for (name, port) in [(killed, 6380), (healthy, 6381)] {
            let mut peer = ClusterNode::new(
                name.to_string(),
                "127.0.0.1".to_string(),
                port,
                port + 10000,
                true,
            );
            peer.flags &= !CLUSTER_NODE_MYSELF;
            peer.shard_id = state.myself.shard_id.clone();
            state.nodes.insert(name.to_string(), peer);
        }
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        for (token, name, port) in [(41, killed, 16380), (43, healthy, 16381)] {
            backend.cluster_bus.register_outgoing_for_node(
                Token(token),
                format!("127.0.0.1:{port}").parse().unwrap(),
                false,
                name.to_string(),
            );
            backend
                .cluster_bus
                .connections
                .get_mut(&token)
                .unwrap()
                .handshake_sent = true;
        }
        backend
            .cluster_bus
            .register_incoming(Token(42), "127.0.0.1:26380".parse().unwrap());
        let inbound = backend.cluster_bus.connections.get_mut(&42).unwrap();
        inbound.node_name = killed.to_string();
        inbound.handshake_sent = true;
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        begin_with_commands(
            &mut connection,
            vec![
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"DEBUG"),
                    Bytes::from_static(b"CLUSTERLINK"),
                    Bytes::from_static(b"KILL"),
                    Bytes::from_static(b"TO"),
                    Bytes::copy_from_slice(killed.as_bytes()),
                ]),
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SPUBLISH"),
                    Bytes::from_static(b"channel"),
                    Bytes::from_static(b"hello"),
                ]),
            ],
        );
        backend.connection_registry.insert(connection);
        let output = backend.exec_run_commands(CommandTarget::new(Token(7)), &mut Vec::new());
        assert_eq!(output.results, vec![Value::ok(), Value::integer(0)]);
        assert!(!backend.cluster_bus.connections.contains_key(&41));
        assert!(backend.cluster_bus.connections[&42].write_buf.is_empty());
        assert!(!backend.cluster_bus.connections[&43].write_buf.is_empty());
        assert!(!backend
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .pending_connects()
            .is_empty());
        backend.propagate_cluster_publishes(
            Vec::new(),
            vec![(Bytes::from_static(b"channel"), Bytes::from_static(b"world"))],
        );
        assert!(
            !backend.cluster_bus.connections[&42].write_buf.is_empty(),
            "EXEC-local close barrier must not suppress later normal reconnect fallback"
        );
    }

    #[test]
    fn exec_does_not_propagate_cluster_rejected_swapdb() {
        let mut backend = ServerWorkerState::new(2).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        begin_with_commands(
            &mut connection,
            vec![CommandArgs::from_parts(vec![
                Bytes::from_static(b"SWAPDB"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
            ])],
        );
        connection.set_skip_propagation(false);
        let _ = backend.connection_registry.insert(connection);

        let output = backend.exec_run_commands(CommandTarget::new(Token(7)), &mut Vec::new());

        assert!(
            matches!(output.results.as_slice(), [Value::Error(error)] if error.starts_with(b"ERR SWAPDB is not allowed in cluster mode"))
        );
        assert!(output.propagation_ops.is_empty());
        assert_eq!(backend.ctx.persistence().dirty(), 0);
    }

    #[test]
    fn exec_freezes_command_time_across_queued_handlers() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.active_expire_enabled = false;
        backend.ctx.config_mut().server.enable_debug_command =
            crate::engine::extension::config::server::ProtectedAction::Yes;
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        begin_with_commands(
            &mut connection,
            vec![
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"HSET"),
                    Bytes::from_static(b"hash"),
                    Bytes::from_static(b"field"),
                    Bytes::from_static(b"v1"),
                ]),
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"HPEXPIRE"),
                    Bytes::from_static(b"hash"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"FIELDS"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"field"),
                ]),
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"DEBUG"),
                    Bytes::from_static(b"SLEEP"),
                    Bytes::from_static(b"0.01"),
                ]),
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"HSETEX"),
                    Bytes::from_static(b"hash"),
                    Bytes::from_static(b"KEEPTTL"),
                    Bytes::from_static(b"FIELDS"),
                    Bytes::from_static(b"1"),
                    Bytes::from_static(b"field"),
                    Bytes::from_static(b"v2"),
                ]),
            ],
        );
        let _ = backend.connection_registry.insert(connection);

        let output = backend.exec_run_commands(CommandTarget::new(Token(7)), &mut Vec::new());

        assert!(
            output
                .results
                .iter()
                .all(|value| !matches!(value, Value::Error(_))),
            "queued commands must execute successfully: {:?}",
            output.results
        );
        assert!(
            backend
                .ctx
                .db(0)
                .unwrap()
                .get_hash_field_expire(b"hash", b"field")
                .is_some(),
            "HSETEX KEEPTTL must observe the same logical time as the preceding HPEXPIRE"
        );
    }
}

#[cfg(test)]
mod standalone_redirect_tests {
    use super::ServerWorkerState;
    use crate::engine::command::CommandArgs;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::flows::request::dispatch::CommandEffects;
    use crate::runtime::server::worker::flows::request::invocation::CommandTarget;
    use crate::runtime::server::worker::flows::request::outcome::{
        CommandDisposition, SpecialCommandResult,
    };
    use bytes::Bytes;

    #[test]
    fn exec_redirects_a_queued_write_after_primary_becomes_replica() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(9));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.capa_flags.push("redirect".to_string());
        assert!(connection.transaction.queue(CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ])));
        let connection_token = backend.connection_registry.insert(connection);
        let mut effects = CommandEffects::default();

        assert_eq!(
            backend.try_handle_exec(CommandTarget::new(Token(9)), "exec", false, &mut effects,),
            SpecialCommandResult::Handled(CommandDisposition::Completed)
        );
        assert_eq!(
            effects.replies(),
            [b"-REDIRECT 192.0.2.1:6380\r\n".to_vec()]
        );
        assert!(!backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .is_active());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .queued_commands()
        .is_empty());
    }

    #[test]
    fn multi_queue_rejects_a_write_when_already_a_redirecting_replica() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(10));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.capa_flags.push("redirect".to_string());
        let connection_token = backend.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ]);
        let mut replies = Vec::new();

        assert_eq!(
            backend.try_handle_multi_queue(
                Token(connection_token),
                &args,
                "set",
                false,
                &mut replies,
            ),
            Some(true)
        );
        assert_eq!(replies, vec![b"-REDIRECT 192.0.2.1:6380\r\n".to_vec()]);
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .has_queue_error());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .queued_commands()
        .is_empty());
    }

    #[test]
    fn multi_queue_acl_denial_precedes_redirect() {
        const USERNAME: &str = "multi-redirect-order-test";
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(13));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.username = USERNAME.to_string();
        connection.capa_flags.push("redirect".to_string());
        let connection_token = backend.connection_registry.insert(connection);
        let mut user = crate::engine::extension::acl::AclUser::new_disabled();
        user.apply_rule("on").unwrap();
        user.apply_rule("nopass").unwrap();
        user.apply_rule("+get").unwrap();
        user.apply_rule("allkeys").unwrap();
        crate::engine::extension::acl::acl_users()
            .lock()
            .unwrap()
            .insert(USERNAME.to_string(), user);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ]);
        let mut replies = Vec::new();

        assert_eq!(
            backend.try_handle_multi_queue(
                Token(connection_token),
                &args,
                "set",
                false,
                &mut replies,
            ),
            Some(true)
        );
        crate::engine::extension::acl::acl_users()
            .lock()
            .unwrap()
            .remove(USERNAME);

        assert_eq!(replies.len(), 1);
        assert!(replies[0].starts_with(b"-NOPERM "));
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .has_queue_error());
    }

    #[test]
    fn multi_queue_postpones_a_write_with_replay_state_during_failover() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(11));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.capa_flags.push("redirect".to_string());
        let connection_token = backend.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ]);

        assert_eq!(
            backend.try_handle_multi_queue(
                Token(connection_token),
                &args,
                "set",
                false,
                &mut Vec::new()
            ),
            Some(false)
        );
        let blocked = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .as_ref()
        .expect("postponed transaction command must retain replay state");
        assert_eq!(blocked.orig_args.name, args.name);
        assert_eq!(blocked.orig_args.args, args.args);
        assert!(backend.ctx.paused_clients().contains(&Token(11)));
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .queued_commands()
        .is_empty());
    }

    #[test]
    fn waking_a_postponed_transaction_command_queues_it_after_promotion() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(12));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.capa_flags.push("redirect".to_string());
        let connection_token = backend.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ]);
        assert_eq!(
            backend.try_handle_multi_queue(
                Token(connection_token),
                &args,
                "set",
                false,
                &mut Vec::new()
            ),
            Some(false)
        );

        backend.ctx.replication_mut().switch_to_primary();
        backend.ctx.replication_mut().failover_state.clear();
        backend.wakeup_blocked_client(Token(12), None, None);

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_none());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .len(),
            1
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()[0]
                .name,
            args.name
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()[0]
                .args,
            args.args
        );
        assert!(backend.ctx.db(0).unwrap().get_no_expire(b"key").is_none());
    }

    #[test]
    fn waking_a_postponed_transaction_command_redirects_after_failover_abort() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;

        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(14));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        connection.capa_flags.push("redirect".to_string());
        let connection_token = backend.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
        ]);
        assert_eq!(
            backend.try_handle_multi_queue(
                Token(connection_token),
                &args,
                "set",
                false,
                &mut Vec::new()
            ),
            Some(false)
        );

        backend.ctx.replication_mut().failover_state.clear();
        backend.wakeup_blocked_client(Token(14), None, None);

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_none());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .queued_commands()
        .is_empty());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .has_queue_error());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
            .back(),
            Some(&b"-REDIRECT 192.0.2.1:6380\r\n".to_vec())
        );
    }
}
