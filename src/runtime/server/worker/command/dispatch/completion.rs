//! Command-local completion stages.
//! Each helper owns one observable concern. `complete_command` is deliberately
//! linear because its order is part of the client, monitoring, and blocking
//! contract.

use super::command_exec::{client_last_command_name, write_client_last_command_name};
use super::side_effects::CommandEffects;
use super::{CmdLogEntry, ServerWorkerState};
use crate::engine::command::context::CommandConn;
use crate::engine::command::CMD_READONLY;
use crate::foundation::io::protocol::serializer::{serialize_resp2, serialize_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::BlockedCmd;
use crate::framework::capabilities::connection::ConnectionCommandEffects;
use crate::runtime::server::worker::command::invocation::{CommandInvocation, CommandOrigin};
use crate::runtime::server::worker::command::outcome::{
    CommandCompletion, CommandDisposition, CommandExecutionStatus,
};
use bytes::Bytes;

#[derive(Clone, Copy)]
struct ObserverCompletion<'a> {
    invocation: CommandInvocation<'a>,
    status: CommandExecutionStatus,
    duration_us: u64,
    connection_token: usize,
    reply_bytes: usize,
    connection_effects: Option<&'a ConnectionCommandEffects>,
}

#[cfg(test)]
mod read_tracking_tests {
    use super::*;
    use crate::engine::command::CommandArgs;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::command::invocation::CommandTarget;

    #[test]
    fn readonly_script_completion_tracks_only_nested_reads() {
        for name in ["eval_ro", "evalsha_ro", "fcall_ro"] {
            let mut runtime = ServerWorkerState::new(1).unwrap();
            crate::framework::commands::register_all(&mut runtime.registry);
            let token = Token(42);
            let mut connection = Connection::new_fake();
            connection.set_token_for_test(token);
            connection.set_tracking(true);
            runtime.connection_registry.insert(connection);
            runtime.ctx.tracking_mut().clients.insert(token.0);
            let nested_key = Bytes::from_static(b"nested-read");
            runtime
                .ctx
                .tracking_mut()
                .remember_key(token.0, &nested_key);
            let args = CommandArgs::from_parts(vec![
                Bytes::copy_from_slice(name.as_bytes()),
                Bytes::from_static(b"script-or-function"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"declared-only"),
                nested_key.clone(),
            ]);
            let spec = runtime.registry.lookup(name.as_bytes()).unwrap();
            let invocation = CommandInvocation::direct(
                CommandTarget::new(token),
                &args,
                name,
                spec.flags,
                spec.dispatch_group,
                &[],
            );

            runtime.capture_read_tracking(invocation, CommandExecutionStatus::Executed, token.0);

            assert!(
                runtime
                    .ctx
                    .tracking_mut()
                    .take_clients_for_key(&Bytes::from_static(b"declared-only"))
                    .is_empty(),
                "{name} must not track keys merely declared by its caller"
            );
            assert_eq!(
                runtime.ctx.tracking_mut().take_clients_for_key(&nested_key),
                vec![token.0],
                "nested reads must remain tracked after {name} completes"
            );
        }
    }
}

impl ServerWorkerState {
    pub(super) fn complete_command(
        &mut self,
        completion: CommandCompletion<'_>,
        mut connection_effects: Option<&mut ConnectionCommandEffects>,
        effects: &mut CommandEffects,
    ) -> CommandDisposition {
        let CommandCompletion {
            invocation,
            reply,
            status,
            duration_us,
            was_blocked_before,
        } = completion;
        debug_assert_eq!(invocation.origin, CommandOrigin::Direct);
        let Some(token) = invocation.target.resolve(self) else {
            return CommandDisposition::ConnectionGone;
        };
        let connection_token = token.0;
        if let Some(connection_effects) = connection_effects.as_mut() {
            self.dispose_data_lazyfree_objects(connection_effects.take_lazyfree_objects());
        }

        let is_replica_link = self.is_downstream_replica(invocation.target.token_value());
        let reply = self.normalize_replica_reply(invocation.flags, is_replica_link, reply);
        self.record_completion_metadata(invocation, status, duration_us, connection_token, &reply);

        if self.complete_blocked_transition(
            invocation,
            duration_us,
            was_blocked_before,
            connection_token,
            effects,
        ) {
            return CommandDisposition::Blocked;
        }
        let reply_bytes = self.capture_reply(
            connection_token,
            &reply,
            connection_effects.as_deref_mut(),
            effects,
        );
        if self.close_replying_replica(
            connection_token,
            completion.invocation,
            is_replica_link,
            &reply,
            effects,
        ) {
            return CommandDisposition::Closing;
        }
        self.capture_read_tracking(invocation, status, connection_token);
        self.capture_observers_and_metrics(
            ObserverCompletion {
                invocation,
                status,
                duration_us,
                connection_token,
                reply_bytes,
                connection_effects: connection_effects.as_deref(),
            },
            effects,
        );
        CommandDisposition::Completed
    }

    fn is_downstream_replica(&self, token_value: usize) -> bool {
        // Fast path: a standalone server with no connected replicas has an
        // empty `primary_links` map, so the per-connection `channel_records`
        // lookup can be skipped entirely. This covers the common PING workload.
        if self.replication_runtime.primary_links().is_empty() {
            return false;
        }
        self.replication_runtime
            .primary_for_connection(crate::framework::distributed::replication::ConnectionId(
                token_value,
            ))
            .is_some()
    }

    fn normalize_replica_reply(
        &self,
        command_flags: u64,
        is_replica_link: bool,
        reply: Value,
    ) -> Value {
        let accesses_keyspace = command_flags
            & (crate::engine::command::CMD_READONLY
                | crate::engine::command::CMD_WRITE
                | crate::engine::command::CMD_MAY_REPLICATE)
            != 0;
        if is_replica_link && accesses_keyspace {
            Value::error("ERR Replica can't interact with the keyspace")
        } else {
            reply
        }
    }

    fn record_completion_metadata(
        &mut self,
        invocation: CommandInvocation<'_>,
        status: CommandExecutionStatus,
        duration_us: u64,
        connection_token: usize,
        reply: &Value,
    ) {
        let rejected = status.is_rejected();
        let stats_name = super::command_stats_name(invocation.name, invocation.args);
        super::record_error_reply(
            &mut self.ctx,
            reply,
            (!rejected).then_some(stats_name.as_ref()),
        );
        if !rejected && duration_us > 0 {
            let event_name = if invocation.flags & crate::engine::command::CMD_FAST != 0 {
                "fast-command"
            } else {
                "command"
            };
            self.ctx
                .latency_mut()
                .add_sample(event_name, (duration_us / 1000) as i64);
        }
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        // Write `last_cmd` directly onto the connection instead of going
        // through `client_info_mut`. The `db` and `flags` fields are derivable
        // from connection state (CLIENT LIST already computes them on the fly),
        // so only `watched_keys` is mirrored to `ClientInfo` for the low-
        // frequency INFO `watched_client_summary` consumer.
        write_client_last_command_name(&mut connection.last_cmd, invocation.name, invocation.args);
        let watched_keys = connection.transaction.watched_key_count();
        // `watched_keys` must be mirrored to `ClientInfo` on every transition,
        // including the 1 -> 0 case after UNWATCH/EXEC/DISCARD. Skipping the
        // update when the count is zero would leave a stale non-zero value and
        // inflate `INFO watching_clients`.
        //
        // Fast path: only pay for the `client_info_mut` HashMap lookup when the
        // count actually changed since the last sync. On a pure SET/GET/INCR
        // workload the count is always 0, so the lookup is skipped entirely.
        // The `watched_key_clients` counter is kept in sync so
        // `process_dirty_keys_in_loop` can skip the per-connection scan when
        // no client holds watched keys.
        if watched_keys != connection.synced_watched_keys {
            let had_watched = connection.synced_watched_keys > 0;
            let has_watched = watched_keys > 0;
            match (had_watched, has_watched) {
                (false, true) => self.watched_key_clients += 1,
                (true, false) => self.watched_key_clients -= 1,
                _ => {}
            }
            if let Some(info) = self.ctx.client_info_mut(invocation.target.token_value()) {
                info.watched_keys = watched_keys;
            }
            connection.synced_watched_keys = watched_keys;
        }
    }

    fn complete_blocked_transition(
        &mut self,
        invocation: CommandInvocation<'_>,
        duration_us: u64,
        was_blocked_before: bool,
        connection_token: usize,
        effects: &mut CommandEffects,
    ) -> bool {
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked
            .is_none()
        {
            return false;
        }
        if !was_blocked_before {
            self.ctx.decr_total_commands(1);
            let stats_name = super::command_stats_name(invocation.name, invocation.args);
            // The command was already recorded by `begin_cmd`/`end_cmd` before
            // it blocked; only adjust the existing entry. A missing entry
            // saturates to zero identically to creating one first.
            if let Some(stat) = self.ctx.stats_mut().cmd_stats.get_mut(stats_name.as_ref()) {
                stat.calls = stat.calls.saturating_sub(1);
                stat.usec = stat.usec.saturating_sub(duration_us);
            }
        }
        self.commit_native_module_initial_effects(connection_token, Some(effects));
        self.commit_blocked_stream_read_effects(connection_token, None, None, Some(effects));
        if !was_blocked_before {
            if let Some(crate::engine::command::context::BlockedState {
                cmd: BlockedCmd::Module { id },
                ..
            }) = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .blocked
            .as_ref()
            {
                if let Some(record) = self.ctx.module_registry_mut().native_blocks.get_mut(id) {
                    record.active_time_usec = duration_us;
                    crate::engine::extension::module::thread_bridge::lock(
                        &record.shared.lifecycle,
                    )
                    .stage = crate::engine::extension::module::blocking::ModuleBlockStage::Armed;
                }
            }
        }
        let request_wait_ack = self.register_blocked_command(connection_token);
        let monitor = (!was_blocked_before).then(|| {
            let connection = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let mut argv = Vec::with_capacity(invocation.args.args.len() + 1);
            argv.push(invocation.args.name.clone());
            argv.extend(invocation.args.args.iter().cloned());
            (
                connection.selected_db,
                connection.peer_addr.to_string(),
                argv,
            )
        });
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_custom_reply_enabled(false);
        if let Some((db, source, argv)) = monitor {
            if let Some(event) = self
                .ctx
                .monitor_event(db, source, argv, invocation.flags, false)
            {
                self.deliver_monitor(vec![event]);
            }
        }
        if request_wait_ack {
            self.propagate_to_replicas_with_ack(&[], true);
        }
        true
    }

    fn register_blocked_command(&mut self, connection_token: usize) -> bool {
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        connection.cmd_count -= 1;
        let Some(blocked) = &connection.blocked else {
            return false;
        };
        match &blocked.cmd {
            BlockedCmd::Wait { .. } | BlockedCmd::WaitAof { .. } => {
                self.ctx
                    .blocking_registry_mut()
                    .register_keyless_client(connection.session_id());
                true
            }
            // SETSLOT already force-propagated its frame together with the
            // GETACK request, so it must not trigger the empty-command GETACK
            // that WAIT/WAITAOF rely on.
            BlockedCmd::SetSlot { .. } => {
                self.ctx
                    .blocking_registry_mut()
                    .register_keyless_client(connection.session_id());
                false
            }
            BlockedCmd::Module { .. } => {
                self.ctx.blocking_registry_mut().register_keyless_client(connection.session_id());
                false
            }
            BlockedCmd::Paused
            | BlockedCmd::Forkless
            | BlockedCmd::Throttled
            | BlockedCmd::Shutdown => false,
            BlockedCmd::Blpop
            | BlockedCmd::Brpop
            | BlockedCmd::Brpoplpush { .. }
            | BlockedCmd::Blmove { .. }
            | BlockedCmd::Blmpop { .. }
            | BlockedCmd::Bzpopmin
            | BlockedCmd::Bzpopmax
            | BlockedCmd::Bzmpop { .. }
            | BlockedCmd::Xread
            | BlockedCmd::Xreadgroup
            | BlockedCmd::SlotMigration(_)
            // Script currently has no constructor; keep the historical
            // fall-through so behaviour is unchanged if it gets enabled.
            | BlockedCmd::Script => {
                self.ctx.blocking_registry_mut().register_client(
                    connection.selected_db,
                    &blocked.keys,
                    connection.session_id(),
                );
                false
            }
        }
    }

    fn capture_reply(
        &mut self,
        connection_token: usize,
        reply: &Value,
        connection_effects: Option<&mut ConnectionCommandEffects>,
        effects: &mut CommandEffects,
    ) -> usize {
        let token = crate::framework::capabilities::io::Token(connection_token);
        // Read the small connection fields we need up front so the connection
        // borrow does not span `acquire_reply_buffer`, which needs `&mut self`.
        let (output_before, resp_version, has_custom_reply) = {
            let connection = &self.connection_registry[token];
            (
                connection.output_before_current_command(),
                connection.resp_version,
                connection_effects
                    .as_deref()
                    .is_some_and(ConnectionCommandEffects::has_custom_reply),
            )
        };
        debug_assert_eq!(
            connection_effects
                .as_deref()
                .map(ConnectionCommandEffects::output_before),
            Some(output_before)
        );
        let reply_bytes = if has_custom_reply {
            connection_effects
                .as_deref()
                .map_or(0, ConnectionCommandEffects::reply_bytes)
        } else {
            // Reuse a pooled reply buffer instead of allocating a fresh
            // `Vec<u8>` for every command reply. `acquire_reply_buffer` pops
            // a recycled buffer (empty but with surviving heap capacity) or
            // falls back to a default-allocated one; the write path returns
            // fully-consumed frames to the pool.
            let mut frame = self.acquire_reply_buffer();
            if resp_version >= 3 {
                serialize_resp3(reply, &mut frame);
            } else {
                serialize_resp2(reply, &mut frame);
            }
            let reply_bytes = frame.len();
            if let Some(connection_effects) = connection_effects {
                connection_effects.set_reply_bytes(reply_bytes);
            }
            effects.push_command_reply(frame);
            reply_bytes
        };
        self.connection_registry[token].set_custom_reply_enabled(false);
        reply_bytes
    }

    fn close_replying_replica(
        &mut self,
        connection_token: usize,
        invocation: CommandInvocation<'_>,
        is_replica_link: bool,
        reply: &Value,
        effects: &mut CommandEffects,
    ) -> bool {
        if !is_replica_link {
            return false;
        }
        if self
            .replication_runtime
            .primary_for_connection(crate::framework::distributed::replication::ConnectionId(
                connection_token,
            ))
            .is_some_and(|link| {
                link.state()
                    == crate::framework::distributed::replication::PrimaryLinkState::AwaitingPsync
                    && link.dual_state()
                        == crate::framework::distributed::replication::PrimaryDualChannelState::Disabled
                    && link.accept_seq().is_none()
            })
        {
            return false;
        }
        if let Value::Error(error) = reply {
            let rendered = replica_error_for_log(
                error,
                invocation.args,
                self.ctx.config().server.hide_user_data_from_log,
            );
            let command = replica_command_name_for_log(
                invocation.name,
                invocation.args,
                self.ctx.config().server.hide_user_data_from_log,
            );
            crate::server_log!(
                warning,
                "== CRITICAL == This primary is sending an error to its replica: '{}' after processing the command '{}'",
                rendered,
                command,
            );
        }
        let command = replica_command_name_for_log(
            invocation.name,
            invocation.args,
            self.ctx.config().server.hide_user_data_from_log,
        );
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        crate::server_log!(
            warning,
            "Replica generated a reply to command '{}', disconnecting it: {}",
            command,
            connection.peer_addr
        );
        effects.clear_replies();
        connection.mark_closing();
        effects.mark_closing = true;
        true
    }

    fn capture_read_tracking(
        &mut self,
        invocation: CommandInvocation<'_>,
        status: CommandExecutionStatus,
        connection_token: usize,
    ) {
        // Script wrappers declare keys for routing. Their nested commands
        // record the actual reads, as in C call(), so do not track declarations.
        if status.is_rejected()
            || invocation.flags & CMD_READONLY == 0
            || matches!(invocation.name, "eval_ro" | "evalsha_ro" | "fcall_ro")
        {
            return;
        }
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        let should_track = connection.tracking()
            && !connection.tracking_bcast()
            && (!connection.tracking_optin() || connection.tracking_caching())
            && (!connection.tracking_optout() || !connection.tracking_caching());
        let keys = should_track
            .then(|| {
                self.registry
                    .lookup(&invocation.args.name)
                    .and_then(|spec| {
                        crate::runtime::server::worker::command::extract_key_args(
                            spec,
                            invocation.args,
                        )
                    })
            })
            .flatten();
        if let Some(keys) = keys {
            for key in keys {
                self.ctx
                    .tracking_mut()
                    .remember_key(invocation.target.token_value(), &key);
            }
        }
    }

    fn capture_observers_and_metrics(
        &mut self,
        completion: ObserverCompletion<'_>,
        effects: &mut CommandEffects,
    ) {
        if completion.status.is_rejected() {
            return;
        }
        let has_monitor = self.ctx.has_monitor_clients();
        let req_bytes = completion
            .connection_effects
            .map_or(0, ConnectionCommandEffects::request_bytes);
        // Skip the argv/CmdLogEntry allocation entirely when no observer needs
        // it. Fast commands (e.g. PING) never cross slowlog/commandlog
        // thresholds and no MONITOR client means argv is not needed there.
        let needs_argv = has_monitor
            || self.ctx.would_log_command(
                completion.duration_us,
                req_bytes,
                completion.reply_bytes,
                completion.invocation.flags,
            );
        if !needs_argv {
            self.capture_completion_slot_metrics(completion);
            return;
        }
        let argv = redacted_argv(completion.connection_effects, completion.invocation.args);
        // Skip the monitor path entirely when no client is subscribed to
        // MONITOR. This avoids `peer_addr.to_string()` and an `argv` clone on
        // the common no-monitor workload.
        if has_monitor {
            self.capture_completion_monitor(completion, argv.clone(), effects);
        }
        self.capture_completion_command_log(completion, argv, effects);
        self.capture_completion_slot_metrics(completion);
    }

    fn capture_completion_monitor(
        &mut self,
        completion: ObserverCompletion<'_>,
        argv: Vec<Bytes>,
        effects: &mut CommandEffects,
    ) {
        let connection = &self.connection_registry
            [crate::framework::capabilities::io::Token(completion.connection_token)];
        if let Some(mut event) = self.ctx.monitor_event(
            connection.selected_db,
            connection.peer_addr.to_string(),
            argv,
            completion.invocation.flags,
            false,
        ) {
            if let Some(index) = event
                .recipients
                .iter()
                .position(|client_id| *client_id == connection.client_id())
            {
                event.recipients.swap_remove(index);
                effects
                    .replies
                    .push(super::monitor::render_monitor_event(&event));
            }
            if !event.recipients.is_empty() {
                effects.monitor.push(event);
            }
        }
    }

    fn capture_completion_command_log(
        &self,
        completion: ObserverCompletion<'_>,
        argv: Vec<Bytes>,
        effects: &mut CommandEffects,
    ) {
        let connection = &self.connection_registry
            [crate::framework::capabilities::io::Token(completion.connection_token)];
        effects.cmd_logs.push(CmdLogEntry {
            argv,
            duration_us: completion.duration_us,
            peer_addr: connection.peer_addr,
            client_name: connection.name.clone(),
            req_bytes: completion
                .connection_effects
                .map_or(0, ConnectionCommandEffects::request_bytes),
            reply_bytes: completion.reply_bytes,
            command_flags: completion.invocation.flags,
        });
    }

    fn capture_completion_slot_metrics(&mut self, completion: ObserverCompletion<'_>) {
        // Fast path: slot metrics are only recorded when cluster mode and
        // slot stats are both enabled. On the common standalone deployment
        // this skips the `dirty_keys().to_vec()` allocation entirely.
        if !self.ctx.config().server.cluster_enabled
            || !self.ctx.config().server.cluster_slot_stats_enabled
        {
            return;
        }
        let connection_effects = completion.connection_effects;
        let observed_keys =
            connection_effects.map_or_else(Vec::new, |effects| effects.dirty_keys().to_vec());
        let sample =
            crate::runtime::server::worker::cluster::migration::accounting::SlotMetricSample {
                cpu_usec: completion.duration_us,
                network_bytes_in: connection_effects
                    .map_or(0, ConnectionCommandEffects::request_bytes)
                    as u64,
                network_bytes_out: if connection_effects
                    .is_some_and(ConnectionCommandEffects::should_suppress_reply)
                {
                    0
                } else {
                    completion.reply_bytes as u64
                },
            };
        self.record_command_slot_metrics(completion.invocation.args, &observed_keys, sample);
    }
}

fn replica_error_for_log(
    error: &[u8],
    args: &crate::engine::command::CommandArgs,
    hide_user_data: bool,
) -> String {
    crate::runtime::server::worker::replication::render_replication_error_for_log(
        error,
        args,
        hide_user_data,
    )
}

pub(super) fn replica_command_name_for_log(
    name: &str,
    args: &crate::engine::command::CommandArgs,
    hide_user_data: bool,
) -> String {
    if hide_user_data {
        super::command_exec::bounded_command_name(name, None)
    } else {
        client_last_command_name(name, args)
    }
}

pub(crate) fn redacted_argv(
    effects: Option<&ConnectionCommandEffects>,
    args: &crate::engine::command::CommandArgs,
) -> Vec<Bytes> {
    let mut argv = Vec::with_capacity(args.args.len() + 1);
    argv.push(args.name.clone());
    argv.extend(args.args.iter().enumerate().map(|(index, arg)| {
        if effects.is_some_and(|effects| effects.is_argument_redacted(index + 1)) {
            Bytes::from_static(b"(redacted)")
        } else {
            arg.clone()
        }
    }));
    argv
}

#[cfg(test)]
mod replication_log_tests {
    use super::replica_error_for_log;
    use crate::engine::command::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn replica_error_log_truncates_on_utf8_boundary() {
        let suffix = ", with args beginning with: '";
        let error = vec![b'x'; 4095 - suffix.len()];
        let mut argument = "é".as_bytes().to_vec();
        argument.extend_from_slice(&vec![b'y'; 1024 * 1024]);
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"set"), Bytes::from(argument)]);

        let rendered = replica_error_for_log(&error, &args, false);

        assert!(rendered.len() <= 4096);
        assert!(rendered.is_char_boundary(rendered.len()));
    }
}
