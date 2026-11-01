//! Command-local completion stages.
//! Each helper owns one observable concern. `complete_command` is deliberately
//! linear because its order is part of the client, monitoring, and blocking
//! contract.

use super::command_exec::client_last_command_name;
use super::side_effects::CommandEffects;
use super::{CmdLogEntry, ServerWorkerState};
use crate::engine::command::context::CommandConn;
use crate::engine::command::CMD_READONLY;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::BlockedCmd;
use crate::framework::capabilities::connection::ConnectionCommandEffects;
use crate::runtime::server::worker::flows::request::invocation::{
    CommandInvocation, CommandOrigin,
};
use crate::runtime::server::worker::flows::request::outcome::{
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
            (!rejected).then_some(stats_name.as_str()),
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
        self.sync_client_info_after_command(
            connection_token,
            invocation.target.token_value(),
            invocation,
        );
    }

    fn sync_client_info_after_command(
        &mut self,
        connection_token: usize,
        token_value: usize,
        invocation: CommandInvocation<'_>,
    ) {
        let connection =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        let selected_db = connection.selected_db;
        let watched_keys = connection.transaction.watched_key_count();
        let flags = connection_info_flags(connection);
        if let Some(info) = self.ctx.client_info_mut(token_value) {
            info.last_cmd = client_last_command_name(invocation.name, invocation.args);
            info.db = selected_db;
            info.watched_keys = watched_keys;
            info.flags = flags;
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
            self.rollback_incomplete_command_stats(invocation.name, invocation.args, duration_us);
        }
        self.commit_blocked_stream_read_effects(connection_token, None, None, Some(effects));
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

    fn rollback_incomplete_command_stats(
        &mut self,
        name: &str,
        args: &crate::engine::command::CommandArgs,
        duration_us: u64,
    ) {
        self.ctx.decr_total_commands(1);
        let stats_name = super::command_stats_name(name, args);
        let stat = self
            .ctx
            .stats_mut()
            .cmd_stats
            .entry(stats_name)
            .or_default();
        stat.calls = stat.calls.saturating_sub(1);
        stat.usec = stat.usec.saturating_sub(duration_us);
    }

    fn register_blocked_command(&mut self, connection_token: usize) -> bool {
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        connection.cmd_count -= 1;
        let Some(blocked) = &connection.blocked else {
            return false;
        };
        if matches!(
            blocked.cmd,
            BlockedCmd::Wait { .. } | BlockedCmd::WaitAof { .. } | BlockedCmd::SetSlot { .. }
        ) {
            self.ctx
                .blocking_registry_mut()
                .register_keyless_client(connection.session_id());
            // SETSLOT already force-propagated its frame together with the
            // GETACK request, so it must not trigger the empty-command GETACK
            // that WAIT/WAITAOF rely on.
            return matches!(
                blocked.cmd,
                BlockedCmd::Wait { .. } | BlockedCmd::WaitAof { .. }
            );
        }
        if !matches!(
            blocked.cmd,
            BlockedCmd::Paused
                | BlockedCmd::Forkless
                | BlockedCmd::Throttled
                | BlockedCmd::Shutdown
        ) {
            self.ctx.blocking_registry_mut().register_client(
                connection.selected_db,
                &blocked.keys,
                connection.session_id(),
            );
        }
        false
    }

    fn capture_reply(
        &mut self,
        connection_token: usize,
        reply: &Value,
        connection_effects: Option<&mut ConnectionCommandEffects>,
        effects: &mut CommandEffects,
    ) -> usize {
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        debug_assert_eq!(
            connection_effects
                .as_deref()
                .map(ConnectionCommandEffects::output_before),
            Some(connection.output_before_current_command())
        );
        let reply_bytes = if connection_effects
            .as_deref()
            .is_some_and(ConnectionCommandEffects::has_custom_reply)
        {
            connection_effects
                .as_deref()
                .map_or(0, ConnectionCommandEffects::reply_bytes)
        } else {
            let frame = if connection.resp_version >= 3 {
                serialize_to_vec_resp3(reply)
            } else {
                serialize_to_vec_resp2(reply)
            };
            let reply_bytes = frame.len();
            if let Some(connection_effects) = connection_effects {
                connection_effects.set_reply_bytes(reply_bytes);
            }
            effects.push_command_reply(frame);
            reply_bytes
        };
        connection.set_custom_reply_enabled(false);
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
            crate::server_log!(
                warning,
                "== CRITICAL == This primary is sending an error to its replica: {}",
                String::from_utf8_lossy(error)
            );
        }
        let command = client_last_command_name(invocation.name, invocation.args);
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
        if status.is_rejected() || invocation.flags & CMD_READONLY == 0 {
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
                        crate::runtime::server::worker::flows::request::extract_key_args(
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
        let argv = redacted_argv(completion.connection_effects, completion.invocation.args);
        self.capture_completion_monitor(completion, argv.clone(), effects);
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
        let connection_effects = completion.connection_effects;
        let observed_keys =
            connection_effects.map_or_else(Vec::new, |effects| effects.dirty_keys().to_vec());
        let sample =
            crate::runtime::server::worker::distributed::migration::accounting::SlotMetricSample {
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

fn connection_info_flags(
    connection: &crate::framework::capabilities::connection::Connection,
) -> String {
    let mut flags = String::new();
    if let Some(role) = connection.slot_migration_role.as_ref() {
        flags.push(match role {
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                _,
            ) => 'E',
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(
                _,
            ) => 'i',
        });
    }
    if connection.import_source {
        flags.push('I');
    }
    if connection.readonly {
        flags.push('r');
    }
    if connection.monitoring {
        flags.push('O');
    }
    if connection
        .blocked
        .as_ref()
        .is_some_and(|blocked| matches!(blocked.cmd, BlockedCmd::Throttled))
    {
        flags.push('h');
    } else if connection.blocked.is_some() {
        flags.push('b');
    }
    if connection.no_touch {
        flags.push('N');
    }
    if connection.is_priority {
        flags.push('H');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    flags
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
