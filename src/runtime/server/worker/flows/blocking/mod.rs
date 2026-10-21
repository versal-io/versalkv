use super::ServerWorkerState;
use crate::engine::command::CMD_WRITE;
use crate::engine::extension::acl::acl_users;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::foundation::io::sys::time::mstime;
use crate::framework::capabilities::connection::BlockedCmd;
use crate::framework::capabilities::io::Token;
use crate::framework::context::PauseAction;
use bytes::Bytes;
use std::collections::HashSet;

mod wakeup;

fn xreadgroup_group_name(args: &[Bytes]) -> Option<&[u8]> {
    let mut i = 0;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"GROUP") {
            return args.get(i + 1).map(|group| group.as_ref());
        }
        if args[i].eq_ignore_ascii_case(b"STREAMS") {
            return None;
        }
        i += 1;
    }
    None
}

fn wakeup_check_acl_keys(
    username: &str,
    spec: &crate::framework::CommandSpec,
    args: &crate::engine::command::CommandArgs,
    dbid: i64,
) -> bool {
    let users = acl_users().lock().unwrap();
    let key_denied = if let Some(user) = users.get(username) {
        let first = (spec.first_key - 1) as isize;
        let last = if spec.last_key < 0 {
            args.args.len() as isize + spec.last_key as isize
        } else {
            spec.last_key as isize - 1
        };
        let step = spec.key_step.max(1) as isize;
        let mut idx = first;
        let mut denied = false;
        while idx <= last {
            if idx >= 0
                && (idx as usize) < args.args.len()
                && !user.can_access_key_db(
                    &args.args[idx as usize],
                    (spec.flags & CMD_WRITE) != 0,
                    dbid,
                )
            {
                denied = true;
                break;
            }
            idx += step;
        }
        denied
    } else {
        false
    };
    drop(users);
    key_denied
}

pub(crate) struct CompletedBlockedCommand {
    args: crate::engine::command::CommandArgs,
    duration_us: u64,
    request_bytes: u64,
}

struct DetachedBlockedCommand {
    completed: Option<CompletedBlockedCommand>,
}

#[derive(Clone, Copy)]
enum BlockedDetachOutcome {
    Discard,
    TerminalReply,
}

struct BlockedReplyOutput {
    frame: Vec<u8>,
    visible_reply_bytes: usize,
    suppress_reply: bool,
    has_custom_reply: bool,
}

impl ServerWorkerState {
    pub(crate) fn check_blocked_cluster_redirects(&mut self) {
        if !self.ctx.config().server.cluster_enabled {
            return;
        }
        if !self.has_routable_blocked_cluster_client() {
            return;
        }
        let Some(snapshot) = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| state.topology_snapshot())
        else {
            return;
        };
        self.check_blocked_cluster_redirects_with_snapshot(snapshot, false);
    }

    fn has_routable_blocked_cluster_client(&self) -> bool {
        self.connection_registry.iter().any(|(_, conn)| {
            conn.blocked.as_ref().is_some_and(|blocked| {
                !matches!(
                    blocked.cmd,
                    BlockedCmd::Paused
                        | BlockedCmd::Wait { .. }
                        | BlockedCmd::WaitAof { .. }
                        | BlockedCmd::SlotMigration(_)
                        | BlockedCmd::Shutdown
                        | BlockedCmd::Script
                ) && !blocked.keys.is_empty()
            })
        })
    }

    /// C `replicationSetMaster` invokes `disconnectOrRedirectAllBlockedClients`
    /// after a primary becomes a replica. A READONLY client blocked by a
    /// non-writing command remains valid on the new replica, so that narrow
    /// transition path must not use a transient cluster-health snapshot to
    /// reject it. Periodic topology checks continue to use the strict path.
    pub(crate) fn check_blocked_cluster_redirects_after_demotion(
        &mut self,
        state: &crate::engine::distributed::cluster::state::ClusterState,
    ) {
        // This path is entered by the cluster role-state machine itself. The
        // state argument is authoritative even while runtime configuration is
        // being synchronized around a role transition.
        self.check_blocked_cluster_redirects_with_snapshot(state.topology_snapshot(), true);
    }

    fn check_blocked_cluster_redirects_with_snapshot(
        &mut self,
        snapshot: crate::engine::distributed::cluster::ClusterTopologySnapshot,
        preserve_readonly_reads: bool,
    ) {
        if !self.has_routable_blocked_cluster_client() {
            return;
        }
        let preferred_endpoint = match self
            .ctx
            .config()
            .server
            .cluster_preferred_endpoint_type
            .as_str()
        {
            "hostname" => crate::engine::distributed::cluster::PreferredEndpoint::Hostname,
            "unknown-endpoint" => crate::engine::distributed::cluster::PreferredEndpoint::Unknown,
            _ => crate::engine::distributed::cluster::PreferredEndpoint::Ip,
        };
        let redirects: Vec<(
            Token,
            crate::engine::distributed::cluster::ClusterRouteDecision,
        )> = self
            .connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                let blocked = conn.blocked.as_ref()?;
                if matches!(
                    blocked.cmd,
                    BlockedCmd::Paused
                        | BlockedCmd::Wait { .. }
                        | BlockedCmd::WaitAof { .. }
                        | BlockedCmd::SlotMigration(_)
                        | BlockedCmd::Shutdown
                        | BlockedCmd::Script
                ) || blocked.keys.is_empty()
                {
                    return None;
                }
                let key_refs: Vec<&[u8]> = blocked.keys.iter().map(|key| key.as_ref()).collect();
                let flags = self
                    .registry
                    .lookup(&blocked.orig_args.name)
                    .map(|spec| spec.flags)
                    .unwrap_or_default();
                let preserve = preserve_readonly_reads
                    && conn.readonly
                    && (flags & crate::engine::command::CMD_WRITE) == 0;
                if preserve {
                    return None;
                }
                let allow_replica_read =
                    conn.readonly && (flags & crate::engine::command::CMD_READONLY) != 0;
                let decision =
                    snapshot.decide_route(crate::engine::distributed::cluster::RouteRequest {
                        keys: &key_refs,
                        command_flags: flags,
                        existing_keys: None,
                        allow_replica_read,
                        preferred_endpoint,
                        transport: crate::engine::distributed::cluster::ClientTransport::Tcp,
                        prefer_ipv6: conn.peer_addr.is_ipv6(),
                        require_cluster_ok: true,
                        route_sharded_pubsub: false,
                        redirect_keyless: false,
                        client_readonly: conn.readonly,
                        client_asking: false,
                        open_slot_policy:
                            crate::engine::distributed::cluster::OpenSlotPolicy::Standard,
                    });
                (decision
                    != crate::engine::distributed::cluster::ClusterRouteDecision::ExecuteLocal)
                    .then_some((conn.token(), decision))
            })
            .collect();
        for (token, decision) in redirects {
            let reply = crate::runtime::server::worker::flows::request::dispatch::route_error_value(
                decision,
            );
            self.complete_blocked_command_reply(token, reply, true);
        }
    }

    pub(crate) fn discard_blocked_client(&mut self, token: Token) -> bool {
        if self
            .detach_blocked_client(token, BlockedDetachOutcome::Discard)
            .is_none()
        {
            return false;
        }
        if let Some(connection) = self.connection_registry.get_mut(token) {
            connection.discard_command_cycle();
        }
        true
    }

    fn detach_blocked_client(
        &mut self,
        token: Token,
        outcome: BlockedDetachOutcome,
    ) -> Option<DetachedBlockedCommand> {
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
        let conn = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        let blocked = conn.blocked.take()?;
        let session = conn.session_id();
        let db_id = conn.selected_db;
        self.unregister_blocked_client(token, session, db_id, &blocked);
        let completed = matches!(outcome, BlockedDetachOutcome::TerminalReply)
            .then(|| self.record_blocked_completion(&blocked))
            .flatten();
        if matches!(blocked.cmd, BlockedCmd::Shutdown) {
            self.ctx.shutdown_state_mut().blocked_clients =
                self.ctx.shutdown_state().blocked_clients.saturating_sub(1);
        }
        if let Some(ci) = self.ctx.client_info_mut(token.0) {
            ci.flags.retain(|c| c != 'b');
            if ci.flags.is_empty() {
                ci.flags.push('N');
            }
        }
        Some(DetachedBlockedCommand { completed })
    }

    fn unregister_blocked_client(
        &mut self,
        token: Token,
        session: crate::engine::command::SessionId,
        db_id: u32,
        blocked: &crate::engine::command::context::BlockedState,
    ) {
        if matches!(
            blocked.cmd,
            BlockedCmd::Wait { .. } | BlockedCmd::WaitAof { .. }
        ) {
            self.ctx
                .blocking_registry_mut()
                .unregister_keyless_client(&session);
        } else if !matches!(blocked.cmd, BlockedCmd::Paused) {
            self.ctx
                .blocking_registry_mut()
                .unregister_client(db_id, &blocked.keys, &session);
        } else {
            self.ctx.paused_clients_mut().shift_remove(&token);
        }
    }

    fn record_blocked_completion(
        &mut self,
        blocked: &crate::engine::command::context::BlockedState,
    ) -> Option<CompletedBlockedCommand> {
        if matches!(blocked.cmd, BlockedCmd::Paused) {
            return None;
        }
        let name = String::from_utf8_lossy(&blocked.orig_args.name).to_ascii_lowercase();
        let duration_us = (mstime().saturating_sub(blocked.start_time_ms)).max(0) as u64 * 1000;
        let stat = self
            .ctx
            .stats_mut()
            .cmd_stats
            .entry(name.clone())
            .or_default();
        stat.calls += 1;
        stat.usec += duration_us;
        self.ctx.incr_total_commands(1);
        self.ctx.add_active_time_usec(duration_us);
        self.ctx.record_command_latency(&name, duration_us);
        Some(CompletedBlockedCommand {
            args: blocked.orig_args.clone(),
            duration_us,
            request_bytes: blocked.req_bytes as u64,
        })
    }

    pub(crate) fn complete_blocked_command_reply(
        &mut self,
        token: Token,
        reply: Value,
        queue_for_reprocessing: bool,
    ) -> bool {
        let Some(detached) = self.detach_blocked_client(token, BlockedDetachOutcome::TerminalReply)
        else {
            return false;
        };
        let Some(output) = self.finish_blocked_reply_cycle(token, reply) else {
            return false;
        };
        if let Some(completed) = detached.completed.as_ref() {
            self.record_blocked_slot_metrics(completed, output.visible_reply_bytes as u64);
        }
        let queued = if output.suppress_reply {
            true
        } else if output.has_custom_reply {
            self.queue_client_reply(token, Vec::new())
        } else {
            self.queue_client_reply(token, output.frame)
        };
        if queue_for_reprocessing {
            self.queue_client_for_reprocessing(token);
        }
        queued
    }

    fn finish_blocked_reply_cycle(
        &mut self,
        token: Token,
        reply: Value,
    ) -> Option<BlockedReplyOutput> {
        let connection = self.connection_registry.get_mut(token)?;
        connection.resume_command_effects();
        let mut effects = connection
            .take_command_effects(
                crate::framework::capabilities::connection::CommandEffectDrain::Completed,
            )
            .expect("terminal blocked reply drains the retained command cycle once");
        let frame = if connection.resp_version >= 3 {
            serialize_to_vec_resp3(&reply)
        } else {
            serialize_to_vec_resp2(&reply)
        };
        let reply_bytes = if effects.has_custom_reply() {
            effects.reply_bytes()
        } else {
            effects.set_reply_bytes(frame.len());
            frame.len()
        };
        let suppress_reply = effects.should_suppress_reply();
        let has_custom_reply = effects.has_custom_reply();
        let visible_reply_bytes = if suppress_reply { 0 } else { reply_bytes };
        connection.cmd_count = connection.cmd_count.saturating_add(1);
        connection.finish_reply_cycle();
        connection.complete_command();
        Some(BlockedReplyOutput {
            frame,
            visible_reply_bytes,
            suppress_reply,
            has_custom_reply,
        })
    }

    pub(crate) fn record_blocked_slot_metrics(
        &mut self,
        completed: &CompletedBlockedCommand,
        reply_bytes: u64,
    ) {
        self.record_command_slot_metrics(
            &completed.args,
            &[],
            crate::runtime::server::worker::distributed::migration::accounting::SlotMetricSample {
                cpu_usec: completed.duration_us,
                network_bytes_in: completed.request_bytes,
                network_bytes_out: reply_bytes,
            },
        );
    }

    pub(crate) fn wakeup_blocked_on_keys(
        &mut self,
        dirty_keys: &[(u32, Bytes)],
        mut pending_replication: Option<&mut Vec<Vec<u8>>>,
        mut pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) {
        if self.processing_blocked_clients {
            return;
        }
        self.processing_blocked_clients = true;

        let mut ready: Vec<(u32, Bytes)> = dirty_keys.to_vec();
        let mut ready_seen: HashSet<(u32, Bytes)> = dirty_keys.iter().cloned().collect();

        while let Some((db_id, key)) = ready.pop() {
            ready_seen.remove(&(db_id, key.clone()));
            self.process_ready_key(
                db_id,
                &key,
                &mut ready,
                &mut ready_seen,
                pending_replication.as_deref_mut(),
                pending_aof.as_deref_mut(),
            );
        }

        self.processing_blocked_clients = false;
    }

    fn process_ready_key(
        &mut self,
        db_id: u32,
        key: &[u8],
        ready: &mut Vec<(u32, Bytes)>,
        ready_seen: &mut HashSet<(u32, Bytes)>,
        mut pending_replication: Option<&mut Vec<Vec<u8>>>,
        mut pending_aof: Option<&mut Vec<Vec<u8>>>,
    ) {
        let waiters = self.ctx.blocking_registry().waiters_for_key(db_id, key);
        for session in waiters {
            let Some(token) = self.connection_registry.token_for_session(session) else {
                continue;
            };
            if !self.blocked_key_is_eligible_for_wake(token, db_id, key) {
                continue;
            }
            let new_dirty = self.wakeup_blocked_client(
                token,
                pending_replication.as_deref_mut(),
                pending_aof.as_deref_mut(),
            );
            for dk in new_dirty {
                if ready_seen.insert(dk.clone()) {
                    ready.push(dk);
                }
            }
        }
    }

    fn blocked_key_is_eligible_for_wake(&self, token: Token, db_id: u32, key: &[u8]) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)
        else {
            return false;
        };
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return false;
        }
        let Some(blocked) = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .as_ref() else {
            return false;
        };
        if matches!(blocked.cmd, BlockedCmd::Xreadgroup) {
            return true;
        }
        let Some(obj) = self.ctx.db(db_id).and_then(|db| db.get_no_expire(key)) else {
            return false;
        };
        matches!(
            (&blocked.cmd, obj),
            (
                BlockedCmd::Blpop
                    | BlockedCmd::Brpop
                    | BlockedCmd::Brpoplpush { .. }
                    | BlockedCmd::Blmove { .. }
                    | BlockedCmd::Blmpop { .. },
                Robj::ListQuicklist(_)
            ) | (
                BlockedCmd::Bzpopmin | BlockedCmd::Bzpopmax | BlockedCmd::Bzmpop { .. },
                Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_)
            ) | (
                BlockedCmd::Xread | BlockedCmd::Xreadgroup,
                Robj::StreamListpack(_) | Robj::StreamRadix(_)
            )
        )
    }
}

impl ServerWorkerState {
    /// Returns the time remaining until the earliest finite blocked command
    /// deadline. This feeds the event-loop poll timeout so blocked commands
    /// remain precise without an unconditional busy-poll cadence.
    pub(crate) fn next_blocked_timeout(&self, now_ms: i64) -> Option<std::time::Duration> {
        self.connection_registry
            .iter()
            .filter_map(|(_, connection)| connection.blocked.as_ref())
            .filter(|blocked| blocked.timeout_ms > 0)
            .map(|blocked| {
                let timeout_ms = blocked.timeout_ms.min(i64::MAX as u64) as i64;
                let deadline_ms = blocked.start_time_ms.saturating_add(timeout_ms);
                if deadline_ms <= now_ms {
                    std::time::Duration::ZERO
                } else {
                    std::time::Duration::from_millis((deadline_ms - now_ms) as u64)
                }
            })
            .min()
    }

    pub(crate) fn check_blocked_timeouts(&mut self) {
        use crate::foundation::io::sys::time::mstime;
        let now = mstime();
        let mut to_timeout: Vec<Token> = Vec::new();
        for (_connection_token, conn) in self.connection_registry.iter() {
            if let Some(blocked) = &conn.blocked {
                if blocked.timeout_ms > 0 {
                    let elapsed = (now - blocked.start_time_ms) as u64;
                    if elapsed >= blocked.timeout_ms {
                        to_timeout.push(conn.token());
                    }
                }
            }
        }
        for token in to_timeout {
            let wait_reply = self.wait_blocked_reply(token);
            let reply = wait_reply.unwrap_or(Value::NullArray);
            self.complete_blocked_command_reply(token, reply, true);
        }
    }

    pub(crate) fn check_wait_replica_wakeups(&mut self) {
        let mut to_wake = Vec::new();
        for (_connection_token, conn) in self.connection_registry.iter() {
            let Some(blocked) = &conn.blocked else {
                continue;
            };
            match blocked.cmd {
                BlockedCmd::Wait {
                    required,
                    target_offset,
                } => {
                    let acked = self
                        .ctx
                        .replication()
                        .count_replicas_acked_to(target_offset);
                    if acked >= required {
                        to_wake.push((conn.token(), self.wait_reply(acked)));
                    }
                }
                BlockedCmd::WaitAof {
                    numlocal,
                    numreplicas,
                    target_offset,
                } => {
                    let local = self.waitaof_local_ack(target_offset);
                    let replicas = self
                        .ctx
                        .replication()
                        .count_replicas_aof_acked_to(target_offset);
                    if local >= numlocal && replicas >= numreplicas {
                        to_wake.push((conn.token(), self.waitaof_reply(local, replicas)));
                    }
                }
                _ => {}
            }
        }
        for (token, reply) in to_wake {
            self.reply_to_unblocked_wait(token, reply);
        }
    }

    fn wait_blocked_reply(&self, token: Token) -> Option<Value> {
        let connection_token = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)?;
        let blocked = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))?
            .blocked
            .as_ref()?;
        match blocked.cmd {
            BlockedCmd::Wait { target_offset, .. } => Some(
                self.wait_reply(
                    self.ctx
                        .replication()
                        .count_replicas_acked_to(target_offset),
                ),
            ),
            BlockedCmd::WaitAof { target_offset, .. } => Some(
                self.waitaof_reply(
                    self.waitaof_local_ack(target_offset),
                    self.ctx
                        .replication()
                        .count_replicas_aof_acked_to(target_offset),
                ),
            ),
            _ => None,
        }
    }

    fn wait_reply(&self, acked: i64) -> Value {
        Value::integer(acked)
    }

    fn waitaof_reply(&self, local: i64, replicas: i64) -> Value {
        Value::Array(vec![Value::integer(local), Value::integer(replicas)])
    }

    fn waitaof_local_ack(
        &self,
        target_offset: crate::engine::distributed::replication::ReplicationOffset,
    ) -> i64 {
        if !self.ctx.config().persistence.aof_enabled
            || self.ctx.aof().journal.is_none()
            || self.ctx.config().persistence.aof_fsync
                == crate::foundation::io::persistence::AofFsync::No
        {
            return 0;
        }
        i64::from(
            self.ctx
                .aof()
                .fsynced_repl_offset
                .is_some_and(|offset| offset >= target_offset.get()),
        )
    }

    pub(crate) fn reject_blocked_waitaof_local_if_aof_disabled(&mut self) {
        if self.ctx.config().persistence.aof_enabled {
            return;
        }
        let tokens: Vec<Token> = self
            .connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                let blocked = conn.blocked.as_ref()?;
                matches!(blocked.cmd, BlockedCmd::WaitAof { numlocal: 1, .. })
                    .then_some(conn.token())
            })
            .collect();
        for token in tokens {
            self.reply_to_unblocked_error(
                token,
                "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
            );
        }
    }

    fn reply_to_unblocked_error(&mut self, token: Token, error: &'static str) {
        self.ctx
            .incr_error_reply(error.split_whitespace().next().unwrap_or("ERR"));
        self.complete_blocked_command_reply(token, Value::error(error), true);
    }

    fn reply_to_unblocked_wait(&mut self, token: Token, reply: Value) {
        self.complete_blocked_command_reply(token, reply, true);
    }

    pub(crate) fn check_xreadgroup_nogroup_wakeups(&mut self) {
        let mut to_wake = Vec::new();
        for (_connection_token, conn) in self.connection_registry.iter() {
            let Some(blocked) = &conn.blocked else {
                continue;
            };
            if !matches!(blocked.cmd, BlockedCmd::Xreadgroup) {
                continue;
            }
            let Some(group_name) = xreadgroup_group_name(&blocked.orig_args.args) else {
                continue;
            };
            let should_wake = blocked.keys.iter().any(|key| {
                match self
                    .ctx
                    .db(conn.selected_db)
                    .and_then(|db| db.get_no_expire(key))
                {
                    Some(Robj::StreamListpack(stream)) => stream.group(group_name).is_none(),
                    Some(_) | None => true,
                }
            });
            if should_wake {
                to_wake.push(conn.token());
            }
        }
        for token in to_wake {
            self.wakeup_blocked_client(token, None, None);
        }
    }

    pub(crate) fn check_pause_state(&mut self) {
        self.drain_pending_unpause_if_any(false);
    }

    /// Drains paused_clients synchronously when CLIENT UNPAUSE (or any other
    /// pause-state lowering) requires postponement wrappers to be lifted
    /// *before* the next command in the same event-loop iteration is
    /// processed. Alignment with C Valkey: unpauseActions() →
    /// updatePausedActions() immediately calls unblockPostponedClients() when
    /// the client pause action mask becomes less restrictive, so the next
    /// command (CLIENT UNBLOCK in the pause.tcl test) sees the real
    /// BLOCKED_LIST/ZSET/STREAM btype rather than the outer postponement.
    pub(crate) fn drain_pending_unpause_if_any(&mut self, sync_from_unpause: bool) {
        let now = mstime();
        let mut do_wake = false;

        if self.ctx.pending_unpause() {
            self.ctx.set_pending_unpause(false);
            do_wake = true;
        }

        if self.ctx.client_pause().end_time_ms > 0 && now >= self.ctx.client_pause().end_time_ms {
            self.ctx.client_pause_mut().action = PauseAction::None;
            self.ctx.client_pause_mut().end_time_ms = 0;
            self.ctx.client_pause_mut().reason = "none";
            do_wake = true;
        }

        // Even if the caller arrived via CLIENT UNPAUSE (sync), also honour
        // the time-based auto-expiry path above: a stale end_time that got
        // overtaken by the wall clock should also trigger the wake now.
        if !sync_from_unpause && self.ctx.client_pause().is_paused(now) != PauseAction::None {
            // Expiry hasn't happened yet and this is the cron caller; do
            // not wake just because pending_unpause happened.
        }

        if do_wake && !self.ctx.paused_clients_mut().is_empty() {
            let tokens: Vec<Token> = self.ctx.paused_clients_mut().drain(..).collect();
            let mut all_dirty: Vec<(u32, Bytes)> = Vec::new();
            for token in tokens {
                let dirty = self.wakeup_blocked_client(token, None, None);
                all_dirty.extend(dirty);
            }
            if !all_dirty.is_empty() {
                self.wakeup_blocked_on_keys(&all_dirty, None, None);
            }
        }
    }

    pub(crate) fn check_script_kill_state(&mut self) {
        if !self.ctx.pending_script_kill() {
            return;
        }
        self.ctx.set_pending_script_kill(false);
        let tokens: Vec<Token> = self
            .ctx
            .script_mut()
            .timedout_script_clients
            .drain(..)
            .map(Token)
            .collect();
        let mut drive_tokens = Vec::new();
        for token in tokens {
            if !self
                .connection_registry
                .get(token)
                .and_then(|connection| connection.blocked.as_ref())
                .is_some_and(|blocked| matches!(blocked.cmd, BlockedCmd::Script))
            {
                continue;
            }
            self.ctx.incr_error_reply("ERR");
            if self.complete_blocked_command_reply(
                token,
                Value::error("ERR Script killed by user"),
                true,
            ) {
                drive_tokens.push(token);
            }
        }
        for token in drive_tokens {
            self.handle_writable(token);
        }
    }

    /// Kill subscribers whose ACL channel permissions were revoked via ACL SETUSER.
    /// Called from handle_readable after command dispatch.
    /// Defers removal to the end of the event loop iteration so buffered replies
    /// (e.g. OK from ACL SETUSER resetchannels) are written before the socket closes.
    pub(crate) fn kill_acl_subscribers(&mut self) {
        let usernames: Vec<String> = std::mem::take(self.ctx.pending_acl_client_kills_mut());
        if usernames.is_empty() {
            return;
        }
        let users = acl_users().lock().unwrap();
        for (_, conn) in self.connection_registry.iter() {
            if !usernames.contains(&conn.username) {
                continue;
            }
            let user = match users.get(&conn.username) {
                Some(u) => u,
                None => {
                    self.ctx.pending_acl_tokens_to_kill_mut().push(conn.token());
                    continue;
                }
            };
            let channels = self.ctx.pubsub_mut().client_channels(conn.token().0);
            let patterns = self.ctx.pubsub_mut().client_patterns_list(conn.token().0);
            let shard_channels = self.ctx.pubsub_mut().client_shard_channels(conn.token().0);
            if channels.is_empty() && patterns.is_empty() && shard_channels.is_empty() {
                continue;
            }
            if user.has_denied_subscriptions(&channels, &patterns, &shard_channels) {
                self.ctx.pending_acl_tokens_to_kill_mut().push(conn.token());
            }
        }
        drop(users);
    }

    /// Drain deferred ACL connection kills. Call at the end of the event loop
    /// iteration, after all IO events (including writable) are processed.
    /// Protected clients are skipped.
    pub(crate) fn drain_acl_kills(&mut self) {
        let tokens: Vec<Token> = std::mem::take(self.ctx.pending_acl_tokens_to_kill_mut());
        for token in tokens {
            // Skip protected clients
            let connection_token = match self.connection_registry.contains(token).then_some(token) {
                Some(k) => k,
                None => continue,
            };
            if self.connection_registry.contains(connection_token)
                && self.connection_registry[connection_token].protected
            {
                continue;
            }
            self.remove_connection(token.0);
        }
    }

    /// Reject blocked clients queued by replicaof/slaveof role change:
    /// unblock each and count the command as rejected (reply with -LOADING style error),
    /// then close the connection.
    pub(crate) fn drain_blocked_role_rejections(&mut self) {
        let sessions = std::mem::take(self.ctx.pending_blocked_to_reject_mut());
        for session in sessions {
            let Some(connection_token) = self.connection_registry.token_for_session(session) else {
                continue;
            };
            let token = connection_token;
            if !self.connection_registry.contains(connection_token) {
                continue;
            }
            let has_blocked = self.connection_registry[connection_token].blocked.is_some();
            if !has_blocked {
                continue;
            }
            let (preserve_readonly_read, redirect_reply) = {
                let conn = &self.connection_registry[connection_token];
                let blocked = conn.blocked.as_ref().expect("blocked state checked above");
                // C leaves BLOCKED_POSTPONE requests for the normal unpause
                // path, which restarts command processing from the beginning.
                if matches!(blocked.cmd, BlockedCmd::Paused) {
                    continue;
                }
                let command_flags = self
                    .registry
                    .lookup(&blocked.orig_args.name)
                    .map(|spec| spec.flags)
                    .unwrap_or_default();
                let preserve =
                    conn.readonly && (command_flags & crate::engine::command::CMD_WRITE) == 0;
                let supports_redirect = conn
                    .capa_flags
                    .iter()
                    .any(|capability| capability.eq_ignore_ascii_case("redirect"));
                let target = self
                    .ctx
                    .config()
                    .replication
                    .master_host
                    .as_ref()
                    .map(|host| {
                        format!(
                            "REDIRECT {host}:{}",
                            self.ctx.config().replication.master_port
                        )
                    });
                let redirect = (supports_redirect && !blocked.keys.is_empty())
                    .then_some(target)
                    .flatten();
                (preserve, redirect)
            };
            if preserve_readonly_read {
                continue;
            }
            let name_lower = {
                let conn = &self.connection_registry[connection_token];
                let blocked = match conn.blocked.as_ref() {
                    Some(b) => b.clone(),
                    None => continue,
                };
                String::from_utf8_lossy(&blocked.orig_args.name).to_ascii_lowercase()
            };
            let should_close = redirect_reply.is_none();
            let (reply, err_name) = match redirect_reply {
                Some(redirect) => (Value::error(redirect), "REDIRECT"),
                None => (
                    Value::error(
                        "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)",
                    ),
                    "UNBLOCKED",
                ),
            };
            self.ctx.incr_error_reply(err_name);
            self.ctx
                .stats_mut()
                .cmd_stats
                .entry(name_lower)
                .or_default()
                .rejected_calls += 1;
            self.complete_blocked_command_reply(token, reply, true);
            // Redirect-capable clients stay connected and can reconnect to the
            // advertised primary. Legacy clients retain the close-after-error
            // behavior used for blocked operations that cannot remain connected.
            if should_close
                && self.connection_registry.contains(connection_token)
                && !self.connection_registry[connection_token].protected
            {
                self.connection_registry[connection_token].mark_closing();
            }
        }
    }

    /// Drain pending protected client requests.
    /// Called after command dispatch to apply protection flags to connections.
    pub(crate) fn drain_protected_clients(&mut self) {
        let ids: Vec<u64> = std::mem::take(self.ctx.protected_clients_mut());
        for client_id in ids {
            if let Some(token_value) = self
                .connection_registry
                .token_for_client_id(client_id)
                .map(|token| token.0)
            {
                let Some(connection_token) = self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token_value))
                    .then_some(token_value)
                else {
                    continue;
                };
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .protected = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod terminal_reply_tests {
    use super::*;
    use crate::engine::command::context::{BlockedState, CommandConn};
    use crate::framework::capabilities::connection::Connection;

    #[test]
    fn timed_out_blocked_reply_honors_client_reply_off() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let token = Token(81);
        let key = Bytes::from_static(b"timeout-key");
        let args = crate::engine::command::CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            key.clone(),
            Bytes::from_static(b"0.001"),
        ]);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.set_custom_reply_enabled(false);
        connection.set_skip_propagation(false);
        connection.begin_nested_command_effects();
        connection.set_request_bytes_for_test(32);
        connection.blocked = Some(BlockedState {
            cmd: BlockedCmd::Blpop,
            keys: vec![key.clone()],
            timeout_ms: 1,
            start_time_ms: mstime().saturating_sub(10),
            orig_args: args,
            req_bytes: 32,
            orig_wrapped: None,
        });
        connection.set_reply_off(true);
        backend.connection_registry.insert(connection);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &[key], session);

        backend.check_blocked_timeouts();

        assert!(backend.connection_registry[token].blocked.is_none());
        assert!(backend.connection_registry[token].output.queue.is_empty());
        assert_eq!(
            backend.connection_registry[token].request_bytes_current_command(),
            0
        );
        assert_eq!(backend.connection_registry[token].cmd_count, 1);
    }
}

#[cfg(test)]
mod role_rejection_tests {
    use super::*;
    use crate::engine::command::context::BlockedState;
    use crate::framework::capabilities::connection::{Connection, ConnectionState};
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn role_change_redirects_capable_key_blocked_client_without_closing_it() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;
        let token = Token(71);
        let key = Bytes::from_static(b"key");
        let args = crate::engine::command::CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            key.clone(),
            Bytes::from_static(b"0"),
        ]);
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(token);
        conn.capa_flags.push("redirect".to_string());
        conn.set_custom_reply_enabled(false);
        conn.begin_nested_command_effects();
        conn.enter_blocking(BlockedCmd::Blpop, vec![key.clone()], 0, &args, 32);
        conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
        let connection_token = backend.connection_registry.insert(conn);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &[key], session);
        backend.ctx.pending_blocked_to_reject_mut().push(session);

        backend.drain_blocked_role_rejections();

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_none());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
            .front(),
            Some(&b"-REDIRECT 192.0.2.1:6380\r\n".to_vec())
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .state,
            ConnectionState::Connected
        );
        assert!(backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
        ));
        backend.connection_registry[Token(connection_token)].begin_nested_command_effects();
        backend.connection_registry[Token(connection_token)].discard_command_cycle();
    }

    #[test]
    fn script_kill_retires_cycle_before_buffered_input_is_reprocessed() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind script client peer");
        let peer_addr = listener.local_addr().expect("script client peer address");
        let connected = backend
            .transport
            .connect_tcp(peer_addr)
            .expect("connect script client transport");
        let (mut peer, _) = listener.accept().expect("accept script client transport");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set script client peer read timeout");
        backend.add_connection(RuntimeConnectedPeer {
            token: connected.token,
            peer_addr,
        });
        let token = connected.token;
        let args = crate::engine::command::CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"while true do end"),
            Bytes::from_static(b"0"),
        ]);
        let buffered_ping = b"*1\r\n$4\r\nPING\r\n";
        let conn = &mut backend.connection_registry[token];
        conn.set_custom_reply_enabled(false);
        conn.begin_nested_command_effects();
        conn.enter_blocking(BlockedCmd::Script, Vec::new(), 0, &args, 32);
        conn.parser.feed(buffered_ping);
        backend
            .ctx
            .script_mut()
            .timedout_script_clients
            .push(token.0);
        backend.ctx.set_pending_script_kill(true);

        backend.check_script_kill_state();

        assert!(backend.connection_registry.contains(token));
        assert!(backend.connection_registry[token].blocked.is_none());
        assert!(backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
        ));
        assert_eq!(
            backend.connection_registry[token].parser.pending_len(),
            buffered_ping.len()
        );

        let expected_kill_reply = b"-ERR Script killed by user\r\n";
        let mut kill_reply = vec![0_u8; expected_kill_reply.len()];
        peer.read_exact(&mut kill_reply)
            .expect("script kill reply must reach the connected peer");
        assert_eq!(kill_reply, expected_kill_reply);

        backend.drive_one_pending_client_read();

        assert!(!backend.connection_registry[token].parser.has_pending_data());
        assert!(!backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
        ));
        backend.event_batch.begin_next_turn();
        backend.writes.ordinary.begin_turn();
        backend.drive_pending_ordinary_writes();
        let mut ping_reply = [0_u8; 7];
        peer.read_exact(&mut ping_reply)
            .expect("buffered PING reply must follow the script kill reply");
        assert_eq!(&ping_reply, b"+PONG\r\n");
    }

    #[test]
    fn role_change_preserves_readonly_blocked_read() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let token = Token(72);
        let key = Bytes::from_static(b"stream");
        let args = crate::engine::command::CommandArgs::from_parts(vec![
            Bytes::from_static(b"XREAD"),
            Bytes::from_static(b"BLOCK"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"STREAMS"),
            key.clone(),
            Bytes::from_static(b"0-0"),
        ]);
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(token);
        conn.readonly = true;
        conn.capa_flags.push("redirect".to_string());
        conn.blocked = Some(BlockedState {
            cmd: BlockedCmd::Xread,
            keys: vec![key.clone()],
            timeout_ms: 0,
            start_time_ms: mstime(),
            orig_args: args,
            req_bytes: 32,
            orig_wrapped: None,
        });
        let connection_token = backend.connection_registry.insert(conn);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &[key], session);
        backend.ctx.pending_blocked_to_reject_mut().push(session);

        backend.drain_blocked_role_rejections();

        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_some());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue
        .is_empty());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .state,
            ConnectionState::Connected
        );
    }
}
