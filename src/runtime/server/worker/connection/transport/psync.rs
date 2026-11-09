use super::*;
use crate::engine::distributed::replication::PsyncRequestOffset;
use crate::framework::capabilities::io::Token;

#[derive(Clone, Copy)]
enum SyncCommand {
    Psync,
    Sync,
}

impl SyncCommand {
    fn from_name(name: &str) -> Self {
        match name {
            "psync" => Self::Psync,
            "sync" => Self::Sync,
            _ => unreachable!("transport dispatch only routes PSYNC and SYNC here"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Psync => "psync",
            Self::Sync => "sync",
        }
    }

    fn is_psync(self) -> bool {
        matches!(self, Self::Psync)
    }
}

#[derive(Clone, Copy)]
struct PsyncRequest<'a> {
    command: SyncCommand,
    connection_id: ConnectionId,
    connection_token: usize,
    requested_replid: &'a str,
    requested_offset: Option<PsyncRequestOffset>,
    requested_offset_raw: i64,
    has_invalid_offset: bool,
    is_failover: bool,
}

impl<'a> PsyncRequest<'a> {
    fn parse(
        args: &'a CommandArgs,
        name: &str,
        connection_token: usize,
        connection_id: ConnectionId,
    ) -> Self {
        let command = SyncCommand::from_name(name);
        let requested_replid = args
            .args
            .first()
            .and_then(|arg| std::str::from_utf8(arg).ok())
            .unwrap_or("?");
        let offset = if command.is_psync() {
            args.args.get(1).and_then(|arg| {
                crate::foundation::io::store::structure::string::encoding::int_encodable(arg)
            })
        } else {
            Some(-1)
        };
        let requested_offset_raw = offset.unwrap_or(-1);
        let requested_offset = command
            .is_psync()
            .then(|| PsyncRequestOffset::try_from_raw(requested_offset_raw).ok())
            .flatten();
        Self {
            command,
            connection_id,
            connection_token,
            requested_replid,
            requested_offset,
            requested_offset_raw,
            has_invalid_offset: offset.is_none(),
            is_failover: args
                .args
                .get(2)
                .is_some_and(|arg| arg.eq_ignore_ascii_case(b"FAILOVER")),
        }
    }

    fn is_partial(self) -> bool {
        self.command.is_psync() && self.requested_replid != "?"
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DualResumeState {
    None,
    Associated,
    AwaitingMainPsync,
}

struct SyncPlanSelection {
    plan: PrimarySyncPlan,
    dual_resume_complete: bool,
}

#[derive(Clone, Copy)]
struct FullSyncOptions {
    diskless: bool,
    delay_seconds: i64,
    max_replicas: usize,
    key_delay_us: i64,
    supports_eof: bool,
    filter_mode: RdbFilterMode,
    now_ms: i64,
}

impl FullSyncOptions {
    fn uses_eof_transport(self) -> bool {
        self.supports_eof && (self.diskless || self.filter_mode != RdbFilterMode::All)
    }

    fn should_defer(self) -> bool {
        self.diskless && self.supports_eof && self.delay_seconds > 0
    }

    fn uses_disk_batch(self) -> bool {
        !self.diskless && self.filter_mode == RdbFilterMode::All
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FullSyncProgress {
    Started,
    WaitingForChild,
}

impl ServerWorkerState {
    fn validate_psync_failover(&self, request: PsyncRequest<'_>) -> Result<bool, &'static [u8]> {
        if !request.is_failover {
            return Ok(false);
        }
        if !self.ctx.replication().is_replica() {
            return Err(b"-ERR PSYNC FAILOVER can't be sent to a master.\r\n");
        }
        if !request
            .requested_replid
            .eq_ignore_ascii_case(self.ctx.replication().backlog.replid())
        {
            return Err(b"-ERR PSYNC FAILOVER replid must match my replid.\r\n");
        }
        Ok(true)
    }

    fn promote_for_psync_failover(
        &mut self,
        requested_replid: &str,
        failover_connection: ConnectionId,
    ) {
        crate::server_log!(
            notice,
            "Failover request received for replid {}.",
            requested_replid
        );
        // C uses clusterPromoteSelfToPrimary() in Cluster mode, but that helper
        // delegates this transition to replicationUnsetPrimary(); Cluster
        // topology promotion is owned by the separate election state machine.
        let downstream: Vec<_> = self
            .replication_runtime
            .primary_links()
            .values()
            .filter_map(|link| {
                (link.connection_id() != failover_connection).then_some(link.connection_id().0)
            })
            .collect();
        self.abort_replica_session_for_role_change();
        self.replication_runtime.clear_replica_resume();
        self.ctx.config_mut().replication.master_host = None;
        self.ctx.config_mut().replication.master_port = 0;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().switch_to_primary();
        self.ctx.replication_mut().repl_seldb = -1;
        for token in downstream {
            self.remove_connection(token);
        }
        crate::server_log!(notice, "PRIMARY MODE enabled (failover request)");
    }

    fn parse_psync_request<'a>(
        &mut self,
        connection_token: usize,
        args: &'a CommandArgs,
        name: &str,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<PsyncRequest<'a>> {
        let connection_id = ConnectionId(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token()
                .0,
        );
        if let Some(info) = self.ctx.client_info_mut(connection_id.0) {
            info.last_cmd = name.to_string();
        }
        if matches!(
            self.replication_runtime.role(connection_id),
            Some(ReplicationConnection::RdbChannel { .. })
        ) {
            self.reject_psync(
                name,
                b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n",
                replies,
            );
            return None;
        }
        Some(PsyncRequest::parse(
            args,
            name,
            connection_token,
            connection_id,
        ))
    }

    fn reject_invalid_psync_offset(
        &mut self,
        request: PsyncRequest<'_>,
        replies: &mut Vec<Vec<u8>>,
    ) {
        let connection_token = request.connection_token;
        let connection_id = request.connection_id;
        let name = request.command.name();
        let peer = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .peer_addr;
        let abort_dual_resume = name == "psync"
            && self
                .replication_runtime
                .primary_for_connection(connection_id)
                .is_some_and(|link| link.expects_dual_channel_main_psync());
        crate::server_log!(
            warning,
            "Replica {}:<unknown-replica-port> asks for synchronization but with a wrong offset",
            peer.ip()
        );
        if abort_dual_resume {
            self.abort_dual_resume(connection_id, connection_token);
        }
        self.reject_psync(
            name,
            b"-ERR value is not an integer or out of range\r\n",
            replies,
        );
        if abort_dual_resume {
            self.refresh_replication_view();
        }
    }

    pub(super) fn is_rdb_channel(&self, connection_id: ConnectionId) -> bool {
        matches!(
            self.replication_runtime.role(connection_id),
            Some(
                ReplicationConnection::RdbChannelCandidate
                    | ReplicationConnection::RdbChannel { .. }
            )
        )
    }

    fn apply_psync_failover(
        &mut self,
        request: PsyncRequest<'_>,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        match self.validate_psync_failover(request) {
            Ok(true) => {
                self.promote_for_psync_failover(request.requested_replid, request.connection_id)
            }
            Ok(false) => {}
            Err(reply) => {
                self.reject_psync(request.command.name(), reply, replies);
                return false;
            }
        }
        true
    }

    fn prepare_psync_transport(
        &mut self,
        request: PsyncRequest<'_>,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if self.ctx.replication().failover_state != "no-failover" {
            self.reject_psync(
                request.command.name(),
                b"-NOMASTERLINK Can't SYNC while failing over\r\n",
                replies,
            );
            return false;
        }
        if self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_link_status != "up"
        {
            self.reject_psync(
                request.command.name(),
                b"-NOMASTERLINK Can't SYNC while not connected with my master\r\n",
                replies,
            );
            return false;
        }
        if self.connection_registry[Token(request.connection_token)].has_pending_output()
            || self
                .io_threads
                .is_write_inflight(Token(request.connection_token))
            || !replies.is_empty()
        {
            self.reject_psync(
                request.command.name(),
                b"-ERR SYNC and PSYNC are invalid with pending output\r\n",
                replies,
            );
            return false;
        }
        if self.ctx.config().replication.repl_disable_tcp_nodelay {
            if let Err(error) = self
                .transport
                .set_tcp_nodelay(Token(request.connection_id.0), false)
            {
                crate::server_log!(
                    warning,
                    "failed to disable TCP_NODELAY for replica {}: {}",
                    request.connection_id.0,
                    error
                );
            }
        }
        true
    }

    fn validate_sync_eof_transport(
        &mut self,
        request: PsyncRequest<'_>,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let (filter_mode, supports_eof) = self
            .replication_runtime
            .replica_sync_preferences(request.connection_id);
        if (filter_mode != RdbFilterMode::All || self.is_rdb_channel(request.connection_id))
            && !supports_eof
        {
            self.reject_psync(
                request.command.name(),
                b"-ERR Filtered replica requires EOF capability\r\n",
                replies,
            );
            return false;
        }
        true
    }

    fn validate_full_sync_transport(
        &mut self,
        request: PsyncRequest<'_>,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if self
            .replication_snapshot_options(request.connection_id)
            .is_none()
        {
            self.reject_psync(
                request.command.name(),
                b"-ERR Replica without EOF capability does not support the on-disk RDB compression algorithm\r\n",
                replies,
            );
            return false;
        }
        true
    }

    fn reject_psync(&mut self, name: &str, reply: &[u8], replies: &mut Vec<Vec<u8>>) {
        replies.push(reply.to_vec());
        self.ctx.end_cmd(name, false);
    }

    fn dual_resume_state(&self, request: PsyncRequest<'_>) -> DualResumeState {
        if !request.command.is_psync() {
            return DualResumeState::None;
        }
        if self
            .replication_runtime
            .primary_for_connection(request.connection_id)
            .is_some_and(|link| link.expects_dual_channel_main_psync())
        {
            return DualResumeState::AwaitingMainPsync;
        }
        if self
            .replication_runtime
            .rdb_channel_for_main(request.connection_id)
            .is_some()
        {
            return DualResumeState::Associated;
        }
        DualResumeState::None
    }

    fn try_negotiate_initial_dual_channel(
        &mut self,
        request: PsyncRequest<'_>,
        resume_state: DualResumeState,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let can_continue = request.requested_offset.is_some_and(|requested_offset| {
            self.ctx
                .replication()
                .backlog
                .resolve_continue(request.requested_replid, requested_offset)
                .is_some()
        });
        if !request.command.is_psync() || resume_state != DualResumeState::None || can_continue {
            return false;
        }
        self.log_partial_resync_miss(request);
        if !self.dual_channel_is_eligible(request.connection_id) {
            return false;
        }
        if !self.validate_full_sync_transport(request, replies) {
            return true;
        }
        let Some(primary_id) = self.replication_runtime.primary_id(request.connection_id) else {
            return false;
        };
        self.log_dual_channel_transition("before", request.connection_id);
        let transitioned = self
            .replication_runtime
            .primary_link_mut(primary_id)
            .is_some_and(|link| link.transition_to_waiting_for_rdb_channel().is_ok());
        if !transitioned {
            return false;
        }
        self.log_dual_channel_transition("after", request.connection_id);
        replies.push(
            crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY.to_vec(),
        );
        if request.is_partial() {
            self.ctx.incr_sync_partial_err();
        }
        self.ctx.end_cmd(request.command.name(), false);
        self.refresh_replication_view();
        true
    }

    fn log_partial_resync_miss(&self, request: PsyncRequest<'_>) {
        if request.requested_replid == "?" {
            return;
        }
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection(request.connection_id)
        {
            crate::server_log!(
                notice,
                "Unable to partial resync with replica {} for lack of backlog (Replica request was {}:{}).",
                link.replica().display_name(),
                request.requested_replid,
                request.requested_offset_raw
            );
        }
    }

    fn dual_channel_is_eligible(&self, connection_id: ConnectionId) -> bool {
        let Some(link) = self
            .replication_runtime
            .primary_for_connection(connection_id)
        else {
            return false;
        };
        let config = &self.ctx.config().replication;
        let failed_link = link.dual_channel_already_failed;
        let replica = link.replica();
        let failed_runtime = self
            .replication_runtime
            .replica_dual_channel_failed(&replica.ip, replica.port);
        let protocol_eligible =
            crate::engine::distributed::replication::protocol::should_use_dual_channel_primary(
                config.dual_channel_replication,
                config.repl_diskless_sync,
                true,
                &replica.capability,
            );
        let accepted = !failed_link && !failed_runtime && protocol_eligible;
        crate::server_log!(
            debug,
            "dual-rdb primary eligibility: main={} enabled={} diskless={} failed_link={} failed_runtime={} caps={:?} state={:?} dual_state={:?} accepted={}",
            connection_id.0,
            config.dual_channel_replication,
            config.repl_diskless_sync,
            failed_link,
            failed_runtime,
            replica.capability,
            link.state(),
            link.dual_state(),
            accepted
        );
        accepted
    }

    fn log_dual_channel_transition(&self, phase: &str, connection_id: ConnectionId) {
        let primary_links: Vec<_> = self
            .replication_runtime
            .primary_links()
            .values()
            .map(|link| (link.connection_id().0, link.state(), link.dual_state()))
            .collect();
        crate::server_log!(
            debug,
            "dual-rdb transition {}: main={} role={:?} primary_links={:?}",
            phase,
            connection_id.0,
            self.replication_runtime.role(connection_id),
            primary_links
        );
    }

    fn record_full_resync_request(&mut self, was_partial_request: bool) {
        self.ctx.incr_sync_full();
        if was_partial_request {
            self.ctx.incr_sync_partial_err();
        }
    }

    pub(super) fn handle_psync(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let Some(request) = self.parse_psync_request(connection_token, args, name_lower, replies)
        else {
            return false;
        };
        if !self.apply_psync_failover(request, replies)
            || !self.prepare_psync_transport(request, replies)
            || !self.validate_sync_eof_transport(request, replies)
        {
            return false;
        }
        self.prioritize_connection(crate::framework::capabilities::io::Token(connection_token));
        if request.has_invalid_offset {
            self.reject_invalid_psync_offset(request, replies);
            return false;
        }
        if matches!(
            self.replication_runtime.role(request.connection_id),
            Some(ReplicationConnection::RdbChannelCandidate)
        ) {
            if !self.validate_full_sync_transport(request, replies) {
                return false;
            }
            if request.command.is_psync()
                || self
                    .full_sync
                    .pending_dual_rdb_snapshots
                    .contains_key(&Token(request.connection_id.0))
            {
                self.reject_psync(
                    request.command.name(),
                    b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n",
                    replies,
                );
                return false;
            }
            self.schedule_dual_rdb_snapshot(
                request.connection_id,
                self.configured_diskless_delay_ms(),
            );
            self.record_full_resync_request(false);
            self.ctx.end_cmd(request.command.name(), false);
            self.refresh_replication_view();
            return false;
        }
        let can_continue = request.command.is_psync()
            && request.requested_offset.is_some_and(|requested_offset| {
                self.ctx
                    .replication()
                    .backlog
                    .resolve_continue(request.requested_replid, requested_offset)
                    .is_some()
            });
        // Plan selection mutates the link; reject unsupported full sync first.
        if !can_continue && !self.validate_full_sync_transport(request, replies) {
            return false;
        }
        if self
            .replication_runtime
            .primary_for_connection(request.connection_id)
            .is_none()
        {
            self.ensure_primary_link(request.connection_token, 0);
        }
        let dual_resume_state = self.dual_resume_state(request);
        if self.try_negotiate_initial_dual_channel(request, dual_resume_state, replies) {
            return false;
        }
        let Some(selection) = self.select_sync_plan(request, dual_resume_state, replies) else {
            return false;
        };
        // C appends the replica to `server.replicas` exactly when PSYNC is
        // accepted: partial CONTINUE (including the dual-channel main PSYNC)
        // at replication.c:961, full-resync start at replication.c:1230, in
        // both cases before the backlog is activated below. The initial
        // dual-channel negotiation (+DUALCHANNELSYNC) returned earlier and is
        // not admitted yet. Stamping here makes INFO replication follow
        // acceptance order rather than REPLCONF registration order.
        self.replication_runtime
            .stamp_primary_acceptance(request.connection_id);
        self.activate_psync_history(&selection.plan);
        self.complete_psync_waits(request, &selection);
        if self.apply_sync_plan(request, selection, replies) {
            return false;
        }
        self.finish_psync(request, replies);
        false
    }

    fn select_sync_plan(
        &mut self,
        request: PsyncRequest<'_>,
        resume_state: DualResumeState,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<SyncPlanSelection> {
        let compression_mode = &self.ctx.config().replication.repl_compression;
        self.replication_runtime
            .primary_for_connection_mut(request.connection_id)
            .expect("primary link registered before PSYNC")
            .freeze_compression(compression_mode);
        let awaiting_main = resume_state == DualResumeState::AwaitingMainPsync;
        let dual_resume_complete = if awaiting_main {
            let Some(requested_offset) = request.requested_offset else {
                self.reject_dual_resume(request, replies);
                return None;
            };
            let backlog = &self.ctx.replication().backlog;
            self.replication_runtime
                .primary_for_connection_mut(request.connection_id)
                .expect("primary link registered before PSYNC")
                .complete_dual_channel_main_psync(
                    request.requested_replid,
                    requested_offset,
                    backlog,
                )
        } else {
            false
        };
        if awaiting_main && !dual_resume_complete {
            self.reject_dual_resume(request, replies);
            return None;
        }
        let plan = if dual_resume_complete {
            PrimarySyncPlan::Continue { new_replid: None }
        } else if let Some(requested_offset) = request.requested_offset {
            let backlog = &self.ctx.replication().backlog;
            self.replication_runtime
                .primary_for_connection_mut(request.connection_id)
                .expect("primary link registered before PSYNC")
                .handle_psync(request.requested_replid, requested_offset, backlog)
        } else {
            // C accepts signed PSYNC integers, then lets
            // `primaryTryPartialResynchronization()` reject negative positions
            // against the backlog range and fall through to full resync
            // (thirdparty/valkey/src/replication.c:885-922). Preserve that for
            // ordinary PSYNC, including the protocol sentinel `PSYNC ? -1`;
            // FAILOVER requests were rejected before any role mutation above.
            let backlog = &self.ctx.replication().backlog;
            self.replication_runtime
                .primary_for_connection_mut(request.connection_id)
                .expect("primary link registered before PSYNC")
                .plan_full_resync(backlog)
        };
        Some(SyncPlanSelection {
            plan,
            dual_resume_complete,
        })
    }

    fn reject_dual_resume(&mut self, request: PsyncRequest<'_>, replies: &mut Vec<Vec<u8>>) {
        self.abort_dual_resume(request.connection_id, request.connection_token);
        self.reject_psync(
            request.command.name(),
            b"-ERR dual-channel PSYNC does not match ENDOFF snapshot\r\n",
            replies,
        );
        self.refresh_replication_view();
    }

    fn abort_dual_resume(&mut self, connection_id: ConnectionId, connection_token: usize) {
        let paired_connections = self.replication_runtime.remove_channel_pair(connection_id);
        for paired_connection in paired_connections {
            self.remove_connection(paired_connection.0);
        }
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state = crate::framework::capabilities::connection::ConnectionState::CloseAfterReply;
    }

    fn activate_psync_history(&mut self, plan: &PrimarySyncPlan) {
        // C creates a new replication history when the first replica enters a
        // full sync. Partial resyncs and later full syncs reuse that history.
        let started_new_history = if matches!(plan, PrimarySyncPlan::FullResync { .. }) {
            self.ctx.replication_mut().backlog.activate_new_history()
        } else {
            self.ctx.replication_mut().backlog.activate();
            false
        };
        if started_new_history {
            let backlog = &self.ctx.replication().backlog;
            crate::server_log!(
                notice,
                "Replication backlog created, my new replication IDs are '{}' and '{}'",
                backlog.replid(),
                backlog.second_replid()
            );
        }
    }

    fn complete_psync_waits(&mut self, request: PsyncRequest<'_>, selection: &SyncPlanSelection) {
        if selection.dual_resume_complete {
            if self
                .replication_runtime
                .has_existing_tail_pin_for_main(request.connection_id)
            {
                self.replication_runtime
                    .set_dual_main_pin_from_existing_tail(request.connection_id);
            }
            crate::server_log!(
                debug,
                "dual-rdb main PSYNC complete: main={} clearing wait pin",
                request.connection_id.0
            );
            self.replication_runtime
                .complete_psync_wait_for_main(request.connection_id);
        }
    }

    fn apply_sync_plan(
        &mut self,
        request: PsyncRequest<'_>,
        selection: SyncPlanSelection,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        match selection.plan {
            PrimarySyncPlan::Continue { new_replid } => {
                self.push_continue_reply(new_replid, replies);
                if request.is_partial() || selection.dual_resume_complete {
                    self.ctx.incr_sync_partial_ok();
                }
                false
            }
            PrimarySyncPlan::FullResync { boundary } => {
                let progress = self.start_full_resync(request, boundary, replies);
                self.record_full_resync_request(request.is_partial());
                if progress == FullSyncProgress::WaitingForChild {
                    self.ctx.end_cmd(request.command.name(), false);
                    self.refresh_replication_view();
                    return true;
                }
                false
            }
        }
    }

    fn push_continue_reply(&self, new_replid: Option<String>, replies: &mut Vec<Vec<u8>>) {
        replies.push(match new_replid {
            Some(replid) => format!("+CONTINUE {}\r\n", replid).into_bytes(),
            None => b"+CONTINUE\r\n".to_vec(),
        });
    }

    fn finish_psync(&mut self, request: PsyncRequest<'_>, replies: &mut Vec<Vec<u8>>) {
        // An inline length-framed full resync transitions the link to the
        // command stream during this same PSYNC dispatch. C queues the RDB
        // before replicaStartCommandStream() seeds an empty history, so drain
        // the handshake/RDB replies first; propagation then appends the
        // bootstrap PING behind them on every streaming link.
        let command_stream_started = self
            .replication_runtime
            .primary_for_connection_mut(request.connection_id)
            .expect("primary link exists while completing PSYNC")
            .take_command_stream_start_pending();
        if command_stream_started {
            {
                let connection = &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(request.connection_token)];
                for reply in replies.drain(..) {
                    connection.queue_reply(reply);
                }
            }
            self.start_replica_command_stream();
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(request.connection_token)]
            .token();
            let _ = self.transport.set_interest(
                token,
                crate::foundation::io::transport::Interest::ReadWritable,
            );
        }
        let pump_result = {
            let backlog = &self.ctx.replication().backlog;
            self.replication_runtime
                .primary_for_connection_mut(request.connection_id)
                .expect("primary link exists while pumping backlog")
                .pump(backlog)
        };
        let tail = match pump_result {
            Ok(tail) => tail,
            Err(_) => {
                self.remove_connection(request.connection_id.0);
                self.ctx.end_cmd(request.command.name(), false);
                self.refresh_replication_view();
                return;
            }
        };
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(request.connection_id)
        {
            link.mark_first_dual_main_stream_sent();
            link.mark_replication_queued(tail.len());
        }
        if !tail.is_empty() {
            if let Some(link) = self
                .replication_runtime
                .primary_for_connection_mut(request.connection_id)
            {
                link.mark_send_activity(mstime());
            }
            {
                let connection = &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(request.connection_token)];
                for reply in replies.drain(..) {
                    connection.queue_reply(reply);
                }
                connection.queue_replication_reply(tail);
            }
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(request.connection_token)]
            .token();
            let _ = self.transport.set_interest(
                token,
                crate::foundation::io::transport::Interest::ReadWritable,
            );
        }
        self.connection_registry
            [crate::framework::capabilities::io::Token(request.connection_token)]
        .selected_db = 0;
        self.ctx.reset_repl_seldb();
        self.ctx.end_cmd(request.command.name(), false);
        self.refresh_replication_view();
    }

    fn start_full_resync(
        &mut self,
        request: PsyncRequest<'_>,
        boundary: crate::engine::distributed::replication::SnapshotBoundary,
        replies: &mut Vec<Vec<u8>>,
    ) -> FullSyncProgress {
        let options = self.full_sync_options(request.connection_id);
        if options.should_defer() {
            self.defer_full_resync(request, options);
            return FullSyncProgress::Started;
        }

        let function_codes = self.full_sync_function_codes(options.filter_mode);
        let batch_boundary = self.try_join_disk_sync_batch(request, options, &function_codes);
        let fullsync_boundary = batch_boundary.unwrap_or(boundary);
        self.push_full_resync_header(
            request.command,
            fullsync_boundary.reported_offset().get(),
            replies,
        );
        if batch_boundary.is_some()
            || self.try_start_diskless_child(
                request.connection_id,
                options,
                &function_codes,
                replies,
            )
        {
            return FullSyncProgress::Started;
        }
        if self.defer_for_active_child(request, options, replies) {
            return FullSyncProgress::WaitingForChild;
        }
        self.start_inline_full_resync(request.connection_id, options, &function_codes, replies);
        FullSyncProgress::Started
    }

    fn full_sync_options(&self, connection_id: ConnectionId) -> FullSyncOptions {
        let config = &self.ctx.config().replication;
        let (filter_mode, supports_eof) = self
            .replication_runtime
            .replica_sync_preferences(connection_id);
        FullSyncOptions {
            diskless: config.repl_diskless_sync || self.replication_snapshot_options(connection_id)
                .is_some_and(|options| (options.compression == crate::foundation::io::persistence::rdb::compression::RdbCompression::Lz4)
                    != (self.ctx.config().persistence.rdbcompression == crate::foundation::io::persistence::rdb::compression::RdbCompression::Lz4)),
            delay_seconds: config.repl_diskless_sync_delay,
            max_replicas: config.repl_diskless_sync_max_replicas as usize,
            key_delay_us: self.ctx.config().persistence.rdb_key_save_delay,
            supports_eof,
            filter_mode,
            now_ms: mstime(),
        }
    }

    fn defer_full_resync(&mut self, request: PsyncRequest<'_>, options: FullSyncOptions) {
        // C defers every EOF-capable diskless full sync during a configured
        // delay window so replicas arriving together can share one RDB child.
        self.replication_runtime
            .primary_for_connection_mut(request.connection_id)
            .expect("primary link registered before PSYNC deferral")
            .defer_diskless_sync(
                options.delay_seconds * 1000,
                options.now_ms,
                request.command.is_psync(),
            );
        if options.max_replicas > 0 {
            let ids = self
                .replication_runtime
                .collect_deferred_links_for_filter(options.filter_mode);
            if ids.len() >= options.max_replicas {
                self.fire_deferred_full_resyncs(ids);
            }
        }
    }

    fn full_sync_function_codes(&self, filter_mode: RdbFilterMode) -> Vec<Vec<u8>> {
        if filter_mode == RdbFilterMode::Empty {
            return Vec::new();
        }
        self.ctx
            .script()
            .function_registry
            .list_libraries()
            .into_iter()
            .map(|library| library.code.as_bytes().to_vec())
            .collect()
    }

    fn try_join_disk_sync_batch(
        &mut self,
        request: PsyncRequest<'_>,
        options: FullSyncOptions,
        function_codes: &[Vec<u8>],
    ) -> Option<crate::engine::distributed::replication::SnapshotBoundary> {
        if !options.uses_disk_batch() {
            if self.full_sync.disk_sync_batch.is_some() {
                crate::server_log!(notice, "Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
            }
            return None;
        }
        let primary_id = self.replication_runtime.primary_id(request.connection_id)?;
        self.join_disk_sync_batch(primary_id)
            .or_else(|| self.begin_disk_sync_batch(primary_id, function_codes))
    }

    fn push_full_resync_header(
        &self,
        command: SyncCommand,
        snapshot_offset: i64,
        replies: &mut Vec<Vec<u8>>,
    ) {
        if command.is_psync() {
            let replid = self.ctx.replication().backlog.replid();
            replies.push(format!("+FULLRESYNC {} {}\r\n", replid, snapshot_offset).into_bytes());
        }
    }

    fn try_start_diskless_child(
        &mut self,
        connection_id: ConnectionId,
        options: FullSyncOptions,
        function_codes: &[Vec<u8>],
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if !options.diskless || !options.supports_eof || options.filter_mode != RdbFilterMode::All {
            return false;
        }
        let Some(primary_id) = self.replication_runtime.primary_id(connection_id) else {
            return false;
        };
        let Some(snapshot) = self.replication_snapshot_options(connection_id) else {
            return false;
        };
        self.replication_runtime
            .freeze_full_sync_snapshot_options(connection_id, snapshot);
        let Some((read_end, marker, release)) = self.try_begin_diskless_fork_with_options(
            function_codes,
            snapshot,
            "normal sync",
            true,
        ) else {
            return false;
        };
        let mut header = b"$EOF:".to_vec();
        header.extend_from_slice(&marker);
        header.extend_from_slice(b"\r\n");
        replies.push(header);
        self.store_diskless_relay(read_end, release, marker, vec![primary_id], true, 0);
        true
    }

    fn defer_for_active_child(
        &mut self,
        request: PsyncRequest<'_>,
        options: FullSyncOptions,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if !self.ctx.persistence().has_active_child() && self.full_sync.diskless_relay.is_none() {
            return false;
        }
        crate::server_log!(
            notice,
            "Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC"
        );
        if request.command.is_psync() {
            replies.pop();
        }
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(request.connection_id)
        {
            link.defer_diskless_sync(0, options.now_ms, request.command.is_psync());
        }
        true
    }

    fn start_inline_full_resync(
        &mut self,
        connection_id: ConnectionId,
        options: FullSyncOptions,
        function_codes: &[Vec<u8>],
        replies: &mut Vec<Vec<u8>>,
    ) {
        let snapshot = self
            .replication_snapshot_options(connection_id)
            .expect("full-sync compression capability was validated");
        self.replication_runtime
            .freeze_full_sync_snapshot_options(connection_id, snapshot);
        let (bulk, total_keys) =
            self.build_inline_full_resync_with_snapshot(options, function_codes, snapshot);
        let link = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
            .expect("primary link exists while starting full resync");
        if options.uses_eof_transport() {
            link.begin_full_resync(bulk, total_keys.max(1), options.now_ms);
        } else {
            link.begin_length_framed_full_resync(bulk, total_keys.max(1), options.now_ms);
        }
        let (first_chunk, _) = link.pump_full_resync(options.now_ms);
        if !first_chunk.is_empty() {
            replies.push(first_chunk);
        }
    }

    fn build_inline_full_resync_with_snapshot(
        &self,
        options: FullSyncOptions,
        function_codes: &[Vec<u8>],
        snapshot: crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions,
    ) -> (Vec<u8>, usize) {
        let replication = self.ctx.rdb_full_sync_replication_info();
        let slot_imports = self.ctx.slot_import_dbs();
        let rdb =
            crate::foundation::io::persistence::rdb::serialize::serialize_replication_snapshot(
                self.ctx.dbs(),
                options.key_delay_us,
                function_codes,
                options.filter_mode == RdbFilterMode::All,
                Some(&replication),
                &slot_imports,
                snapshot,
            );
        let total_keys = (0..self.ctx.num_dbs() as u32)
            .map(|db| self.ctx.db_size(db))
            .sum::<usize>();
        let bulk = if options.uses_eof_transport() {
            let marker = crate::engine::distributed::replication::protocol::generate_replid();
            let mut bulk = format!("$EOF:{}\r\n", marker).into_bytes();
            bulk.extend_from_slice(&rdb);
            bulk.extend_from_slice(marker.as_bytes());
            bulk
        } else {
            let mut bulk = format!("${}\r\n", rdb.len()).into_bytes();
            bulk.extend_from_slice(&rdb);
            bulk
        };
        (bulk, total_keys)
    }

    pub(super) fn ensure_rdb_candidate(&mut self, connection_token: usize) -> bool {
        let connection_id = ConnectionId(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token()
                .0,
        );
        if matches!(
            self.replication_runtime.role(connection_id),
            Some(ReplicationConnection::Primary(_))
        ) && self
            .replication_runtime
            .convert_primary_to_rdb_candidate(connection_id)
            .is_err()
        {
            return false;
        }
        if matches!(
            self.replication_runtime.role(connection_id),
            Some(
                ReplicationConnection::RdbChannelCandidate
                    | ReplicationConnection::RdbChannel { .. }
            )
        ) {
            return true;
        }
        if self
            .replication_runtime
            .register_rdb_candidate(connection_id)
            .is_err()
        {
            return false;
        }
        let (current_ip, current_port) = self
            .replication_runtime
            .channel_endpoint(connection_id)
            .map(|(ip, port)| (ip.to_string(), port))
            .unwrap_or_default();
        let ip = if current_ip.is_empty() {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .peer_addr
                .ip()
                .to_string()
        } else {
            current_ip
        };
        self.replication_runtime
            .set_channel_endpoint(connection_id, ip, current_port);
        true
    }

    pub(super) fn ensure_primary_link(&mut self, connection_token: usize, port: u16) {
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        let connection_id = ConnectionId(conn.token().0);
        if matches!(
            self.replication_runtime.role(connection_id),
            Some(
                ReplicationConnection::RdbChannelCandidate
                    | ReplicationConnection::RdbChannel { .. }
            )
        ) {
            let ip = self
                .replication_runtime
                .channel_endpoint(connection_id)
                .map(|(ip, _)| ip.to_string())
                .filter(|ip| !ip.is_empty())
                .unwrap_or_else(|| conn.peer_addr.ip().to_string());
            self.replication_runtime
                .set_channel_endpoint(connection_id, ip, port);
            return;
        }
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.set_replica_port(port);
        } else {
            self.replication_runtime.register_primary(
                connection_id,
                ReplicaConnectionInfo {
                    ip: conn.peer_addr.ip().to_string(),
                    port,
                    capability: Vec::new(),
                    rdb_filter_mode: RdbFilterMode::All,
                },
            );
        }
        self.refresh_replication_view();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer;
    use bytes::Bytes;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn accepted_full_resync_activates_backlog_for_the_next_disconnected_write() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"?"), Bytes::from_static(b"-1")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &args, "psync", &mut replies));
        assert!(
            backend.ctx.replication().backlog.is_active(),
            "a completed full-resync peer must leave its primary ready to retain the first write after disconnect"
        );
    }

    #[test]
    fn sync_full_resync_uses_disk_sync_batch_for_single_target() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_diskless_sync = false;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"SYNC"),
            args: Vec::new(),
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &args, "sync", &mut replies));

        assert!(replies.is_empty());
        let replica_id = backend
            .replication_runtime
            .primary_id(ConnectionId(connection_token))
            .expect("SYNC target must be registered as a primary link");
        assert!(backend.full_sync.disk_sync_batch.is_some());
        assert!(backend
            .full_sync
            .disk_sync_batch
            .as_ref()
            .is_some_and(|batch| batch.contains_target(replica_id)));
        backend.cancel_disk_sync_batch();
    }

    #[test]
    fn invalid_psync_offsets_return_integer_error_without_registering_replica() {
        for offset in ["offset_str", "9223372036854775808"] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
            let listener = backend.transport.bind_tcp(peer_addr).unwrap();
            let token = listener.token;
            backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
            let connection_token = token.0;
            let args = CommandArgs {
                name: Bytes::from_static(b"PSYNC"),
                args: vec![
                    Bytes::from_static(b"replicationid"),
                    Bytes::from(offset.to_string()),
                ],
            };
            let mut replies = Vec::new();

            assert!(!backend.handle_psync(connection_token, &args, "psync", &mut replies));

            assert_eq!(
                replies,
                vec![b"-ERR value is not an integer or out of range\r\n".to_vec()],
                "offset {offset} must be rejected"
            );
            assert_eq!(
                backend.replication_runtime.role(ConnectionId(token.0)),
                None
            );
            assert_eq!(backend.ctx.stats().sync_full, 0);
        }
    }

    #[test]
    fn negative_psync_offset_falls_back_to_full_resync() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from_static(b"replicationid"),
                Bytes::from_static(b"-2"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(token.0, &args, "psync", &mut replies));

        assert!(replies
            .first()
            .is_some_and(|reply| reply.starts_with(b"+FULLRESYNC ")));
        assert!(backend
            .replication_runtime
            .primary_for_connection(ConnectionId(token.0))
            .is_some());
        assert_eq!(backend.ctx.stats().sync_full, 1);
        assert_eq!(backend.ctx.stats().sync_partial_err, 1);
    }

    #[test]
    fn negative_failover_psync_promotes_before_full_resync() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;
        backend.ctx.config_mut().replication.master_link_status = "up".to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(replid.clone()),
                Bytes::from_static(b"-2"),
                Bytes::from_static(b"FAILOVER"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(token.0, &args, "psync", &mut replies));

        assert_eq!(
            backend.ctx.replication().role,
            crate::engine::distributed::replication::ReplicationRole::Primary
        );
        assert_eq!(
            backend.ctx.config().replication.master_host.as_deref(),
            None
        );
        assert_eq!(backend.ctx.config().replication.master_port, 0);
        assert_eq!(backend.ctx.config().replication.master_link_status, "down");
        // A fresh full-sync backlog starts a new lineage, unlike CONTINUE.
        assert_ne!(backend.ctx.replication().backlog.replid(), replid);
        assert!(backend.ctx.replication().backlog.second_replid().is_empty());
        assert!(replies
            .first()
            .is_some_and(|reply| reply.starts_with(b"+FULLRESYNC ")));
        assert!(backend.is_priority_connection(token));
        assert!(backend
            .replication_runtime
            .primary_for_connection(ConnectionId(token.0))
            .is_some());
        assert_eq!(backend.ctx.stats().sync_full, 1);
        assert_eq!(backend.ctx.stats().sync_partial_err, 1);
    }

    #[test]
    fn overflowing_failover_psync_promotes_before_offset_error() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;
        backend.ctx.config_mut().replication.master_link_status = "up".to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(replid.clone()),
                Bytes::from_static(b"9223372036854775808"),
                Bytes::from_static(b"FAILOVER"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(token.0, &args, "psync", &mut replies));

        assert_eq!(
            backend.ctx.replication().role,
            crate::engine::distributed::replication::ReplicationRole::Primary
        );
        assert_eq!(
            backend.ctx.config().replication.master_host.as_deref(),
            None
        );
        assert_eq!(backend.ctx.config().replication.master_port, 0);
        assert_eq!(backend.ctx.config().replication.master_link_status, "down");
        assert_eq!(backend.ctx.replication().backlog.second_replid(), replid);
        assert!(backend.is_priority_connection(token));
        assert_eq!(
            replies,
            vec![b"-ERR value is not an integer or out of range\r\n".to_vec()]
        );
        assert_eq!(
            backend.replication_runtime.role(ConnectionId(token.0)),
            None
        );
        assert_eq!(backend.ctx.stats().sync_full, 0);
        assert_eq!(backend.ctx.stats().sync_partial_err, 0);
    }

    #[test]
    fn restored_full_resync_projects_reported_offset_and_installs_stream_cursor() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .replication_mut()
            .backlog
            .restore_primary_from_rdb(
                "restored",
                crate::engine::distributed::replication::ReplicationOffset::try_from(41).unwrap(),
            )
            .unwrap();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"missing"), Bytes::from_static(b"42")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(token.0, &args, "psync", &mut replies));

        assert_eq!(
            replies.first(),
            Some(&format!("+FULLRESYNC {replid} 41\r\n").into_bytes())
        );
        let link = backend
            .replication_runtime
            .primary_for_connection(ConnectionId(token.0))
            .unwrap();
        assert_eq!(link.next_send_cursor().get(), 42);
        assert_eq!(link.sent_cursor().get(), 42);
    }

    #[test]
    fn matching_psync_failover_promotes_replica_and_continues_from_second_replid() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().backlog.activate();
        let old_replid = backend.ctx.replication().backlog.replid().to_string();
        let offset = backend.ctx.replication().backlog.reported_offset().get();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;
        backend.ctx.config_mut().replication.master_link_status = "up".to_string();

        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        // The ordinary replication handshake registers the inbound link while
        // processing REPLCONF, before the PSYNC FAILOVER command arrives.
        backend.ensure_primary_link(connection_token, 5113);
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(old_replid.clone()),
                Bytes::from((offset + 1).to_string()),
                Bytes::from_static(b"FAILOVER"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &args, "psync", &mut replies));

        let new_replid = backend.ctx.replication().backlog.replid().to_string();
        assert_eq!(
            backend.ctx.replication().role,
            crate::engine::distributed::replication::ReplicationRole::Primary
        );
        assert!(backend.ctx.config().replication.master_host.is_none());
        assert_eq!(
            backend.ctx.replication().backlog.second_replid(),
            old_replid
        );
        assert_ne!(new_replid, old_replid);
        assert_eq!(
            replies,
            vec![format!("+CONTINUE {new_replid}\r\n").into_bytes()]
        );
    }

    #[test]
    fn psync_failover_rejects_a_primary_target() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(replid),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"FAILOVER"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &args, "psync", &mut replies));

        assert_eq!(
            replies,
            vec![b"-ERR PSYNC FAILOVER can't be sent to a master.\r\n".to_vec()]
        );
        assert_eq!(
            backend.replication_runtime.role(ConnectionId(token.0)),
            None
        );
    }

    #[test]
    fn psync_failover_rejects_a_mismatched_replid() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        backend.ctx.config_mut().replication.master_port = 6380;
        backend.ctx.config_mut().replication.master_link_status = "up".to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from_static(b"0000000000000000000000000000000000000000"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"FAILOVER"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &args, "psync", &mut replies));

        assert_eq!(
            replies,
            vec![b"-ERR PSYNC FAILOVER replid must match my replid.\r\n".to_vec()]
        );
        assert!(backend.ctx.replication().is_replica());
        assert_eq!(
            backend.replication_runtime.role(ConnectionId(token.0)),
            None
        );
    }

    fn boundary_connection(backend: &mut ServerWorkerState) -> (usize, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let peer = backend.transport.connect_tcp(peer_addr).unwrap();
        let (target, _) = listener.accept().unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        });
        (peer.token.0, target)
    }

    #[test]
    fn boundary_full_sync_options_separate_eof_from_configured_delay() {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for filter_mode in [
            RdbFilterMode::All,
            RdbFilterMode::Functions,
            RdbFilterMode::Empty,
        ] {
            for (diskless, supports_eof, delay_seconds) in [
                (true, true, 30),
                (true, true, 0),
                (true, false, 30),
                (false, false, 30),
                (false, true, 0),
                (false, true, 30),
            ] {
                let options = FullSyncOptions {
                    diskless,
                    supports_eof,
                    delay_seconds,
                    filter_mode,
                    max_replicas: 2,
                    key_delay_us: 0,
                    now_ms: 0,
                };
                actual.push((
                    filter_mode,
                    diskless,
                    supports_eof,
                    delay_seconds,
                    options.uses_eof_transport(),
                    options.should_defer(),
                ));
                expected.push((
                    filter_mode,
                    diskless,
                    supports_eof,
                    delay_seconds,
                    supports_eof && (diskless || filter_mode != RdbFilterMode::All),
                    diskless && supports_eof && delay_seconds > 0,
                ));
            }
        }
        assert_eq!(
            actual, expected,
            "EOF framing must not enable a disabled diskless delay"
        );
    }

    #[test]
    fn boundary_filtered_full_sync_without_diskless_delay_decodes_real_body() {
        use crate::foundation::io::persistence::rdb::load_rdb_bytes_checked_with_functions;
        use crate::foundation::io::store::{kv_engine::DbSet, Robj};
        use crate::framework::distributed::replication::PrimaryLinkState;

        let code = "#!lua name=boundarylib\nserver.register_function('boundary_fn', function() return 7 end)";
        let mut observations = Vec::new();
        for (filter_mode, filter) in [
            (RdbFilterMode::Functions, b"functions".as_slice()),
            (RdbFilterMode::Empty, b"".as_slice()),
        ] {
            for delay_seconds in [0, 30] {
                let mut backend = ServerWorkerState::new(2).unwrap();
                backend.ctx.config_mut().replication.repl_diskless_sync = false;
                backend
                    .ctx
                    .config_mut()
                    .replication
                    .repl_diskless_sync_delay = delay_seconds;
                backend
                    .ctx
                    .db_mut(0)
                    .unwrap()
                    .set(b"first".to_vec(), Robj::string(b"one".as_slice()));
                backend
                    .ctx
                    .db_mut(1)
                    .unwrap()
                    .set(b"second".to_vec(), Robj::string(b"two".as_slice()));
                backend
                    .ctx
                    .script_mut()
                    .function_registry
                    .load_library("boundarylib", code)
                    .unwrap();
                let (token, _target) = boundary_connection(&mut backend);
                let args = CommandArgs::from_parts(vec![
                    Bytes::from_static(b"REPLCONF"),
                    Bytes::from_static(b"capa"),
                    Bytes::from_static(b"eof"),
                    Bytes::from_static(b"rdb-filter-only"),
                    Bytes::copy_from_slice(filter),
                ]);
                let mut replies = Vec::new();
                backend.handle_replconf(token, token, &args, false, &mut replies);
                assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
                replies.clear();
                let psync = CommandArgs::from_parts(vec![
                    Bytes::from_static(b"PSYNC"),
                    Bytes::from_static(b"?"),
                    Bytes::from_static(b"-1"),
                ]);
                backend.handle_psync(token, &psync, "psync", &mut replies);
                let deferred = backend.replication_runtime.count_deferred_diskless_links();
                let state = backend
                    .replication_runtime
                    .primary_for_connection(ConnectionId(token))
                    .unwrap()
                    .state();
                let mut decoded = None;
                if let [fullresync, bulk] = replies.as_slice() {
                    assert!(fullresync.starts_with(b"+FULLRESYNC "));
                    let header_end = bulk.windows(2).position(|bytes| bytes == b"\r\n").unwrap();
                    let marker = bulk[..header_end]
                        .strip_prefix(b"$EOF:")
                        .expect("filtered transport must remain EOF framed");
                    assert_eq!(marker.len(), 40);
                    assert!(bulk.ends_with(marker));
                    let body = &bulk[header_end + 2..bulk.len() - marker.len()];
                    let mut dbs = DbSet::new(2);
                    let (loaded, consumed, expired, functions) =
                        load_rdb_bytes_checked_with_functions(body, &mut dbs, true).unwrap();
                    assert_eq!((loaded, consumed, expired), (0, body.len(), 0));
                    assert_eq!(dbs.get(0).unwrap().dbsize(), 0);
                    assert_eq!(dbs.get(1).unwrap().dbsize(), 0);
                    let expected_functions = if filter_mode == RdbFilterMode::Functions {
                        vec![code.as_bytes().to_vec()]
                    } else {
                        Vec::new()
                    };
                    assert_eq!(functions, expected_functions);
                    decoded = Some(());
                }
                observations.push((
                    filter_mode,
                    delay_seconds,
                    deferred,
                    decoded,
                    state,
                    backend.ctx.stats().sync_full,
                ));
            }
        }
        for (filter, delay, deferred, decoded, state, sync_full) in observations {
            assert_eq!(
                (deferred, decoded, state, sync_full),
                (0, Some(()), PrimaryLinkState::WaitingForReplicaAck, 1),
                "filtered/no must start and decode without deferral: filter={filter:?}, delay={delay}"
            );
        }
    }

    #[test]
    fn boundary_dual_sync_counters_follow_candidate_and_main_phases() {
        let mut phases = Vec::new();
        for requested_replid in ["?", "missing"] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            backend
                .ctx
                .config_mut()
                .replication
                .dual_channel_replication = true;
            backend.ctx.config_mut().replication.repl_diskless_sync = true;
            backend
                .ctx
                .config_mut()
                .replication
                .repl_diskless_sync_delay = 30;
            backend
                .ctx
                .config_mut()
                .replication
                .repl_diskless_sync_max_replicas = 0;
            let (main, _main_target) = boundary_connection(&mut backend);
            let (rdb, _rdb_target) = boundary_connection(&mut backend);
            let mut replies = Vec::new();
            let main_capa = CommandArgs::from_parts(vec![
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"dual-channel"),
            ]);
            backend.handle_replconf(main, main, &main_capa, false, &mut replies);
            assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
            replies.clear();
            let psync = CommandArgs::from_parts(vec![
                Bytes::from_static(b"PSYNC"),
                Bytes::copy_from_slice(requested_replid.as_bytes()),
                Bytes::from_static(b"-1"),
            ]);
            backend.handle_psync(main, &psync, "psync", &mut replies);
            assert_eq!(
                replies,
                vec![
                    crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY
                        .to_vec()
                ]
            );
            phases.push((
                requested_replid,
                "initial",
                backend.ctx.stats().sync_full,
                backend.ctx.stats().sync_partial_ok,
                backend.ctx.stats().sync_partial_err,
            ));
            replies.clear();
            let rdb_capa = CommandArgs::from_parts(vec![
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
                Bytes::from_static(b"rdb-only"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"rdb-channel"),
                Bytes::from_static(b"1"),
            ]);
            backend.handle_replconf(rdb, rdb, &rdb_capa, false, &mut replies);
            assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
            replies.clear();
            let sync = CommandArgs::from_parts(vec![Bytes::from_static(b"SYNC")]);
            backend.handle_psync(rdb, &sync, "sync", &mut replies);
            assert!(replies.is_empty());
            phases.push((
                requested_replid,
                "candidate",
                backend.ctx.stats().sync_full,
                backend.ctx.stats().sync_partial_ok,
                backend.ctx.stats().sync_partial_err,
            ));
            backend.handle_psync(rdb, &sync, "sync", &mut replies);
            assert_eq!(
                replies,
                vec![b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n".to_vec()]
            );
            phases.push((
                requested_replid,
                "duplicate",
                backend.ctx.stats().sync_full,
                backend.ctx.stats().sync_partial_ok,
                backend.ctx.stats().sync_partial_err,
            ));
            replies.clear();

            let frame = backend
                .replication_runtime
                .take_endoff_for_rdb_candidate_if_unambiguous(
                    ConnectionId(rdb),
                    true,
                    &mut backend.ctx.replication_mut().backlog,
                )
                .unwrap();
            let endoff =
                crate::engine::distributed::replication::protocol::parse_end_offset(&frame)
                    .unwrap();
            let public_id = backend.connection_registry[Token(rdb)].client_id();
            let set_id = CommandArgs::from_parts(vec![
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(public_id.to_string()),
            ]);
            backend.handle_replconf(main, main, &set_id, false, &mut replies);
            assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
            replies.clear();
            let resume = CommandArgs::from_parts(vec![
                Bytes::from_static(b"PSYNC"),
                Bytes::from(endoff.replid.clone()),
                Bytes::from((endoff.offset.get() + 1).to_string()),
            ]);
            backend.handle_psync(main, &resume, "psync", &mut replies);
            assert_eq!(replies.len(), 1);
            let line = replies[0]
                .strip_suffix(b"\r\n")
                .expect("CONTINUE must be a complete reply line");
            let continued = crate::engine::distributed::replication::protocol::parse_continue(line);
            assert!(
                matches!(&continued, Some(None))
                    || matches!(&continued, Some(Some(replid)) if replid == &endoff.replid),
                "CONTINUE must omit the ID or name the matching snapshot ID: {continued:?}"
            );
            phases.push((
                requested_replid,
                "continue",
                backend.ctx.stats().sync_full,
                backend.ctx.stats().sync_partial_ok,
                backend.ctx.stats().sync_partial_err,
            ));
        }

        let mut classic = ServerWorkerState::new(1).unwrap();
        classic.ctx.config_mut().replication.repl_diskless_sync = false;
        let (token, _target) = boundary_connection(&mut classic);
        let sync = CommandArgs::from_parts(vec![Bytes::from_static(b"SYNC")]);
        classic.handle_psync(token, &sync, "sync", &mut Vec::new());
        assert_eq!(
            (
                classic.ctx.stats().sync_full,
                classic.ctx.stats().sync_partial_ok,
                classic.ctx.stats().sync_partial_err
            ),
            (1, 0, 0)
        );
        classic.cancel_disk_sync_batch();
        let mut expected = Vec::new();
        for requested_replid in ["?", "missing"] {
            let partial_err = u64::from(requested_replid != "?");
            for (phase, full, partial_ok) in [
                ("initial", 0, 0),
                ("candidate", 1, 0),
                ("duplicate", 1, 0),
                ("continue", 1, 1),
            ] {
                expected.push((requested_replid, phase, full, partial_ok, partial_err));
            }
        }
        assert_eq!(
            phases, expected,
            "one dual full sync is counted only at candidate SYNC"
        );
    }
}
