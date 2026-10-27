use super::super::super::ServerWorkerState;
use super::replica_aof_ack_offset;
use crate::engine::distributed::replication::{
    backlog::ReplicationBacklog, ReplicationOffset, SnapshotBoundary,
};
use crate::engine::extension::script::{PropagationOp, PropagationTargets};
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::{CommandEffectDrain, TransactionEnd};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    CommandOrigin, PendingReplicationBuffer, PushError, RdbCommit, ReplicaEvent, ReplicaLink,
    ReplicaRdbChannelState, ReplicationApply,
};
use crate::framework::CommandArgs;
use crate::runtime::server::worker::distributed::propagation::{
    encode_command_args, sequence_propagation,
};
use crate::runtime::server::worker::reactor::blocked_events::{
    BlockedOperation, PendingReplayContinuation, ResumeDecision,
};
use bytes::Bytes;
use std::io::Read;

/// Outcome of applying a replicated `Command` to the local dataset.
/// Collected by [`ServerWorkerState::execute_replicated_command`] and applied by
/// [`ServerWorkerState::apply_replication_command_outcome`]. Splitting execution
/// from side-effect application keeps the replica-apply owner in charge of
/// relay ordering, AOF sequencing and lifecycle cleanup while letting the
/// command core focus on dataset mutation.
struct ReplicationCommandOutcome {
    frame: Vec<u8>,
    selected_db: u32,
    dirty_keys: Vec<(u32, Bytes)>,
    lazyfree_objects: Vec<Robj>,
    swapdb: Option<(u32, u32)>,
    notifications: Vec<(Bytes, Bytes)>,
    shard_notifications: Vec<(Bytes, Bytes)>,
    monitor_event: Option<crate::framework::context::MonitorEvent>,
    cmd_name: String,
    duration_us: u64,
}

mod commit;
use commit::*;

impl ServerWorkerState {
    pub(crate) fn apply_replica_handshake_transitions(
        &mut self,
        link: &mut ReplicaLink,
    ) -> Result<(), crate::framework::distributed::replication::LinkError> {
        let partial_resync_accepted = link.take_partial_resync_accepted();
        if let Some(new_replid) = link.take_primary_replid_change() {
            self.adopt_changed_primary_replid(
                link.connection_id(),
                &new_replid,
                link.applied_position(),
            )?;
        }
        if partial_resync_accepted {
            // C replicaProcessPsyncReply() creates a backlog after every accepted +CONTINUE.
            self.ctx.replication_mut().backlog.activate();
        }
        Ok(())
    }

    pub(crate) fn adopt_changed_primary_replid(
        &mut self,
        upstream_connection: crate::framework::distributed::replication::ConnectionId,
        new_replid: &str,
        primary_offset: ReplicationOffset,
    ) -> Result<(), crate::framework::distributed::replication::LinkError> {
        self.ctx
            .replication_mut()
            .backlog
            .try_adopt_changed_primary_replid(new_replid, primary_offset)
            .map_err(|_| crate::framework::distributed::replication::LinkError::Protocol)?;
        let downstream: Vec<usize> = self
            .replication_runtime
            .primary_links()
            .values()
            .filter_map(|link| {
                (link.connection_id() != upstream_connection).then_some(link.connection_id().0)
            })
            .collect();
        for token in downstream {
            self.remove_connection(token);
        }
        Ok(())
    }

    pub(crate) fn reset_downstream_after_full_sync(
        &mut self,
        upstream_connection: crate::framework::distributed::replication::ConnectionId,
    ) {
        let downstream: Vec<usize> = self
            .replication_runtime
            .primary_links()
            .values()
            .filter_map(|link| {
                (link.connection_id() != upstream_connection).then_some(link.connection_id().0)
            })
            .collect();
        for token in downstream {
            self.remove_connection(token);
        }
    }

    pub(crate) fn close_replica_rdb_transport_after_load(&mut self) {
        let Some(rdb_connection_id) = self
            .replication_runtime
            .replica_rdb_link()
            .map(|link| link.connection_id())
        else {
            return;
        };
        // Keep the replica-side RDB role visible while centralized teardown
        // runs. Clear only the replica-side pair first; otherwise generic
        // pair removal treats the RDB close as a primary-side disconnect and
        // recursively closes the still-live main session too.
        self.replication_runtime
            .clear_rdb_channel(rdb_connection_id);
        self.remove_connection(rdb_connection_id.0);
    }

    pub(super) fn apply_replica_events(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        events: Vec<ReplicaEvent>,
    ) -> Result<(), ()> {
        for event in events {
            self.apply_replica_event(connection_token, link, event, true)?;
        }
        Ok(())
    }

    fn apply_replica_event(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        event: ReplicaEvent,
        advance_offset: bool,
    ) -> Result<(), ()> {
        let ReplicaEvent::Apply { apply, end_offset } = event;
        let should_ack = matches!(apply, ReplicationApply::GetAck);
        let was_in_transaction = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .is_active();
        let is_exec = matches!(
            &apply,
            ReplicationApply::Command(args) if args.name.eq_ignore_ascii_case(b"EXEC")
        );
        let is_multi = matches!(
            &apply,
            ReplicationApply::Command(args) if args.name.eq_ignore_ascii_case(b"MULTI")
        );
        self.apply_replication_command(connection_token, link, apply, CommandOrigin::Replication)?;
        if advance_offset && !is_multi && (!was_in_transaction || is_exec) {
            link.finish_command_apply(end_offset).map_err(|_| ())?;
        }
        if should_ack
            && !self.ctx.is_loading()
            && !self.ctx.replication().async_loading
            && !link.has_pending_rdb_receive()
            && link.can_ack_primary()
        {
            let aof_offset = replica_aof_ack_offset(&self.ctx, link.applied_position());
            let action = link.acknowledge_applied(aof_offset);
            self.queue_replica_actions(connection_token, vec![action]);
        }
        Ok(())
    }

    pub(super) fn buffer_replica_events_into_pending(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        events: Vec<ReplicaEvent>,
    ) -> Result<(), ()> {
        self.buffer_replica_events_into_pending_inner(connection_token, link, events, true)
    }

    fn buffer_replica_events_into_pending_inner(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        events: Vec<ReplicaEvent>,
        allow_handoff: bool,
    ) -> Result<(), ()> {
        // This is the replica-local pending_repl_data equivalent. C applies
        // the replica class hard limit here and then pauses the primary stream;
        // zero keeps the buffer unbounded.
        let max_bytes = self
            .ctx
            .config()
            .replication
            .replica_output_buffer_hard_limit;
        let max_bytes = if max_bytes != 0 {
            max_bytes
        } else {
            usize::MAX
        };

        let session_id = link.session_id();
        if self
            .replication_runtime
            .replica_pending_buffer_for(session_id)
            .is_none()
        {
            let (snapshot_end_offset, selected_db) = match link.provisional_primary() {
                Some(pp) => (pp.end_offset, pp.db_id),
                None => {
                    crate::server_log!(
                        warning,
                        "buffer_replica_events_into_pending: dual mode active but ProvisionalPrimary absent, skip buffering"
                    );
                    return Ok(());
                }
            };
            let buffer = PendingReplicationBuffer::new(max_bytes, snapshot_end_offset, selected_db);
            let displaced = self
                .replication_runtime
                .set_replica_pending_buffer(session_id, buffer);
            self.dispose_replica_pending_buffer(displaced);
            crate::server_log!(
                debug,
                "dual handoff buffer: created snapshot_end={} db={} max_bytes={}",
                snapshot_end_offset.get(),
                selected_db,
                max_bytes
            );
        }

        let mut reached_limit = None;
        {
            let Some(buffer) = self
                .replication_runtime
                .replica_pending_buffer_mut_for(session_id)
            else {
                return Ok(());
            };

            for event in events {
                match buffer.push(event) {
                    Ok(outcome) => {
                        if let Some(limit) = outcome.limit_reached() {
                            reached_limit = Some(limit);
                        }
                    }
                    Err(PushError::BeforeSnapshot { offset, snapshot }) => {
                        crate::server_log!(
                            warning,
                            "PendingReplicationBuffer received event before snapshot: offset={}, snapshot_end={}",
                            offset.get(),
                            snapshot.get()
                        );
                    }
                }
            }
        }
        self.refresh_replica_output_buffer_bytes();

        if let Some((_, limit)) = reached_limit {
            if self.pause_replica_main_reads(connection_token)? {
                crate::server_log!(notice, "{}", replication_buffer_limit_message(limit));
            }
            return Ok(());
        }

        if allow_handoff
            && matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
            && link.is_streaming()
        {
            self.commit_and_replay_dual_handoff_only(connection_token, link)?;
        }

        Ok(())
    }

    fn pause_replica_main_reads(&mut self, connection_token: usize) -> Result<bool, ()> {
        use crate::foundation::io::transport::Interest;
        if self.replication_runtime.replica_main_read_paused() {
            return Ok(false);
        }
        let main_token = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
            .map(|c| c.token())
            .ok_or(())?;
        if let Err(error) = self.transport.set_interest(main_token, Interest::Writable) {
            crate::server_log!(
                warning,
                "Failed to pause replica main reads: token={} error={}",
                main_token.0,
                error
            );
            return Err(());
        }
        self.replication_runtime.set_replica_main_read_paused(true);
        Ok(true)
    }

    fn resume_replica_main_reads(&mut self, connection_token: usize) -> Result<(), ()> {
        use crate::foundation::io::transport::Interest;
        if !self.replication_runtime.replica_main_read_paused() {
            return Ok(());
        }
        let main_token = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
            .map(|c| c.token())
            .ok_or(())?;
        if let Err(error) = self
            .transport
            .set_interest(main_token, Interest::ReadWritable)
        {
            crate::server_log!(
                warning,
                "Failed to resume replica main reads: token={} error={}",
                main_token.0,
                error
            );
            return Err(());
        }
        self.replication_runtime.set_replica_main_read_paused(false);
        crate::server_log!(
            notice,
            "dual handoff complete, resumed main socket readable"
        );
        Ok(())
    }

    /// Terminal commit for a streaming full-sync load. The decoder already
    /// applied every record into the target DbSet (swap temp or serving), so
    /// unlike the old buffered path there is no `rdbLoad` here — only the
    /// commit side that used to follow it: swap-in, FUNCTION replay, loading
    /// flags, the success notice, AOF base/rewrite, and cursor adoption.
    /// Mirrors the tail of C's `readSyncBulkPayload` (replication.c:2439-2560).
    pub(super) fn commit_streamed_rdb(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        commit: RdbCommit,
    ) -> Result<(), ()> {
        let rdb_apply_position = link.prevalidate_rdb_apply_position().map_err(|_| ())?;
        let rdb_boundary =
            ReplicationBacklog::rdb_snapshot_boundary(rdb_apply_position).map_err(|_| ())?;
        let is_dual = link.is_dual_rdb_mode_active();
        let main_connection_token = if self
            .connection_registry
            .contains(Token(link.connection_id().0))
        {
            link.connection_id().0
        } else {
            connection_token
        };
        let mut prepared = self.prepare_streamed_rdb_commit(commit)?;
        let mut aof_base = self.prepare_synced_rdb_aof_base(&prepared)?;
        let received_rdb = self.prepare_received_rdb(&prepared, link.session_id())?;
        let delete_received_rdb = self.should_delete_received_sync_rdb();
        let received_rdb_target = received_rdb.as_ref().map(|rdb| rdb.target.clone());
        if let Some(aof_base) = aof_base.as_mut() {
            aof_base.manifest.commit_manifest().map_err(|_| ())?;
        }
        if let Some(received_rdb) = received_rdb {
            received_rdb.commit().map_err(|_| ())?;
        }
        self.install_prepared_dataset(link, &mut prepared, rdb_boundary);
        self.install_prepared_aux(&prepared.aux_fields);
        self.install_prepared_functions(&prepared.function_codes);
        let reused_aof_base = aof_base.is_some();
        self.publish_prepared_aof_base(aof_base);
        if delete_received_rdb && !reused_aof_base {
            if let Some(target) = received_rdb_target {
                if let Err(error) = std::fs::remove_file(&target) {
                    crate::server_log!(
                        warning,
                        "Failed removing RDB file obtained from primary synchronization: {}",
                        error
                    );
                }
            }
        }
        self.finish_streamed_loading_state(link, reused_aof_base);
        self.publish_streamed_rdb_offset(
            main_connection_token,
            link,
            prepared.repl_stream_db,
            rdb_apply_position,
        );
        // Mirror C's full-sync RDB-as-AOF-base reuse (aof.c:1068-1073): publish
        // the current replication offset as both the optimistic pending and the
        // confirmed fsynced offset. When the base is not reused there is nothing
        // durable yet, so the confirmed offset reverts to the None/"-1" sentinel.
        if reused_aof_base {
            let rdb_offset = rdb_apply_position.get();
            self.ctx.aof_mut().set_fsynced_pending(rdb_offset);
            self.ctx.aof_mut().fsynced_repl_offset = Some(rdb_offset);
        } else {
            self.ctx.aof_mut().fsynced_repl_offset = None;
        }
        if is_dual && !matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded) {
            link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
                .map_err(|_| ())?;
        }
        self.finish_streamed_rdb_handoff(main_connection_token, link, is_dual, rdb_boundary)?;
        self.log_streamed_rdb_commit(is_dual, link);
        self.replication_runtime.clear_rejected_authentication();
        Ok(())
    }

    fn should_delete_received_sync_rdb(&self) -> bool {
        let config = &self.ctx.config().persistence;
        config.rdb_del_sync_files && config.saveparams.is_empty() && !config.aof_enabled
    }

    fn prepare_streamed_rdb_commit(&self, commit: RdbCommit) -> Result<PreparedRdbCommit, ()> {
        let RdbCommit {
            function_codes,
            aux_fields,
            repl_stream_db,
            diskless,
            disk_backed,
            raw,
            swapped,
        } = commit;
        self.validate_streamed_rdb_metadata(&aux_fields, &function_codes)?;
        if disk_backed && raw.is_none() {
            crate::server_log!(
                warning,
                "Failed trying to persist the PRIMARY synchronization DB: missing RDB bytes (diskless transfer: {})",
                diskless
            );
            return Err(());
        }
        Ok(PreparedRdbCommit {
            function_codes,
            aux_fields,
            repl_stream_db,
            disk_backed,
            raw,
            swapped,
        })
    }

    fn prepare_received_rdb(
        &self,
        prepared: &PreparedRdbCommit,
        session_id: u64,
    ) -> Result<Option<PreparedReceivedRdb>, ()> {
        if !prepared.disk_backed {
            return Ok(None);
        }
        let config = &self.ctx.config().persistence;
        let dir = std::path::Path::new(&config.dir);
        let temp = dir.join(format!(
            "temp-repl-{}-{}.rdb",
            std::process::id(),
            session_id
        ));
        let target = dir.join(&config.dbfilename);
        let result = (|| -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(prepared.raw.as_deref().unwrap())?;
            file.sync_all()
        })();
        result.map_err(|error| {
            crate::server_log!(warning, "Failed trying to sync the temp DB to disk in PRIMARY <-> REPLICA synchronization: {}", error);
            let _ = std::fs::remove_file(&temp);
        })?;
        Ok(Some(PreparedReceivedRdb { temp, target }))
    }

    fn prepare_synced_rdb_aof_base(
        &mut self,
        prepared: &PreparedRdbCommit,
    ) -> Result<Option<PreparedAofBaseInstall>, ()> {
        let config = self.ctx.config().persistence.clone();
        let should_reuse =
            config.aof_enabled && config.aof_use_rdb_preamble && prepared.disk_backed;
        if !should_reuse {
            return Ok(None);
        }
        if let Some(journal) = self.ctx.aof_mut().journal.as_mut() {
            journal.appender.flush(true).map_err(|_| ())?;
        }
        let manifest = crate::foundation::io::persistence::prepare_synced_rdb_as_aof_base(
            &config.dir,
            &config.aof_filename,
            &config.aof_appenddirname,
            prepared.raw.as_deref().unwrap(),
        )
        .map_err(|_| ())?;
        let appender = crate::foundation::io::persistence::AofAppender::open_append_path(
            manifest.incremental_path(),
            config.aof_fsync,
        )
        .map_err(|_| ())?;
        Ok(Some(PreparedAofBaseInstall {
            manifest,
            journal: crate::foundation::io::persistence::journal::AofJournal::new(appender),
        }))
    }

    fn publish_prepared_aof_base(&mut self, prepared: Option<PreparedAofBaseInstall>) {
        let Some(prepared) = prepared else {
            return;
        };
        self.ctx.reset_repl_seldb();
        self.ctx.persistence_mut().reset_aof_selected_db();
        self.ctx
            .persistence_mut()
            .record_aof_size(prepared.journal.appender.current_size());
        self.ctx.aof_mut().journal = Some(prepared.journal);
        prepared.manifest.finalize();
        crate::server_log!(notice, "Reused RDB file from primary sync as AOF base file");
    }

    fn install_prepared_dataset(
        &mut self,
        link: &ReplicaLink,
        prepared: &mut PreparedRdbCommit,
        rdb_boundary: SnapshotBoundary,
    ) {
        if prepared.swapped.is_some() {
            if let Some(rid) = link.master_replid() {
                self.ctx
                    .replication_mut()
                    .backlog
                    .adopt_primary_from_rdb_boundary(rid, rdb_boundary);
            }
            self.replication_runtime.discard_replica_resume();
            self.reset_downstream_after_full_sync(link.connection_id());
        }
        if let Some(dbs) = prepared.swapped.take() {
            *self.ctx.dbs_mut() = dbs;
        }
    }

    fn install_prepared_aux(&mut self, aux_fields: &[(Vec<u8>, Vec<u8>)]) {
        self.ctx
            .replace_slot_imports_from_rdb_aux(aux_fields)
            .expect("prevalidated cluster slot import AUX must install");
        if let Some(cluster) = self.ctx.cluster_state_mut().as_deref_mut() {
            let slot_states = aux_fields
                .iter()
                .find_map(|(key, value)| {
                    key.eq_ignore_ascii_case(b"cluster-slot-states")
                        .then_some(value.as_slice())
                })
                .unwrap_or_default();
            cluster
                .apply_open_slots_aux_field(slot_states)
                .expect("prevalidated cluster open-slot AUX must install");
        }
        self.ctx.dbs_mut().set_replica_local_expire_only_all(false);
    }

    fn install_prepared_functions(&mut self, function_codes: &[Vec<u8>]) {
        self.ctx.script_mut().function_registry.flush();
        for code in function_codes {
            let code = String::from_utf8_lossy(code);
            let name = crate::engine::extension::lua::shebang::extract_shebang_name(&code)
                .expect("prevalidated function library must have a name");
            self.ctx
                .script_mut()
                .function_registry
                .load_library(&name, &code)
                .expect("prevalidated function library must install");
        }
    }

    fn finish_streamed_loading_state(&mut self, link: &ReplicaLink, reused_aof_base: bool) {
        if !link.is_dual_rdb_mode_active() {
            self.ctx.replication_mut().async_loading = false;
            self.ctx.set_loading(false);
        }
        self.classic_rdb_loading_progress.reset();
        crate::server_log!(notice, "PRIMARY <-> REPLICA sync: Finished with success");
        if self.ctx.config().persistence.aof_enabled && !reused_aof_base {
            let _ =
                crate::framework::commands::server::bgrewriteaof::start_aof_rewrite(&mut self.ctx);
        }
    }

    fn publish_streamed_rdb_offset(
        &mut self,
        main_connection_token: usize,
        link: &mut ReplicaLink,
        repl_stream_db: u32,
        rdb_apply_position: ReplicationOffset,
    ) {
        link.finish_rdb_apply(rdb_apply_position);
        let applied_offset = link.applied_position();
        self.ctx.replication_mut().slave_repl_offset = applied_offset;
        if let Some(cluster) = self.ctx.cluster_state_mut().as_deref_mut() {
            cluster.set_local_replication_offset(applied_offset);
        }
        link.set_selected_db(repl_stream_db);
        self.connection_registry
            [crate::framework::capabilities::io::Token(main_connection_token)]
        .selected_db = repl_stream_db;
    }

    fn finish_streamed_rdb_handoff(
        &mut self,
        main_connection_token: usize,
        link: &mut ReplicaLink,
        is_dual: bool,
        rdb_boundary: SnapshotBoundary,
    ) -> Result<(), ()> {
        if is_dual {
            if let Some(rid) = link.master_replid() {
                self.ctx
                    .replication_mut()
                    .backlog
                    .adopt_primary_from_rdb_boundary(rid, rdb_boundary);
            }
            self.commit_and_replay_dual_handoff_only(main_connection_token, link)?;
            if !link.is_streaming() {
                self.close_replica_rdb_transport_after_load();
            }
        } else {
            self.ctx.config_mut().replication.master_link_status = "up".to_string();
            self.ctx.replication_mut().mark_failover_complete();
            if link.can_ack_primary() {
                // C sends the classic full-sync terminal ACK before restarting
                // AOF, so it cannot carry the newly installed base as FACK.
                let ack = link.acknowledge_applied(None);
                self.queue_replica_actions(main_connection_token, vec![ack]);
            }
        }
        Ok(())
    }

    fn log_streamed_rdb_commit(&self, is_dual: bool, link: &ReplicaLink) {
        crate::server_log!(
            debug,
            "replica RDB commit: dual={} loading={} async_loading={} master_status={} rdb_state={:?}",
            is_dual,
            self.ctx.is_loading(),
            self.ctx.replication().async_loading,
            self.ctx.config().replication.master_link_status,
            link.rdb_channel_state()
        );
    }

    pub(crate) fn commit_and_replay_dual_handoff_only(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
    ) -> Result<(), ()> {
        let main_connection_token = if self
            .connection_registry
            .contains(Token(link.connection_id().0))
        {
            link.connection_id().0
        } else {
            connection_token
        };
        if !link.is_streaming()
            || !matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
        {
            return Ok(());
        }
        crate::server_log!(
            notice,
            "Dual channel replication: Psync established after rdb load"
        );
        self.drain_dual_main_before_handoff(main_connection_token, link)?;
        if self
            .replication_runtime
            .replica_pending_buffer_for(link.session_id())
            .is_some()
            || self.pending_replication_replay.is_some()
        {
            self.pause_replica_main_reads(main_connection_token)?;
            if self.pending_replication_replay.is_none() {
                self.set_pending_replication_replay(PendingReplayContinuation::new(
                    link.connection_id(),
                    link.session_id(),
                    crate::foundation::io::sys::time::mstime(),
                ));
            }
            return Ok(());
        }

        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);

        // Successful handoff closes only the RDB channel; use the replica-side
        // teardown helper so removing that transport cannot erase the main
        // channel's role and make the live stream look disconnected.
        self.close_replica_rdb_transport_after_load();

        let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::None);
        link.clear_provisional_primary();

        self.ctx.replication_mut().slave_repl_offset = link.applied_position();

        self.resume_replica_main_reads(main_connection_token)?;

        self.ctx.config_mut().replication.master_link_status = "up".to_string();
        self.ctx.replication_mut().mark_failover_complete();
        self.replication_runtime.reset_master_reconnect_backoff();

        let aof_offset = replica_aof_ack_offset(&self.ctx, link.applied_position());
        let ack = link.acknowledge_applied(aof_offset);
        self.queue_replica_actions(main_connection_token, vec![ack]);

        Ok(())
    }

    fn drain_dual_main_before_handoff(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
    ) -> Result<(), ()> {
        let Some(conn) = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
        else {
            return Err(());
        };
        let Some(mut stream) = self.transport.stream(conn.token()) else {
            return Ok(());
        };
        let mut read_data = Vec::new();
        let close = crate::foundation::io::sys::os::drain_nonblocking(
            &mut self.io_buf,
            &mut read_data,
            |buf| stream.read(buf),
        )
        .unwrap_or(true);
        if close && read_data.is_empty() {
            self.tear_down_master_link(link.connection_id().0);
            return Err(());
        }
        if read_data.is_empty() {
            return Ok(());
        }

        let listening_port = self
            .ctx
            .config()
            .replication
            .effective_replica_announce_port(self.ctx.config().server.listening_port);
        let replica_announce_ip = self.ctx.config().replication.replica_announce_ip.clone();
        let (events, actions) = match link.on_bytes_with_announce_ip(
            &read_data,
            listening_port,
            &replica_announce_ip,
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                crate::server_log!(
                        warning,
                        "dual-rdb pre-handoff main decode rejected: token={} rdb_state={:?} error={:?} bytes={:?}",
                        link.connection_id().0,
                        link.rdb_channel_state(),
                        error,
                        String::from_utf8_lossy(&read_data)
                    );
                self.abort_master_link_on_error(link.connection_id().0, error);
                return Err(());
            }
        };
        if let Err(error) = self.apply_replica_handshake_transitions(link) {
            self.abort_master_link_on_error(link.connection_id().0, error);
            return Err(());
        }
        self.ctx.replication_mut().slave_read_repl_offset = link.received_position();
        self.buffer_replica_events_into_pending_inner(connection_token, link, events, false)?;
        self.queue_replica_actions(connection_token, actions);
        if close {
            self.tear_down_master_link(link.connection_id().0);
            return Err(());
        }
        Ok(())
    }

    pub(crate) fn drive_one_pending_replication_replay(&mut self) {
        let Some(continuation) = self.pending_replication_replay.take() else {
            return;
        };
        let main = continuation.main;
        let session_id = continuation.session_id;
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main.0))
            .then_some(main.0)
        else {
            return;
        };
        let Some(mut link) = self.retake_pending_replay_session(main, session_id) else {
            self.take_and_dispose_replica_pending_buffer_for(session_id);
            let replacement = self.replication_runtime.replica_link().and_then(|link| {
                (link.connection_id() == main).then_some((
                    link.session_id(),
                    link.is_streaming()
                        && matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded),
                ))
            });
            let replacement_replay = replacement.and_then(|(replacement_session_id, ready)| {
                (ready
                    && self
                        .replication_runtime
                        .replica_pending_buffer_for(replacement_session_id)
                        .is_some())
                .then_some(replacement_session_id)
            });
            if let Some(replacement_session_id) = replacement_replay {
                self.set_pending_replication_replay(PendingReplayContinuation::new(
                    main,
                    replacement_session_id,
                    crate::foundation::io::sys::time::mstime(),
                ));
            } else if self.replication_runtime.replica_main_read_paused() {
                if replacement.is_some() {
                    if self.resume_replica_main_reads(connection_token).is_err() {
                        self.remove_connection(main.0);
                    }
                } else {
                    self.replication_runtime.set_replica_main_read_paused(false);
                }
            }
            return;
        };
        if self
            .drive_pending_replication_replay_unit(connection_token, &mut link, continuation)
            .is_err()
        {
            self.abort_pending_replication_replay(main, session_id);
            return;
        }
        self.replication_runtime.set_replica_link(link);
    }

    fn abort_pending_replication_replay(
        &mut self,
        main: crate::framework::distributed::replication::ConnectionId,
        session_id: u64,
    ) {
        self.take_and_dispose_replica_pending_buffer_for(session_id);
        if self
            .replication_runtime
            .replica_link()
            .is_some_and(|replacement| replacement.session_id() != session_id)
        {
            return;
        }
        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.remove_connection(main.0);
    }

    fn drive_pending_replication_replay_unit(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        mut continuation: PendingReplayContinuation,
    ) -> Result<(), ()> {
        if !link.is_streaming()
            || !matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
        {
            return Ok(());
        }

        let Some(mut pending) = self
            .replication_runtime
            .take_replica_pending_buffer_for(continuation.session_id)
        else {
            self.finish_pending_replication_replay(
                connection_token,
                link,
                continuation.buffered_getack,
            )?;
            return Ok(());
        };
        if self
            .apply_pending_replay_entries(connection_token, link, &mut pending, &mut continuation)
            .is_err()
        {
            self.dispose_replica_pending_buffer(Some(pending));
            return Err(());
        }

        if pending.is_empty() {
            drop(pending);
            self.finish_pending_replication_replay(
                connection_token,
                link,
                continuation.buffered_getack,
            )?;
            return Ok(());
        }

        let displaced = self
            .replication_runtime
            .set_replica_pending_buffer(continuation.session_id, pending);
        debug_assert!(displaced.is_none());
        self.dispose_replica_pending_buffer(displaced);
        self.refresh_replica_output_buffer_bytes();
        self.set_pending_replication_replay(continuation);
        match self.service_pending_replay_events(link) {
            ResumeDecision::Resume(()) => Ok(()),
            ResumeDecision::Aborted => Err(()),
        }
    }

    fn apply_pending_replay_entries(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        pending: &mut PendingReplicationBuffer,
        continuation: &mut PendingReplayContinuation,
    ) -> Result<(), ()> {
        if !continuation.selected_db_applied {
            link.reset_applied_offset_to_dual_snapshot();
            self.apply_replication_command(
                connection_token,
                link,
                ReplicationApply::Select(pending.selected_db()),
                CommandOrigin::Replication,
            )?;
            continuation.selected_db_applied = true;
        }
        let interval = self
            .ctx
            .config()
            .persistence
            .loading_process_events_interval_bytes;
        while let Some(entry) = pending.pop_front() {
            let storage_bytes = entry.storage_bytes();
            let end_offset = entry.end_offset();
            let ReplicaEvent::Apply { apply, .. } = entry.event;
            continuation.buffered_getack |= matches!(apply, ReplicationApply::GetAck);
            self.apply_replication_command(
                connection_token,
                link,
                apply,
                CommandOrigin::Replication,
            )?;
            link.finish_command_apply(end_offset).map_err(|_| ())?;
            if continuation.progress.record(
                storage_bytes,
                interval,
                crate::foundation::io::sys::time::mstime(),
            ) {
                break;
            }
        }
        Ok(())
    }

    fn service_pending_replay_events(&mut self, link: &mut ReplicaLink) -> ResumeDecision<()> {
        let main = link.connection_id();
        let session_id = link.session_id();
        let placeholder = ReplicaLink::new(main, link.master().clone());
        let owned = std::mem::replace(link, placeholder);
        self.replication_runtime.set_replica_link(owned);
        if !self.queue_replica_loading_newline(main) {
            return ResumeDecision::Aborted;
        }
        let processed_bytes = self
            .pending_replication_replay
            .as_ref()
            .map_or(0, |continuation| continuation.progress.processed_bytes());
        let control = self.with_host_dispatcher(
            crate::runtime::server::worker::tasks::hosted::HostTaskScope::blocked(
                BlockedOperation::PendingReplay { main },
            ),
            |dispatcher| crate::engine::host::yield_replication_replay(dispatcher, processed_bytes),
        );
        if !matches!(control, Ok(crate::engine::host::HostControl::Continue)) {
            return ResumeDecision::Aborted;
        }
        if self.ctx.is_shutdown_requested() {
            return ResumeDecision::Aborted;
        }
        let Some(restored) = self.retake_pending_replay_session(main, session_id) else {
            self.clear_pending_replication_replay();
            return ResumeDecision::Aborted;
        };
        *link = restored;
        ResumeDecision::Resume(())
    }

    pub(crate) fn retake_pending_replay_session(
        &mut self,
        main: crate::framework::distributed::replication::ConnectionId,
        session_id: u64,
    ) -> Option<ReplicaLink> {
        let link = self.take_matching_replica_session(main, None)?;
        if link.session_id() == session_id
            && link.is_streaming()
            && link.rdb_channel_state() == ReplicaRdbChannelState::RdbLoaded
        {
            Some(link)
        } else {
            self.replication_runtime.set_replica_link(link);
            None
        }
    }

    fn finish_pending_replication_replay(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        _buffered_getack: bool,
    ) -> Result<(), ()> {
        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);
        self.close_replica_rdb_transport_after_load();
        let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::None);
        link.clear_provisional_primary();
        self.ctx.replication_mut().slave_repl_offset = link.applied_position();
        self.refresh_replica_output_buffer_bytes();

        self.resume_replica_main_reads(connection_token)?;

        self.ctx.config_mut().replication.master_link_status = "up".to_string();
        self.ctx.replication_mut().mark_failover_complete();
        self.replication_runtime.reset_master_reconnect_backoff();
        self.clear_pending_replication_replay();

        let aof_offset = replica_aof_ack_offset(&self.ctx, link.applied_position());
        let ack = link.acknowledge_applied(aof_offset);
        self.queue_replica_actions(connection_token, vec![ack]);

        Ok(())
    }

    pub(super) fn apply_replication_command(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        apply: ReplicationApply,
        origin: CommandOrigin,
    ) -> Result<(), ()> {
        if origin != CommandOrigin::Replication {
            return Err(());
        }
        if self.handle_replication_transaction_frame(connection_token, link, &apply)? {
            return Ok(());
        }
        self.apply_replication_command_now(connection_token, link, apply, true)
    }

    fn handle_replication_transaction_frame(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        apply: &ReplicationApply,
    ) -> Result<bool, ()> {
        if !self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .is_active()
        {
            return Ok(false);
        }
        match apply {
            ReplicationApply::Command(args) if args.name.eq_ignore_ascii_case(b"EXEC") => {
                self.finish_replication_transaction(connection_token, link)?;
                Ok(true)
            }
            ReplicationApply::Command(args) if args.name.eq_ignore_ascii_case(b"MULTI") => {
                Ok(false)
            }
            ReplicationApply::Command(args) => {
                let frame = encode_command_args(args);
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .transaction
                .queue(self.ctx.num_dbs(), args.clone());
                self.relay_replication_transaction_frame(&frame);
                Ok(true)
            }
            ReplicationApply::Select(db) => {
                self.queue_replication_transaction_select(connection_token, *db)?;
                Ok(true)
            }
            ReplicationApply::Ping | ReplicationApply::GetAck => Ok(false),
        }
    }

    fn queue_replication_transaction_select(
        &mut self,
        connection_token: usize,
        db: u32,
    ) -> Result<(), ()> {
        if self.ctx.db(db).is_none() {
            return Err(());
        }
        let db_text = db.to_string();
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queue(
                self.ctx.num_dbs(),
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SELECT"),
                    Bytes::copy_from_slice(db_text.as_bytes()),
                ]),
            );
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SELECT",
            db_text.as_bytes(),
        ]);
        self.relay_replication_transaction_frame(&frame);
        self.ctx.set_repl_seldb(i64::from(db));
        self.ctx
            .persistence_mut()
            .set_aof_selected_db(i64::from(db));
        Ok(())
    }

    fn finish_replication_transaction(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
    ) -> Result<(), ()> {
        let queued = {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let queued = conn.transaction.take_queued_commands();
            conn.transaction.end(TransactionEnd::ExecutionCompleted);
            queued
        };
        for args in queued {
            let apply = if args.name.eq_ignore_ascii_case(b"SELECT") {
                let db = args
                    .args
                    .first()
                    .and_then(|arg| std::str::from_utf8(arg).ok())
                    .and_then(|arg| arg.parse().ok())
                    .ok_or(())?;
                ReplicationApply::Select(db)
            } else {
                ReplicationApply::Command(args)
            };
            self.apply_replication_command_now(connection_token, link, apply, false)?;
        }
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[b"EXEC"]);
        self.relay_replication_transaction_frame(&frame);
        Ok(())
    }

    fn relay_replication_transaction_frame(&mut self, frame: &[u8]) {
        self.propagate_replication_frame(frame);
        if self.ctx.config().persistence.aof_enabled {
            self.feed_aof(frame);
            self.flush_aof_if_needed();
        }
    }

    fn apply_replication_command_now(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        apply: ReplicationApply,
        relay_stream: bool,
    ) -> Result<(), ()> {
        match apply {
            ReplicationApply::Select(db) => {
                if self.ctx.db(db).is_none() {
                    return Err(());
                }
                link.set_selected_db(db);
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .selected_db = db;
                let frame = crate::engine::distributed::replication::protocol::encode_command(&[
                    b"SELECT",
                    db.to_string().as_bytes(),
                ]);
                let command = CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SELECT"),
                    Bytes::from(db.to_string()),
                ]);
                crate::framework::distributed::migration::apply_tracking_import_command(
                    &command,
                    &self.registry,
                    &mut self.ctx,
                )
                .map_err(|_| ())?;
                if relay_stream {
                    self.propagate_replication_frame(&frame);
                }
                if relay_stream && self.ctx.config().persistence.aof_enabled {
                    self.feed_aof(&frame);
                    self.flush_aof_if_needed();
                }
                self.ctx.set_repl_seldb(i64::from(db));
                self.ctx
                    .persistence_mut()
                    .set_aof_selected_db(i64::from(db));
            }
            ReplicationApply::Ping => {
                let frame =
                    crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
                if relay_stream {
                    self.propagate_replication_frame(&frame);
                }
            }
            ReplicationApply::GetAck => {
                let frame = crate::engine::distributed::replication::protocol::encode_command(&[
                    b"REPLCONF",
                    b"GETACK",
                    b"*",
                ]);
                if relay_stream {
                    self.propagate_replication_frame(&frame);
                }
            }
            ReplicationApply::Command(args) => {
                if let Some(outcome) =
                    self.execute_replicated_command(connection_token, &args, relay_stream)?
                {
                    self.apply_replication_command_outcome(
                        connection_token,
                        &args,
                        outcome,
                        relay_stream,
                    );
                }
            }
        }
        Ok(())
    }

    /// Execute a replicated `Command` against the local dataset and collect the
    /// resulting side effects into a [`ReplicationCommandOutcome`].
    /// Slot-import, tracking-import and unknown-command short-circuits are
    /// handled here because they bypass the command core and only need relay
    /// + AOF. Regular commands go through the registry handler and their
    ///   effects are drained for the caller to apply.
    fn execute_replicated_command(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        relay_stream: bool,
    ) -> Result<Option<ReplicationCommandOutcome>, ()> {
        let cleanup_command =
            args.name.eq_ignore_ascii_case(b"DEL") || args.name.eq_ignore_ascii_case(b"UNLINK");
        let replica_local_expire_only = !cleanup_command
            && self.ctx.replication().is_replica()
            && !self.ctx.config().replication.replica_read_only;
        if let Some(db) = self.ctx.db_mut(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .selected_db,
        ) {
            db.set_replica_local_expire_only(replica_local_expire_only);
        }
        let frame = encode_command_args(args);
        let import_job = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .and_then(|role| match role {
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(
                id,
            ) => Some(id.clone()),
            _ => None,
        });
        let is_syncslots = args.name.eq_ignore_ascii_case(b"cluster")
            && args
                .args
                .first()
                .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"syncslots"));
        if let Some(id) = import_job.filter(|_| !is_syncslots) {
            self.ctx
                .apply_slot_import(&id, args.clone(), &self.registry)
                .map_err(|_| ())?;
            self.propagate_and_aof(relay_stream, &frame);
            return Ok(None);
        }
        if !is_syncslots
            && crate::framework::distributed::migration::apply_tracking_import_command(
                args,
                &self.registry,
                &mut self.ctx,
            )
            .map_err(|_| ())?
        {
            self.propagate_and_aof(relay_stream, &frame);
            return Ok(None);
        }
        let Some(spec) = self.registry.lookup(&args.name).cloned() else {
            if self
                .ctx
                .config()
                .replication
                .should_panic_on_propagation_error(false)
            {
                panic!(
                    "This replica panicked applying unknown command from its primary: '{}'",
                    String::from_utf8_lossy(&args.name)
                );
            }
            crate::server_log!(
                warning,
                "== CRITICAL == This primary is sending an error to its replica: unknown command '{}'",
                String::from_utf8_lossy(&args.name)
            );
            self.propagate_and_aof(relay_stream, &frame);
            return Ok(None);
        };
        let command_flags =
            crate::runtime::server::worker::flows::request::dispatch::subcmd_flags::effective_command_flags_for_call(
                spec.name.as_str(), spec.flags, args, &self.ctx,
            );
        let cmd_name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        let started = std::time::Instant::now();
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            conn.begin_nested_command_effects();
            conn.set_skip_propagation(true);
            conn.import_source = true;
        }
        let replication_cluster_failover = args.name.eq_ignore_ascii_case(b"CLUSTER")
            && args
                .args
                .first()
                .is_some_and(|subcmd| subcmd.eq_ignore_ascii_case(b"FAILOVER"));
        let result = if cleanup_command {
            let mut deleted = 0i64;
            let selected_db = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .selected_db;
            if let Some(db) = self.ctx.db_mut(selected_db) {
                for key in &args.args {
                    if db.del(key) {
                        deleted += 1;
                    }
                }
            }
            crate::foundation::io::protocol::Value::Integer(deleted)
        } else if replication_cluster_failover {
            crate::framework::commands::cluster::failover::cluster_failover(
                &args.args[1..],
                true,
                &mut self.ctx,
            )
        } else if let Some(prepare) = spec.hosted_handler() {
            match prepare(
                &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)],
                args,
                &mut self.ctx,
            ) {
                Ok(task) => self.run_prepared_host_task(Token(connection_token), task),
                Err(reply) => reply,
            }
        } else {
            spec.execute(
                &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)],
                args,
                &mut self.ctx,
            )
        };
        let propagation_error = matches!(result, crate::foundation::io::protocol::Value::Error(_));
        let should_panic_on_error = propagation_error
            && self
                .ctx
                .config()
                .replication
                .should_panic_on_propagation_error(false);
        let (selected_db, dirty_keys, lazyfree_objects, swapdb, pending, pending_shard) = {
            let Some(conn) = self.connection_registry.get_mut(Token(connection_token)) else {
                return Err(());
            };
            if cleanup_command {
                if let Some(db) = self.ctx.db_mut(conn.selected_db) {
                    db.set_replica_local_expire_only(false);
                }
            }
            conn.import_source = false;
            if let crate::foundation::io::protocol::Value::Error(error) = &result {
                crate::server_log!(
                    warning,
                    "== CRITICAL == This primary is sending an error to its replica: '{}' after processing the command '{}'",
                    String::from_utf8_lossy(error),
                    String::from_utf8_lossy(&args.name)
                );
            }
            let mut effects = conn
                .take_command_effects(CommandEffectDrain::Completed)
                .expect("replica apply drains one command effect set");
            let dirty_keys = if !effects.dirty_keys().is_empty() {
                effects.take_dirty_keys()
            } else if matches!(result, crate::foundation::io::protocol::Value::Error(_)) {
                Vec::new()
            } else if (spec.flags & crate::engine::command::CMD_WRITE) != 0 {
                crate::runtime::server::worker::flows::request::extract_write_keys(
                    &spec,
                    args,
                    conn.selected_db,
                )
                .unwrap_or_default()
            } else {
                Vec::new()
            };
            let pending = effects.take_notifications();
            let pending_shard = effects.take_shard_notifications();
            let lazyfree_objects = effects.take_lazyfree_objects();
            let swapdb = effects.take_swapdb();
            conn.complete_command();
            (
                conn.selected_db,
                dirty_keys,
                lazyfree_objects,
                swapdb,
                pending,
                pending_shard,
            )
        };
        if should_panic_on_error {
            panic!(
                "This replica panicked applying an error from its primary after command '{}'",
                String::from_utf8_lossy(&args.name)
            );
        }
        let duration_us = (started.elapsed().as_micros() as u64).max(1);
        let monitor_event = {
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let mut argv = Vec::with_capacity(args.args.len() + 1);
            argv.push(args.name.clone());
            argv.extend(args.args.iter().cloned());
            self.ctx.monitor_event(
                conn.selected_db,
                conn.peer_addr.to_string(),
                argv,
                command_flags,
                false,
            )
        };
        Ok(Some(ReplicationCommandOutcome {
            frame,
            selected_db,
            dirty_keys,
            lazyfree_objects,
            swapdb,
            notifications: pending,
            shard_notifications: pending_shard,
            monitor_event,
            cmd_name,
            duration_us,
        }))
    }

    /// Apply the side effects collected by [`execute_replicated_command`]:
    /// lazyfree disposal, stats, monitor, notifications, SWAPDB watch state,
    /// blocked-key wakeup, downstream relay and AOF sequencing.
    fn apply_replication_command_outcome(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        outcome: ReplicationCommandOutcome,
        relay_stream: bool,
    ) {
        self.dispose_data_lazyfree_objects(outcome.lazyfree_objects);
        {
            let stat = self
                .ctx
                .stats_mut()
                .cmd_stats
                .entry(outcome.cmd_name.clone())
                .or_default();
            stat.calls = stat.calls.saturating_add(1);
            stat.usec = stat.usec.saturating_add(outcome.duration_us);
        }
        self.ctx
            .record_command_latency(&outcome.cmd_name, outcome.duration_us);
        self.record_command_slot_metrics(
            args,
            &outcome.dirty_keys,
            crate::runtime::server::worker::distributed::migration::accounting::SlotMetricSample {
                cpu_usec: outcome.duration_us,
                network_bytes_in: outcome.frame.len() as u64,
                network_bytes_out: 0,
            },
        );
        if let Some(event) = outcome.monitor_event {
            self.deliver_monitor(vec![event]);
        }
        self.deliver_published(outcome.notifications, outcome.shard_notifications);
        if let Some((db1, db2)) = outcome.swapdb {
            self.apply_swapdb_watch_state(db1, db2);
        }
        self.wakeup_blocked_on_keys(&outcome.dirty_keys, None, None);
        if relay_stream {
            self.propagate_replication_frame(&outcome.frame);
        }
        if relay_stream && self.ctx.config().persistence.aof_enabled {
            let op =
                PropagationOp::new(outcome.selected_db, PropagationTargets::Aof, outcome.frame);
            let sequenced = sequence_propagation(
                &[op],
                self.ctx.repl_seldb(),
                self.ctx.persistence().aof_selected_db(),
            );
            for frame in &sequenced.aof_frames {
                self.feed_aof(frame);
            }
            self.ctx
                .persistence_mut()
                .set_aof_selected_db(sequenced.aof_selected_db);
            self.flush_aof_if_needed();
        }
        let _ = connection_token;
    }

    fn propagate_and_aof(&mut self, relay_stream: bool, frame: &[u8]) {
        if relay_stream {
            self.propagate_replication_frame(frame);
        }
        if relay_stream && self.ctx.config().persistence.aof_enabled {
            self.feed_aof(frame);
        }
    }
}

/// Log an RDB decode failure on the replica full-sync load path with C's
/// rdb.c:131-135 wording ("log and proceed"). This is the complete-but-corrupt
/// case (defense-in-depth); the mid-transfer short read is handled at
/// `remove_connection`. The caller still returns `Err(())` so the link is torn
/// down and the reconnect/backoff drives a fresh full sync.
pub(crate) fn log_rdb_decode_error(error: &crate::foundation::io::persistence::LoadError) {
    crate::server_log!(
        warning,
        "Internal error in RDB reading: {:?}. Failure loading rdb format from socket, assuming connection error, resuming operation.",
        error
    );
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod boundary_tests {
    use super::ServerWorkerState;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::framework::distributed::replication::{
        ConnectionId, LinkError, MasterInfo, RdbFilterMode, ReplicaConnectionInfo, ReplicaLink,
    };

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn cached_resume_offset_overflow_rejects_before_psync() {
        let mut link = ReplicaLink::new(
            ConnectionId(40),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.set_dual_channel_enabled(false);
        link.set_resume_position(
            "0123456789abcdef0123456789abcdef01234567".to_string(),
            offset(i64::MAX),
            0,
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n", 6380).unwrap();
        link.on_bytes(b"+OK\r\n", 6380).unwrap();

        assert!(matches!(
            link.on_bytes(b"+OK\r\n", 6380),
            Err(LinkError::Protocol)
        ));
    }

    #[test]
    fn changed_primary_replid_overflow_preserves_history_and_downstreams() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        {
            let backlog = &mut runtime.ctx.replication_mut().backlog;
            backlog.activate();
            backlog.append(b"abcdef").unwrap();
            backlog.shift_replication_id();
            backlog.append(b"gh").unwrap();
        }
        let downstream_connection = ConnectionId(41);
        runtime.replication_runtime.register_primary(
            downstream_connection,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        let downstream_before = runtime
            .replication_runtime
            .primary_id(downstream_connection);
        let before = {
            let backlog = &runtime.ctx.replication().backlog;
            let first = backlog.first_byte_cursor();
            (
                backlog.replid().to_string(),
                backlog.second_replid().to_string(),
                backlog.second_replid_request_boundary(),
                backlog.snapshot_boundary(),
                first,
                backlog.history_len(),
                backlog.read_from(first),
                backlog.is_active(),
            )
        };

        assert_eq!(
            runtime.adopt_changed_primary_replid(
                ConnectionId(40),
                "fedcba9876543210fedcba9876543210fedcba98",
                offset(i64::MAX),
            ),
            Err(LinkError::Protocol)
        );

        let backlog = &runtime.ctx.replication().backlog;
        let first = backlog.first_byte_cursor();
        let after = (
            backlog.replid().to_string(),
            backlog.second_replid().to_string(),
            backlog.second_replid_request_boundary(),
            backlog.snapshot_boundary(),
            first,
            backlog.history_len(),
            backlog.read_from(first),
            backlog.is_active(),
        );
        assert_eq!(after, before);
        assert_eq!(
            runtime
                .replication_runtime
                .primary_id(downstream_connection),
            downstream_before
        );
    }
}
