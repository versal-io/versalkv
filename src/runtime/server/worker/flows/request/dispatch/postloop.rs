use super::super::super::ServerWorkerState;
use super::side_effects::ReadBatchEffects;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;

impl ServerWorkerState {
    fn promote_default_user_authentication(&mut self) {
        // ACL SETUSER does not retroactively deauthenticate existing clients.
        // Only promote connections created while the default user was off.
        let default_user_is_on = {
            let users = crate::engine::extension::acl::acl_users().lock().unwrap();
            users.get("default").is_some_and(|user| user.is_on())
        };
        if self.ctx.config().security.requirepass.is_some() || !default_user_is_on {
            return;
        }
        for (_, connection) in self.connection_registry.iter_mut() {
            if !connection.authenticated {
                connection.authenticated = true;
                connection.ever_authenticated = true;
            }
        }
    }

    pub(super) fn queue_command_propagation(&mut self, effects: &mut ReadBatchEffects) {
        // C `shouldPropagate(PROPAGATE_REPL)` only feeds a local command to
        // the replication stream when this server has no upstream primary.
        // A writable replica may accept local writes, but those writes must
        // neither corrupt its cached upstream history nor be replayed to a
        // former primary after failover. Upstream frames take the separate
        // replica-apply relay path and remain eligible for chaining.
        if !self.ctx.replication().is_replica() {
            for frame in effects.replication_only.drain(..) {
                self.propagate_to_replicas(&frame);
            }
        } else {
            effects.replication_only.clear();
        }
        for frame in effects.aof_only.drain(..) {
            self.feed_aof(&frame);
        }
        self.reject_blocked_waitaof_local_if_aof_disabled();
    }

    fn deliver_observer_effects(&mut self, effects: &mut ReadBatchEffects) {
        self.kill_acl_subscribers();
        for (target_id, frame) in effects.tracking.drain(..) {
            self.queue_client_reply(Token(target_id), frame);
        }
        for entry in effects.cmd_logs.drain(..) {
            self.ctx.push_cmd_logs(
                entry.argv,
                entry.duration_us,
                &entry.peer_addr,
                entry.client_name,
                entry.req_bytes,
                entry.reply_bytes,
                entry.command_flags,
            );
        }
        effects
            .monitor
            .append(&mut self.ctx.script_mut().pending_internal_monitor);
        self.propagate_cluster_publishes(
            std::mem::take(&mut effects.cluster_publish),
            std::mem::take(&mut effects.cluster_publish_shard),
        );
        self.deliver_published(
            std::mem::take(&mut effects.publish),
            std::mem::take(&mut effects.publish_shard),
        );
        let changed_slots = self.take_changed_shard_slots();
        self.prune_unserved_shard_subscriptions(&changed_slots);
        self.deliver_monitor(std::mem::take(&mut effects.monitor));
    }

    fn take_changed_shard_slots(&mut self) -> Vec<u16> {
        let changed_slots = self
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .map(|state| state.take_shard_slot_cleanup_requests())
            .unwrap_or_default();
        for &slot in &changed_slots {
            self.ctx.slot_metrics_mut().reset_slot(slot as usize);
        }
        changed_slots
    }

    fn process_pending_unblocks(&mut self, pending: Vec<(usize, bool)>) {
        for (client_id, is_error) in pending {
            self.process_pending_unblock(client_id, is_error);
        }
    }

    fn process_pending_unblock(&mut self, client_id: usize, is_error: bool) {
        let token = Token(client_id);
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(client_id))
            .then_some(client_id)
        else {
            return;
        };
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
            || self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .blocked
                .is_none()
        {
            return;
        }
        let blocked_name = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .as_ref()
        .map(|blocked| String::from_utf8_lossy(&blocked.orig_args.name).to_ascii_lowercase());
        let resp3 = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .resp_version
            >= 3;
        let reply = self.unblock_reply(is_error, resp3, blocked_name.as_deref());
        self.complete_blocked_command_reply(token, reply, true);
    }

    fn unblock_reply(&mut self, is_error: bool, resp3: bool, command_name: Option<&str>) -> Value {
        if is_error {
            if let Some(command_name) = command_name {
                self.ctx
                    .stats_mut()
                    .cmd_stats
                    .entry(command_name.to_string())
                    .or_default()
                    .rejected_calls += 1;
            }
            self.ctx.incr_error_reply("UNBLOCKED");
            return Value::error("UNBLOCKED client unblocked via CLIENT UNBLOCK");
        }
        if resp3 {
            Value::Null
        } else {
            Value::NullArray
        }
    }

    fn apply_watch_and_wakeup_effects(&mut self, effects: &ReadBatchEffects) {
        for (db_id, key) in &effects.watch_dirty {
            let exists_after = self
                .ctx
                .db(*db_id)
                .is_some_and(|db| db.exists_no_expire(key));
            for (_, connection) in self.connection_registry.iter_mut() {
                connection
                    .transaction
                    .touch_watched_key(*db_id, key, exists_after);
            }
        }
        if !effects.watch_dirty.is_empty() {
            self.signal_keys_ready(&effects.watch_dirty);
            self.process_ready_keys();
        }
        if let Some((db1, db2)) = effects.swapdb {
            self.apply_swapdb_watch_state(db1, db2);
        }
        if !effects.wakeup_only.is_empty() {
            self.signal_keys_ready(&effects.wakeup_only);
            self.process_ready_keys();
        }
    }

    pub(crate) fn finalize_handle_readable(
        &mut self,
        connection_token: usize,
        token_value: usize,
        mut fx: ReadBatchEffects,
    ) {
        // Match C REPLICAOF/SLAVEOF: tear down the old primary before +OK.
        self.apply_pending_replicaof_request();
        if !self.prepare_slot_import_output(connection_token) {
            return;
        }
        self.promote_default_user_authentication();
        self.evict_tracking_table_if_needed(&mut fx.tracking);
        self.queue_command_propagation(&mut fx);

        let is_slot_export =
            self.queue_origin_replies(connection_token, token_value, &mut fx.replies);

        self.deliver_observer_effects(&mut fx);
        self.process_pending_unblocks(std::mem::take(&mut fx.unblock));
        self.apply_watch_and_wakeup_effects(&fx);
        // C's beforeSleep broadcasts a locally finalized imported slot before
        // the next heartbeat can reintroduce the source's older ownership.
        // The engine only records this intent; L5 owns the bus write buffers.
        self.flush_pending_topology_broadcast();

        // Match C prepareClientToWrite by scheduling propagated output for the
        // DistributedWrites pass in beforeSleep, before the next event-loop
        // poll.
        self.schedule_pending_replica_writes_before_sleep();

        self.finish_caller_connection(
            connection_token,
            token_value,
            is_slot_export,
            fx.mark_closing,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::{BlockedCmd, CommandConn};
    use crate::engine::command::CommandArgs;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    #[test]
    fn client_unblock_timeout_honors_skip_and_advances_reply_policy() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let token = Token(82);
        let key = Bytes::from_static(b"unblock-key");
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            key.clone(),
            Bytes::from_static(b"0"),
        ]);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.set_custom_reply_enabled(false);
        connection.set_skip_propagation(false);
        connection.begin_nested_command_effects();
        connection.set_request_bytes_for_test(32);
        connection.enter_blocking(BlockedCmd::Blpop, vec![key.clone()], 0, &args, 32);
        connection.set_reply_skip(true);
        backend.connection_registry.insert(connection);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &[key], session);

        backend.process_pending_unblock(token.0, false);

        assert!(backend.connection_registry[token].blocked.is_none());
        assert!(backend.connection_registry[token].output.queue.is_empty());
        assert!(!backend.connection_registry[token].reply_skip());
        assert_eq!(
            backend.connection_registry[token].request_bytes_current_command(),
            0
        );
        assert_eq!(backend.connection_registry[token].cmd_count, 1);
    }
}
