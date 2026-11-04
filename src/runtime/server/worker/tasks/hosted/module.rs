use crate::engine::extension::module::api::call::pending::ModuleCallEffects;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::distributed::propagation::sequence_propagation;
use crate::runtime::server::worker::flows::request::dispatch::EffectQueue;
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;

impl ServerWorkerState {
    pub(crate) fn apply_pending_module_effects(&mut self) {
        let pending = std::mem::take(&mut self.ctx.module_registry_mut().pending_effects);
        let monitor_events = std::mem::take(&mut self.ctx.script_mut().pending_internal_monitor);
        if pending.is_empty() {
            self.deliver_monitor(monitor_events);
            return;
        }
        let dirty_keys = self.commit_pending_module_effects(pending, None, None, None);
        self.deliver_monitor(monitor_events);
        if !dirty_keys.is_empty() {
            self.wakeup_blocked_on_keys(&dirty_keys, None, None);
        }
        self.flush_aof_if_needed();
        self.schedule_pending_replica_writes_before_sleep();
    }

    pub(crate) fn commit_pending_module_effects(
        &mut self,
        pending: Vec<ModuleCallEffects>,
        mut pending_replication: Option<&mut Vec<Vec<u8>>>,
        mut pending_aof: Option<&mut Vec<Vec<u8>>>,
        mut pending_effects: Option<&mut EffectQueue>,
    ) -> Vec<(u32, Bytes)> {
        let mut dirty_keys = Vec::new();
        for effects in pending {
            if let Some(delta) = effects.dirty_delta {
                self.ctx.persistence_mut().add_dirty(delta);
            }
            self.queue_slot_migration_mutations(&effects.propagation);
            if self.ctx.persistence_mut().take_rewrite_force_select() {
                self.ctx.persistence_mut().reset_aof_selected_db();
            }
            let sequenced = sequence_propagation(
                &effects.propagation,
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
            if let Some(queue) = pending_effects.as_deref_mut() {
                queue.replication_only.extend(sequenced.replication_frames);
                queue.aof_only.extend(sequenced.aof_frames);
                queue.cluster_publish.extend(effects.cluster_notifications);
                queue
                    .cluster_publish_shard
                    .extend(effects.cluster_shard_notifications);
                queue.publish.extend(effects.notifications);
                queue.publish_shard.extend(effects.shard_notifications);
            } else {
                if let Some(frames) = pending_replication.as_deref_mut() {
                    frames.extend(sequenced.replication_frames);
                } else if !self.ctx.replication().is_replica() {
                    for frame in &sequenced.replication_frames {
                        self.propagate_to_replicas(frame);
                    }
                }
                if let Some(frames) = pending_aof.as_deref_mut() {
                    frames.extend(sequenced.aof_frames);
                } else {
                    for frame in &sequenced.aof_frames {
                        self.feed_aof(frame);
                    }
                }
                self.propagate_cluster_publishes(
                    effects.cluster_notifications,
                    effects.cluster_shard_notifications,
                );
                self.deliver_published(effects.notifications, effects.shard_notifications);
            }
            for (db_idx, key) in &effects.dirty_keys {
                let exists_after = self
                    .ctx
                    .db(*db_idx)
                    .is_some_and(|db| db.exists_no_expire(key));
                for (_, conn) in self.connection_registry.iter_mut() {
                    conn.transaction
                        .touch_watched_key(*db_idx, key, exists_after);
                }
            }
            let mut tracking = Vec::new();
            self.collect_tracking_invalidations(&effects.dirty_keys, None, &mut tracking);
            if let Some(queue) = pending_effects.as_deref_mut() {
                queue.tracking.extend(tracking);
            } else {
                for (token, frame) in tracking {
                    self.queue_client_reply(Token(token), frame);
                }
            }
            dirty_keys.extend(effects.dirty_keys);
        }
        dirty_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::{CommandConn, WatchedKey};
    use crate::engine::extension::module::api::call::pending::ModuleCallEffects;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    #[test]
    fn module_effects_record_position_before_fsync_for_direct_and_batched_sinks() {
        use crate::engine::extension::script::PropagationOp;
        use crate::foundation::io::persistence::{AofAppender, AofFsync};
        for is_backlog_active in [false, true] {
            for sink in 0..3 {
                let mut backend = ServerWorkerState::new(1).unwrap();
                let path = std::env::temp_dir().join(format!(
                    "module-effects-durability-{}-{}-{}-{}.aof",
                    std::process::id(),
                    is_backlog_active,
                    sink,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                ));
                let appender = AofAppender::open_append_path(&path, AofFsync::Always).unwrap();
                backend.ctx.config_mut().persistence.aof_enabled = true;
                backend.ctx.config_mut().persistence.aof_fsync = AofFsync::Always;
                backend.ctx.aof_mut().journal =
                    Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
                backend.ctx.aof_mut().fsynced_repl_offset = Some(0);
                let bootstrap =
                    crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
                backend.feed_aof(&bootstrap);
                backend.flush_aof_if_needed();
                assert_eq!(backend.ctx.aof().fsynced_repl_offset, Some(0));
                assert_eq!(
                    backend
                        .ctx
                        .aof()
                        .journal
                        .as_ref()
                        .unwrap()
                        .appender
                        .fsynced_reported_offset(),
                    Some(0)
                );
                if is_backlog_active {
                    backend.ctx.replication_mut().backlog.activate();
                }
                backend.ctx.set_repl_seldb(0);
                backend.ctx.persistence_mut().set_aof_selected_db(0);
                let key = Bytes::from_static(b"module:durability");
                backend.ctx.db_mut(0).unwrap().set(
                    key.to_vec(),
                    crate::foundation::io::store::Robj::string(Bytes::from_static(b"2")),
                );
                let mut propagation = vec![PropagationOp::both(
                    0,
                    crate::engine::distributed::replication::protocol::encode_command(&[
                        b"SET",
                        b"module:durability",
                        b"1",
                    ]),
                )];
                if sink != 0 {
                    propagation.push(PropagationOp::both(
                        0,
                        crate::engine::distributed::replication::protocol::encode_command(&[
                            b"INCR",
                            b"module:durability",
                        ]),
                    ));
                }
                let sequenced = sequence_propagation(&propagation, 0, 0);
                let expected_stream = sequenced.replication_frames.concat();
                let expected_aof = sequenced.aof_frames.concat();
                let before = backend.ctx.replication().backlog.next_stream_cursor();
                backend
                    .ctx
                    .module_registry_mut()
                    .pending_effects
                    .push(ModuleCallEffects {
                        dirty_delta: Some(1),
                        dirty_keys: vec![(0, key)],
                        propagation,
                        ..ModuleCallEffects::default()
                    });
                let pending =
                    std::mem::take(&mut backend.ctx.module_registry_mut().pending_effects);
                let mut replication = Vec::new();
                let mut aof = Vec::new();
                let mut queue = EffectQueue::default();
                match sink {
                    0 => {
                        backend.commit_pending_module_effects(pending, None, None, None);
                    }
                    1 => {
                        backend.commit_pending_module_effects(
                            pending,
                            Some(&mut replication),
                            Some(&mut aof),
                            None,
                        );
                    }
                    _ => {
                        backend.commit_pending_module_effects(
                            pending,
                            None,
                            None,
                            Some(&mut queue),
                        );
                        replication = queue.replication_only;
                        aof = queue.aof_only;
                    }
                }
                let expected_position = if is_backlog_active {
                    expected_stream.len() as i64
                } else {
                    1
                };
                assert_eq!(
                    backend.ctx.replication().primary_repl_offset().get(),
                    expected_position
                );
                assert_eq!(backend.ctx.aof().fsynced_repl_offset, Some(0));
                assert!(backend.ctx.aof().fsynced_repl_offset.unwrap() < expected_position);
                if sink != 0 {
                    assert_eq!(replication.concat(), expected_stream);
                    assert_eq!(aof.concat(), expected_aof);
                    for frame in replication {
                        backend.propagate_to_replicas(&frame);
                    }
                    for frame in aof {
                        backend.feed_aof(&frame);
                    }
                }
                assert_eq!(
                    backend.ctx.replication().primary_repl_offset().get(),
                    expected_position
                );
                if is_backlog_active {
                    assert_eq!(
                        backend.ctx.replication().backlog.read_from(before),
                        Some(expected_stream)
                    );
                }
                backend.flush_aof_if_needed();
                assert_eq!(
                    backend.ctx.aof().fsynced_repl_offset,
                    Some(expected_position)
                );
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    [bootstrap, expected_aof].concat()
                );
                backend.apply_pending_module_effects();
                assert_eq!(
                    backend.ctx.replication().primary_repl_offset().get(),
                    expected_position
                );
                drop(backend);
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    #[test]
    fn module_effect_position_overflow_requests_shutdown() {
        use crate::engine::distributed::replication::ReplicationOffset;
        use crate::engine::extension::script::PropagationOp;
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .replication_mut()
            .backlog
            .restore_primary_from_rdb(
                "persisted",
                ReplicationOffset::try_from(i64::MAX - 1).unwrap(),
            )
            .unwrap();
        let mut queue = EffectQueue::default();
        backend.commit_pending_module_effects(
            vec![ModuleCallEffects {
                propagation: vec![PropagationOp::both(0, b"frame".to_vec())],
                ..ModuleCallEffects::default()
            }],
            None,
            None,
            Some(&mut queue),
        );
        assert!(backend.ctx.is_shutdown_requested());
    }

    #[test]
    fn pending_callback_effects_invalidate_watch_and_tracking_exactly_once() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let key = Bytes::from_static(b"callback:effect");
        let mut watcher = Connection::new_fake();
        watcher.set_token_for_test(Token(42));
        watcher.set_resp_version(3);
        watcher.set_tracking(true);
        watcher
            .transaction
            .watch_many(vec![WatchedKey::new(0, key.clone(), false)]);
        let token = Token(backend.connection_registry.insert(watcher));
        backend.ctx.tracking_mut().remember_key(token.0, &key);
        backend
            .ctx
            .module_registry_mut()
            .pending_effects
            .push(ModuleCallEffects {
                dirty_delta: Some(1),
                dirty_keys: vec![(0, key)],
                ..ModuleCallEffects::default()
            });
        let dirty = backend.ctx.persistence().dirty();
        backend.apply_pending_module_effects();
        assert!(backend.connection_registry[token]
            .transaction
            .is_watch_dirty());
        assert_eq!(backend.ctx.tracking().total_keys(), 0);
        assert_eq!(backend.ctx.persistence().dirty(), dirty + 1);
        let output = backend.connection_registry[token].output_buffer_bytes();
        assert!(output > 0);
        backend.apply_pending_module_effects();
        assert_eq!(backend.ctx.persistence().dirty(), dirty + 1);
        assert_eq!(
            backend.connection_registry[token].output_buffer_bytes(),
            output
        );
    }
}
