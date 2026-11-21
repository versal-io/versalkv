//! Native BlockClient completion, never original-command replay.

use crate::engine::command::context::{BlockedCmd, CommandConn};
use crate::engine::extension::module::api::call::pending::{
    ModuleCallEffectScope, ModuleCallEffects,
};
use crate::engine::extension::module::api::{ModuleReplyFrame, ReplyBuilder};
use crate::engine::extension::module::blocking::{ModuleBlockStage, NativeModuleBlock};
use crate::engine::extension::module::thread_bridge::{lock, ModuleBlockedId, ModuleContextKind};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;
use crate::framework::capabilities::module::{
    invoke_native_module_callback, queue_module_reply_frames, NativeModuleCallback,
};
use crate::runtime::server::worker::connection::write::AofBarrierOutcome;
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;

impl ServerWorkerState {
    pub(crate) fn commit_native_module_initial_effects(
        &mut self,
        connection_token: usize,
        sink: Option<&mut crate::runtime::server::worker::command::dispatch::EffectQueue>,
    ) {
        let token = Token(connection_token);
        let Some(conn) = self.connection_registry.get_mut(token) else {
            return;
        };
        if !matches!(
            conn.blocked.as_ref().map(|blocked| &blocked.cmd),
            Some(BlockedCmd::Module { .. })
        ) {
            return;
        }
        let Some(mut effects) = conn.take_command_effects(
            crate::framework::capabilities::connection::CommandEffectDrain::Nested,
        ) else {
            return;
        };
        conn.resume_command_effects();
        let mut propagation = effects.take_script_propagation();
        propagation.extend(effects.take_custom_propagation().into_iter().map(|frame| {
            crate::engine::extension::script::PropagationOp::both(conn.selected_db, frame)
        }));
        let mut unit = ModuleCallEffects {
            dirty_delta: effects.take_dirty_delta(),
            dirty_keys: effects.take_dirty_keys(),
            propagation,
            notifications: effects.take_notifications(),
            shard_notifications: effects.take_shard_notifications(),
            cluster_notifications: effects.take_cluster_notifications(),
            cluster_shard_notifications: effects.take_cluster_shard_notifications(),
        };
        let mut trailing = effects.take_module_notification_effects();
        if self.command_execution_depth <= 1 {
            trailing.extend(
                crate::framework::capabilities::module::finish_module_post_notification_jobs(
                    &mut self.ctx,
                ),
            );
        }
        for effect in trailing {
            if unit.append(effect).is_err() {
                if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
                    bridge.close();
                }
                self.ctx.set_shutdown_requested(true);
            }
        }
        self.dispose_data_lazyfree_objects(effects.take_lazyfree_objects());
        let units = if unit.is_empty() {
            Vec::new()
        } else {
            vec![unit]
        };
        let dirty = self.commit_pending_module_effects(units, None, None, sink);
        self.signal_keys_ready(&dirty);
    }

    fn native_module_owner(&self, block: &NativeModuleBlock) -> Option<Token> {
        let origin = &block.shared.origin;
        self.connection_registry
            .get(origin.token)
            .filter(|conn| {
                conn.session_id() == origin.session
                    && conn.command_seq() == origin.command_seq
                    && matches!(conn.blocked.as_ref().map(|blocked| &blocked.cmd),
                    Some(BlockedCmd::Module { id }) if *id == block.shared.id)
            })
            .map(|_| origin.token)
    }

    pub(crate) fn service_native_module_completions(&mut self) {
        if self.command_execution_depth != 0
            || ModuleCallEffectScope::is_active()
            || self.ctx.is_shutdown_requested()
        {
            return;
        }
        if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
            // A successful TryLock reservation has priority over every C
            // lifecycle callback. The lease service owns its fulfillment.
            if !bridge.resume_main() {
                return;
            }
            let _ = bridge.take_completions();
        }
        let disconnects: Vec<_> = self
            .ctx
            .module_registry()
            .native_blocks
            .iter()
            .filter_map(|(id, block)| {
                (lock(&block.shared.lifecycle).stage == ModuleBlockStage::DisconnectPending)
                    .then_some(*id)
            })
            .collect();
        for id in disconnects {
            let Some(block) = self.ctx.module_registry_mut().native_blocks.remove(&id) else {
                continue;
            };
            lock(&block.shared.lifecycle).stage = ModuleBlockStage::DetachedDisconnect;
            let scope = ModuleCallEffectScope::enter();
            invoke_native_module_callback(&mut self.ctx, &block, NativeModuleCallback::Disconnect);
            let dirty = self.commit_native_callback_effects(scope.finish());
            self.ctx
                .module_registry_mut()
                .native_blocks
                .insert(id, block);
            self.signal_keys_ready(&dirty);
            if self.ctx.is_shutdown_requested() {
                return;
            }
        }
        // Publishing can outlive an early Unblock. The owned lifecycle remains
        // authoritative until command transfer arms it at an outer safe point.
        let ids: Vec<_> = self
            .ctx
            .module_registry()
            .native_blocks
            .iter()
            .filter_map(|(id, block)| {
                let state = lock(&block.shared.lifecycle);
                (state.completion_queued && state.stage != ModuleBlockStage::Publishing)
                    .then_some(*id)
            })
            .collect();
        for id in ids {
            self.complete_native_module_block(id);
            if self.ctx.is_shutdown_requested() {
                return;
            }
        }
    }

    fn complete_native_module_block(&mut self, id: ModuleBlockedId) {
        let Some(block) = self.ctx.module_registry_mut().native_blocks.remove(&id) else {
            return;
        };
        let stage = lock(&block.shared.lifecycle).stage;
        let owner = (stage == ModuleBlockStage::Armed)
            .then(|| self.native_module_owner(&block))
            .flatten();
        let scope = ModuleCallEffectScope::enter();
        if stage == ModuleBlockStage::Armed && owner.is_none() {
            lock(&block.shared.lifecycle).stage = ModuleBlockStage::DetachedDisconnect;
            invoke_native_module_callback(&mut self.ctx, &block, NativeModuleCallback::Disconnect);
        }
        let (mut frames, callback_usec) = if owner.is_some() {
            invoke_native_module_callback(&mut self.ctx, &block, NativeModuleCallback::Reply)
        } else {
            (Vec::new(), 0)
        };
        let free_kind = if owner.is_some() {
            ModuleContextKind::Synchronous
        } else {
            ModuleContextKind::DisconnectedCleanup
        };
        invoke_native_module_callback(&mut self.ctx, &block, NativeModuleCallback::Free(free_kind));
        {
            let mut state = lock(&block.shared.lifecycle);
            state.private_data.0 = std::ptr::null_mut();
            state.stage = ModuleBlockStage::Completed;
        }
        let thread_frames = {
            let mut replies = lock(&block.shared.replies);
            replies.is_sealed = true;
            std::mem::replace(&mut replies.builder, ReplyBuilder::new()).into_frames()
        };
        let dirty = self.commit_native_callback_effects(scope.finish());
        if let Some(token) = owner.filter(|_| self.native_module_owner(&block).is_some()) {
            frames.extend(thread_frames);
            self.finish_native_module_reply(token, &block, frames, callback_usec);
        }
        self.signal_keys_ready(&dirty);
    }

    pub(crate) fn timeout_native_module_client(
        &mut self,
        token: Token,
        forced_reply: Option<Value>,
    ) -> bool {
        let id = self
            .connection_registry
            .get(token)
            .and_then(|conn| conn.blocked.as_ref())
            .and_then(|blocked| match blocked.cmd {
                BlockedCmd::Module { id } => Some(id),
                _ => None,
            });
        let Some(id) = id else { return false };
        if self.command_execution_depth == 0 && !ModuleCallEffectScope::is_active() {
            // Only the outer timeout path can service a pending reservation.
            // Nested CLIENT UNBLOCK already runs under MainExecuting.
            self.service_module_thread_leases();
        }
        if self.ctx.is_shutdown_requested() {
            return true;
        }
        let Some(block) = self.ctx.module_registry_mut().native_blocks.remove(&id) else {
            return true;
        };
        if block.callbacks.timeout.is_none() || self.native_module_owner(&block).is_none() {
            self.ctx
                .module_registry_mut()
                .native_blocks
                .insert(id, block);
            return true;
        }
        lock(&block.shared.lifecycle).stage = ModuleBlockStage::DetachedTimeout;
        lock(&block.shared.replies).is_sealed = true;
        let scope = ModuleCallEffectScope::enter();
        let (frames, callback_usec) = match forced_reply {
            Some(reply) => {
                self.ctx.incr_error_reply("UNBLOCKED");
                (vec![ModuleReplyFrame::Value(reply)], 0)
            }
            None => {
                invoke_native_module_callback(&mut self.ctx, &block, NativeModuleCallback::Timeout)
            }
        };
        let dirty = self.commit_native_callback_effects(scope.finish());
        self.finish_native_module_reply(token, &block, frames, callback_usec);
        self.ctx
            .module_registry_mut()
            .native_blocks
            .insert(id, block);
        self.signal_keys_ready(&dirty);
        true
    }

    pub(crate) fn disconnect_native_module_client(&mut self, token: Token) {
        let ids: Vec<_> = self
            .ctx
            .module_registry()
            .native_blocks
            .iter()
            .filter_map(|(id, block)| {
                let stage = lock(&block.shared.lifecycle).stage;
                (block.shared.origin.token == token
                    && matches!(
                        stage,
                        ModuleBlockStage::Publishing | ModuleBlockStage::Armed
                    ))
                .then_some(*id)
            })
            .collect();
        for id in ids {
            if let Some(block) = self.ctx.module_registry().native_blocks.get(&id) {
                // Teardown may be reached by publication or tracking delivery
                // while Release is committing. Defer C until the outer safe point.
                lock(&block.shared.lifecycle).stage = ModuleBlockStage::DisconnectPending;
                lock(&block.shared.replies).is_sealed = true;
            }
        }
    }

    fn finish_native_module_reply(
        &mut self,
        token: Token,
        block: &NativeModuleBlock,
        frames: Vec<ModuleReplyFrame>,
        callback_usec: u64,
    ) {
        if self.ctx.is_shutdown_requested() {
            return;
        }
        let reply_bytes = if let Some(conn) = self.connection_registry.get_mut(token) {
            conn.resume_command_effects();
            let before = conn.output_buffer_bytes();
            queue_module_reply_frames(conn, frames);
            conn.set_last_write_offset(self.ctx.replication().primary_repl_offset());
            conn.output_buffer_bytes().saturating_sub(before) as u64
        } else {
            return;
        };
        // Even an empty frame list owns the reply path; it must not invent NULL.
        self.complete_blocked_command_reply(token, Value::Null, true);
        let name = String::from_utf8_lossy(&block.shared.origin.args.name).to_ascii_lowercase();
        let duration_us = block.active_time_usec.saturating_add(callback_usec);
        let stat = self
            .ctx
            .stats_mut()
            .cmd_stats
            .entry(name.clone())
            .or_default();
        stat.calls = stat.calls.saturating_add(1);
        stat.usec = stat.usec.saturating_add(duration_us);
        self.ctx.incr_total_commands(1);
        self.ctx.add_active_time_usec(callback_usec);
        self.ctx.record_command_latency(&name, duration_us);
        self.record_command_slot_metrics(
            &block.shared.origin.args,
            &[],
            crate::runtime::server::worker::cluster::migration::accounting::SlotMetricSample {
                cpu_usec: duration_us,
                network_bytes_in: block.shared.origin.request_bytes as u64,
                network_bytes_out: reply_bytes,
            },
        );
    }

    fn commit_native_callback_effects(
        &mut self,
        effects: Vec<ModuleCallEffects>,
    ) -> Vec<(u32, Bytes)> {
        let mut unit = ModuleCallEffects::default();
        for effect in effects {
            if unit.append(effect).is_err() {
                if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
                    bridge.close();
                }
                self.ctx.set_shutdown_requested(true);
            }
        }
        for effect in crate::framework::capabilities::module::finish_module_post_notification_jobs(
            &mut self.ctx,
        ) {
            if unit.append(effect).is_err() {
                if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
                    bridge.close();
                }
                self.ctx.set_shutdown_requested(true);
            }
        }
        let units = if unit.is_empty() {
            Vec::new()
        } else {
            vec![unit]
        };
        let dirty = self.commit_pending_module_effects(units, None, None, None);
        match self.flush_native_module_aof_barrier() {
            AofBarrierOutcome::Ready
            | AofBarrierOutcome::PendingInitialRewrite
            | AofBarrierOutcome::RetryPending => {}
            AofBarrierOutcome::Failed => {
                if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
                    bridge.close();
                }
                self.ctx.set_shutdown_requested(true);
            }
        }
        dirty
    }

    pub(crate) fn shutdown_native_module_blocks(&mut self) {
        if self.command_execution_depth != 0 || ModuleCallEffectScope::is_active() {
            return;
        }
        if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
            // Closing revokes executable leases before terminal cleanup C.
            bridge.close();
        }
        let blocks = std::mem::take(&mut self.ctx.module_registry_mut().native_blocks);
        for (_, block) in blocks {
            lock(&block.shared.replies).is_sealed = true;
            let stage = lock(&block.shared.lifecycle).stage;
            let scope = ModuleCallEffectScope::enter();
            if matches!(
                stage,
                ModuleBlockStage::Publishing
                    | ModuleBlockStage::Armed
                    | ModuleBlockStage::DisconnectPending
            ) {
                lock(&block.shared.lifecycle).stage = ModuleBlockStage::DetachedDisconnect;
                invoke_native_module_callback(
                    &mut self.ctx,
                    &block,
                    NativeModuleCallback::Disconnect,
                );
            }
            if lock(&block.shared.lifecycle).completion_queued {
                invoke_native_module_callback(
                    &mut self.ctx,
                    &block,
                    NativeModuleCallback::Free(ModuleContextKind::DisconnectedCleanup),
                );
                let mut state = lock(&block.shared.lifecycle);
                state.private_data.0 = std::ptr::null_mut();
                state.stage = ModuleBlockStage::Completed;
            }
            let dirty = self.commit_native_callback_effects(scope.finish());
            self.signal_keys_ready(&dirty);
        }
    }
}

#[cfg(test)]
#[path = "module_blocked_tests.rs"]
mod module_blocked_tests;
