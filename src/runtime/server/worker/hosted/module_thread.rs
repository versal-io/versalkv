//! Exclusive native-thread leases serviced only at outer safe points.

use crate::engine::extension::module::api::call::pending::{
    ModuleCallEffectScope, ModuleCallEffects,
};
use crate::engine::extension::module::thread_bridge::ModuleThreadRequest;
use crate::runtime::server::worker::connection::write::AofBarrierOutcome;
use crate::runtime::server::worker::ServerWorkerState;

impl ServerWorkerState {
    pub(crate) fn close_module_thread_bridge(&mut self) {
        if let Some(bridge) = &self.ctx.module_registry().thread_bridge {
            bridge.close();
        }
        self.shutdown_native_module_blocks();
    }

    pub(crate) fn service_module_thread_leases(&mut self) {
        if self.command_execution_depth != 0 || ModuleCallEffectScope::is_active() {
            return;
        }
        let Some(bridge) = self.ctx.module_registry().thread_bridge.clone() else {
            return;
        };
        loop {
            let Some(lease) = bridge.take_lease() else {
                // Taking MainExecuting and publishing a successful TryLock
                // reservation share the bridge mutex. No C runs in between.
                if bridge.resume_main() || bridge.is_closing() {
                    break;
                }
                continue;
            };
            let time_scope =
                crate::foundation::io::sys::time::enter_isolated_command_time_snapshot();
            let scope = ModuleCallEffectScope::enter();
            let mut release_reply = None;
            loop {
                // No cron, readiness or callback continuation runs while a
                // native thread owns the lease, including during timed waits.
                self.check_pending_signal();
                if self.ctx.is_shutdown_requested() || self.ctx.shutdown_state().asap {
                    bridge.close();
                    break;
                }
                let request = match bridge.wait_request(lease) {
                    Ok(Some(request)) => request,
                    Ok(None) => continue,
                    Err(_) => break,
                };
                if let Err(errno) = bridge.validate_request(&request) {
                    request.cancel(errno);
                    continue;
                }
                match request {
                    ModuleThreadRequest::Call { request, reply, .. } => {
                        let outcome =
                            crate::framework::capabilities::module::execute_module_thread_call(
                                &mut self.ctx,
                                request,
                            );
                        reply.complete(outcome);
                    }
                    ModuleThreadRequest::ContextFlags { context, reply, .. } => {
                        reply.complete(
                            crate::framework::capabilities::module::module_thread_context_flags(
                                &self.ctx, &context,
                            ),
                        );
                    }
                    ModuleThreadRequest::SelectDb { db_idx, reply, .. } => {
                        reply.complete(self.ctx.db(db_idx).is_some());
                    }
                    ModuleThreadRequest::Release { reply, .. } => {
                        release_reply = Some(reply);
                        break;
                    }
                }
            }
            let mut unit = ModuleCallEffects::default();
            for effects in scope.finish() {
                if unit.append(effects).is_err() {
                    bridge.close();
                    self.ctx.set_shutdown_requested(true);
                }
            }
            // Post jobs are appended after the original lease has been
            // aggregated, preserving one propagation unit for the lock range.
            for effects in
                crate::framework::capabilities::module::finish_module_post_notification_jobs(
                    &mut self.ctx,
                )
            {
                if unit.append(effects).is_err() {
                    bridge.close();
                    self.ctx.set_shutdown_requested(true);
                }
            }
            let units = if unit.is_empty() {
                Vec::new()
            } else {
                vec![unit]
            };
            let dirty_keys = self.commit_pending_module_effects(units, None, None, None);
            match self.flush_native_module_aof_barrier() {
                AofBarrierOutcome::Ready
                | AofBarrierOutcome::PendingInitialRewrite
                | AofBarrierOutcome::RetryPending => {}
                AofBarrierOutcome::Failed => self.ctx.set_shutdown_requested(true),
            }
            if self.ctx.is_shutdown_requested() {
                bridge.close();
            }
            bridge.finish_lease(lease);
            drop(time_scope);
            if let Some(reply) = release_reply {
                if bridge.is_closing() {
                    reply.cancel(libc::ESHUTDOWN);
                } else {
                    reply.complete(());
                }
            }
            self.refresh_event_loop_cycle_time_after_execution();
            self.signal_keys_ready(&dirty_keys);
            self.schedule_pending_replica_writes_before_sleep();
            if !bridge.is_closing() {
                // Ordinary wakeups may re-enter commands only after the lease
                // and its isolated clock are gone and Release is acknowledged.
                self.reject_blocked_waitaof_local_if_aof_disabled();
                self.check_wait_replica_wakeups();
            }
        }
        if bridge.is_closing() {
            self.ctx.set_shutdown_requested(true);
            return;
        }
        self.service_native_module_completions();
    }
}
