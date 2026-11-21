//! Notification callbacks and outer execution-unit jobs.

use super::ModuleInvocationScope;
use crate::engine::command::context::CommandConn;
use crate::engine::extension::module::api::call::call_conn::ModuleCallConn;
use crate::engine::extension::module::api::call::pending::{
    ModuleCallEffectScope, ModuleCallEffects,
};
use crate::engine::extension::module::api::{ActiveModuleContextScope, ActiveModuleRegistryScope};
use crate::engine::extension::module::capability::{
    ActiveModuleRuntimeScope, ModuleCommandConnScope,
};
use crate::engine::extension::module::ModuleRegistry;
use crate::framework::context::ServerContext;

#[cfg(test)]
#[path = "module_notification_review_tests.rs"]
mod review_tests;

pub(crate) fn dispatch_module_keyspace_notifications(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
) -> ModuleCallEffects {
    let events = conn.take_module_keyspace_events();
    if events.is_empty() {
        return ModuleCallEffects::default();
    }
    let tracking = crate::engine::extension::module::capability::ModuleClientTracking {
        id: conn.tracking_token().0,
        is_tracking: conn.tracking(),
        is_broadcast: conn.tracking_bcast(),
        is_optin: conn.tracking_optin(),
        is_optout: conn.tracking_optout(),
        is_caching: conn.tracking_caching(),
    };
    let mut callback_conn = ModuleCallConn::with_tracking(conn.selected_db(), Some(tracking));
    callback_conn.set_resp_version(conn.resp_version());
    callback_conn.set_username(conn.username().to_string());
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        "",
        "module.notification",
        conn.selected_db(),
        conn.resp_version(),
        Some(conn.username()),
        Vec::new(),
    );
    let collected = ModuleCallEffectScope::enter();
    {
        let _conn = ModuleCommandConnScope::enter(&mut callback_conn);
        let _runtime = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
        let _registry = ActiveModuleRegistryScope::enter(invocation.registry());
        // SAFETY: invocation owns the source context and scopes outlive synchronous callbacks.
        unsafe {
            crate::engine::extension::module::api::notifications::dispatch_module_keyspace_events(
                invocation.module_ctx(),
                events,
            );
        }
    }
    drop(invocation);
    let mut effects = callback_conn.take_recorded_effects();
    for callback in collected.finish() {
        append_notification_effects(&mut effects, callback, ctx);
    }
    effects
}

fn append_notification_effects(
    effects: &mut ModuleCallEffects,
    other: ModuleCallEffects,
    ctx: &mut ServerContext,
) {
    if effects.append(other).is_err() {
        log::error!("Module notification dirty effect overflow; requesting server shutdown");
        ctx.set_shutdown_requested(true);
        if let Some(bridge) = &ctx.module_registry().thread_bridge {
            bridge.close();
        }
    }
}

struct JobExecutionScope(*mut ModuleRegistry);

impl Drop for JobExecutionScope {
    fn drop(&mut self) {
        // SAFETY: the owning ServerContext outlives this execution scope; no callback is active.
        unsafe { (*self.0).notifications.end_jobs() };
    }
}

/// Run only at an outer execution-unit boundary, after capturing the source propagation.
/// Returns effects in FIFO order without submitting propagation.
pub(crate) fn finish_module_post_notification_jobs(
    ctx: &mut ServerContext,
) -> Vec<ModuleCallEffects> {
    if !ctx.module_registry_mut().notifications.begin_jobs() {
        return Vec::new();
    }
    let _jobs = JobExecutionScope(ctx.module_registry_mut());
    let mut effects = Vec::new();
    while let Some(job) = ctx.module_registry_mut().notifications.pop_job() {
        let module_name = job.module_name.clone();
        let mut conn = ModuleCallConn::with_tracking(job.db_idx, None);
        let mut invocation = ModuleInvocationScope::new(
            ctx,
            &module_name,
            "module.post-notification",
            job.db_idx,
            2,
            None,
            Vec::new(),
        );
        let collected = ModuleCallEffectScope::enter();
        {
            // SAFETY: the fresh invocation context is live and exclusively owned.
            unsafe {
                (*(*invocation.module_ctx()).internal).kind =
                    crate::engine::extension::module::thread_bridge::ModuleContextKind::InternalCall;
            }
            let _conn = ModuleCommandConnScope::enter(&mut conn);
            let _context = ActiveModuleContextScope::enter(invocation.module_ctx());
            let _runtime = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
            let _registry = ActiveModuleRegistryScope::enter(invocation.registry());
            (job.callback)(invocation.module_ctx(), job.data);
            // The library remains pinned while the private-data destructor runs.
            drop(job);
        }
        drop(invocation);
        let mut effect = conn.take_recorded_effects();
        for job_effect in collected.finish() {
            append_notification_effects(&mut effect, job_effect, ctx);
        }
        let callbacks = dispatch_module_keyspace_notifications(&mut conn, ctx);
        append_notification_effects(&mut effect, callbacks, ctx);
        if !effect.is_empty() {
            effects.push(effect);
        }
        ctx.module_registry_mut()
            .notifications
            .release_module(&module_name);
    }
    effects
}
