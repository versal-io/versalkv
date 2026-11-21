//! Main-thread adapters for owned native thread requests.

use super::ModuleInvocationScope;
use crate::engine::command::context::CommandConn;
use crate::engine::extension::module::api::call::call_conn::ModuleCallConn;
use crate::engine::extension::module::api::call::call_with_reply_owned;
use crate::engine::extension::module::api::call::pending::{
    ModuleCallEffectScope, ModuleCallEffects,
};
use crate::engine::extension::module::api::{ActiveModuleContextScope, ActiveModuleRegistryScope};
use crate::engine::extension::module::capability::{
    ActiveModuleHostScope, ActiveModuleRuntimeScope, ModuleCommandConnScope, ModuleRuntime,
};
use crate::engine::extension::module::thread_bridge::{
    ModuleCallOutcome, ModuleContextKind, ModuleThreadSnapshot, OwnedModuleCall,
};
use crate::engine::extension::module::ValkeyModuleString;
use crate::framework::context::ServerContext;
use std::ffi::CString;

impl ServerContext {
    /// Capture actual lazy deletions before the source Call and its callbacks.
    /// The caller appends these owned effects ahead of source propagation.
    pub(crate) fn take_module_call_expiration_effects(&mut self) -> ModuleCallEffects {
        if !self.dbs().has_pending_expirations() {
            return ModuleCallEffects::default();
        }
        let expired = self.drain_expired_deletions();
        let fields = self.drain_pending_hexpired();
        let expired_fields = self.drain_lazy_expired_hash_fields();
        self.incr_expired_keys(expired.len() as u64);
        self.incr_expired_fields(expired_fields);
        let dirty_delta = expired
            .len()
            .checked_add(fields.len())
            .and_then(|count| i64::try_from(count).ok());
        if dirty_delta.is_none() {
            if let Some(bridge) = &self.module_registry().thread_bridge {
                bridge.close();
            }
            self.set_shutdown_requested(true);
        }
        let mut effects = ModuleCallEffects {
            dirty_delta,
            ..Default::default()
        };
        let delete = if self.config().server.lazyfree_lazy_expire {
            b"UNLINK".as_slice()
        } else {
            b"DEL".as_slice()
        };
        let mut notified_hashes = std::collections::HashSet::new();
        let mut hash_keys = Vec::new();
        for (db_idx, key, field) in fields {
            effects
                .dirty_keys
                .push((db_idx, bytes::Bytes::copy_from_slice(&key)));
            effects
                .propagation
                .push(crate::engine::extension::script::PropagationOp::both(
                    db_idx,
                    crate::engine::distributed::replication::protocol::encode_command(&[
                        b"HDEL", &key, &field,
                    ]),
                ));
            if notified_hashes.insert((db_idx, key.clone())) {
                self.append_module_expiration_notification(
                    &mut effects,
                    db_idx,
                    &key,
                    'h',
                    "hexpired",
                );
                hash_keys.push((db_idx, key));
            }
        }
        for (db_idx, key) in hash_keys {
            if self
                .db(db_idx)
                .is_none_or(|db| db.get_allow_expired(&key).is_none())
            {
                effects
                    .propagation
                    .push(crate::engine::extension::script::PropagationOp::both(
                        db_idx,
                        crate::engine::distributed::replication::protocol::encode_command(&[
                            delete, &key,
                        ]),
                    ));
                self.append_module_expiration_notification(&mut effects, db_idx, &key, 'g', "del");
            }
        }
        for (db_idx, key) in expired {
            effects
                .dirty_keys
                .push((db_idx, bytes::Bytes::copy_from_slice(&key)));
            effects
                .propagation
                .push(crate::engine::extension::script::PropagationOp::both(
                    db_idx,
                    crate::engine::distributed::replication::protocol::encode_command(&[
                        delete, &key,
                    ]),
                ));
            self.append_module_expiration_notification(&mut effects, db_idx, &key, 'x', "expired");
        }
        effects
    }

    fn append_module_expiration_notification(
        &self,
        effects: &mut ModuleCallEffects,
        db_idx: u32,
        key: &[u8],
        class: char,
        event: &str,
    ) {
        crate::engine::distributed::pubsub::PubSubState::publish_keyspace_event(
            self.config(),
            &mut effects.notifications,
            db_idx,
            key,
            class,
            event,
        );
    }
}

#[derive(Clone, Copy)]
pub(crate) enum NativeModuleCallback {
    Reply,
    Timeout,
    Disconnect,
    Free(ModuleContextKind),
}

pub(crate) fn invoke_native_module_callback(
    ctx: &mut ServerContext,
    block: &crate::engine::extension::module::blocking::NativeModuleBlock,
    callback: NativeModuleCallback,
) -> (
    Vec<crate::engine::extension::module::api::ModuleReplyFrame>,
    u64,
) {
    use crate::engine::extension::module::thread_bridge::lock;
    let _time_scope = crate::foundation::io::sys::time::enter_command_time_snapshot_lazy();
    let _host_scope = ActiveModuleHostScope::enter(None);
    let kind = match callback {
        NativeModuleCallback::Reply => ModuleContextKind::BlockedReply,
        NativeModuleCallback::Timeout => ModuleContextKind::BlockedTimeout,
        NativeModuleCallback::Disconnect => ModuleContextKind::DisconnectedCleanup,
        NativeModuleCallback::Free(kind) => kind,
    };
    let origin = &block.shared.origin;
    let args = super::module_argv(&origin.args, ctx.module_registry());
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        &block.shared.module.name,
        "module.blocked-callback",
        origin.db_idx,
        origin.resp_version,
        Some(&origin.username),
        args,
    );
    let private_data = lock(&block.shared.lifecycle).private_data.0;
    // SAFETY: only this fresh main-thread invocation owns the internal context.
    // The callback receives private data borrowed from the live blocked record.
    unsafe {
        let internal = &mut *(*invocation.module_ctx()).internal;
        internal.kind = kind;
        internal.blocked_client = Some(block.shared.clone());
        internal.blocked_private_data = private_data;
    }
    let mut conn = ModuleCallConn::with_tracking(origin.db_idx, None);
    conn.set_resp_version(origin.resp_version);
    conn.set_username(origin.username.clone());
    let start = std::time::Instant::now();
    {
        let _conn_scope = ModuleCommandConnScope::enter(&mut conn);
        let _ctx_scope = ActiveModuleContextScope::enter(invocation.module_ctx());
        let _runtime_scope = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
        let _registry_scope = ActiveModuleRegistryScope::enter(invocation.registry());
        match callback {
            NativeModuleCallback::Reply | NativeModuleCallback::Timeout => {
                let func = if matches!(callback, NativeModuleCallback::Reply) {
                    block.callbacks.reply
                } else {
                    block.callbacks.timeout
                };
                if let Some(func) = func {
                    func(
                        invocation.module_ctx(),
                        invocation.argv(),
                        invocation.argc(),
                    );
                }
            }
            NativeModuleCallback::Disconnect => {
                if let Some(func) = block.callbacks.disconnect {
                    let handle = std::sync::Arc::as_ptr(&block.shared).cast_mut().cast();
                    func(invocation.module_ctx(), handle);
                }
            }
            NativeModuleCallback::Free(_) => {
                if let Some(func) = block.callbacks.free.filter(|_| !private_data.is_null()) {
                    func(invocation.module_ctx(), private_data);
                }
            }
        }
    }
    let frames = invocation.take_reply_frames();
    drop(invocation);
    ctx.apply_module_call_effects(conn.take_recorded_effects());
    let notifications = super::dispatch_module_keyspace_notifications(&mut conn, ctx);
    ctx.apply_module_call_effects(notifications);
    (
        frames,
        start.elapsed().as_micros().min(u64::MAX as u128) as u64,
    )
}

pub(crate) fn execute_module_thread_call(
    ctx: &mut ServerContext,
    request: OwnedModuleCall,
) -> ModuleCallOutcome {
    let _time_scope = crate::foundation::io::sys::time::enter_command_time_snapshot_lazy();
    if request.argv.is_empty()
        || request.argv.len() > libc::c_int::MAX as usize
        || ctx.db(request.context.db_idx).is_none()
    {
        return ModuleCallOutcome::cancelled(libc::EINVAL);
    }
    let effects = ModuleCallEffectScope::enter();
    let _host_scope = ActiveModuleHostScope::enter(None);
    let module_name = request
        .context
        .module
        .as_ref()
        .map_or("", |module| module.name.as_str());
    let argv = request
        .argv
        .iter()
        .map(|arg| Box::into_raw(ValkeyModuleString::from_bytes(arg)))
        .collect();
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        module_name,
        "module.thread-call",
        request.context.db_idx,
        request.context.resp_version,
        Some(&request.context.username),
        argv,
    );
    // SAFETY: the invocation owns a fresh main-thread context. Only copied
    // authentication and kind facts are installed, never a thread context pointer.
    unsafe {
        let internal = &mut *(*invocation.module_ctx()).internal;
        internal.kind = ModuleContextKind::InternalCall;
        if let Some(user) = &request.context.auth_user {
            match CString::new(user.as_str()) {
                Ok(user) => internal.module_auth_user = user.into_raw(),
                Err(_) => return ModuleCallOutcome::cancelled(libc::EINVAL),
            }
        }
    }
    let mut conn = ModuleCallConn::with_tracking(request.context.db_idx, None);
    conn.set_resp_version(request.context.resp_version);
    conn.set_username(request.context.username.clone());
    let outcome = {
        let _conn_scope = ModuleCommandConnScope::enter(&mut conn);
        let _ctx_scope = ActiveModuleContextScope::enter(invocation.module_ctx());
        let _runtime_scope = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
        let _registry_scope = ActiveModuleRegistryScope::enter(invocation.registry());
        // SAFETY: all scopes and argv belong to this main-thread invocation and
        // end before the owned outcome is returned to a native thread.
        unsafe {
            call_with_reply_owned(
                invocation.module_ctx(),
                invocation.argv(),
                invocation.argc(),
                request.format.raw(),
                None,
            )
        }
    };
    drop(invocation);
    // The general Call hook drains before notifications. Preserve a fallback
    // for older capability implementations without moving deletions to Release.
    let mut unit = ctx.take_module_call_expiration_effects();
    for effect in effects
        .finish()
        .into_iter()
        .chain([conn.take_recorded_effects()])
    {
        if unit.append(effect).is_err() {
            if let Some(bridge) = &ctx.module_registry().thread_bridge {
                bridge.close();
            }
            ctx.set_shutdown_requested(true);
        }
    }
    if !unit.is_empty() {
        ctx.apply_module_call_effects(unit);
    }
    outcome
}

pub(crate) fn module_thread_context_flags(
    ctx: &ServerContext,
    snapshot: &ModuleThreadSnapshot,
) -> libc::c_int {
    let mut flags = <ServerContext as ModuleRuntime>::context_flags(ctx);
    if snapshot.resp_version == 3 {
        flags |= 1 << 22;
    }
    flags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::api::call::{
        pending::ModuleCallEffectScope, ModuleCallFormat,
    };
    use crate::engine::extension::module::thread_bridge::{next_id, ModuleThreadContextId};
    use crate::engine::extension::script::PropagationTargets;
    use crate::foundation::io::store::Robj;
    use crate::framework::CommandRegistry;
    use bytes::Bytes;

    fn call(ctx: &mut ServerContext, parts: &[&[u8]], flags: libc::c_int) -> ModuleCallOutcome {
        execute_module_thread_call(
            ctx,
            OwnedModuleCall {
                context: ModuleThreadSnapshot {
                    id: ModuleThreadContextId(next_id().unwrap()),
                    module: None,
                    db_idx: 0,
                    resp_version: 2,
                    username: "default".into(),
                    auth_user: None,
                },
                argv: parts
                    .iter()
                    .map(|part| Bytes::copy_from_slice(part))
                    .collect(),
                format: ModuleCallFormat::from_raw(flags),
            },
        )
    }

    #[test]
    fn thread_runtime_review_call_expiry_precedes_recreation_and_drains_once() {
        for is_lazyfree in [false, true] {
            let mut ctx = ServerContext::new(1);
            let mut registry = CommandRegistry::new();
            crate::framework::commands::register_all(&mut registry);
            ctx.set_command_registry_snapshot(&registry);
            ctx.config_mut().server.lazyfree_lazy_expire = is_lazyfree;
            let db = ctx.db_mut(0).unwrap();
            db.set(b"lease-key".to_vec(), Robj::string(b"old".as_slice()));
            db.set_expire(b"lease-key", 1);
            let _time = crate::foundation::io::sys::time::enter_command_time_snapshot(10_000);
            let scope = ModuleCallEffectScope::enter();
            assert_eq!(call(&mut ctx, &[b"GET", b"lease-key"], 0).call_errno, 0);
            assert!(ctx.drain_expired_deletions().is_empty());
            assert_eq!(
                call(&mut ctx, &[b"SET", b"lease-key", b"new"], 1).call_errno,
                0
            );
            assert_eq!(call(&mut ctx, &[b"PING"], 0).call_errno, 0);
            let mut unit =
                crate::engine::extension::module::api::call::pending::ModuleCallEffects::default();
            for effect in scope.finish() {
                unit.append(effect).unwrap();
            }
            let delete = if is_lazyfree {
                b"UNLINK".as_slice()
            } else {
                b"DEL".as_slice()
            };
            let encode = crate::engine::distributed::replication::protocol::encode_command;
            assert_eq!(
                unit.propagation
                    .iter()
                    .map(|op| (op.db_id, op.targets, op.bytes.clone()))
                    .collect::<Vec<_>>(),
                vec![
                    (0, PropagationTargets::Both, encode(&[delete, b"lease-key"])),
                    (
                        0,
                        PropagationTargets::Both,
                        encode(&[b"SET", b"lease-key", b"new"])
                    ),
                ]
            );
            assert_eq!(unit.dirty_delta, Some(2));
            assert!(unit
                .dirty_keys
                .contains(&(0, Bytes::from_static(b"lease-key"))));
            assert_eq!(
                ctx.db_mut(0).unwrap().get_string(b"lease-key"),
                Ok(Some(b"new".as_slice()))
            );
            assert!(ctx.drain_expired_deletions().is_empty());
        }
    }
}
