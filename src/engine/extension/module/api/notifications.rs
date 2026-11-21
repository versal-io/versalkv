//! Keyspace callback dispatch and post-notification job ABI.

use libc::{c_int, c_void};
use std::ffi::{CStr, CString};

use super::active_registry::with_active_registry;
use super::ActiveModuleContextScope;
use crate::engine::extension::module::capability::runtime_from_opaque;
use crate::engine::extension::module::notifications::{
    ModuleKeyspaceEvent, ModulePostNotificationCallback, ModulePostNotificationFree,
    ModulePostNotificationJob,
};
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleCtx, ValkeyModuleString,
    VALKEYMODULE_ERR, VALKEYMODULE_OK,
};

struct BroadcastScope(Vec<String>);

impl Drop for BroadcastScope {
    fn drop(&mut self) {
        with_active_registry(|registry| {
            for name in &self.0 {
                registry.notifications.release_module(name);
            }
        });
    }
}

/// # Safety
/// Source is a live module context; its runtime and active registry outlive all callbacks.
// SAFETY: the caller owns source and all synchronous runtime/registry scopes through dispatch.
pub(crate) unsafe fn dispatch_module_keyspace_events(
    source: *mut ValkeyModuleCtx,
    events: Vec<ModuleKeyspaceEvent>,
) {
    if source.is_null() {
        return;
    }
    // SAFETY: source is live and copied state carries no borrow across callbacks.
    let (runtime_handle, resp_version, getapi, username) = unsafe {
        let Some(internal) = (*source).internal.as_ref() else {
            return;
        };
        let username = if internal.username.is_null() {
            CString::new("default").expect("static username has no NUL")
        } else {
            CStr::from_ptr(internal.username).to_owned()
        };
        (
            internal.runtime_handle,
            internal.resp_version,
            (*source).getapifuncptr,
            username,
        )
    };
    for event in events {
        let Ok(event_name) = CString::new(event.event) else {
            continue;
        };
        let subscribers = with_active_registry(|registry| {
            let subscribers = registry.keyspace_callbacks(event.event_type);
            for subscriber in &subscribers {
                registry
                    .notifications
                    .protect_module(&subscriber.module_name);
            }
            subscribers
        })
        .unwrap_or_default();
        let _broadcast = BroadcastScope(
            subscribers
                .iter()
                .map(|subscriber| subscriber.module_name.clone())
                .collect(),
        );
        let key = Box::into_raw(ValkeyModuleString::from_bytes(&event.key));
        for subscriber in subscribers {
            let is_active = with_active_registry(|registry| {
                registry.notifications.enter_subscriber(
                    &subscriber.module_name,
                    subscriber.subscription,
                    subscriber.options.get() & (1 << 3) != 0,
                )
            })
            .unwrap_or(false);
            if !is_active {
                continue;
            }
            let mut internal = create_module_cmd_ctx(
                &subscriber.module_name,
                "",
                subscriber.module_ref,
                runtime_handle,
            );
            internal.module_options = Some(subscriber.options);
            // SAFETY: the constructor allocated this uniquely owned username.
            unsafe { drop(CString::from_raw(internal.username)) };
            internal.username = username.clone().into_raw();
            internal.kind =
                crate::engine::extension::module::thread_bridge::ModuleContextKind::Notification;
            internal.db_idx = event.db_idx as c_int;
            internal.resp_version = resp_version;
            internal.module_identity = with_active_registry(|registry| {
                registry
                    .lookup(&subscriber.module_name)
                    .and_then(|module| module.identity.clone())
            })
            .flatten();
            let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
            // SAFETY: this callback context is freshly allocated and uniquely owned.
            unsafe { (*ctx).getapifuncptr = getapi };
            {
                let _context = ActiveModuleContextScope::enter(ctx);
                (subscriber.callback)(ctx, event.event_type, event_name.as_ptr(), key);
            }
            // SAFETY: callback returned; automatic allocations are reclaimed before unpinning.
            unsafe { free_module_ctx(ctx) };
            with_active_registry(|registry| {
                registry
                    .notifications
                    .leave_subscriber(&subscriber.module_name, subscriber.subscription);
            });
        }
        // SAFETY: the dispatcher owns one key reference; retained module references survive.
        unsafe { ValkeyModuleString::release(key) };
    }
}

// SAFETY: a non-null ctx is live for this synchronous ABI call. Private data is opaque.
pub unsafe extern "C" fn ValkeyModule_AddPostNotificationJob(
    ctx: *mut ValkeyModuleCtx,
    callback: Option<ModulePostNotificationCallback>,
    data: *mut c_void,
    free: Option<ModulePostNotificationFree>,
) -> c_int {
    if ctx.is_null() {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: ctx is live; validate its internal context before copying runtime state.
    let Some(internal) = (unsafe { (*ctx).internal.as_ref() }) else {
        return VALKEYMODULE_ERR;
    };
    let Some(callback) = callback else {
        return VALKEYMODULE_ERR;
    };
    if internal.db_idx < 0 {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: the runtime is borrowed only for admission, never across a module callback.
    let is_rejected = unsafe { runtime_from_opaque(internal.runtime_handle) }
        .is_none_or(|runtime| runtime.is_loading() || runtime.is_readonly_replica());
    if is_rejected {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: the live context owns its NUL-terminated module name.
    let module_name = unsafe { super::ctx::get_module_name(ctx) };
    let db_idx = internal.db_idx as u32;
    let is_enqueued = with_active_registry(|registry| {
        if registry.lookup(&module_name).is_none() {
            return false;
        }
        // Ownership transfers only on success; rejected admission never runs free.
        registry.notifications.enqueue(ModulePostNotificationJob {
            module_name,
            db_idx,
            callback,
            data,
            free,
        });
        true
    })
    .unwrap_or(false);
    if is_enqueued {
        VALKEYMODULE_OK
    } else {
        VALKEYMODULE_ERR
    }
}
