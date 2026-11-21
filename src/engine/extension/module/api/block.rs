//! Native blocked-client handles and completion ownership.

#![allow(non_snake_case)]

use super::super::blocking::{
    ModuleBlockCallbacks, ModuleBlockStage, ModuleBlockedShared, ModuleDisconnectCallback,
    ModuleFreePrivateData,
};
use super::super::thread_bridge::{lock, ModuleContextKind};
use super::super::{
    ValkeyModuleBlockedClient, ValkeyModuleCmdFunc, ValkeyModuleCtx, VALKEYMODULE_ERR,
    VALKEYMODULE_OK,
};
use libc::{c_int, c_longlong, c_void};

/// # Safety
/// ctx is a live synchronous command context; callbacks obey the module ABI.
// SAFETY: the caller owns ctx throughout this synchronous ABI invocation.
pub unsafe extern "C" fn ValkeyModule_BlockClient(
    ctx: *mut ValkeyModuleCtx,
    reply_callback: ValkeyModuleCmdFunc,
    timeout_callback: ValkeyModuleCmdFunc,
    free_privdata: ModuleFreePrivateData,
    timeout_ms: c_longlong,
) -> *mut ValkeyModuleBlockedClient {
    // SAFETY: all context borrows end before the runtime invokes module code.
    let result = unsafe {
        let Some(internal) = ctx.as_mut().and_then(|ctx| ctx.internal.as_mut()) else {
            crate::foundation::io::sys::os::set_errno(libc::EINVAL);
            return std::ptr::null_mut();
        };
        if internal.kind != ModuleContextKind::Synchronous || internal.blocked_client.is_some() {
            crate::foundation::io::sys::os::set_errno(libc::EINVAL);
            return std::ptr::null_mut();
        }
        let (Some(module), Some(origin)) = (
            internal.module_identity.clone(),
            internal.block_origin.clone(),
        ) else {
            crate::foundation::io::sys::os::set_errno(libc::EINVAL);
            return std::ptr::null_mut();
        };
        let Some(runtime) = super::super::capability::runtime_from_opaque(internal.runtime_handle)
        else {
            crate::foundation::io::sys::os::set_errno(libc::ENOTSUP);
            return std::ptr::null_mut();
        };
        runtime.block_module_client(
            module,
            origin,
            ModuleBlockCallbacks {
                reply: reply_callback,
                timeout: timeout_callback,
                free: free_privdata,
                disconnect: None,
            },
            timeout_ms,
        )
    };
    match result {
        Ok(shared) => {
            // SAFETY: ctx is still live; the returned C handle receives its own reference.
            unsafe {
                (*(*ctx).internal).blocked_client = Some(shared.clone());
                ModuleBlockedShared::export(shared)
            }
        }
        Err(errno) => {
            crate::foundation::io::sys::os::set_errno(errno);
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// bc holds an unconsumed C export; successful completion transfers privdata.
// SAFETY: bc owns an unconsumed export and data remains valid until main-thread cleanup.
pub unsafe extern "C" fn ValkeyModule_UnblockClient(
    bc: *mut ValkeyModuleBlockedClient,
    privdata: *mut c_void,
) -> c_int {
    // SAFETY: bc's caller-owned export remains live until acceptance is decided.
    let Some(shared) = (unsafe { ModuleBlockedShared::clone_export(bc) }) else {
        crate::foundation::io::sys::os::set_errno(libc::EINVAL);
        return VALKEYMODULE_ERR;
    };
    match shared.accept_unblock(privdata) {
        Ok(()) => {
            // SAFETY: acceptance consumes the sole C export, once.
            unsafe { ModuleBlockedShared::consume_export(bc) };
            VALKEYMODULE_OK
        }
        Err(errno) => {
            crate::foundation::io::sys::os::set_errno(errno);
            VALKEYMODULE_ERR
        }
    }
}

/// # Safety
/// bc is a live export and this setter runs on the database owner thread.
// SAFETY: the caller retains bc and invokes this setter on the main thread.
pub unsafe extern "C" fn ValkeyModule_SetDisconnectCallback(
    bc: *mut ValkeyModuleBlockedClient,
    callback: ModuleDisconnectCallback,
) {
    // SAFETY: the valid exported handle is retained during registry access.
    let Some(shared) = (unsafe { ModuleBlockedShared::clone_export(bc) }) else {
        return;
    };
    super::active_registry::with_active_registry(|registry| {
        if let Some(record) = registry.native_blocks.get_mut(&shared.id) {
            record.callbacks.disconnect = callback;
        }
    });
}

/// # Safety
/// ctx is null or a live exclusively accessed module context.
// SAFETY: the caller retains exclusive access to ctx for the scalar query.
pub unsafe extern "C" fn ValkeyModule_GetBlockedClientPrivateData(
    ctx: *mut ValkeyModuleCtx,
) -> *mut c_void {
    // SAFETY: the optional internal context is live while reading the scalar.
    unsafe {
        ctx.as_ref()
            .and_then(|ctx| ctx.internal.as_ref())
            .map_or(std::ptr::null_mut(), |internal| {
                internal.blocked_private_data
            })
    }
}

/// # Safety
/// bc is null or a live unconsumed exported handle.
// SAFETY: the caller keeps the exported handle live while its state is queried.
pub unsafe extern "C" fn ValkeyModule_BlockClientGetPrivateData(
    bc: *mut ValkeyModuleBlockedClient,
) -> *mut c_void {
    // SAFETY: clone_export retains the valid export through the short mutex borrow.
    unsafe { ModuleBlockedShared::clone_export(bc) }.map_or(std::ptr::null_mut(), |shared| {
        lock(&shared.lifecycle).private_data.0
    })
}

/// # Safety
/// bc is live; data remains valid until replaced or transferred through Unblock.
// SAFETY: the caller retains the handle and the supplied private data.
pub unsafe extern "C" fn ValkeyModule_BlockClientSetPrivateData(
    bc: *mut ValkeyModuleBlockedClient,
    data: *mut c_void,
) {
    // SAFETY: the caller's export is retained through the short mutex borrow.
    if let Some(shared) = unsafe { ModuleBlockedShared::clone_export(bc) } {
        let mut state = lock(&shared.lifecycle);
        if !state.completion_queued && state.stage != ModuleBlockStage::Completed {
            state.private_data = super::super::blocking::ModulePrivateData(data);
        }
    }
}

// SAFETY: ctx is null or a live module callback context.
unsafe fn context_kind(ctx: *mut ValkeyModuleCtx) -> Option<ModuleContextKind> {
    // SAFETY: the optional internal allocation is live for this scalar read.
    unsafe {
        ctx.as_ref()?
            .internal
            .as_ref()
            .map(|internal| internal.kind)
    }
}

/// # Safety
/// ctx is null or a live module context.
// SAFETY: the caller keeps the callback context live for this scalar query.
pub unsafe extern "C" fn ValkeyModule_IsBlockedReplyRequest(ctx: *mut ValkeyModuleCtx) -> c_int {
    // SAFETY: the caller keeps ctx live for the query.
    c_int::from(unsafe { context_kind(ctx) } == Some(ModuleContextKind::BlockedReply))
}

/// # Safety
/// ctx is null or a live module context.
// SAFETY: the caller keeps the callback context live for this scalar query.
pub unsafe extern "C" fn ValkeyModule_IsBlockedTimeoutRequest(ctx: *mut ValkeyModuleCtx) -> c_int {
    // SAFETY: the caller keeps ctx live for the query.
    c_int::from(unsafe { context_kind(ctx) } == Some(ModuleContextKind::BlockedTimeout))
}

/// # Safety
/// ctx is null or a live module context.
// SAFETY: the caller keeps the callback context live for this scalar query.
pub unsafe extern "C" fn ValkeyModule_BlockedClientDisconnected(
    ctx: *mut ValkeyModuleCtx,
) -> c_int {
    // SAFETY: the caller keeps ctx live for the query.
    c_int::from(unsafe { context_kind(ctx) } == Some(ModuleContextKind::DisconnectedCleanup))
}
