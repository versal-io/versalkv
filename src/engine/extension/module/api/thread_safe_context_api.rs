use libc::{c_char, size_t};
use std::ffi::{CStr, CString};

use super::super::{ValkeyModuleBlockedClient, ValkeyModuleCtx};
use crate::engine::extension::module::thread_bridge::{ModuleContextKind, ModuleThreadContext};
use crate::engine::extension::module::{create_module_ctx, free_module_ctx};

fn allocate_thread_context(context: ModuleThreadContext) -> *mut ValkeyModuleCtx {
    let name = context
        .snapshot
        .module
        .as_ref()
        .map_or("unknown", |module| module.name.as_str());
    let mut internal = super::super::create_module_internal_ctx(name, std::ptr::null_mut());
    internal.onload = 0;
    internal.kind = ModuleContextKind::ThreadSafe;
    internal.module_identity = context.snapshot.module.clone();
    internal.module_options = context
        .snapshot
        .module
        .as_ref()
        .map(|module| module.options.clone());
    internal.db_idx = context.snapshot.db_idx as libc::c_int;
    internal.resp_version = context.snapshot.resp_version as libc::c_int;
    // SAFETY: the constructor owns this CString and has not published internal.
    unsafe { drop(CString::from_raw(internal.username)) };
    internal.username = CString::new(context.snapshot.username.as_str())
        .unwrap_or_else(|_| CString::new("default").expect("static username has no NUL"))
        .into_raw();
    if let Some(username) = &context.snapshot.auth_user {
        if let Ok(username) = CString::new(username.as_str()) {
            internal.module_auth_user = username.into_raw();
        }
    }
    internal.thread_bridge = Some(context.bridge.clone());
    internal.blocked_client = context.blocked.clone();
    internal.thread_context = Some(context);
    let mut ctx = create_module_ctx(Box::into_raw(internal));
    ctx.getapifuncptr = super::module_get_api_impl as *mut libc::c_void;
    Box::into_raw(ctx)
}

fn finish_context(result: Result<ModuleThreadContext, libc::c_int>) -> *mut ValkeyModuleCtx {
    match result {
        Ok(context) => allocate_thread_context(context),
        Err(errno) => {
            crate::foundation::io::sys::os::set_errno(errno);
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// bc is null or a live BlockClient export not yet consumed by Unblock.
// SAFETY: the caller retains bc's exported reference through construction.
pub unsafe extern "C" fn ValkeyModule_GetThreadSafeContext(
    bc: *mut ValkeyModuleBlockedClient,
) -> *mut ValkeyModuleCtx {
    if bc.is_null() {
        let Some(bridge) = super::super::thread_bridge::process_bridge() else {
            return finish_context(Err(libc::ENOTSUP));
        };
        return finish_context(ModuleThreadContext::new(
            bridge,
            None,
            0,
            2,
            "default".to_string(),
            None,
        ));
    }
    // SAFETY: the caller guarantees a live C-owned reference while cloning it.
    let Some(shared) = (unsafe { super::super::blocking::ModuleBlockedShared::clone_export(bc) })
    else {
        return finish_context(Err(libc::EINVAL));
    };
    finish_context(ModuleThreadContext::new(
        shared.bridge.clone(),
        Some(shared.module.clone()),
        shared.origin.db_idx,
        shared.origin.resp_version,
        shared.origin.username.clone(),
        Some(shared),
    ))
}

/// Return a detached context associated with the same module as `ctx`.
// SAFETY: `ctx` must be null or point to a live ValkeyModuleCtx whose internal
// context remains valid for the duration of this call.
pub unsafe extern "C" fn ValkeyModule_GetDetachedThreadSafeContext(
    ctx: *mut ValkeyModuleCtx,
) -> *mut ValkeyModuleCtx {
    // SAFETY: source is only borrowed on its owning thread and copied by value.
    let Some(source) = (unsafe { ctx.as_ref().and_then(|ctx| ctx.internal.as_ref()) }) else {
        return finish_context(Err(libc::EINVAL));
    };
    let Some(bridge) = source.thread_bridge.clone() else {
        return finish_context(Err(libc::ENOTSUP));
    };
    let username = if source.username.is_null() {
        "default".to_string()
    } else {
        // SAFETY: the source context owns this live username CString.
        unsafe { CStr::from_ptr(source.username) }
            .to_string_lossy()
            .into_owned()
    };
    let auth_user = if source.module_auth_user.is_null() {
        None
    } else {
        // SAFETY: the source context owns this live authentication CString.
        Some(
            unsafe { CStr::from_ptr(source.module_auth_user) }
                .to_string_lossy()
                .into_owned(),
        )
    };
    finish_context(
        ModuleThreadContext::new(
            bridge,
            source.module_identity.clone(),
            source.db_idx as u32,
            source.resp_version as u8,
            username,
            None,
        )
        .map(|mut context| {
            context.snapshot.auth_user = auth_user;
            context
        }),
    )
}

/// # Safety
/// ctx is exclusively accessed by its native calling thread.
// SAFETY: the caller owns both context allocations throughout this borrow.
pub(crate) unsafe fn thread_context_mut<'a>(
    ctx: *mut ValkeyModuleCtx,
) -> Option<&'a mut ModuleThreadContext> {
    // SAFETY: the caller keeps both owned context allocations live and exclusive.
    unsafe { ctx.as_mut()?.internal.as_mut()?.thread_context.as_mut() }
}

/// # Safety
/// ctx is a live thread-safe context owned by the calling thread.
pub unsafe extern "C" fn ValkeyModule_ThreadSafeContextLock(ctx: *mut ValkeyModuleCtx) {
    // SAFETY: the caller provides its exclusively owned context.
    if let Some(context) = unsafe { thread_context_mut(ctx) } {
        match context
            .validate_identity()
            .and_then(|()| context.bridge.acquire())
        {
            Ok(()) => context.cancelled_errno = 0,
            Err(errno) => {
                context.cancelled_errno = errno;
                crate::foundation::io::sys::os::set_errno(errno);
            }
        }
    } else {
        crate::foundation::io::sys::os::set_errno(libc::EINVAL);
    }
}

/// # Safety
/// ctx is a live thread-safe context; the calling thread owns the bridge lease.
// SAFETY: the caller exclusively owns ctx and releases its own thread lease.
pub unsafe extern "C" fn ValkeyModule_ThreadSafeContextUnlock(ctx: *mut ValkeyModuleCtx) {
    // SAFETY: the caller provides its exclusively owned context.
    if let Some(context) = unsafe { thread_context_mut(ctx) } {
        if let Err(errno) = context.bridge.release() {
            context.cancelled_errno = errno;
            crate::foundation::io::sys::os::set_errno(errno);
        }
    } else {
        crate::foundation::io::sys::os::set_errno(libc::EINVAL);
    }
}

/// # Safety
/// ctx is a live thread-safe context owned by the calling thread.
// SAFETY: the caller exclusively owns ctx throughout the nonblocking attempt.
pub unsafe extern "C" fn ValkeyModule_ThreadSafeContextTryLock(
    ctx: *mut ValkeyModuleCtx,
) -> libc::c_int {
    // SAFETY: the caller provides its exclusively owned context.
    let result = unsafe { thread_context_mut(ctx) }
        .ok_or(libc::EINVAL)
        .and_then(|context| {
            context.validate_identity()?;
            context.bridge.try_acquire()?;
            context.cancelled_errno = 0;
            Ok(())
        });
    match result {
        Ok(()) => super::super::VALKEYMODULE_OK,
        Err(errno) => {
            crate::foundation::io::sys::os::set_errno(errno);
            super::super::VALKEYMODULE_ERR
        }
    }
}
/// Release a context returned by a thread-safe context constructor.
// SAFETY: `ctx` must be null or uniquely own a live context allocated by this
// module's context constructors.
pub unsafe extern "C" fn ValkeyModule_FreeThreadSafeContext(ctx: *mut ValkeyModuleCtx) {
    unsafe {
        free_module_ctx(ctx);
    }
}

/// Return the module name retained by a context for the variadic logging shim.
// SAFETY: `ctx` must be null or point to a live ValkeyModuleCtx for the
// duration of the returned pointer's use.
#[no_mangle]
pub unsafe extern "C" fn VM_ModuleName(ctx: *mut ValkeyModuleCtx) -> *const c_char {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return std::ptr::null();
        }
        (*(*ctx).internal).module_name
    }
}

/// Emit an already-formatted module message through the server logger.
// SAFETY: `level` must point to a NUL-terminated string when non-null, and
// `message` must point to `message_len` readable bytes when the length is nonzero.
#[no_mangle]
pub unsafe extern "C" fn VM_LogBuffer(
    ctx: *mut ValkeyModuleCtx,
    level: *const c_char,
    message: *const c_char,
    message_len: size_t,
) {
    // SAFETY: the ABI contract guarantees all non-null inputs remain readable
    // for this synchronous logging call.
    unsafe {
        if message.is_null() && message_len != 0 {
            return;
        }
        let module_name_ptr = VM_ModuleName(ctx);
        let module_name = if module_name_ptr.is_null() {
            "module".into()
        } else {
            CStr::from_ptr(module_name_ptr)
                .to_string_lossy()
                .into_owned()
        };
        let level = if level.is_null() {
            "notice"
        } else {
            CStr::from_ptr(level).to_str().unwrap_or("verbose")
        };
        let message = if message_len == 0 {
            ""
        } else {
            std::str::from_utf8(std::slice::from_raw_parts(
                message.cast::<u8>(),
                message_len,
            ))
            .unwrap_or("<invalid utf-8 module log>")
        };
        match level.to_ascii_lowercase().as_str() {
            "debug" | "verbose" => log::debug!("<{}> {}", module_name, message),
            "notice" | "info" => log::info!("<{}> {}", module_name, message),
            "warning" | "warn" => log::warn!("<{}> {}", module_name, message),
            _ => log::debug!("<{}> {}", module_name, message),
        }
    }
}
