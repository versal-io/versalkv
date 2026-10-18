use libc::{c_char, size_t};
use std::ffi::{CStr, CString};

use super::super::{ModuleInternalCtx, ValkeyModuleCtx};
use crate::engine::extension::module::{create_module_ctx, free_module_ctx};

/// Return a detached context associated with the same module as `ctx`.
// SAFETY: `ctx` must be null or point to a live ValkeyModuleCtx whose internal
// context remains valid for the duration of this call.
pub unsafe extern "C" fn ValkeyModule_GetDetachedThreadSafeContext(
    ctx: *mut ValkeyModuleCtx,
) -> *mut ValkeyModuleCtx {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return std::ptr::null_mut();
        }
        let source = &*(*ctx).internal;
        let module_name = if source.module_name.is_null() {
            CString::new("unknown").expect("static string has no NUL")
        } else {
            CStr::from_ptr(source.module_name).to_owned()
        };
        let internal = Box::new(ModuleInternalCtx {
            module_name: module_name.into_raw(),
            module_ref: source.module_ref,
            onload: 0,
            reply_ptr: std::ptr::null_mut(),
            runtime_handle: std::ptr::null_mut(),
            db_idx: source.db_idx,
            resp_version: source.resp_version,
            username: CString::new("default").unwrap().into_raw(),
            module_auth_user: std::ptr::null_mut(),
            current_command_name: std::ptr::null_mut(),
        });
        Box::into_raw(create_module_ctx(Box::into_raw(internal)))
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
