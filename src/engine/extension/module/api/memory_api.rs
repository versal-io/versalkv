#![allow(non_snake_case)]

use libc::{c_char, c_void, size_t};

use super::super::{VALKEYMODULE_ERR, VALKEYMODULE_OK};
use crate::engine::extension::module::ValkeyModuleCtx;

// SAFETY: Called by C module code; size is a valid allocation size for libc::malloc
pub unsafe extern "C" fn ValkeyModule_Alloc(size: size_t) -> *mut c_void {
    unsafe {
        log::trace!("ValkeyModule_Alloc({}) called", size);
        libc::malloc(size)
    }
}

// SAFETY: Called by C module code; ptr must be a valid pointer from ValkeyModule_Alloc/Calloc/Realloc (or null, which is handled by realloc), size is a valid allocation size
pub unsafe extern "C" fn ValkeyModule_Realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
    unsafe {
        log::trace!("ValkeyModule_Realloc({:p}, {}) called", ptr, size);
        libc::realloc(ptr, size)
    }
}

// SAFETY: Called by C module code; ptr must be a valid pointer from ValkeyModule_Alloc/Calloc/Realloc (or null, which is handled by free)
pub unsafe extern "C" fn ValkeyModule_Free(ptr: *mut c_void) {
    unsafe {
        log::trace!("ValkeyModule_Free({:p}) called", ptr);
        libc::free(ptr);
    }
}

// SAFETY: Called by C module code; s is a non-null C string (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_Strdup(s: *const c_char) -> *mut c_char {
    unsafe {
        log::trace!("ValkeyModule_Strdup() called");
        if s.is_null() {
            return std::ptr::null_mut();
        }
        libc::strdup(s)
    }
}

// SAFETY: Called by C module code; count and size are valid allocation parameters for libc::calloc
pub unsafe extern "C" fn ValkeyModule_Calloc(count: size_t, size: size_t) -> *mut c_void {
    unsafe {
        log::trace!("ValkeyModule_Calloc({}, {}) called", count, size);
        libc::calloc(count, size)
    }
}

/// ValkeyModule_FreeMemoryIfNeeded — best-effort memory pressure hook.
// SAFETY: Called by C module code; ctx may be null for detached contexts. The
// current Rust server has no module-specific eviction hook, so this validates
// the call shape and reports success for smoke-compatible modules.
pub unsafe extern "C" fn ValkeyModule_FreeMemoryIfNeeded(
    ctx: *mut ValkeyModuleCtx,
    bytes: size_t,
) -> libc::c_int {
    log::trace!(
        "ValkeyModule_FreeMemoryIfNeeded({:p}, {}) called",
        ctx,
        bytes
    );
    if ctx.is_null() {
        return VALKEYMODULE_ERR;
    }
    VALKEYMODULE_OK
}
