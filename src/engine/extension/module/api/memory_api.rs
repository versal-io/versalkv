#![allow(non_snake_case)]

use libc::{c_char, c_void, size_t};

use super::super::{VALKEYMODULE_ERR, VALKEYMODULE_OK};
use crate::engine::extension::module::ValkeyModuleCtx;

/// Account for resident memory obtained outside the server allocator.
pub extern "C" fn ValkeyModule_IncrExternalMemory(bytes: size_t) -> libc::c_int {
    if crate::foundation::memory::util_alloc::increase_external_memory(bytes).is_err() {
        crate::foundation::io::sys::os::set_errno(libc::ERANGE);
        return VALKEYMODULE_ERR;
    }
    VALKEYMODULE_OK
}

/// Remove external memory accounting without freeing the reported memory.
pub extern "C" fn ValkeyModule_DecrExternalMemory(bytes: size_t) -> libc::c_int {
    if crate::foundation::memory::util_alloc::decrease_external_memory(bytes).is_err() {
        crate::foundation::io::sys::os::set_errno(libc::ERANGE);
        return VALKEYMODULE_ERR;
    }
    VALKEYMODULE_OK
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::sys::os;
    use crate::foundation::memory::{external::lock_external_memory, util_alloc};

    #[test]
    fn module_external_memory_c_api_range_errors_preserve_accounting_and_errno() {
        let _lock = lock_external_memory();
        let before = util_alloc::external_allocated();
        let increase = ValkeyModule_IncrExternalMemory(1);
        let after_increase = util_alloc::external_allocated();
        os::set_errno(libc::EDOM);
        let zero_increase = ValkeyModule_IncrExternalMemory(0);
        let success_errno = os::errno();
        let overflow = ValkeyModule_IncrExternalMemory(usize::MAX);
        let overflow_errno = os::errno();
        let after_overflow = util_alloc::external_allocated();
        let underflow = ValkeyModule_DecrExternalMemory(after_increase + 1);
        let underflow_errno = os::errno();
        let after_underflow = util_alloc::external_allocated();
        let decrease = ValkeyModule_DecrExternalMemory(1);
        let after_decrease = util_alloc::external_allocated();
        os::set_errno(libc::EDOM);
        let zero_decrease = ValkeyModule_DecrExternalMemory(0);
        let decrease_errno = os::errno();

        assert_eq!(increase, VALKEYMODULE_OK);
        assert_eq!(after_increase, before + 1);
        assert_eq!(zero_increase, VALKEYMODULE_OK);
        assert_eq!(success_errno, libc::EDOM);
        assert_eq!(overflow, VALKEYMODULE_ERR);
        assert_eq!(overflow_errno, libc::ERANGE);
        assert_eq!(after_overflow, after_increase);
        assert_eq!(underflow, VALKEYMODULE_ERR);
        assert_eq!(underflow_errno, libc::ERANGE);
        assert_eq!(after_underflow, after_increase);
        assert_eq!(decrease, VALKEYMODULE_OK);
        assert_eq!(after_decrease, before);
        assert_eq!(zero_decrease, VALKEYMODULE_OK);
        assert_eq!(decrease_errno, libc::EDOM);
    }

    #[test]
    fn module_external_memory_c_callbacks_work_on_threads_without_contexts() {
        let _lock = lock_external_memory();
        let before = util_alloc::external_allocated();
        assert_eq!(ValkeyModule_IncrExternalMemory(1), VALKEYMODULE_OK);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        assert_eq!(ValkeyModule_IncrExternalMemory(19), VALKEYMODULE_OK);
                        assert_eq!(ValkeyModule_DecrExternalMemory(19), VALKEYMODULE_OK);
                    }
                    os::set_errno(0);
                    assert_eq!(
                        ValkeyModule_IncrExternalMemory(usize::MAX),
                        VALKEYMODULE_ERR
                    );
                    assert_eq!(os::errno(), libc::ERANGE);
                });
            }
        });
        assert_eq!(ValkeyModule_DecrExternalMemory(1), VALKEYMODULE_OK);
        assert_eq!(util_alloc::external_allocated(), before);
    }
}
