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

// SAFETY: Called by C module code; successful allocations are returned to the
// module, which must release them using the module allocator API.
pub unsafe extern "C" fn ValkeyModule_Calloc(count: size_t, size: size_t) -> *mut c_void {
    let Some(bytes) = count.checked_mul(size) else {
        crate::server_panic!("ValkeyModule_Calloc({}, {}) overflows", count, size);
    };
    // SAFETY: the product was checked above; libc owns allocation bookkeeping.
    let allocation = unsafe { ValkeyModule_TryCalloc(count, size) };
    if allocation.is_null() {
        crate::server_panic!(
            "Out of memory allocating {} bytes in ValkeyModule_Calloc",
            bytes
        );
    }
    allocation
}

// SAFETY: Called by C module code; successful allocations are returned to the
// module, which must release them using the module allocator API.
pub unsafe extern "C" fn ValkeyModule_TryCalloc(count: size_t, size: size_t) -> *mut c_void {
    let Some(bytes) = count.checked_mul(size) else {
        return std::ptr::null_mut();
    };
    // Match the allocator's representable-size guard before calling libc.
    if bytes >= usize::MAX / 2 {
        return std::ptr::null_mut();
    }
    // SAFETY: the allocation size cannot overflow; libc reports failure as NULL.
    unsafe {
        log::trace!("ValkeyModule_TryCalloc({}, {}) called", count, size);
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
    fn ordinary_calloc_terminates_on_non_overflow_allocation_failure() {
        const CHILD_ENV: &str = "VERSAL_TEST_CALLOC_ALLOCATION_FAILURE";
        if std::env::var_os(CHILD_ENV).is_some() {
            // SAFETY: this is a dedicated subprocess testing the allocator's
            // fatal failure policy; the product is valid and cannot be allocated.
            unsafe { ValkeyModule_Free(ValkeyModule_Calloc(usize::MAX / 2, 1)) };
            return;
        }
        let test = format!(
            "{}::ordinary_calloc_terminates_on_non_overflow_allocation_failure",
            module_path!().trim_start_matches("versal::")
        );
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test, "--nocapture"])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            !result.status.success(),
            "ordinary Calloc must terminate on failure"
        );
        let diagnostics = String::from_utf8_lossy(&result.stderr);
        assert!(
            diagnostics.contains("ValkeyModule_Calloc") && diagnostics.contains("Out of memory"),
            "the allocator must diagnose the fatal allocation failure: {diagnostics}"
        );
    }

    #[test]
    fn calloc_apis_have_distinct_overflow_contracts_and_zeroed_allocations() {
        use std::ffi::CString;

        let mut addresses = [std::ptr::null_mut(); 2];
        for (index, name) in ["ValkeyModule_Calloc", "ValkeyModule_TryCalloc"]
            .into_iter()
            .enumerate()
        {
            let name = CString::new(name).unwrap();
            // SAFETY: the NUL-terminated name and writable result slot remain
            // live throughout this synchronous API lookup.
            assert_eq!(
                unsafe { super::super::module_get_api_impl(name.as_ptr(), &mut addresses[index]) },
                VALKEYMODULE_OK
            );
        }
        assert_eq!(addresses[0], ValkeyModule_Calloc as *mut c_void);
        assert_eq!(addresses[1], ValkeyModule_TryCalloc as *mut c_void);
        assert_ne!(addresses[0], addresses[1]);
        // SAFETY: the API entries above were checked against functions with
        // exactly this two-size allocation ABI.
        let functions = unsafe {
            addresses.map(|address| {
                std::mem::transmute::<
                    *mut c_void,
                    unsafe extern "C" fn(size_t, size_t) -> *mut c_void,
                >(address)
            })
        };
        for (count, size) in [(usize::MAX, 2), (2, usize::MAX), (usize::MAX / 2 + 1, 2)] {
            // SAFETY: TryCalloc accepts overflow inputs and returns NULL; this
            // test intentionally never calls the fatal Calloc overflow path.
            assert!(unsafe { functions[1](count, size) }.is_null());
        }
        for allocate in functions {
            // SAFETY: 8 * 4 is a valid small allocation, the returned storage is
            // read only after the NULL check, and is freed exactly once.
            unsafe {
                let allocation = allocate(8, 4);
                assert!(!allocation.is_null());
                assert_eq!(
                    std::slice::from_raw_parts(allocation.cast::<u8>(), 32),
                    &[0; 32]
                );
                ValkeyModule_Free(allocation);
            }
        }
    }

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
