#![allow(non_snake_case)]

use libc::{c_char, c_int, size_t};
use std::mem;
use std::ptr;

use super::super::super::{
    ValkeyModuleCtx, ValkeyModuleKey, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK,
};

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim; caller must pass a valid or NULL pointer. If data is non-NULL, len must be the exact byte length of the pointed-to buffer.
#[no_mangle]
pub unsafe extern "C" fn VM_CreateStringFromBuffer(
    _ctx: *mut ValkeyModuleCtx,
    data: *const c_char,
    len: size_t,
) -> *mut ValkeyModuleString {
    unsafe {
        if data.is_null() || len == 0 {
            return Box::into_raw(ValkeyModuleString::from_bytes(b"" as &[u8]));
        }
        let slice = std::slice::from_raw_parts(data as *const u8, len);
        Box::into_raw(ValkeyModuleString::from_bytes(slice))
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL ValkeyModuleCtx, ValkeyModuleString, and data pointers. If data is non-NULL, len must be the exact byte length.
pub unsafe extern "C" fn ValkeyModule_StringAppendBuffer(
    ctx: *mut ValkeyModuleCtx,
    str_: *mut ValkeyModuleString,
    data: *const c_char,
    len: size_t,
) -> c_int {
    if str_.is_null() || data.is_null() {
        return VALKEYMODULE_ERR;
    }
    let _ = ctx;
    // SAFETY: str_ and data satisfy the caller contract and remain live while
    // their bytes are copied into owned storage.
    let mut new_data = unsafe { (*str_).as_bytes().to_vec() };
    // SAFETY: data points to exactly len readable bytes by the ABI contract.
    new_data.extend_from_slice(unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) });
    // SAFETY: str_ is exclusively owned for this mutation and its old payload
    // is released exactly once before replacement.
    unsafe { (*str_).free() };
    let new_str = ValkeyModuleString::from_bytes(&new_data);
    // SAFETY: ptr::read moves the replacement value, forget prevents its Box
    // from freeing that moved value, and ptr::write initializes str_ again.
    unsafe {
        let value = ptr::read(new_str.as_ref());
        mem::forget(new_str);
        ptr::write(str_, value);
    }
    VALKEYMODULE_OK
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL pointers.
pub unsafe extern "C" fn ValkeyModule_RetainString(
    _ctx: *mut ValkeyModuleCtx,
    str_: *mut ValkeyModuleString,
) -> *mut ValkeyModuleString {
    if !str_.is_null() {
        let _ = super::super::active_registry::with_active_registry(|registry| {
            registry.retain_string(str_);
        });
    }
    str_
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL ValkeyModuleKey and ValkeyModuleString pointers.
pub unsafe extern "C" fn ValkeyModule_StringSet(
    key: *mut ValkeyModuleKey,
    str_: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if key.is_null() || str_.is_null() {
            return VALKEYMODULE_ERR;
        }
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_ERR,
        };
        if handle.db.is_null() {
            return VALKEYMODULE_ERR;
        }
        let data = (*str_).as_bytes().to_vec();
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        db.set(
            handle.key.clone(),
            crate::foundation::io::store::Robj::StrRaw(
                crate::foundation::io::store::structure::string::StrRaw(data.into()),
            ),
        );
        VALKEYMODULE_OK
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL ValkeyModuleKey and size_t pointers.
pub unsafe extern "C" fn ValkeyModule_StringDMA(
    key: *mut ValkeyModuleKey,
    len: *mut size_t,
    _mode: c_int,
) -> *mut c_char {
    unsafe {
        if key.is_null() || len.is_null() {
            return std::ptr::null_mut();
        }
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        if handle.db.is_null() {
            return std::ptr::null_mut();
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        match db.get(&handle.key) {
            Some(robj) if robj.is_string() => {
                let bytes = robj.as_bytes();
                *len = bytes.len() as size_t;
                bytes.as_ptr() as *mut c_char
            }
            _ => {
                *len = 0;
                std::ptr::null_mut()
            }
        }
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL ValkeyModuleKey pointer.
pub unsafe extern "C" fn ValkeyModule_StringTruncate(
    key: *mut ValkeyModuleKey,
    newlen: size_t,
) -> c_int {
    unsafe {
        if key.is_null() {
            return VALKEYMODULE_ERR;
        }
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_ERR,
        };
        if handle.db.is_null() {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        match db.get(&handle.key) {
            Some(robj) if robj.is_string() => {
                let bytes = robj.as_bytes();
                let mut truncated = Vec::with_capacity(newlen);
                truncated.extend(bytes.iter().copied().take(newlen));
                truncated.resize(newlen, 0);
                db.del(&handle.key);
                db.set(
                    handle.key.clone(),
                    crate::foundation::io::store::Robj::StrRaw(
                        crate::foundation::io::store::structure::string::StrRaw(truncated.into()),
                    ),
                );
                VALKEYMODULE_OK
            }
            _ => VALKEYMODULE_ERR,
        }
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL const pointers to ValkeyModuleString.
pub unsafe extern "C" fn ValkeyModule_StringCompare(
    a: *const ValkeyModuleString,
    b: *const ValkeyModuleString,
) -> c_int {
    unsafe {
        if a.is_null() || b.is_null() {
            return if a.is_null() && b.is_null() {
                0
            } else if a.is_null() {
                -1
            } else {
                1
            };
        }
        let a_bytes = (*a).as_bytes();
        let b_bytes = (*b).as_bytes();
        a_bytes.cmp(b_bytes) as c_int
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL mutable pointer to ValkeyModuleString.
pub unsafe extern "C" fn ValkeyModule_MallocSizeString(str_: *mut ValkeyModuleString) -> size_t {
    unsafe {
        if str_.is_null() {
            return 0;
        }
        let bytes = (*str_).as_bytes();
        (bytes.len() + std::mem::size_of::<ValkeyModuleString>()) as size_t
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL mutable pointer to ValkeyModuleString.
pub unsafe extern "C" fn ValkeyModule_TrimStringAllocation(_str_: *mut ValkeyModuleString) {
    // NOP
}
