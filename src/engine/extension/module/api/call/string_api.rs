#![allow(non_snake_case)]

use super::super::super::auto_memory::{create_auto_string, internal_owner, remove_string};
use libc::{c_char, c_int, size_t};

use super::super::super::{
    ValkeyModuleCtx, ValkeyModuleKey, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK,
};

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim; caller must pass a valid or NULL pointer. If data is non-NULL, len must be the exact byte length of the pointed-to buffer.
#[no_mangle]
pub unsafe extern "C" fn VM_CreateStringFromBuffer(
    ctx: *mut ValkeyModuleCtx,
    data: *const c_char,
    len: size_t,
) -> *mut ValkeyModuleString {
    unsafe {
        if data.is_null() || len == 0 {
            return create_auto_string(ctx, b"");
        }
        let slice = std::slice::from_raw_parts(data.cast::<u8>(), len);
        create_auto_string(ctx, slice)
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
    // SAFETY: str_ is live and data points to len readable bytes. Copy first
    // because the source may alias the string's current buffer.
    unsafe {
        if ValkeyModuleString::ref_count(str_) != 1 {
            return VALKEYMODULE_ERR;
        }
        let suffix = std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec();
        if !ValkeyModuleString::append(str_, &suffix) {
            return VALKEYMODULE_ERR;
        }
    }
    VALKEYMODULE_OK
}

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL pointers.
pub unsafe extern "C" fn ValkeyModule_RetainString(
    ctx: *mut ValkeyModuleCtx,
    str_: *mut ValkeyModuleString,
) -> *mut ValkeyModuleString {
    if !str_.is_null() {
        // SAFETY: str_ is live; a matching automatic entry transfers its
        // existing reference, otherwise retaining adds a new reference.
        unsafe {
            if !remove_string(internal_owner(ctx), str_) {
                ValkeyModuleString::incr_ref(str_);
            }
        }
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
