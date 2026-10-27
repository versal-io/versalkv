#![allow(non_snake_case)]

use libc::{c_char, c_double, c_int, c_longlong, c_ulonglong, size_t};

use super::super::auto_memory::{create_auto_string, internal_owner, remove_string};
use super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, ptr is a valid buffer of len bytes (or null/empty, which is handled)
#[export_name = "VM_CreateString"]
pub unsafe extern "C" fn ValkeyModule_CreateString(
    ctx: *mut ValkeyModuleCtx,
    ptr: *const c_char,
    len: size_t,
) -> *mut ValkeyModuleString {
    unsafe {
        log::trace!("ValkeyModule_CreateString(len={}) called", len);
        if ptr.is_null() || len == 0 {
            return create_auto_string(ctx, b"");
        }
        let slice = std::slice::from_raw_parts(ptr.cast::<u8>(), len);
        create_auto_string(ctx, slice)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_CreateStringFromLongLong(
    ctx: *mut ValkeyModuleCtx,
    ll: c_longlong,
) -> *mut ValkeyModuleString {
    log::trace!("ValkeyModule_CreateStringFromLongLong({}) called", ll);
    let s = format!("{}", ll);
    // SAFETY: ctx is null or live by the API contract.
    unsafe { create_auto_string(ctx, s.as_bytes()) }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_CreateStringFromULongLong(
    ctx: *mut ValkeyModuleCtx,
    value: c_ulonglong,
) -> *mut ValkeyModuleString {
    let value = value.to_string();
    // SAFETY: ctx is null or live by the API contract.
    unsafe { create_auto_string(ctx, value.as_bytes()) }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_CreateStringFromString(
    ctx: *mut ValkeyModuleCtx,
    s: *mut ValkeyModuleString,
) -> *mut ValkeyModuleString {
    unsafe {
        log::trace!("ValkeyModule_CreateStringFromString() called");
        if s.is_null() {
            return create_auto_string(ctx, b"");
        }
        let data = (*s).as_bytes();
        create_auto_string(ctx, data)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_FreeString(
    ctx: *mut ValkeyModuleCtx,
    s: *mut ValkeyModuleString,
) {
    unsafe {
        log::trace!("ValkeyModule_FreeString() called");
        if !s.is_null() {
            remove_string(internal_owner(ctx), s);
            ValkeyModuleString::release(s);
        }
    }
}

// SAFETY: Called by C module code; s is a valid ValkeyModuleString pointer (or null, which is handled), len is a valid pointer to size_t (or null)
pub unsafe extern "C" fn ValkeyModule_StringPtrLen(
    s: *mut ValkeyModuleString,
    len: *mut size_t,
) -> *const c_char {
    unsafe {
        log::trace!("ValkeyModule_StringPtrLen() called");
        if s.is_null() {
            if !len.is_null() {
                *len = 0;
            }
            return std::ptr::null();
        }
        let data = (*s).as_bytes();
        if !len.is_null() {
            *len = data.len() as size_t;
        }
        data.as_ptr() as *const c_char
    }
}

/// C shim entrypoint for reading a module string before parsing a long double.
// SAFETY: Called by module_call_shim.c; s and len follow ValkeyModule_StringPtrLen's contract.
#[export_name = "VM_StringPtrLenForShim"]
pub unsafe extern "C" fn string_ptr_len_for_shim(
    s: *const ValkeyModuleString,
    len: *mut size_t,
) -> *const c_char {
    unsafe { ValkeyModule_StringPtrLen(s.cast_mut(), len) }
}

// SAFETY: Called by C module code; s is a valid ValkeyModuleString pointer, ll is a valid pointer to c_longlong (or either is null, which is handled)
pub unsafe extern "C" fn ValkeyModule_StringToLongLong(
    s: *mut ValkeyModuleString,
    ll: *mut c_longlong,
) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_StringToLongLong() called");
        if s.is_null() || ll.is_null() {
            return VALKEYMODULE_ERR;
        }
        let data = (*s).as_bytes();
        let s_str = std::str::from_utf8(data).unwrap_or("");
        match s_str.parse::<i64>() {
            Ok(val) => {
                *ll = val as c_longlong;
                VALKEYMODULE_OK
            }
            Err(_) => VALKEYMODULE_ERR,
        }
    }
}

// SAFETY: Called by C module code; s and value are valid pointers or null.
pub unsafe extern "C" fn ValkeyModule_StringToULongLong(
    s: *mut ValkeyModuleString,
    value: *mut c_ulonglong,
) -> c_int {
    unsafe {
        if s.is_null() || value.is_null() {
            return VALKEYMODULE_ERR;
        }
        let data = (*s).as_bytes();
        let Ok(text) = std::str::from_utf8(data) else {
            return VALKEYMODULE_ERR;
        };
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return VALKEYMODULE_ERR;
        }
        match text.parse::<u64>() {
            Ok(parsed) => {
                *value = parsed as c_ulonglong;
                VALKEYMODULE_OK
            }
            Err(_) => VALKEYMODULE_ERR,
        }
    }
}

// SAFETY: Called by C module code; s is a valid ValkeyModuleString pointer, d is a valid pointer to c_double (or either is null, which is handled)
pub unsafe extern "C" fn ValkeyModule_StringToDouble(
    s: *mut ValkeyModuleString,
    d: *mut c_double,
) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_StringToDouble() called");
        if s.is_null() || d.is_null() {
            return VALKEYMODULE_ERR;
        }
        let data = (*s).as_bytes();
        let s_str = std::str::from_utf8(data).unwrap_or("");
        match s_str.parse::<f64>() {
            Ok(val) => {
                *d = val as c_double;
                VALKEYMODULE_OK
            }
            Err(_) => VALKEYMODULE_ERR,
        }
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_AutoMemory(ctx: *mut ValkeyModuleCtx) {
    log::trace!("ValkeyModule_AutoMemory() called");
    // SAFETY: ctx is null or live; enabling does not register existing allocations.
    unsafe {
        if let Some(owner) = internal_owner(ctx) {
            (*owner.as_ptr()).auto_memory.is_enabled = true;
        }
    }
}

// SAFETY: the shim passes a non-null live string for each borrowed argv slot.
#[export_name = "VM_StringIncrRefForShim"]
pub unsafe extern "C" fn string_incr_ref_for_shim(s: *mut ValkeyModuleString) {
    // SAFETY: the caller owns a live reference.
    unsafe { ValkeyModuleString::incr_ref(s) };
}

// SAFETY: the shim releases one reference for each constructed argv slot.
#[export_name = "VM_StringReleaseForShim"]
pub unsafe extern "C" fn string_release_for_shim(s: *mut ValkeyModuleString) {
    // SAFETY: the slot owns one reference, or is null.
    unsafe { ValkeyModuleString::release(s) };
}
