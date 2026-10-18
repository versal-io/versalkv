#![allow(non_snake_case)]

use libc::{c_char, c_double, c_int, c_longlong, c_ulonglong, size_t};

use super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, ptr is a valid buffer of len bytes (or null/empty, which is handled)
#[export_name = "VM_CreateString"]
pub unsafe extern "C" fn ValkeyModule_CreateString(
    _ctx: *mut ValkeyModuleCtx,
    ptr: *const c_char,
    len: size_t,
) -> *mut ValkeyModuleString {
    unsafe {
        log::trace!("ValkeyModule_CreateString(len={}) called", len);
        if ptr.is_null() || len == 0 {
            return Box::into_raw(ValkeyModuleString::from_bytes(b"" as &[u8]));
        }
        let slice = std::slice::from_raw_parts(ptr as *const u8, len);
        Box::into_raw(ValkeyModuleString::from_bytes(slice))
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_CreateStringFromLongLong(
    _ctx: *mut ValkeyModuleCtx,
    ll: c_longlong,
) -> *mut ValkeyModuleString {
    log::trace!("ValkeyModule_CreateStringFromLongLong({}) called", ll);
    let s = format!("{}", ll);
    Box::into_raw(ValkeyModuleString::from_bytes(s.as_bytes()))
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_CreateStringFromULongLong(
    _ctx: *mut ValkeyModuleCtx,
    value: c_ulonglong,
) -> *mut ValkeyModuleString {
    let value = value.to_string();
    Box::into_raw(ValkeyModuleString::from_bytes(value.as_bytes()))
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_CreateStringFromString(
    _ctx: *mut ValkeyModuleCtx,
    s: *mut ValkeyModuleString,
) -> *mut ValkeyModuleString {
    unsafe {
        log::trace!("ValkeyModule_CreateStringFromString() called");
        if s.is_null() {
            return Box::into_raw(ValkeyModuleString::from_bytes(b"" as &[u8]));
        }
        let data = (*s).as_bytes();
        Box::into_raw(ValkeyModuleString::from_bytes(data))
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_FreeString(
    _ctx: *mut ValkeyModuleCtx,
    s: *mut ValkeyModuleString,
) {
    unsafe {
        log::trace!("ValkeyModule_FreeString() called");
        if !s.is_null() {
            let _ = super::active_registry::with_active_registry(|registry| {
                registry.release_string(s);
            });
            (*s).free();
            let _ = Box::from_raw(s);
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
pub unsafe extern "C" fn ValkeyModule_AutoMemory(_ctx: *mut ValkeyModuleCtx) {
    log::trace!("ValkeyModule_AutoMemory() called");
}
