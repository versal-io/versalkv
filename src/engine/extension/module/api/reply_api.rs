#![allow(non_snake_case)]

use bytes::Bytes;
use libc::{c_char, c_double, c_int, c_long, c_longlong, size_t};
use std::ffi::CStr;

use super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::reply_builder::ReplyBuilder;

// SAFETY: ctx is NULL or live. The callback cannot retain the builder borrow
// or enter C code; a bound thread context holds its reply mutex only here.
pub(crate) unsafe fn with_reply_builder<R>(
    ctx: *mut ValkeyModuleCtx,
    callback: impl FnOnce(&mut ReplyBuilder) -> R,
) -> Option<R> {
    unsafe {
        if ctx.is_null() {
            return None;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return None;
        }
        if (*internal).kind == super::super::thread_bridge::ModuleContextKind::ThreadSafe {
            return (*internal).blocked_client.as_ref()?.with_replies(callback);
        }
        if (*internal).kind == super::super::thread_bridge::ModuleContextKind::DisconnectedCleanup
            || (*internal).reply_ptr.is_null()
        {
            return None;
        }
        Some(callback(&mut *((*internal).reply_ptr as *mut ReplyBuilder)))
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer set up by the module dispatcher
pub unsafe extern "C" fn ValkeyModule_ReplyWithLongLong(
    ctx: *mut ValkeyModuleCtx,
    ll: c_longlong,
) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_integer(ll))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, err is a non-null C string
pub unsafe extern "C" fn ValkeyModule_ReplyWithError(
    ctx: *mut ValkeyModuleCtx,
    err: *const c_char,
) -> c_int {
    unsafe {
        let msg = if err.is_null() {
            Bytes::from_static(b"ERR")
        } else {
            let s = CStr::from_ptr(err).to_bytes();
            Bytes::copy_from_slice(s)
        };
        with_reply_builder(ctx, |builder| builder.add_error(msg))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

/// C shim entrypoint for already-formatted error bytes.
// SAFETY: Called by module_call_shim.c; data points to len bytes for the duration of the call.
#[export_name = "VM_ReplyWithErrorBuffer"]
pub unsafe extern "C" fn reply_with_error_buffer(
    ctx: *mut ValkeyModuleCtx,
    data: *const c_char,
    len: size_t,
) -> c_int {
    unsafe {
        let message = if data.is_null() || len == 0 {
            Bytes::from_static(b"ERR")
        } else {
            Bytes::copy_from_slice(std::slice::from_raw_parts(data.cast::<u8>(), len))
        };
        with_reply_builder(ctx, |builder| builder.add_error(message))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, msg is a non-null C string
pub unsafe extern "C" fn ValkeyModule_ReplyWithSimpleString(
    ctx: *mut ValkeyModuleCtx,
    msg: *const c_char,
) -> c_int {
    unsafe {
        let s = if msg.is_null() {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(CStr::from_ptr(msg).to_bytes())
        };
        with_reply_builder(ctx, |builder| builder.add_simple_string(s))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithArray(
    ctx: *mut ValkeyModuleCtx,
    len: c_long,
) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_array(len as i32))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_ReplyWithMap(
    ctx: *mut ValkeyModuleCtx,
    len: c_long,
) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_map(len as i32))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_ReplyWithSet(
    ctx: *mut ValkeyModuleCtx,
    len: c_long,
) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_set(len as i32))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_ReplyWithAttribute(
    ctx: *mut ValkeyModuleCtx,
    len: c_long,
) -> c_int {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() || (*(*ctx).internal).resp_version != 3 {
            return VALKEYMODULE_ERR;
        }
        with_reply_builder(ctx, |builder| builder.add_attribute(len as i32))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithNullArray(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        with_reply_builder(ctx, ReplyBuilder::add_null_array)
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithEmptyArray(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_array(0))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplySetArrayLength(
    ctx: *mut ValkeyModuleCtx,
    len: c_long,
) -> c_int {
    unsafe {
        if len < 0 {
            return VALKEYMODULE_ERR;
        }
        if with_reply_builder(ctx, |builder| builder.set_array_length(len as usize)) == Some(true) {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithString(
    ctx: *mut ValkeyModuleCtx,
    s: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if s.is_null() {
            return with_reply_builder(ctx, ReplyBuilder::add_null)
                .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK);
        }
        let data = (*s).as_bytes();
        with_reply_builder(ctx, |builder| {
            builder.add_bulk_string(Bytes::copy_from_slice(data))
        })
        .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithEmptyString(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_bulk_string(Bytes::new()))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithNull(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        with_reply_builder(ctx, ReplyBuilder::add_null)
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithBool(ctx: *mut ValkeyModuleCtx, b: c_int) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_bool(b != 0))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithDouble(
    ctx: *mut ValkeyModuleCtx,
    d: c_double,
) -> c_int {
    unsafe {
        with_reply_builder(ctx, |builder| builder.add_double(d))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

/// C shim entrypoint for long-double replies after ABI-safe conversion.
// SAFETY: Called by module_call_shim.c with a valid module context.
#[export_name = "VM_ReplyWithDoubleValue"]
pub unsafe extern "C" fn reply_with_double_value(
    ctx: *mut ValkeyModuleCtx,
    value: c_double,
) -> c_int {
    unsafe { ValkeyModule_ReplyWithDouble(ctx, value) }
}

// SAFETY: Called by C module code; ctx is valid and bignum points to len bytes.
pub unsafe extern "C" fn ValkeyModule_ReplyWithBigNumber(
    ctx: *mut ValkeyModuleCtx,
    bignum: *const c_char,
    len: size_t,
) -> c_int {
    unsafe {
        let value = if bignum.is_null() || len == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(std::slice::from_raw_parts(bignum.cast::<u8>(), len))
        };
        with_reply_builder(ctx, |builder| builder.add_big_number(value))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, buf is a valid buffer of len bytes (or null/empty, which is handled)
pub unsafe extern "C" fn ValkeyModule_ReplyWithStringBuffer(
    ctx: *mut ValkeyModuleCtx,
    buf: *const c_char,
    len: size_t,
) -> c_int {
    unsafe {
        let data = if buf.is_null() || len == 0 {
            Bytes::new()
        } else {
            let slice = std::slice::from_raw_parts(buf.cast::<u8>(), len);
            Bytes::copy_from_slice(slice)
        };
        with_reply_builder(ctx, |builder| builder.add_bulk_string(data))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, buf is a non-null C string
pub unsafe extern "C" fn ValkeyModule_ReplyWithCString(
    ctx: *mut ValkeyModuleCtx,
    buf: *const c_char,
) -> c_int {
    unsafe {
        let data = if buf.is_null() {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(CStr::from_ptr(buf).to_bytes())
        };
        with_reply_builder(ctx, |builder| builder.add_bulk_string(data))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, s is a valid ValkeyModuleString pointer
pub unsafe extern "C" fn ValkeyModule_ReplyWithVerbatimString(
    ctx: *mut ValkeyModuleCtx,
    buf: *const c_char,
    len: size_t,
) -> c_int {
    unsafe {
        let data = if buf.is_null() || len == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(std::slice::from_raw_parts(buf.cast::<u8>(), len))
        };
        with_reply_builder(ctx, |builder| builder.add_verbatim(*b"txt", data))
            .map_or(VALKEYMODULE_ERR, |()| VALKEYMODULE_OK)
    }
}
