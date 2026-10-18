use bytes::Bytes;
use libc::{c_char, c_int, c_longlong, c_void};
use std::ffi::{CStr, CString};

use super::super::super::{ValkeyModuleCtx, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::super::ReplyBuilder;
use crate::engine::extension::module::capability::record_module_latency;

// SAFETY: This ABI entrypoint takes no pointers and reads no external state.
pub unsafe extern "C" fn ValkeyModule_GetServerVersion() -> c_int {
    0x0008_0000
}

/// ValkeyModule_RegisterInfoFunc — register a module INFO callback.
/// INFO callback execution is not wired into the Rust server yet, but the
/// registration must succeed so modules can load without calling a null ABI
/// function pointer during OnLoad.
// SAFETY: ctx and callback are optional ABI values and are not dereferenced.
pub unsafe extern "C" fn ValkeyModule_RegisterInfoFunc(
    _ctx: *mut ValkeyModuleCtx,
    _callback: Option<super::super::super::ValkeyModuleInfoFunc>,
) -> c_int {
    VALKEYMODULE_OK
}

// SAFETY: Called from C shim or module code; caller must pass a valid ValkeyModuleCtx pointer or NULL.
pub unsafe extern "C" fn ValkeyModule_WrongArity(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        if ctx.is_null() {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if !internal.is_null() && !(*internal).reply_ptr.is_null() {
            let builder = &mut *((*internal).reply_ptr as *mut ReplyBuilder);
            let command_name = if (*internal).current_command_name.is_null() {
                "module"
            } else {
                CStr::from_ptr((*internal).current_command_name)
                    .to_str()
                    .unwrap_or("module")
            };
            builder.add_error(Bytes::from(format!(
                "ERR wrong number of arguments for '{}' command",
                command_name
            )));
        }
        VALKEYMODULE_ERR
    }
}

// SAFETY: ctx is a live module context and pos is a validated positive argument index.
pub unsafe extern "C" fn ValkeyModule_RedactClientCommandArgument(
    ctx: *mut ValkeyModuleCtx,
    pos: c_int,
) -> c_int {
    unsafe {
        if ctx.is_null() || pos <= 0 {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return VALKEYMODULE_ERR;
        }
        let Some(runtime) = crate::engine::extension::module::capability::runtime_from_opaque(
            (*internal).runtime_handle,
        ) else {
            return VALKEYMODULE_ERR;
        };
        if runtime.redact_client_command_argument(pos as usize) {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: Called from C shim or module code; has no pointer arguments, only an integer predicate.
pub unsafe extern "C" fn ValkeyModule_Assert(expr: c_int) {
    if expr == 0 {
        log::error!("ValkeyModule_Assert failed");
        panic!("ValkeyModule_Assert failed");
    }
}

#[repr(C)]
pub struct ValkeyModuleServerInfoData {
    _data: *mut c_void,
}

struct ServerInfoOwned {
    mem_allocator: CString,
}

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL ValkeyModuleCtx pointer and C string pointer.
pub unsafe extern "C" fn ValkeyModule_GetServerInfo(
    ctx: *mut ValkeyModuleCtx,
    _section: *const c_char,
) -> *mut ValkeyModuleServerInfoData {
    let _ = ctx;
    let owned = Box::new(ServerInfoOwned {
        mem_allocator: CString::new("libc").expect("static allocator name has no NUL"),
    });
    Box::into_raw(Box::new(ValkeyModuleServerInfoData {
        _data: Box::into_raw(owned).cast(),
    }))
}

// SAFETY: Called from C shim or module code; caller must pass valid or NULL pointers.
pub unsafe extern "C" fn ValkeyModule_FreeServerInfo(
    _ctx: *mut ValkeyModuleCtx,
    info: *mut ValkeyModuleServerInfoData,
) {
    unsafe {
        if info.is_null() {
            return;
        }
        let info = Box::from_raw(info);
        if !info._data.is_null() {
            let _ = Box::from_raw(info._data as *mut ServerInfoOwned);
        }
    }
}

// SAFETY: Called from C shim or module code; caller must pass valid or NULL pointers.
pub unsafe extern "C" fn ValkeyModule_ServerInfoGetFieldC(
    info: *mut ValkeyModuleServerInfoData,
    field: *const c_char,
) -> *const c_char {
    unsafe {
        if info.is_null() || field.is_null() || (*info)._data.is_null() {
            return std::ptr::null();
        }
        if CStr::from_ptr(field).to_bytes() != b"mem_allocator" {
            return std::ptr::null();
        }
        let owned = &*((*info)._data as *const ServerInfoOwned);
        owned.mem_allocator.as_ptr()
    }
}

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL ValkeyModuleCtx pointer and a valid function pointer.
pub unsafe extern "C" fn ValkeyModule_SubscribeToKeyspaceEvents(
    ctx: *mut ValkeyModuleCtx,
    types: c_int,
    callback: Option<super::super::super::ValkeyModuleNotificationFunc>,
) -> c_int {
    unsafe {
        let (Some(callback), false) = (callback, ctx.is_null()) else {
            return VALKEYMODULE_ERR;
        };
        let module_name = super::super::ctx::get_module_name(ctx);
        super::super::active_registry::with_active_registry(|registry| {
            if registry.subscribe_keyspace_events(&module_name, types, callback) {
                VALKEYMODULE_OK
            } else {
                VALKEYMODULE_ERR
            }
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

// SAFETY: Called from C shim or module code; caller must pass a valid C string pointer or NULL.
pub unsafe extern "C" fn ValkeyModule_LatencyAddSample(
    event: *const c_char,
    latency_ms: c_longlong,
) {
    unsafe {
        if event.is_null() {
            return;
        }
        let event_cstr = CStr::from_ptr(event);
        if let Ok(event_str) = event_cstr.to_str() {
            record_module_latency(event_str, latency_ms);
        }
    }
}

// SAFETY: Called from C shim or module code; caller must pass a valid or NULL ValkeyModuleCtx pointer.
pub unsafe extern "C" fn ValkeyModule_GetCurrentCommandName(
    ctx: *mut ValkeyModuleCtx,
) -> *const c_char {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return std::ptr::null();
        }
        (*(*ctx).internal).current_command_name
    }
}
