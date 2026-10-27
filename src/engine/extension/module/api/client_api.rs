//! Client inspection APIs for synchronous module callbacks.

#![allow(non_snake_case)]

use libc::{c_char, c_int, c_ulonglong, c_void};

use super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use crate::engine::extension::module::capability::{
    runtime_from_opaque, with_active_runtime, ModuleClientInfo,
};

const CLIENTINFO_VERSION: u64 = 1;
const CLIENTINFO_FLAG_PUBSUB: u64 = 1 << 1;
const CLIENTINFO_FLAG_BLOCKED: u64 = 1 << 2;
const CLIENTINFO_FLAG_TRACKING: u64 = 1 << 3;
const CLIENTINFO_FLAG_MULTI: u64 = 1 << 5;
const CLIENTINFO_FLAG_READONLY: u64 = 1 << 6;
const CLIENTINFO_FLAG_REPLICA: u64 = 1 << 8;
const CLIENTINFO_FLAG_MONITOR: u64 = 1 << 9;
const CLIENTINFO_FLAG_AUTHENTICATED: u64 = 1 << 11;
const CLIENTINFO_FLAG_EVER_AUTHENTICATED: u64 = 1 << 12;

#[repr(C)]
pub struct ValkeyModuleClientInfoV1 {
    version: u64,
    flags: u64,
    id: u64,
    addr: [c_char; 46],
    port: u16,
    db: u16,
}

// SAFETY: ctx must be null or point to a live module context.
pub unsafe extern "C" fn ValkeyModule_GetClientId(ctx: *mut ValkeyModuleCtx) -> c_ulonglong {
    unsafe {
        let Some(runtime) = runtime_from_ctx(ctx) else {
            return 0;
        };
        runtime.module_client_id() as c_ulonglong
    }
}

// SAFETY: ci must be null or point to writable ValkeyModuleClientInfoV1 storage.
pub unsafe extern "C" fn ValkeyModule_GetClientInfoById(
    ci: *mut c_void,
    client_id: c_ulonglong,
) -> c_int {
    unsafe {
        let Some(info) =
            with_active_runtime(|runtime| runtime.module_client_info(client_id)).flatten()
        else {
            return VALKEYMODULE_ERR;
        };
        if ci.is_null() {
            return VALKEYMODULE_OK;
        }
        let ci = &mut *ci.cast::<ValkeyModuleClientInfoV1>();
        if ci.version != CLIENTINFO_VERSION {
            return VALKEYMODULE_ERR;
        }
        populate_client_info(ci, &info);
        VALKEYMODULE_OK
    }
}

// SAFETY: ctx must point to a live module context.
pub unsafe extern "C" fn ValkeyModule_GetClientNameById(
    ctx: *mut ValkeyModuleCtx,
    client_id: c_ulonglong,
) -> *mut ValkeyModuleString {
    unsafe {
        let Some(runtime) = runtime_from_ctx(ctx) else {
            return std::ptr::null_mut();
        };
        let Some(name) = runtime.module_client_name(client_id) else {
            return std::ptr::null_mut();
        };
        super::super::auto_memory::create_auto_string(ctx, &name)
    }
}

// SAFETY: name must be null or point to a live module string.
pub unsafe extern "C" fn ValkeyModule_SetClientNameById(
    client_id: c_ulonglong,
    name: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if name.is_null() {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let name = (*name).as_bytes();
        if !is_valid_client_name(name) {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let updated = with_active_runtime(|runtime| {
            runtime.set_module_client_name(client_id, bytes::Bytes::copy_from_slice(name))
        })
        .unwrap_or(false);
        if updated {
            VALKEYMODULE_OK
        } else {
            set_errno(libc::ENOENT);
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: ctx must be null or point to a live module context.
pub unsafe extern "C" fn ValkeyModule_GetClientCertificate(
    ctx: *mut ValkeyModuleCtx,
    client_id: c_ulonglong,
) -> *mut ValkeyModuleString {
    unsafe {
        let Some(runtime) = runtime_from_ctx(ctx) else {
            return std::ptr::null_mut();
        };
        if runtime.module_client_info(client_id).is_none() {
            return std::ptr::null_mut();
        }
        std::ptr::null_mut()
    }
}

fn populate_client_info(output: &mut ValkeyModuleClientInfoV1, info: &ModuleClientInfo) {
    *output = ValkeyModuleClientInfoV1 {
        version: CLIENTINFO_VERSION,
        flags: client_flags(info),
        id: info.id,
        addr: [0; 46],
        port: info.peer_addr.port(),
        db: info.db as u16,
    };
    let address = info.peer_addr.ip().to_string();
    let bytes = address.as_bytes();
    let len = bytes.len().min(output.addr.len() - 1);
    for (target, source) in output.addr[..len].iter_mut().zip(&bytes[..len]) {
        *target = *source as c_char;
    }
}

fn client_flags(info: &ModuleClientInfo) -> u64 {
    let mut flags = 0;
    if info.is_pubsub {
        flags |= CLIENTINFO_FLAG_PUBSUB;
    }
    if info.is_blocked {
        flags |= CLIENTINFO_FLAG_BLOCKED;
    }
    if info.is_tracking {
        flags |= CLIENTINFO_FLAG_TRACKING;
    }
    if info.is_multi {
        flags |= CLIENTINFO_FLAG_MULTI;
    }
    if info.is_readonly {
        flags |= CLIENTINFO_FLAG_READONLY;
    }
    if info.is_replica {
        flags |= CLIENTINFO_FLAG_REPLICA;
    }
    if info.is_monitor {
        flags |= CLIENTINFO_FLAG_MONITOR;
    }
    if info.is_authenticated {
        flags |= CLIENTINFO_FLAG_AUTHENTICATED;
    }
    if info.was_authenticated {
        flags |= CLIENTINFO_FLAG_EVER_AUTHENTICATED;
    }
    flags
}

fn is_valid_client_name(name: &[u8]) -> bool {
    !name.iter().any(u8::is_ascii_whitespace)
}

// SAFETY: ctx must be null or point to a live module context whose runtime
// handle remains valid for this synchronous callback.
unsafe fn runtime_from_ctx(
    ctx: *mut ValkeyModuleCtx,
) -> Option<&'static mut dyn crate::engine::extension::module::capability::ModuleRuntime> {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return None;
        }
        runtime_from_opaque((*(*ctx).internal).runtime_handle)
    }
}

fn set_errno(value: c_int) {
    // SAFETY: __errno_location returns the calling thread's valid errno slot.
    crate::foundation::io::sys::os::set_errno(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    fn info() -> ModuleClientInfo {
        ModuleClientInfo {
            id: 7,
            peer_addr: ([127, 0, 0, 1], 6380).into(),
            db: 9,
            is_pubsub: true,
            is_blocked: true,
            is_tracking: true,
            is_multi: true,
            is_readonly: true,
            is_replica: true,
            is_monitor: true,
            is_authenticated: true,
            was_authenticated: true,
        }
    }

    #[test]
    fn client_info_layout_matches_the_c_v1_structure() {
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, version), 0);
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, flags), 8);
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, id), 16);
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, addr), 24);
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, port), 70);
        assert_eq!(std::mem::offset_of!(ValkeyModuleClientInfoV1, db), 72);
        assert_eq!(std::mem::size_of::<ValkeyModuleClientInfoV1>(), 80);
    }

    #[test]
    fn populates_client_info_flags_address_and_selected_db() {
        let mut output = ValkeyModuleClientInfoV1 {
            version: 1,
            flags: 0,
            id: 0,
            addr: [0; 46],
            port: 0,
            db: 0,
        };
        populate_client_info(&mut output, &info());

        assert_eq!(output.id, 7);
        assert_eq!(output.port, 6380);
        assert_eq!(output.db, 9);
        assert_eq!(
            output.flags,
            CLIENTINFO_FLAG_PUBSUB
                | CLIENTINFO_FLAG_BLOCKED
                | CLIENTINFO_FLAG_TRACKING
                | CLIENTINFO_FLAG_MULTI
                | CLIENTINFO_FLAG_READONLY
                | CLIENTINFO_FLAG_REPLICA
                | CLIENTINFO_FLAG_MONITOR
                | CLIENTINFO_FLAG_AUTHENTICATED
                | CLIENTINFO_FLAG_EVER_AUTHENTICATED
        );
        // SAFETY: populate_client_info always writes a trailing NUL.
        assert_eq!(
            unsafe { CStr::from_ptr(output.addr.as_ptr()) }.to_bytes(),
            b"127.0.0.1"
        );
    }

    #[test]
    fn client_names_reject_ascii_whitespace() {
        assert!(is_valid_client_name(b"nobody"));
        assert!(!is_valid_client_name(b"name with spaces"));
        assert!(!is_valid_client_name(b"line\nbreak"));
    }
}
