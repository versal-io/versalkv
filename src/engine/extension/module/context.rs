use libc::c_void;
use std::ffi::CString;

use super::{api, ModuleInternalCtx, ValkeyModuleCtx};

pub fn create_module_internal_ctx(name: &str, module_ref: *mut c_void) -> Box<ModuleInternalCtx> {
    let cname = CString::new(name).unwrap_or_else(|_| CString::new("unknown").unwrap());
    Box::new(ModuleInternalCtx {
        module_name: cname.into_raw(),
        module_ref,
        onload: 1,
        reply_ptr: std::ptr::null_mut(),
        runtime_handle: std::ptr::null_mut(),
        db_idx: 0,
        resp_version: 2,
        username: CString::new("default").unwrap().into_raw(),
        module_auth_user: std::ptr::null_mut(),
        current_command_name: std::ptr::null_mut(),
    })
}

pub fn create_module_cmd_ctx(
    name: &str,
    command_name: &str,
    module_ref: *mut c_void,
    runtime_handle: *mut c_void,
) -> Box<ModuleInternalCtx> {
    let cname = CString::new(name).unwrap_or_else(|_| CString::new("unknown").unwrap());
    let command_name =
        CString::new(command_name).unwrap_or_else(|_| CString::new("unknown").unwrap());
    let reply = Box::new(api::ReplyBuilder::new());
    let reply_ptr = Box::into_raw(reply) as *mut c_void;
    Box::new(ModuleInternalCtx {
        module_name: cname.into_raw(),
        module_ref,
        onload: 0,
        reply_ptr,
        runtime_handle,
        db_idx: 0,
        resp_version: 2,
        username: CString::new("default").unwrap().into_raw(),
        module_auth_user: std::ptr::null_mut(),
        current_command_name: command_name.into_raw(),
    })
}

// SAFETY: The caller must ensure that `internal` is either null or points to a
// valid, heap-allocated ModuleInternalCtx that was created by
// create_module_internal_ctx or create_module_cmd_ctx and has not been freed
// yet. The reply_ptr field must point to a Box<ReplyBuilder> allocation.
pub unsafe fn free_reply_buffer(internal: *mut ModuleInternalCtx) {
    unsafe {
        if !internal.is_null() && !(*internal).reply_ptr.is_null() {
            let _ = Box::from_raw((*internal).reply_ptr as *mut api::ReplyBuilder);
            (*internal).reply_ptr = std::ptr::null_mut();
        }
    }
}

pub fn create_module_ctx(internal: *mut ModuleInternalCtx) -> Box<ValkeyModuleCtx> {
    Box::new(ValkeyModuleCtx {
        getapifuncptr: std::ptr::null_mut(),
        internal,
    })
}

// SAFETY: The caller must ensure that `ctx` is either null or points to a
// valid, heap-allocated ValkeyModuleCtx that was created by create_module_ctx
// and has not been freed yet. This function takes ownership of both the ctx
// and its internal ModuleInternalCtx, dropping them both.
pub unsafe fn free_module_ctx(ctx: *mut ValkeyModuleCtx) {
    unsafe {
        if !ctx.is_null() {
            let internal = (*ctx).internal;
            if !internal.is_null() {
                free_reply_buffer(internal);
                if !(*internal).module_name.is_null() {
                    let _ = CString::from_raw((*internal).module_name);
                }
                if !(*internal).username.is_null() {
                    let _ = CString::from_raw((*internal).username);
                }
                if !(*internal).module_auth_user.is_null() {
                    let _ = CString::from_raw((*internal).module_auth_user);
                }
                if !(*internal).current_command_name.is_null() {
                    let _ = CString::from_raw((*internal).current_command_name);
                }
                let _ = Box::from_raw(internal);
            }
            let _ = Box::from_raw(ctx);
        }
    }
}
