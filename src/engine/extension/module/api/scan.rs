//! Module keyspace and aggregate scan APIs.

#![allow(non_snake_case)]

use libc::{c_int, c_void};

use super::super::{
    ValkeyModuleCtx, ValkeyModuleKey, ValkeyModuleScanCB, ValkeyModuleScanKeyCB,
    ValkeyModuleScanKeyRawBorrowedCB, ValkeyModuleString, VALKEYMODULE_OK,
};
use crate::engine::extension::module::capability::runtime_from_opaque;

fn set_errno(value: c_int) {
    crate::foundation::io::sys::os::set_errno(value);
}

#[repr(C)]
pub struct ValkeyModuleScanCursor {
    pub index: usize,
    pub done: bool,
}

// SAFETY: This function creates and returns an owned cursor allocation.
pub unsafe extern "C" fn ValkeyModule_ScanCursorCreate() -> *mut ValkeyModuleScanCursor {
    Box::into_raw(Box::new(ValkeyModuleScanCursor {
        index: 0,
        done: false,
    }))
}

// SAFETY: cursor is null or a live cursor allocated by this API.
pub unsafe extern "C" fn ValkeyModule_ScanCursorRestart(cursor: *mut ValkeyModuleScanCursor) {
    unsafe {
        if !cursor.is_null() {
            (*cursor).index = 0;
            (*cursor).done = false;
        }
    }
}

// SAFETY: cursor is null or an owned cursor allocation returned by this API.
pub unsafe extern "C" fn ValkeyModule_ScanCursorDestroy(cursor: *mut ValkeyModuleScanCursor) {
    unsafe {
        if !cursor.is_null() {
            let _ = Box::from_raw(cursor);
        }
    }
}

// SAFETY: ctx, cursor, callback, and privdata follow the synchronous module scan ABI.
pub unsafe extern "C" fn ValkeyModule_Scan(
    ctx: *mut ValkeyModuleCtx,
    cursor: *mut ValkeyModuleScanCursor,
    callback: Option<ValkeyModuleScanCB>,
    privdata: *mut c_void,
) -> c_int {
    unsafe {
        if ctx.is_null() || cursor.is_null() || callback.is_none() {
            set_errno(libc::EINVAL);
            return 0;
        }
        if (*cursor).done {
            set_errno(libc::ENOENT);
            return 0;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            set_errno(libc::EINVAL);
            return 0;
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            set_errno(libc::EINVAL);
            return 0;
        };
        let keys = runtime.module_scan_keys((*internal).db_idx as u32);
        let Some(key) = keys.get((*cursor).index).cloned() else {
            (*cursor).done = true;
            set_errno(0);
            return VALKEYMODULE_OK;
        };
        (*cursor).index += 1;
        let key_string = Box::into_raw(ValkeyModuleString::from_bytes(&key));
        {
            let _context_scope = super::ActiveModuleContextScope::enter(ctx);
            callback.expect("callback checked above")(
                ctx,
                key_string,
                std::ptr::null_mut(),
                privdata,
            );
        }
        ValkeyModuleString::release(key_string);
        if (*cursor).index >= keys.len() {
            (*cursor).done = true;
            set_errno(0);
            VALKEYMODULE_OK
        } else {
            set_errno(0);
            1
        }
    }
}

// SAFETY: key, cursor, callback, and privdata follow the synchronous module scan ABI.
pub unsafe extern "C" fn ValkeyModule_ScanKey(
    key: *mut ValkeyModuleKey,
    cursor: *mut ValkeyModuleScanCursor,
    callback: Option<ValkeyModuleScanKeyCB>,
    privdata: *mut c_void,
) -> c_int {
    // SAFETY: the caller keeps all handles and the callback valid for this scan.
    unsafe {
        let Some(callback) = callback else {
            set_errno(libc::EINVAL);
            return 0;
        };
        scan_key_entries(key, cursor, |field, value| {
            let field_ptr = Box::into_raw(ValkeyModuleString::from_bytes(field));
            let value_ptr = value
                .map(|bytes| Box::into_raw(ValkeyModuleString::from_bytes(bytes)))
                .unwrap_or(std::ptr::null_mut());
            callback(key, field_ptr, value_ptr, privdata);
            ValkeyModuleString::release(field_ptr);
            ValkeyModuleString::release(value_ptr);
        })
    }
}

// SAFETY: handles and callback remain live for this synchronous scan. Byte
// pointers are borrowed only for the duration of each callback.
pub unsafe extern "C" fn ValkeyModule_ScanKeyRawBorrowed(
    key: *mut ValkeyModuleKey,
    cursor: *mut ValkeyModuleScanCursor,
    callback: Option<ValkeyModuleScanKeyRawBorrowedCB>,
    privdata: *mut c_void,
) -> c_int {
    // SAFETY: scan_key_entries owns the byte vectors throughout each callback.
    unsafe {
        let Some(callback) = callback else {
            set_errno(libc::EINVAL);
            return 0;
        };
        scan_key_entries(key, cursor, |field, value| {
            let (value_ptr, value_len) = value
                .map(|bytes| (bytes.as_ptr().cast(), bytes.len()))
                .unwrap_or((std::ptr::null(), 0));
            callback(
                key,
                field.as_ptr().cast(),
                field.len(),
                value_ptr,
                value_len,
                privdata,
            );
        })
    }
}

// SAFETY: key and cursor are live module handles when non-null; delivery is
// synchronous and cannot retain a reference to the callback-scoped byte slices.
unsafe fn scan_key_entries(
    key: *mut ValkeyModuleKey,
    cursor: *mut ValkeyModuleScanCursor,
    mut deliver: impl FnMut(&[u8], Option<&[u8]>),
) -> c_int {
    // SAFETY: validate handles before dereferencing and end the Db borrow before callbacks.
    unsafe {
        if key.is_null() || cursor.is_null() {
            set_errno(libc::EINVAL);
            return 0;
        }
        if (*cursor).done {
            set_errno(libc::ENOENT);
            return 0;
        }
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return 0;
        };
        let internal = if let Some(owner) = handle.owner {
            owner.as_ptr()
        } else if let Some(ctx) = super::active_registry::active_module_context() {
            (*ctx).internal
        } else {
            std::ptr::null_mut()
        };
        let (entries, is_incremental) = if !internal.is_null() {
            let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
                set_errno(libc::EINVAL);
                return 0;
            };
            let entries = match runtime.module_scan_key((*internal).db_idx as u32, &handle.key) {
                Ok(entries) => entries,
                Err(()) => {
                    set_errno(libc::EINVAL);
                    return 0;
                }
            };
            (entries.entries, entries.is_incremental)
        } else {
            if handle.db.is_null() {
                set_errno(libc::EINVAL);
                return 0;
            }
            let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
            use crate::foundation::io::store::Robj;
            let Some(object) = db.get_no_expire(&handle.key) else {
                set_errno(libc::EINVAL);
                return 0;
            };
            let is_incremental = matches!(
                object,
                Robj::HashHashtable(_) | Robj::SetHashtable(_) | Robj::ZSetSkiplist(_)
            );
            let entries = match db.module_scan_key(&handle.key) {
                Ok(entries) => entries,
                Err(()) => {
                    set_errno(libc::EINVAL);
                    return 0;
                }
            };
            (entries, is_incremental)
        };
        let end = if is_incremental {
            (*cursor).index.saturating_add(1).min(entries.len())
        } else {
            entries.len()
        };
        for (field, value) in entries.iter().take(end).skip((*cursor).index) {
            deliver(field, value.as_deref());
        }
        (*cursor).index = end;
        (*cursor).done = end >= entries.len();
        set_errno(0);
        c_int::from(!(*cursor).done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_apis_return_zero_for_invalid_arguments() {
        let mut cursor = ValkeyModuleScanCursor {
            index: 0,
            done: false,
        };

        // SAFETY: Null handles intentionally exercise the C API validation path.
        let scan_result = unsafe {
            ValkeyModule_Scan(
                std::ptr::null_mut(),
                &mut cursor,
                None,
                std::ptr::null_mut(),
            )
        };
        // SAFETY: Null handles intentionally exercise the C API validation path.
        let scan_key_result = unsafe {
            ValkeyModule_ScanKey(
                std::ptr::null_mut(),
                &mut cursor,
                None,
                std::ptr::null_mut(),
            )
        };

        assert_eq!(scan_result, 0);
        assert_eq!(scan_key_result, 0);
    }
}
