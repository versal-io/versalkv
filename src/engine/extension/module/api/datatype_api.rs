#![allow(non_snake_case)]

use libc::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::hash::{Hash, Hasher};

use super::super::{
    ValkeyModuleCtx, ValkeyModuleIO, ValkeyModuleKey, ValkeyModuleType, VALKEYMODULE_ERR,
    VALKEYMODULE_OK,
};
use super::active_registry::with_active_registry;
use super::ctx::get_module_name;

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, name is a non-null C string,
// typemethods_ptr points to a valid ValkeyModuleTypeMethods C struct
pub unsafe extern "C" fn ValkeyModule_CreateDataType(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    _encver: c_int,
    typemethods_ptr: *mut c_void,
) -> *mut ValkeyModuleType {
    unsafe {
        log::trace!(
            "ValkeyModule_CreateDataType({:?}) called",
            if name.is_null() {
                "null"
            } else {
                CStr::from_ptr(name).to_str().unwrap_or("?")
            }
        );
        if ctx.is_null() || name.is_null() || typemethods_ptr.is_null() {
            return std::ptr::null_mut();
        }
        let type_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        if type_name.len() > 9 {
            log::warn!("Module data type name '{}' too long (max 9)", type_name);
            return std::ptr::null_mut();
        }
        let module_name = get_module_name(ctx);
        if module_name.is_empty() {
            return std::ptr::null_mut();
        }

        let type_id = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            module_name.hash(&mut hasher);
            type_name.hash(&mut hasher);
            hasher.finish()
        };

        //   [0] version: u64
        //   [1] rdb_load: *mut c_void
        //   [2] rdb_save: *mut c_void
        //   [3] aof_rewrite: *mut c_void
        //   [4] mem_usage: *mut c_void
        //   [5] digest: *mut c_void
        //   [6] free: *mut c_void
        //   ...
        let methods = typemethods_ptr as *mut *mut c_void;
        let rdb_load = if !(*methods.add(1)).is_null() {
            Some(std::mem::transmute::<
                *mut c_void,
                extern "C" fn(*mut ValkeyModuleIO, c_int) -> *mut c_void,
            >(*methods.add(1)))
        } else {
            None
        };
        let rdb_save = if !(*methods.add(2)).is_null() {
            Some(std::mem::transmute::<
                *mut c_void,
                extern "C" fn(*mut ValkeyModuleIO, *mut c_void),
            >(*methods.add(2)))
        } else {
            None
        };
        let aof_rewrite = if !(*methods.add(3)).is_null() {
            Some(std::mem::transmute::<
                *mut c_void,
                extern "C" fn(*mut ValkeyModuleIO, *mut c_void, *mut c_void),
            >(*methods.add(3)))
        } else {
            None
        };
        let free = if !(*methods.add(6)).is_null() {
            Some(std::mem::transmute::<*mut c_void, extern "C" fn(*mut c_void)>(*methods.add(6)))
        } else {
            None
        };

        // c_char is signed on x86_64 but unsigned on aarch64 Linux; cast through
        // the platform alias so the generated struct field matches on both ABIs.
        let mut name_bytes = [0 as c_char; 10];
        let name_cstr = CString::new(type_name).unwrap_or_default();
        let name_cbytes = name_cstr.as_bytes_with_nul();
        let copy_len = name_cbytes.len().min(9);
        for (i, &b) in name_cbytes[..copy_len].iter().enumerate() {
            name_bytes[i] = b as c_char;
        }
        name_bytes[copy_len] = 0;

        let type_ = Box::new(ValkeyModuleType {
            id: type_id,
            module: std::ptr::null_mut(),
            rdb_load,
            rdb_save,
            aof_rewrite,
            mem_usage: None,
            digest: None,
            free,
            free_effort: None,
            unlink: None,
            copy: None,
            defrag: None,
            mem_usage2: None,
            free2: None,
            free_effort2: None,
            unlink2: None,
            copy2: None,
            aux_save: None,
            aux_save2: None,
            aux_load: None,
            aux_save_triggers: 0,
            name: name_bytes,
        });

        let type_ptr = Box::into_raw(type_);
        let type_ref: &'static ValkeyModuleType = &*type_ptr;

        let result = match with_active_registry(|reg| reg.register_type(&module_name, type_ref)) {
            Some(result) => result,
            None => {
                let _ = Box::from_raw(type_ptr);
                return std::ptr::null_mut();
            }
        };
        match result {
            Ok(()) => {
                log::info!(
                    "Module '{}' created data type '{}' (id: {:#x})",
                    module_name,
                    type_name,
                    type_id
                );
                type_ptr
            }
            Err(e) => {
                log::warn!("Failed to register type '{}': {}", type_name, e);
                let _ = Box::from_raw(type_ptr);
                std::ptr::null_mut()
            }
        }
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer, mt is a valid ValkeyModuleType pointer, value is an opaque pointer to module-owned data
pub unsafe extern "C" fn ValkeyModule_ModuleTypeSetValue(
    key: *mut ValkeyModuleKey,
    mt: *mut ValkeyModuleType,
    value: *mut c_void,
) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_ModuleTypeSetValue() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_ERR,
        };
        if handle.db.is_null() || mt.is_null() {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        db.set_module_value(handle.key.clone(), (*mt).id, value);
        VALKEYMODULE_OK
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey
pub unsafe extern "C" fn ValkeyModule_ModuleTypeGetValue(key: *mut ValkeyModuleKey) -> *mut c_void {
    unsafe {
        log::trace!("ValkeyModule_ModuleTypeGetValue() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        if handle.db.is_null() {
            return std::ptr::null_mut();
        }
        let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
        db.get_module_value(&handle.key)
            .map(|(_, v)| *v)
            .unwrap_or(std::ptr::null_mut())
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer, mt is a valid ValkeyModuleType pointer, value is an opaque pointer to module-owned data
pub unsafe extern "C" fn ValkeyModule_ModuleTypeReplaceValue(
    key: *mut ValkeyModuleKey,
    mt: *mut ValkeyModuleType,
    value: *mut c_void,
) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_ModuleTypeReplaceValue() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_ERR,
        };
        if handle.db.is_null() || mt.is_null() {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        db.set_module_value(handle.key.clone(), (*mt).id, value);
        VALKEYMODULE_OK
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey
pub unsafe extern "C" fn ValkeyModule_ModuleTypeGetType(
    key: *mut ValkeyModuleKey,
) -> *mut ValkeyModuleType {
    unsafe {
        log::trace!("ValkeyModule_ModuleTypeGetType() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        if handle.db.is_null() {
            return std::ptr::null_mut();
        }
        let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
        let type_id = match db.get_module_value(&handle.key) {
            Some((id, _)) => *id,
            None => return std::ptr::null_mut(),
        };
        with_active_registry(|reg| {
            reg.lookup_type_by_id(type_id)
                .map(|t| *t as *const ValkeyModuleType as *mut ValkeyModuleType)
                .unwrap_or(std::ptr::null_mut())
        })
        .unwrap_or(std::ptr::null_mut())
    }
}
