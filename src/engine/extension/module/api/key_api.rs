#![allow(non_snake_case)]

use libc::{c_int, size_t};

use super::super::{
    ModuleKeyHandle, ValkeyModuleKey, VALKEYMODULE_ERR, VALKEYMODULE_KEYTYPE_EMPTY,
    VALKEYMODULE_KEYTYPE_HASH, VALKEYMODULE_KEYTYPE_LIST, VALKEYMODULE_KEYTYPE_MODULE,
    VALKEYMODULE_KEYTYPE_SET, VALKEYMODULE_KEYTYPE_STREAM, VALKEYMODULE_KEYTYPE_STRING,
    VALKEYMODULE_KEYTYPE_ZSET, VALKEYMODULE_OK,
};
use crate::engine::extension::module::capability::runtime_from_opaque;
use crate::foundation::io::store::robj::Collection;
use crate::foundation::io::store::Robj;

const MODULE_WRITE: c_int = 1 << 1;
const OPEN_KEY_MODES_ALL: c_int = 0x1f0003;

pub extern "C" fn ValkeyModule_GetOpenKeyModesAll() -> c_int {
    OPEN_KEY_MODES_ALL
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, keyname is a valid ValkeyModuleString pointer
pub unsafe extern "C" fn ValkeyModule_OpenKey(
    ctx: *mut super::super::ValkeyModuleCtx,
    keyname: *mut super::super::ValkeyModuleString,
    mode: c_int,
) -> *mut ValkeyModuleKey {
    unsafe {
        log::trace!("ValkeyModule_OpenKey(mode={}) called", mode);
        if ctx.is_null() || keyname.is_null() {
            return std::ptr::null_mut();
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return std::ptr::null_mut();
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return std::ptr::null_mut();
        };
        let db_idx = (*internal).db_idx as u32;
        let list_max_listpack_size = runtime.list_max_listpack_size();
        let uses_lfu = runtime.uses_lfu();
        let db = match runtime.db_mut(db_idx) {
            Some(db) => db,
            None => return std::ptr::null_mut(),
        };
        let key_bytes = (*keyname).as_bytes().to_vec();
        let key_exists = db.get_no_expire(&key_bytes).is_some() || db.is_module_key(&key_bytes);
        if !key_exists && !should_open_missing_key(mode) {
            return std::ptr::null_mut();
        }
        if key_exists && uses_lfu {
            db.initialize_lfu_freq(&key_bytes, 5);
        }
        let handle = ModuleKeyHandle::new(
            (db as *mut crate::foundation::io::store::kv_engine::Db).cast(),
            key_bytes,
            mode,
            list_max_listpack_size,
            uses_lfu,
        );
        ValkeyModuleKey::from_handle(Box::into_raw(handle))
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey (or null, which is handled)
pub unsafe extern "C" fn ValkeyModule_CloseKey(key: *mut ValkeyModuleKey) {
    unsafe {
        log::trace!("ValkeyModule_CloseKey() called");
        if !key.is_null() {
            let _ = Box::from_raw(key as *mut ModuleKeyHandle);
        }
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey
pub unsafe extern "C" fn ValkeyModule_KeyType(key: *mut ValkeyModuleKey) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_KeyType() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_KEYTYPE_EMPTY,
        };
        if handle.db.is_null() {
            return VALKEYMODULE_KEYTYPE_EMPTY;
        }
        let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);

        if let Some(robj) = db.get_no_expire(&handle.key) {
            return match robj {
                Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => {
                    VALKEYMODULE_KEYTYPE_STRING
                }
                Robj::ListQuicklist(_) | Robj::ListListpack(_) => VALKEYMODULE_KEYTYPE_LIST,
                Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => {
                    VALKEYMODULE_KEYTYPE_SET
                }
                Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => VALKEYMODULE_KEYTYPE_ZSET,
                Robj::HashHashtable(_) | Robj::HashListpack(_) => VALKEYMODULE_KEYTYPE_HASH,
                Robj::StreamListpack(_) | Robj::StreamRadix(_) => VALKEYMODULE_KEYTYPE_STREAM,
            };
        }

        if db.is_module_key(&handle.key) {
            return VALKEYMODULE_KEYTYPE_MODULE;
        }

        VALKEYMODULE_KEYTYPE_EMPTY
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey
pub unsafe extern "C" fn ValkeyModule_ValueLength(key: *mut ValkeyModuleKey) -> size_t {
    unsafe {
        log::trace!("ValkeyModule_ValueLength() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return 0,
        };
        if handle.db.is_null() {
            return 0;
        }
        let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
        if let Some(robj) = db.get_no_expire(&handle.key) {
            return match robj {
                Robj::StrInt(s) => s.len(),
                Robj::StrEmbstr(s) => s.len(),
                Robj::StrRaw(s) => s.0.len(),
                Robj::ListQuicklist(l) => l.len(),
                Robj::ListListpack(l) => l.len(),
                Robj::SetHashtable(s) => s.data.len(),
                Robj::SetIntset(s) => s.len(),
                Robj::SetListpack(s) => s.len(),
                Robj::ZSetSkiplist(z) => z.len(),
                Robj::ZSetListpack(z) => z.len(),
                Robj::HashHashtable(h) => h.len(),
                Robj::HashListpack(h) => h.len(),
                Robj::StreamListpack(st) => st.len(),
                Robj::StreamRadix(_) => 0,
            } as size_t;
        }
        0
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx pointer, keyname is a valid ValkeyModuleString pointer.
pub unsafe extern "C" fn ValkeyModule_KeyExists(
    ctx: *mut super::super::ValkeyModuleCtx,
    keyname: *mut super::super::ValkeyModuleString,
) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_KeyExists() called");
        if ctx.is_null() || keyname.is_null() {
            return 0;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return 0;
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return 0;
        };
        let db_idx = (*internal).db_idx as u32;
        let db = match runtime.db_mut(db_idx) {
            Some(db) => db,
            None => return 0,
        };
        let key = (*keyname).as_bytes();
        if db.get_no_expire(key).is_some() || db.is_module_key(key) {
            1
        } else {
            0
        }
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey
pub unsafe extern "C" fn ValkeyModule_DeleteKey(key: *mut ValkeyModuleKey) -> c_int {
    unsafe {
        log::trace!("ValkeyModule_DeleteKey() called");
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(h) => h,
            None => return VALKEYMODULE_ERR,
        };
        if handle.db.is_null() {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        if db.del(&handle.key) {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: Called by C module code; key is a valid ValkeyModuleKey pointer returned by ValkeyModule_OpenKey; delegates to ValkeyModule_DeleteKey
pub unsafe extern "C" fn ValkeyModule_UnlinkKey(key: *mut ValkeyModuleKey) -> c_int {
    unsafe { ValkeyModule_DeleteKey(key) }
}

fn should_open_missing_key(mode: c_int) -> bool {
    mode & MODULE_WRITE != 0
}

#[cfg(test)]
mod tests {
    use super::should_open_missing_key;

    #[test]
    fn read_only_open_rejects_missing_keys_but_write_open_allows_them() {
        assert!(!should_open_missing_key(1));
        assert!(should_open_missing_key(2));
        assert!(should_open_missing_key(3));
    }
}
