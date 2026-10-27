#![allow(non_snake_case)]

use bytes::Bytes;
use libc::{c_int, c_long};

use super::super::{ValkeyModuleKey, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use crate::foundation::io::store::structure::list::{
    would_grow_exceed_limit, would_shrink_fit_limit,
};

const MODULE_READ: c_int = 1;
const MODULE_WRITE: c_int = 1 << 1;

fn set_errno(value: c_int) {
    // SAFETY: __errno_location returns the calling thread's valid errno slot.
    crate::foundation::io::sys::os::set_errno(value);
}

fn existing_index(index: c_long, len: usize) -> Option<usize> {
    let index = if index < 0 {
        (len as i128).checked_add(index as i128)?
    } else {
        index as i128
    };
    (index >= 0 && index < len as i128).then_some(index as usize)
}

fn insertion_index(index: c_long, len: usize) -> Option<usize> {
    let index = if index < 0 {
        (len as i128).checked_add(index as i128)?.checked_add(1)?
    } else {
        index as i128
    };
    (index >= 0 && index <= len as i128).then_some(index as usize)
}

// SAFETY: The key must be a live module key handle opened for reading.
pub unsafe extern "C" fn ValkeyModule_ListGet(
    key: *mut ValkeyModuleKey,
    index: c_long,
) -> *mut ValkeyModuleString {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return std::ptr::null_mut();
        };
        if handle.mode & MODULE_READ == 0 {
            set_errno(libc::EBADF);
            return std::ptr::null_mut();
        }
        if handle.db.is_null() {
            set_errno(libc::EINVAL);
            return std::ptr::null_mut();
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let list = match db.get_list(&handle.key) {
            Ok(Some(list)) => list,
            Ok(None) | Err(()) => {
                set_errno(libc::ENOTSUP);
                return std::ptr::null_mut();
            }
        };
        let Some(index) = existing_index(index, list.len()) else {
            set_errno(libc::EDOM);
            return std::ptr::null_mut();
        };
        super::super::auto_memory::create_string_for_owner(handle.owner, &list[index])
    }
}

// SAFETY: The key must be a live module key handle and value a valid module string.
pub unsafe extern "C" fn ValkeyModule_ListSet(
    key: *mut ValkeyModuleKey,
    index: c_long,
    value: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if value.is_null() {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        if handle.mode & MODULE_WRITE == 0 {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let list = match db.get_list_mut(&handle.key) {
            Ok(Some(list)) => list,
            Ok(None) | Err(()) => {
                set_errno(libc::ENOTSUP);
                return VALKEYMODULE_ERR;
            }
        };
        let Some(index) = existing_index(index, list.len()) else {
            set_errno(libc::EDOM);
            return VALKEYMODULE_ERR;
        };
        let value = (*value).as_bytes();
        if list.encoding_name(handle.list_max_listpack_size) == "listpack"
            && would_grow_exceed_limit(list, value, handle.list_max_listpack_size)
        {
            list.set_encoding_override("quicklist");
        }
        list[index] = Bytes::copy_from_slice(value);
        if list.encoding_name(handle.list_max_listpack_size) == "quicklist"
            && would_shrink_fit_limit(list, handle.list_max_listpack_size)
        {
            list.clear_encoding_override();
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: The key must be a live module key handle and value a valid module string.
pub unsafe extern "C" fn ValkeyModule_ListInsert(
    key: *mut ValkeyModuleKey,
    index: c_long,
    value: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if value.is_null() {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        if handle.mode & MODULE_WRITE == 0 {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        if db.get_no_expire(&handle.key).is_some() && db.get_list_mut(&handle.key).is_err() {
            set_errno(libc::ENOTSUP);
            return VALKEYMODULE_ERR;
        }
        let len = db
            .get_list(&handle.key)
            .ok()
            .flatten()
            .map_or(0, |list| list.len());
        let Some(index) = insertion_index(index, len) else {
            set_errno(libc::EDOM);
            return VALKEYMODULE_ERR;
        };
        let value = (*value).as_bytes();
        let list = match db.get_or_create_list(&handle.key) {
            Ok(list) => list,
            Err(()) => {
                set_errno(libc::ENOTSUP);
                return VALKEYMODULE_ERR;
            }
        };
        list.insert(index, Bytes::copy_from_slice(value));
        list.update_encoding_after_grow(handle.list_max_listpack_size);
        VALKEYMODULE_OK
    }
}

// SAFETY: The key must be a live module key handle opened for writing.
pub unsafe extern "C" fn ValkeyModule_ListDelete(
    key: *mut ValkeyModuleKey,
    index: c_long,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        if handle.mode & MODULE_WRITE == 0 {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let is_empty = {
            let list = match db.get_list_mut(&handle.key) {
                Ok(Some(list)) => list,
                Ok(None) | Err(()) => {
                    set_errno(libc::ENOTSUP);
                    return VALKEYMODULE_ERR;
                }
            };
            let Some(index) = existing_index(index, list.len()) else {
                set_errno(libc::EDOM);
                return VALKEYMODULE_ERR;
            };
            list.note_encoding_before_shrink(handle.list_max_listpack_size);
            list.remove(index);
            if !list.is_empty() {
                list.update_encoding_after_shrink(handle.list_max_listpack_size);
            }
            list.is_empty()
        };
        if is_empty {
            db.del(&handle.key);
        }
        VALKEYMODULE_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::ModuleKeyHandle;
    use crate::foundation::io::store::kv_engine::Db;

    fn key(db: &mut Db) -> *mut ValkeyModuleKey {
        ValkeyModuleKey::from_handle(Box::into_raw(ModuleKeyHandle::new(
            (db as *mut Db).cast(),
            b"list".to_vec(),
            MODULE_READ | MODULE_WRITE,
            4,
            false,
        )))
    }

    #[test]
    fn insertion_index_matches_module_negative_index_rules() {
        assert_eq!(insertion_index(0, 3), Some(0));
        assert_eq!(insertion_index(3, 3), Some(3));
        assert_eq!(insertion_index(-1, 3), Some(3));
        assert_eq!(insertion_index(-2, 3), Some(2));
        assert_eq!(insertion_index(-4, 3), Some(0));
        assert_eq!(insertion_index(-5, 3), None);
    }

    #[test]
    fn deleting_last_element_removes_key() {
        let mut db = Db::new();
        db.get_or_create_list(b"list")
            .unwrap()
            .push_back(Bytes::from_static(b"value"));
        let key = key(&mut db);
        // SAFETY: key wraps a live db pointer for this synchronous call and is
        // reclaimed exactly once after the module API finishes using it.
        unsafe {
            assert_eq!(ValkeyModule_ListDelete(key, 0), VALKEYMODULE_OK);
            let _ = Box::from_raw(key as *mut ModuleKeyHandle);
        }
        assert!(!db.exists_no_expire(b"list"));
    }
}
