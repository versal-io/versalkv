#![allow(non_snake_case)]

use libc::{c_int, c_void, size_t};

use super::super::{ValkeyModuleKey, ValkeyModuleString};
#[cfg(test)]
use bytes::Bytes;

const HASH_NX: c_int = 1 << 0;
const HASH_XX: c_int = 1 << 1;
const HASH_CFIELDS: c_int = 1 << 2;
const HASH_COUNT_ALL: c_int = 1 << 4;
const HASH_EXISTS: c_int = 1 << 3;
const HASH_ALLOWED_FLAGS: c_int = HASH_NX | HASH_XX | HASH_CFIELDS | HASH_COUNT_ALL;
const MODULE_WRITE: c_int = 1 << 1;
const HASH_DELETE: usize = 1;

// SAFETY: the variadic shim provides live key/field handles and an output slot
// matching HASH_EXISTS. A null field is used only to validate the key type.
#[export_name = "VM_HashGetField"]
pub unsafe extern "C" fn hash_get_field(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    field: *mut c_void,
    output: *mut c_void,
) -> c_int {
    // SAFETY: validate handles before dereferencing; all borrows end before return.
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return super::super::VALKEYMODULE_ERR;
        };
        if handle.db.is_null() {
            return super::super::VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        if db.get_hash_object(&handle.key).is_err() {
            return super::super::VALKEYMODULE_ERR;
        }
        if field.is_null() {
            return super::super::VALKEYMODULE_OK;
        }
        if output.is_null() {
            return super::super::VALKEYMODULE_ERR;
        }
        let field = if flags & HASH_CFIELDS != 0 {
            std::ffi::CStr::from_ptr(field.cast()).to_bytes()
        } else {
            (*(field as *mut ValkeyModuleString)).as_bytes()
        };
        let value = if db.hash_field_expired(&handle.key, field) {
            None
        } else {
            db.get_hash_object(&handle.key)
                .ok()
                .flatten()
                .and_then(|hash| hash.hash_get(field))
        };
        if flags & HASH_EXISTS != 0 {
            *(output as *mut c_int) = c_int::from(value.is_some());
        } else {
            *(output as *mut *mut ValkeyModuleString) = value
                .map(|bytes| {
                    super::super::auto_memory::create_string_for_owner(handle.owner, &bytes)
                })
                .unwrap_or(std::ptr::null_mut());
        }
        super::super::VALKEYMODULE_OK
    }
}

/// Non-variadic entrypoint used by the C shim after collecting field/value pairs.
/// Returns a non-negative field count on success or a negative errno value.
// SAFETY: The C shim passes arrays containing `pair_count` valid field and value
// pointers. Fields are either module strings or C strings according to flags.
#[export_name = "VM_HashSetPairs"]
pub unsafe extern "C" fn hash_set_pairs(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    fields: *const *mut c_void,
    values: *const *mut ValkeyModuleString,
    pair_count: size_t,
) -> c_int {
    // SAFETY: the shim contract guarantees the key and both pair arrays remain
    // valid for pair_count entries during this synchronous operation.
    unsafe {
        if key.is_null() || flags & !HASH_ALLOWED_FLAGS != 0 || fields.is_null() || values.is_null()
        {
            return -libc::EINVAL;
        }
        let handle = match ValkeyModuleKey::as_handle(key) {
            Some(handle) => handle,
            None => return -libc::EINVAL,
        };
        if handle.mode & MODULE_WRITE == 0 {
            return -libc::EBADF;
        }
        if handle.db.is_null() {
            return -libc::EINVAL;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        if db.get_hash_object(&handle.key).is_err() {
            return -libc::ENOTSUP;
        }

        let mut count = 0;
        for index in 0..pair_count {
            let field_ptr = *fields.add(index);
            let value_ptr = *values.add(index);
            if field_ptr.is_null() {
                return -libc::EINVAL;
            }
            let field = if flags & HASH_CFIELDS != 0 {
                std::ffi::CStr::from_ptr(field_ptr.cast()).to_bytes()
            } else {
                (*(field_ptr as *mut ValkeyModuleString)).as_bytes()
            };

            let exists = db
                .get_hash_object(&handle.key)
                .ok()
                .flatten()
                .is_some_and(|hash| hash.hash_contains(field))
                && !db.hash_field_expired(&handle.key, field);
            if flags & HASH_XX != 0 && !exists || flags & HASH_NX != 0 && exists {
                continue;
            }

            if value_ptr as usize == HASH_DELETE {
                if exists {
                    if let Ok(Some(hash)) = db.get_hash_object_mut(&handle.key) {
                        count += c_int::from(hash.hash_del(field));
                    }
                    db.remove_hash_field_expire(&handle.key, field);
                    db.clear_hash_string_ref(&handle.key, field);
                }
                continue;
            }
            if value_ptr.is_null() {
                return -libc::EINVAL;
            }

            let value = (*value_ptr).as_bytes();
            let inserted = match db.hash_set_with_limits(&handle.key, field, value, 512, 64) {
                Ok(inserted) => inserted,
                Err(()) => return -libc::ENOTSUP,
            };
            db.remove_hash_field_expire(&handle.key, field);
            db.clear_hash_string_ref(&handle.key, field);
            count += c_int::from(!inserted && exists || flags & HASH_COUNT_ALL != 0);
        }

        if db
            .get_hash_object(&handle.key)
            .ok()
            .flatten()
            .is_some_and(|hash| hash.hash_is_empty())
        {
            db.del(&handle.key);
        }
        count
    }
}

// SAFETY: key is a module key handle, field is a live module string, and buf
// points to len readable bytes for the duration of this call.
pub unsafe extern "C" fn ValkeyModule_HashSetStringRef(
    key: *mut ValkeyModuleKey,
    field: *mut ValkeyModuleString,
    buf: *const libc::c_char,
    len: size_t,
) -> c_int {
    unsafe {
        if key.is_null() || field.is_null() || buf.is_null() {
            return super::super::VALKEYMODULE_ERR;
        }
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return super::super::VALKEYMODULE_ERR;
        };
        if handle.mode & MODULE_WRITE == 0 || handle.db.is_null() {
            return super::super::VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let field = (*field).as_bytes();
        let value = std::slice::from_raw_parts(buf.cast::<u8>(), len);
        if db.set_hash_string_ref(&handle.key, field, value) {
            super::super::VALKEYMODULE_OK
        } else {
            super::super::VALKEYMODULE_ERR
        }
    }
}

// SAFETY: key is a module key handle and field is a live module string.
pub unsafe extern "C" fn ValkeyModule_HashHasStringRef(
    key: *mut ValkeyModuleKey,
    field: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if key.is_null() || field.is_null() {
            return 0;
        }
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return 0;
        };
        if handle.db.is_null() {
            return 0;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let field = (*field).as_bytes();
        c_int::from(db.hash_has_string_ref(&handle.key, field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::{ModuleKeyHandle, ValkeyModuleKey};
    use crate::foundation::io::store::kv_engine::Db;

    fn module_string(value: &[u8]) -> *mut ValkeyModuleString {
        Box::into_raw(ValkeyModuleString::from_bytes(value))
    }

    // SAFETY: This helper owns every raw allocation it creates, keeps the Db
    // live for the synchronous ABI call, and reclaims each non-sentinel pointer.
    unsafe fn set_pairs(db: &mut Db, flags: c_int, pairs: &[(&[u8], Option<&[u8]>)]) -> c_int {
        unsafe {
            let handle = ModuleKeyHandle::new(
                (db as *mut Db).cast(),
                b"hash".to_vec(),
                MODULE_WRITE,
                -2,
                false,
            );
            let key = ValkeyModuleKey::from_handle(Box::into_raw(handle));
            let fields: Vec<_> = pairs
                .iter()
                .map(|(field, _)| module_string(field).cast::<c_void>())
                .collect();
            let values: Vec<_> = pairs
                .iter()
                .map(|(_, value)| {
                    value
                        .map(module_string)
                        .unwrap_or(HASH_DELETE as *mut ValkeyModuleString)
                })
                .collect();
            let result = hash_set_pairs(key, flags, fields.as_ptr(), values.as_ptr(), pairs.len());
            for field in fields {
                ValkeyModuleString::release(field.cast());
            }
            for value in values {
                if value as usize != HASH_DELETE {
                    ValkeyModuleString::release(value);
                }
            }
            let _ = Box::from_raw(key as *mut ModuleKeyHandle);
            result
        }
    }

    #[test]
    fn hash_stringref_queries_preserve_physical_encoding_and_field_expiry() {
        use crate::foundation::io::store::Robj;

        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        let mut db = Db::new();
        db.hash_set_with_limits(b"hash", b"field", b"value", 512, 64)
            .unwrap();
        assert!(!db.hash_has_string_ref(b"hash", b"missing"));
        assert!(matches!(
            db.get_no_expire(b"hash"),
            Some(Robj::HashListpack(_))
        ));
        assert!(!db.set_hash_string_ref(b"hash", b"missing", b"other"));
        assert!(matches!(
            db.get_no_expire(b"hash"),
            Some(Robj::HashListpack(_))
        ));

        db.set_hash_field_expire(b"hash", b"field", 200);
        assert!(db.set_hash_string_ref(b"hash", b"field", b"other"));
        assert!(db.hash_has_string_ref(b"hash", b"field"));
        assert!(!db.hash_has_string_ref(b"hash", b"missing"));
        assert!(matches!(
            db.get_no_expire(b"hash"),
            Some(Robj::HashHashtable(_))
        ));
        assert_eq!(db.get_hash_field_expire(b"hash", b"field"), Some(200));
        assert_eq!(
            db.get_hash_object(b"hash")
                .unwrap()
                .unwrap()
                .hash_get(b"field"),
            Some(b"other".to_vec())
        );

        db.set_hash_field_expire(b"hash", b"field", 99);
        assert!(!db.hash_has_string_ref(b"hash", b"field"));
        assert!(!db.set_hash_string_ref(b"hash", b"field", b"expired"));
        assert!(!db.hash_has_string_ref(b"missing", b"field"));
        db.set(b"string".to_vec(), Robj::string("value"));
        assert!(!db.hash_has_string_ref(b"string", b"field"));
    }

    #[test]
    fn hfe_module_hash_pairs_keep_listpack_and_hide_ghosts() {
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        let mut db = Db::new();
        db.hash_set_with_limits(b"hash", b"live", b"v", 128, 64)
            .unwrap();
        db.hash_set_with_limits(b"hash", b"ghost", b"v", 128, 64)
            .unwrap();
        db.set_hash_field_expire(b"hash", b"live", 200);
        db.set_hash_field_expire(b"hash", b"ghost", 99);
        // SAFETY: set_pairs owns its synchronous ABI allocations and db is live.
        unsafe {
            assert_eq!(set_pairs(&mut db, 0, &[(b"ghost", None)]), 0);
            assert_eq!(set_pairs(&mut db, HASH_XX, &[(b"ghost", Some(b"new"))]), 0);
            assert_eq!(set_pairs(&mut db, 0, &[(b"live", Some(b"new"))]), 1);
        }
        assert!(matches!(
            db.get_no_expire(b"hash"),
            Some(crate::foundation::io::store::Robj::HashListpack(_))
        ));
        assert_eq!(db.get_hash_field_expire(b"hash", b"live"), None);
        assert_eq!(db.get_hash_field_expire(b"hash", b"ghost"), Some(99));
        assert_eq!(db.get_hash_object(b"hash").unwrap().unwrap().hash_len(), 2);
    }

    #[test]
    fn hash_set_pairs_matches_count_and_condition_semantics() {
        let mut db = Db::new();
        // SAFETY: set_pairs constructs and reclaims valid module ABI pointers
        // while db and every borrowed byte slice remain live for each call.
        unsafe {
            assert_eq!(set_pairs(&mut db, 0, &[(b"field", Some(b"one"))]), 0);
            assert_eq!(
                set_pairs(&mut db, HASH_COUNT_ALL, &[(b"new", Some(b"two"))]),
                1
            );
            assert_eq!(set_pairs(&mut db, 0, &[(b"field", Some(b"updated"))]), 1);
            assert_eq!(set_pairs(&mut db, HASH_NX, &[(b"field", Some(b"no"))]), 0);
            assert_eq!(set_pairs(&mut db, HASH_XX, &[(b"missing", Some(b"no"))]), 0);
            assert_eq!(set_pairs(&mut db, 0, &[(b"field", None)]), 1);
        }
        let hash = db.get_hash(b"hash").unwrap().unwrap();
        assert_eq!(
            hash.get(b"new".as_slice()).map(Bytes::as_ref),
            Some(b"two".as_slice())
        );
        assert!(!hash.contains_key(b"field".as_slice()));
    }

    #[test]
    fn hash_set_pairs_rejects_wrong_type() {
        let mut db = Db::new();
        db.set(
            b"hash".to_vec(),
            crate::foundation::io::store::Robj::string("value"),
        );
        // SAFETY: set_pairs constructs and reclaims valid module ABI pointers
        // while db and the borrowed field/value slices remain live for the call.
        unsafe {
            assert_eq!(
                set_pairs(&mut db, 0, &[(b"field", Some(b"value"))]),
                -libc::ENOTSUP
            );
        }
    }
}
