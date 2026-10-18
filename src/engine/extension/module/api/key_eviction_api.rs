//! Key eviction metadata APIs.

#![allow(non_snake_case)]

use libc::{c_int, c_longlong};

use super::super::{ModuleKeyHandle, ValkeyModuleKey, VALKEYMODULE_ERR, VALKEYMODULE_OK};

// SAFETY: key must be a live module key handle returned by ValkeyModule_OpenKey.
pub unsafe extern "C" fn ValkeyModule_SetLRU(
    key: *mut ValkeyModuleKey,
    lru_idle_ms: c_longlong,
) -> c_int {
    unsafe {
        let Some(handle) = valid_key_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        if handle.uses_lfu || lru_idle_ms < 0 {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        db.set_lru_idle(&handle.key, lru_idle_ms / 1000);
        VALKEYMODULE_OK
    }
}

// SAFETY: key and lru_idle_ms must point to live, writable values.
pub unsafe extern "C" fn ValkeyModule_GetLRU(
    key: *mut ValkeyModuleKey,
    lru_idle_ms: *mut c_longlong,
) -> c_int {
    unsafe {
        if lru_idle_ms.is_null() {
            return VALKEYMODULE_ERR;
        }
        *lru_idle_ms = -1;
        let Some(handle) = valid_key_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        if !handle.uses_lfu {
            let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
            *lru_idle_ms = db.get_lru_idle(&handle.key).saturating_mul(1000);
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: key must be a live module key handle returned by ValkeyModule_OpenKey.
pub unsafe extern "C" fn ValkeyModule_SetLFU(
    key: *mut ValkeyModuleKey,
    lfu_freq: c_longlong,
) -> c_int {
    unsafe {
        let Some(handle) = valid_key_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        let Ok(lfu_freq) = u8::try_from(lfu_freq) else {
            return VALKEYMODULE_ERR;
        };
        if !handle.uses_lfu {
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        db.set_lfu_freq(&handle.key, lfu_freq);
        VALKEYMODULE_OK
    }
}

// SAFETY: key and lfu_freq must point to live, writable values.
pub unsafe extern "C" fn ValkeyModule_GetLFU(
    key: *mut ValkeyModuleKey,
    lfu_freq: *mut c_longlong,
) -> c_int {
    unsafe {
        if lfu_freq.is_null() {
            return VALKEYMODULE_ERR;
        }
        *lfu_freq = -1;
        let Some(handle) = valid_key_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        if handle.uses_lfu {
            let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
            *lfu_freq = db.get_lfu_freq(&handle.key) as c_longlong;
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: key must be null or point to a live ModuleKeyHandle.
unsafe fn valid_key_handle(key: *mut ValkeyModuleKey) -> Option<&'static mut ModuleKeyHandle> {
    unsafe {
        let handle = ValkeyModuleKey::as_handle(key)?;
        if handle.db.is_null() {
            return None;
        }
        let db = &*(handle.db as *const crate::foundation::io::store::kv_engine::Db);
        (db.get_no_expire(&handle.key).is_some() || db.is_module_key(&handle.key)).then_some(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::{ModuleKeyHandle, ValkeyModuleKey};
    use crate::foundation::io::store::kv_engine::Db;
    use crate::foundation::io::store::Robj;

    fn open_key(db: &mut Db, uses_lfu: bool) -> *mut ValkeyModuleKey {
        ValkeyModuleKey::from_handle(Box::into_raw(ModuleKeyHandle::new(
            (db as *mut Db).cast(),
            b"key".to_vec(),
            1,
            -2,
            uses_lfu,
        )))
    }

    #[test]
    fn lru_api_uses_milliseconds_and_is_disabled_under_lfu_policy() {
        let mut db = Db::new();
        db.set(b"key".to_vec(), Robj::string("value"));
        let key = open_key(&mut db, false);
        let mut idle_ms = -1;

        // SAFETY: key and idle_ms are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_SetLRU(key, 100_000) },
            VALKEYMODULE_OK
        );
        // SAFETY: key and idle_ms are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLRU(key, &mut idle_ms) },
            VALKEYMODULE_OK
        );
        assert!(idle_ms >= 100_000);
        // SAFETY: key was allocated by open_key and is consumed once.
        unsafe { super::super::key_api::ValkeyModule_CloseKey(key) };

        let key = open_key(&mut db, true);
        // SAFETY: key and idle_ms are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLRU(key, &mut idle_ms) },
            VALKEYMODULE_OK
        );
        assert_eq!(idle_ms, -1);
        // SAFETY: key was allocated by open_key and is consumed once.
        unsafe { super::super::key_api::ValkeyModule_CloseKey(key) };
    }

    #[test]
    fn lfu_api_validates_policy_and_frequency_range() {
        let mut db = Db::new();
        db.set(b"key".to_vec(), Robj::string("value"));
        let key = open_key(&mut db, true);
        let mut frequency = -1;

        // SAFETY: key is live and uniquely owned by this test.
        assert_eq!(unsafe { ValkeyModule_SetLFU(key, 100) }, VALKEYMODULE_OK);
        // SAFETY: key and frequency are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLFU(key, &mut frequency) },
            VALKEYMODULE_OK
        );
        assert_eq!(frequency, 100);
        // SAFETY: key is live and uniquely owned by this test.
        assert_eq!(unsafe { ValkeyModule_SetLFU(key, 256) }, VALKEYMODULE_ERR);
        // SAFETY: key was allocated by open_key and is consumed once.
        unsafe { super::super::key_api::ValkeyModule_CloseKey(key) };

        let key = open_key(&mut db, false);
        // SAFETY: key and frequency are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLFU(key, &mut frequency) },
            VALKEYMODULE_OK
        );
        assert_eq!(frequency, -1);
        // SAFETY: key was allocated by open_key and is consumed once.
        unsafe { super::super::key_api::ValkeyModule_CloseKey(key) };
    }

    #[test]
    fn eviction_api_rejects_missing_keys() {
        let mut db = Db::new();
        let key = open_key(&mut db, false);
        let mut value = 0;

        // SAFETY: key and value are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLRU(key, &mut value) },
            VALKEYMODULE_ERR
        );
        // SAFETY: key and value are live and uniquely owned by this test.
        assert_eq!(
            unsafe { ValkeyModule_GetLFU(key, &mut value) },
            VALKEYMODULE_ERR
        );
        // SAFETY: key was allocated by open_key and is consumed once.
        unsafe { super::super::key_api::ValkeyModule_CloseKey(key) };
    }
}
