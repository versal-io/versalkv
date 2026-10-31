use crate::engine::extension::module::api::{
    ValkeyModuleScanCursor, ValkeyModule_CloseKey, ValkeyModule_OpenKey, ValkeyModule_ScanKey,
    ValkeyModule_ScanKeyRawBorrowed,
};
use crate::engine::extension::module::capability::ModuleRuntimeHandle;
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleKey, ValkeyModuleString,
};
use crate::foundation::io::store::structure::{
    hash::HashLp,
    set::{IntSet, SetLp},
    zset::ZSetLp,
};
use crate::foundation::io::store::Robj;
use crate::framework::context::ServerContext;

#[test]
fn lifecycle_compact_scan_completes_both_public_apis_in_one_call() {
    extern "C" fn count_field(
        _key: *mut ValkeyModuleKey,
        _field: *mut ValkeyModuleString,
        _value: *mut ValkeyModuleString,
        private: *mut libc::c_void,
    ) {
        // SAFETY: private points to a live callback counter for this synchronous scan.
        unsafe { *private.cast::<usize>() += 1 };
    }

    extern "C" fn count_raw_field(
        _key: *mut ValkeyModuleKey,
        _field: *const libc::c_char,
        _field_len: usize,
        _value: *const libc::c_char,
        _value_len: usize,
        private: *mut libc::c_void,
    ) {
        // SAFETY: private points to a live callback counter for this synchronous scan.
        unsafe { *private.cast::<usize>() += 1 };
    }

    let mut hash = Robj::HashListpack(HashLp::new());
    hash.hash_set(b"one", b"1");
    hash.hash_set(b"two", b"2");
    let mut intset = Robj::SetIntset(IntSet::new());
    intset.set_add(b"1", 512);
    intset.set_add(b"2", 512);
    let mut set = Robj::SetListpack(SetLp::new());
    set.set_add(b"one", 512);
    set.set_add(b"two", 512);
    let mut zset = Robj::ZSetListpack(ZSetLp::new());
    zset.zset_add(b"one", 1.0);
    zset.zset_add(b"two", 2.0);
    let mut failures = Vec::new();
    for object in [hash, intset, set, zset] {
        let encoding = object.encoding_name();
        let mut runtime = ServerContext::new(1);
        runtime.db_mut(0).unwrap().set(b"key".to_vec(), object);
        let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(create_module_cmd_ctx(
            "test",
            "test.lifecycle",
            std::ptr::null_mut(),
            (&mut *handle as *mut ModuleRuntimeHandle).cast(),
        ))));
        // SAFETY: context, name, opened key and callback counter live through both scans.
        unsafe {
            let name = Box::into_raw(ValkeyModuleString::from_bytes(b"key"));
            let key = ValkeyModule_OpenKey(ctx, name, 1);
            assert!(!key.is_null());
            for raw in [false, true] {
                let mut cursor = ValkeyModuleScanCursor {
                    index: 0,
                    done: false,
                };
                let mut callbacks = 0usize;
                let private = (&mut callbacks as *mut usize).cast();
                let result = if raw {
                    ValkeyModule_ScanKeyRawBorrowed(
                        key,
                        &mut cursor,
                        Some(count_raw_field),
                        private,
                    )
                } else {
                    ValkeyModule_ScanKey(key, &mut cursor, Some(count_field), private)
                };
                if (result, callbacks, cursor.done) != (0, 2, true) {
                    failures.push((encoding, raw, result, callbacks, cursor.done));
                }
            }
            ValkeyModule_CloseKey(key);
            ValkeyModuleString::release(name);
            free_module_ctx(ctx);
        }
    }
    assert!(failures.is_empty(), "compact scan results: {failures:?}");
}
