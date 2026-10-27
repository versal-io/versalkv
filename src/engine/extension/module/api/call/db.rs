//! Database selection helpers for module contexts.

#![allow(non_snake_case)]

use libc::{c_int, c_ulonglong};

use super::super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use crate::engine::extension::module::capability::{runtime_from_opaque, with_active_runtime};

// SAFETY: Called from C shim or module code; caller must pass a valid ValkeyModuleCtx pointer or NULL.
pub unsafe extern "C" fn ValkeyModule_GetSelectedDb(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        if ctx.is_null() {
            return -1;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return -1;
        }
        (*internal).db_idx
    }
}

// SAFETY: Called from C shim or module code; caller must pass a valid ValkeyModuleCtx pointer or NULL.
pub unsafe extern "C" fn ValkeyModule_SelectDb(ctx: *mut ValkeyModuleCtx, dbid: c_int) -> c_int {
    unsafe {
        if ctx.is_null() {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return VALKEYMODULE_ERR;
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return VALKEYMODULE_ERR;
        };
        if dbid < 0 || dbid as usize >= runtime.num_dbs() {
            return VALKEYMODULE_ERR;
        }
        (*internal).db_idx = dbid;
        VALKEYMODULE_OK
    }
}

// SAFETY: This no-context API may only be called during a synchronous module
// callback guarded by ActiveModuleRuntimeScope.
pub unsafe extern "C" fn ValkeyModule_ResetDataset(_restart_aof: c_int, _async: c_int) {
    let _ = with_active_runtime(|runtime| runtime.flush_all_dbs());
}

// SAFETY: Called by C module code; ctx must point to a live module context.
pub unsafe extern "C" fn ValkeyModule_DbSize(ctx: *mut ValkeyModuleCtx) -> c_ulonglong {
    unsafe {
        let Some(db) = selected_db(ctx) else {
            return 0;
        };
        db.dbsize() as c_ulonglong
    }
}

// SAFETY: Called by C module code; ctx must point to a live module context.
pub unsafe extern "C" fn ValkeyModule_RandomKey(
    ctx: *mut ValkeyModuleCtx,
) -> *mut ValkeyModuleString {
    unsafe {
        let Some(db) = selected_db(ctx) else {
            return std::ptr::null_mut();
        };
        let Some(key) = db.random_key().map(<[u8]>::to_vec) else {
            return std::ptr::null_mut();
        };
        super::super::super::auto_memory::create_auto_string(ctx, &key)
    }
}

// SAFETY: ctx must point to a live module context whose runtime handle remains
// exclusively accessible for the duration of the returned database borrow.
unsafe fn selected_db(
    ctx: *mut ValkeyModuleCtx,
) -> Option<&'static mut crate::foundation::io::store::kv_engine::Db> {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return None;
        }
        let internal = (*ctx).internal;
        let db_idx = u32::try_from((*internal).db_idx).ok()?;
        runtime_from_opaque((*internal).runtime_handle)?.db_mut(db_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::CommandConn;
    use crate::engine::command::{CmdFlags, CommandArgs, CommandDescriptor};
    use crate::engine::extension::module::capability::{
        ActiveModuleRuntimeScope, ModuleRuntime, ModuleRuntimeHandle,
    };
    use crate::engine::extension::module::{
        create_module_cmd_ctx, create_module_ctx, free_module_ctx,
    };
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::{Db, DbSet};
    use crate::foundation::io::store::Robj;

    struct FakeRuntime {
        dbs: DbSet,
    }

    impl ModuleRuntime for FakeRuntime {
        fn num_dbs(&self) -> usize {
            self.dbs.num_dbs()
        }

        fn db_mut(&mut self, db_idx: u32) -> Option<&mut Db> {
            self.dbs.get_mut(db_idx)
        }

        fn flush_all_dbs(&mut self) {
            self.dbs.flush_all();
        }

        fn uses_lfu(&self) -> bool {
            false
        }

        fn module_client_id(&self) -> u64 {
            0
        }

        fn module_client_info(
            &self,
            _client_id: u64,
        ) -> Option<crate::engine::extension::module::capability::ModuleClientInfo> {
            None
        }

        fn module_client_name(&self, _client_id: u64) -> Option<bytes::Bytes> {
            None
        }

        fn set_module_client_name(&mut self, _client_id: u64, _name: bytes::Bytes) -> bool {
            false
        }

        fn list_max_listpack_size(&self) -> i64 {
            -2
        }

        fn lookup_command(&self, _name: &[u8]) -> Option<CommandDescriptor> {
            None
        }

        fn execute_command(
            &mut self,
            _name: &[u8],
            _conn: &mut dyn CommandConn,
            _args: &CommandArgs,
        ) -> Value {
            Value::Null
        }

        fn register_module_command(
            &mut self,
            _name: &str,
            _arity: i32,
            _flags: CmdFlags,
            _first_key: i32,
            _last_key: i32,
            _key_step: i32,
        ) -> bool {
            false
        }

        fn update_module_command(
            &mut self,
            _name: &str,
            _arity: Option<i32>,
            _first_key: i32,
            _last_key: i32,
            _key_step: i32,
        ) -> bool {
            false
        }

        fn publish_module_message(
            &mut self,
            _channel: bytes::Bytes,
            _message: bytes::Bytes,
            _is_shard: bool,
        ) -> Option<usize> {
            None
        }

        fn propagate_module_command(&mut self, _frame: Vec<u8>) -> bool {
            false
        }
    }

    #[test]
    fn db_apis_use_the_context_selected_database() {
        let mut runtime = FakeRuntime { dbs: DbSet::new(2) };
        runtime
            .dbs
            .get_mut(1)
            .unwrap()
            .set(b"selected".to_vec(), Robj::string("value"));
        let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
        let mut internal = create_module_cmd_ctx(
            "test",
            "test.db",
            std::ptr::null_mut(),
            (&mut *handle as *mut ModuleRuntimeHandle).cast(),
        );
        internal.db_idx = 1;
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));

        // SAFETY: ctx and its runtime handle remain live for these synchronous calls.
        assert_eq!(unsafe { ValkeyModule_DbSize(ctx) }, 1);
        // SAFETY: ctx and its selected database remain live for this call.
        let key = unsafe { ValkeyModule_RandomKey(ctx) };
        assert!(!key.is_null());
        // SAFETY: RandomKey returned a live module string owned by this test.
        assert_eq!(unsafe { (*key).as_bytes() }, b"selected");
        // SAFETY: key is a live module string returned above and is consumed once.
        unsafe { super::super::super::string_obj_api::ValkeyModule_FreeString(ctx, key) };
        // SAFETY: ctx was allocated above and is no longer used after this call.
        unsafe { free_module_ctx(ctx) };
        drop(handle);
    }

    #[test]
    fn reset_dataset_requires_an_active_runtime_and_flushes_all_databases() {
        let mut runtime = FakeRuntime { dbs: DbSet::new(2) };
        for db_idx in 0..2 {
            runtime
                .dbs
                .get_mut(db_idx)
                .unwrap()
                .set(vec![db_idx as u8], Robj::string("value"));
        }
        let mut handle = ModuleRuntimeHandle::new(&mut runtime);

        // SAFETY: no runtime scope is active, so this must be a no-op.
        unsafe { ValkeyModule_ResetDataset(1, 0) };
        {
            let _scope = ActiveModuleRuntimeScope::enter(&mut handle).unwrap();
            // SAFETY: the active scope supplies a live runtime for this synchronous call.
            unsafe { ValkeyModule_ResetDataset(1, 0) };
        }
        assert_eq!(runtime.dbs.db_size(0), 0);
        assert_eq!(runtime.dbs.db_size(1), 0);
    }

    #[test]
    fn random_key_returns_null_for_an_empty_database() {
        let mut runtime = FakeRuntime { dbs: DbSet::new(1) };
        let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
        let internal = create_module_cmd_ctx(
            "test",
            "test.randomkey",
            std::ptr::null_mut(),
            (&mut *handle as *mut ModuleRuntimeHandle).cast(),
        );
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));

        // SAFETY: ctx and its runtime handle remain live for this synchronous call.
        assert!(unsafe { ValkeyModule_RandomKey(ctx) }.is_null());

        // SAFETY: ctx was allocated above and is no longer used after this call.
        unsafe { free_module_ctx(ctx) };
        drop(handle);
    }
}
