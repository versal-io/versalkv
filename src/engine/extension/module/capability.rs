use crate::engine::command::context::CommandConn;
use crate::engine::command::CmdFlags;
use crate::engine::command::{CommandArgs, CommandDescriptor};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::Db;
use libc::c_void;
use std::cell::Cell;
use std::net::SocketAddr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleClientInfo {
    pub id: u64,
    pub peer_addr: SocketAddr,
    pub db: u32,
    pub is_pubsub: bool,
    pub is_blocked: bool,
    pub is_tracking: bool,
    pub is_multi: bool,
    pub is_readonly: bool,
    pub is_replica: bool,
    pub is_monitor: bool,
    pub is_authenticated: bool,
    pub was_authenticated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModuleClientTracking {
    pub id: usize,
    pub is_tracking: bool,
    pub is_broadcast: bool,
    pub is_optin: bool,
    pub is_optout: bool,
    pub is_caching: bool,
}

pub struct ModuleKeyScan {
    pub entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub is_incremental: bool,
}

pub trait ModuleRuntime {
    fn apply_module_call_effects(
        &mut self,
        _effects: super::api::call::pending::ModuleCallEffects,
    ) {
    }
    fn module_server_info(&mut self, _section: Option<&str>) -> String {
        String::new()
    }
    fn retain_pending_module_call(
        &mut self,
        _call: super::api::call::pending::PendingModuleCall,
    ) -> Option<usize> {
        None
    }
    fn abort_pending_module_call(&mut self, _handle: usize) -> bool {
        false
    }
    fn num_dbs(&self) -> usize;
    fn db_mut(&mut self, db_idx: u32) -> Option<&mut Db>;
    fn flush_all_dbs(&mut self);
    fn uses_lfu(&self) -> bool;
    fn is_oom(&self) -> bool {
        false
    }
    fn has_paused_writes(&self) -> bool {
        false
    }
    fn command_would_block(
        &self,
        _command: &CommandDescriptor,
        _args: &CommandArgs,
        _db_idx: u32,
    ) -> bool {
        false
    }
    fn effective_command_flags(
        &self,
        command: &CommandDescriptor,
        _args: &CommandArgs,
    ) -> CmdFlags {
        command.flags
    }
    fn has_enough_replicas_to_write(&self) -> bool {
        true
    }
    fn is_readonly_replica(&self) -> bool {
        false
    }
    fn is_stale_replica(&self) -> bool {
        false
    }
    fn has_persistence_write_error(&self) -> bool {
        false
    }
    fn context_flags(&self) -> libc::c_int {
        1 << 2
    }
    fn module_acl_error(&self, _command: CommandDescriptor, _args: &CommandArgs) -> Option<String> {
        None
    }
    fn module_client_id(&self) -> u64;
    fn module_client_info(&self, client_id: u64) -> Option<ModuleClientInfo>;
    fn module_client_name(&self, client_id: u64) -> Option<bytes::Bytes>;
    fn set_module_client_name(&mut self, client_id: u64, name: bytes::Bytes) -> bool;
    fn module_client_tracking(&self) -> Option<ModuleClientTracking> {
        None
    }
    fn remember_module_tracked_key(&mut self, _client_id: usize, _key: &[u8]) {}
    fn reply_module_raw(&mut self, _reply: Vec<u8>) -> bool {
        false
    }
    fn list_max_listpack_size(&self) -> i64;
    fn lookup_command(&self, name: &[u8]) -> Option<CommandDescriptor>;
    fn command_acl_categories(
        &self,
        command: CommandDescriptor,
        _args: &CommandArgs,
    ) -> &'static [&'static str] {
        command.acl_categories
    }
    fn execute_command(
        &mut self,
        name: &[u8],
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
    ) -> Value;
    fn register_module_command(
        &mut self,
        name: &str,
        arity: i32,
        flags: CmdFlags,
        first_key: i32,
        last_key: i32,
        key_step: i32,
    ) -> bool;
    fn unregister_module_command(&mut self, _name: &str) -> bool {
        false
    }
    fn update_module_command(
        &mut self,
        name: &str,
        arity: Option<i32>,
        first_key: i32,
        last_key: i32,
        key_step: i32,
    ) -> bool;
    fn publish_module_message(
        &mut self,
        channel: bytes::Bytes,
        message: bytes::Bytes,
        is_shard: bool,
    ) -> Option<usize>;
    fn propagate_module_command(&mut self, frame: Vec<u8>) -> bool;
    fn authenticate_client(
        &mut self,
        _username: &str,
        _callback: *mut c_void,
        _privdata: *mut c_void,
    ) -> Option<u64> {
        None
    }
    fn deauthenticate_client(&mut self, _username: &str) {}
    fn redact_client_command_argument(&mut self, _pos: usize) -> bool {
        false
    }
    fn set_module_command_info(
        &mut self,
        _name: &str,
        _info: crate::engine::extension::module::registry::ModuleCommandInfo,
    ) -> bool {
        false
    }
    fn module_scan_keys(&mut self, _db_idx: u32) -> Vec<Vec<u8>> {
        Vec::new()
    }
    fn module_scan_key(&mut self, _db_idx: u32, _key: &[u8]) -> Result<ModuleKeyScan, ()> {
        Ok(ModuleKeyScan {
            entries: Vec::new(),
            is_incremental: false,
        })
    }
    fn record_latency_sample(&mut self, _event: &str, _latency_ms: i64) {}
    fn record_internal_monitor(
        &mut self,
        _command: CommandDescriptor,
        _db: u32,
        _source: &str,
        _argv: Vec<bytes::Bytes>,
    ) {
    }
}

pub struct ModuleRuntimeHandle {
    runtime: *mut dyn ModuleRuntime,
}

impl ModuleRuntimeHandle {
    pub fn new(runtime: &mut dyn ModuleRuntime) -> Self {
        Self {
            runtime: erase_runtime_lifetime(runtime),
        }
    }

    /// # Safety
    /// The handle must not outlive the runtime passed to `new`, and callers must
    /// preserve exclusive access while the returned reference is in use.
    // SAFETY: callers uphold the documented lifetime and exclusivity contract.
    pub unsafe fn runtime_mut(&mut self) -> &mut dyn ModuleRuntime {
        unsafe { &mut *self.runtime }
    }
}

fn erase_runtime_lifetime(runtime: &mut dyn ModuleRuntime) -> *mut dyn ModuleRuntime {
    // SAFETY: ModuleRuntimeHandle is only installed in synchronous module
    // contexts and is destroyed before the borrowed runtime can be released.
    unsafe { std::mem::transmute::<&mut dyn ModuleRuntime, *mut dyn ModuleRuntime>(runtime) }
}

/// # Safety
/// `ptr` must be null or point to a live, exclusively accessible
/// `ModuleRuntimeHandle`.
// SAFETY: callers pass an opaque pointer produced from a live ModuleRuntimeHandle.
pub unsafe fn runtime_from_opaque(ptr: *mut c_void) -> Option<&'static mut dyn ModuleRuntime> {
    unsafe {
        if ptr.is_null() {
            return None;
        }
        let handle = &mut *(ptr as *mut ModuleRuntimeHandle);
        Some(handle.runtime_mut())
    }
}

thread_local! {
    static ACTIVE_MODULE_RUNTIME: Cell<*mut ModuleRuntimeHandle> =
        const { Cell::new(std::ptr::null_mut()) };
    static ACTIVE_MODULE_HOST: Cell<Option<*mut crate::engine::host::HostDispatcher<'static>>> =
        const { Cell::new(None) };
    static MODULE_YIELD_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static MODULE_HOST_STARTED: Cell<Option<std::time::Instant>> = const { Cell::new(None) };
}

pub struct ActiveModuleHostScope {
    previous: Option<*mut crate::engine::host::HostDispatcher<'static>>,
    previous_started: Option<std::time::Instant>,
}

impl ActiveModuleHostScope {
    pub fn enter(dispatcher: Option<&mut crate::engine::host::HostDispatcher<'_>>) -> Self {
        let dispatcher = dispatcher.map(|dispatcher| {
            // SAFETY: the synchronous run scope restores the slot before the
            // borrowed dispatcher is released. Yield never retains a request.
            unsafe {
                std::mem::transmute::<
                    &mut crate::engine::host::HostDispatcher<'_>,
                    *mut crate::engine::host::HostDispatcher<'static>,
                >(dispatcher)
            }
        });
        Self {
            previous: ACTIVE_MODULE_HOST.with(|slot| slot.replace(dispatcher)),
            previous_started: MODULE_HOST_STARTED
                .with(|slot| slot.replace(Some(std::time::Instant::now()))),
        }
    }
}

impl Drop for ActiveModuleHostScope {
    fn drop(&mut self) {
        ACTIVE_MODULE_HOST.with(|slot| slot.set(self.previous));
        MODULE_HOST_STARTED.with(|slot| slot.set(self.previous_started));
    }
}

pub(crate) fn yield_module_to_host(flags: i32, busy_reply: Option<&str>) {
    let Some(host) = ACTIVE_MODULE_HOST.with(Cell::get) else {
        return;
    };
    if MODULE_YIELD_ACTIVE.with(|slot| slot.replace(true)) {
        return;
    }
    struct YieldGuard;
    impl Drop for YieldGuard {
        fn drop(&mut self) {
            MODULE_YIELD_ACTIVE.with(|slot| slot.set(false));
        }
    }
    let _guard = YieldGuard;
    let elapsed_ms = MODULE_HOST_STARTED
        .with(|slot| slot.get())
        .map_or(0, |started| {
            started.elapsed().as_millis().min(u64::MAX as u128) as u64
        });
    // SAFETY: the host scope keeps this dispatcher live and YieldGuard prevents
    // nested access. No ModuleRuntime reference spans this host invocation.
    let result = unsafe {
        (&mut *host)(crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::Module {
                flags,
                busy_reply,
                elapsed_ms,
            },
        ))
    };
    if let Err(error) = result {
        log::warn!("Module yield failed: {error}");
    }
}

pub struct ActiveModuleRuntimeScope {
    previous: *mut ModuleRuntimeHandle,
}

impl ActiveModuleRuntimeScope {
    pub fn enter(handle: *mut ModuleRuntimeHandle) -> Option<Self> {
        if handle.is_null() {
            return None;
        }
        let previous = ACTIVE_MODULE_RUNTIME.with(|active| {
            let previous = active.get();
            active.set(handle);
            previous
        });
        Some(Self { previous })
    }
}

impl Drop for ActiveModuleRuntimeScope {
    fn drop(&mut self) {
        ACTIVE_MODULE_RUNTIME.with(|active| active.set(self.previous));
    }
}

pub(crate) fn with_active_runtime<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut dyn ModuleRuntime) -> R,
{
    let handle = ACTIVE_MODULE_RUNTIME.with(Cell::get);
    if handle.is_null() {
        return None;
    }
    // SAFETY: ActiveModuleRuntimeScope installs a live, exclusively borrowed
    // handle for one synchronous module callback and restores it on drop.
    Some(unsafe { f((&mut *handle).runtime_mut()) })
}

pub fn record_module_latency(event: &str, latency_ms: i64) -> bool {
    with_active_runtime(|runtime| runtime.record_latency_sample(event, latency_ms)).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeRuntime {
        dbs: crate::foundation::io::store::kv_engine::DbSet,
        num_dbs: usize,
        command_called: bool,
        client_name: Option<bytes::Bytes>,
        latency_samples: Vec<(String, i64)>,
    }

    impl ModuleRuntime for FakeRuntime {
        fn num_dbs(&self) -> usize {
            self.num_dbs
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
            7
        }

        fn module_client_info(&self, client_id: u64) -> Option<ModuleClientInfo> {
            (client_id == 7).then_some(ModuleClientInfo {
                id: 7,
                peer_addr: ([127, 0, 0, 1], 6380).into(),
                db: 9,
                is_pubsub: false,
                is_blocked: false,
                is_tracking: true,
                is_multi: false,
                is_readonly: false,
                is_replica: false,
                is_monitor: false,
                is_authenticated: true,
                was_authenticated: true,
            })
        }

        fn module_client_name(&self, client_id: u64) -> Option<bytes::Bytes> {
            (client_id == 7).then(|| self.client_name.clone()).flatten()
        }

        fn set_module_client_name(&mut self, client_id: u64, name: bytes::Bytes) -> bool {
            if client_id != 7 {
                return false;
            }
            self.client_name = Some(name);
            true
        }

        fn list_max_listpack_size(&self) -> i64 {
            -2
        }

        fn lookup_command(&self, _name: &[u8]) -> Option<CommandDescriptor> {
            Some(CommandDescriptor {
                name: crate::engine::command::CommandName::Static("ping"),
                arity: 1,
                flags: 0,
                first_key: 0,
                last_key: 0,
                key_step: 0,
                key_flags: &[],
                acl_categories: &[],
                get_keys: None,
            })
        }

        fn execute_command(
            &mut self,
            name: &[u8],
            _conn: &mut dyn CommandConn,
            _args: &CommandArgs,
        ) -> Value {
            self.command_called = name.eq_ignore_ascii_case(b"ping");
            Value::ok()
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
            true
        }

        fn update_module_command(
            &mut self,
            _name: &str,
            _arity: Option<i32>,
            _first_key: i32,
            _last_key: i32,
            _key_step: i32,
        ) -> bool {
            true
        }

        fn publish_module_message(
            &mut self,
            _channel: bytes::Bytes,
            _message: bytes::Bytes,
            _is_shard: bool,
        ) -> Option<usize> {
            Some(0)
        }

        fn propagate_module_command(&mut self, _frame: Vec<u8>) -> bool {
            true
        }

        fn record_latency_sample(&mut self, event: &str, latency_ms: i64) {
            self.latency_samples.push((event.to_string(), latency_ms));
        }
    }

    #[test]
    fn handle_delegates_without_exposing_concrete_runtime() {
        let mut runtime = FakeRuntime {
            dbs: crate::foundation::io::store::kv_engine::DbSet::new(2),
            num_dbs: 2,
            command_called: false,
            client_name: None,
            latency_samples: Vec::new(),
        };
        {
            let mut handle = ModuleRuntimeHandle::new(&mut runtime);
            // SAFETY: runtime outlives handle and this test holds exclusive access.
            let capability = unsafe { handle.runtime_mut() };
            assert_eq!(capability.num_dbs(), 2);
            assert_eq!(capability.module_client_id(), 7);
            assert_eq!(capability.module_client_info(7).unwrap().db, 9);
            assert!(capability.set_module_client_name(7, bytes::Bytes::from_static(b"named")));
            assert_eq!(
                capability.module_client_name(7),
                Some(bytes::Bytes::from_static(b"named"))
            );
            capability.db_mut(1).unwrap().set(
                b"key".to_vec(),
                crate::foundation::io::store::Robj::string("value"),
            );
            let descriptor = capability.lookup_command(b"PING").unwrap();
            assert_eq!(descriptor.name.as_str(), "ping");
            let mut conn =
                crate::engine::extension::module::api::call::call_conn::ModuleCallConn::new(1);
            let args = CommandArgs::from_parts(vec![bytes::Bytes::from_static(b"PING")]);
            assert_eq!(
                capability.execute_command(b"PING", &mut conn, &args),
                Value::ok()
            );
        }

        assert!(runtime.dbs.get_mut(1).unwrap().exists_no_expire(b"key"));
        assert!(runtime.command_called);
    }

    #[test]
    fn active_runtime_scope_restores_the_previous_runtime() {
        let mut outer_runtime = FakeRuntime {
            dbs: crate::foundation::io::store::kv_engine::DbSet::new(2),
            num_dbs: 2,
            command_called: false,
            client_name: None,
            latency_samples: Vec::new(),
        };
        let mut inner_runtime = FakeRuntime {
            dbs: crate::foundation::io::store::kv_engine::DbSet::new(3),
            num_dbs: 3,
            command_called: false,
            client_name: None,
            latency_samples: Vec::new(),
        };
        outer_runtime.dbs.get_mut(0).unwrap().set(
            b"key".to_vec(),
            crate::foundation::io::store::Robj::string("value"),
        );
        let mut outer_handle = ModuleRuntimeHandle::new(&mut outer_runtime);
        let mut inner_handle = ModuleRuntimeHandle::new(&mut inner_runtime);

        let outer_scope = ActiveModuleRuntimeScope::enter(&mut outer_handle).unwrap();
        assert_eq!(with_active_runtime(|runtime| runtime.num_dbs()), Some(2));
        with_active_runtime(|runtime| runtime.flush_all_dbs()).unwrap();
        {
            let inner_scope = ActiveModuleRuntimeScope::enter(&mut inner_handle).unwrap();
            assert_eq!(with_active_runtime(|runtime| runtime.num_dbs()), Some(3));
            drop(inner_scope);
        }
        assert_eq!(with_active_runtime(|runtime| runtime.num_dbs()), Some(2));
        drop(outer_scope);
        assert_eq!(with_active_runtime(|runtime| runtime.num_dbs()), None);
        assert_eq!(outer_runtime.dbs.db_size(0), 0);
    }

    #[test]
    fn module_latency_uses_only_the_active_runtime_scope() {
        let mut runtime = FakeRuntime {
            dbs: crate::foundation::io::store::kv_engine::DbSet::new(1),
            num_dbs: 1,
            command_called: false,
            client_name: None,
            latency_samples: Vec::new(),
        };

        assert!(!record_module_latency("module-event", 7));
        {
            let mut handle = ModuleRuntimeHandle::new(&mut runtime);
            let _scope = ActiveModuleRuntimeScope::enter(&mut handle).unwrap();
            assert!(record_module_latency("module-event", 7));
        }
        assert_eq!(
            runtime.latency_samples,
            vec![("module-event".to_string(), 7)]
        );
    }
}
