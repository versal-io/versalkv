use libc::{c_int, c_void};
use std::collections::HashMap;
use std::time::Duration;

use crate::foundation::io::sys::time::MonotonicInstant;

use super::{
    ModuleInternalCtx, ModuleUnloadFunc, ValkeyModuleCmdFunc, ValkeyModuleEvent,
    ValkeyModuleEventCallback, ValkeyModuleTimerProc, ValkeyModuleType,
    VALKEYMODULE_EVENT_EVENTLOOP,
};

// ============================================================================
// ============================================================================

#[derive(Clone)]
pub struct ModuleCmdEntry {
    pub func: ValkeyModuleCmdFunc,
    pub module_name: String,
    pub info: Option<ModuleCommandInfo>,
}

pub struct ModuleCommandHandle {
    pub module_name: String,
    pub command_name: String,
    pub runtime_handle: *mut c_void,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleCommandInfo {
    pub key_specs: Vec<ModuleKeySpec>,
    pub acl_categories: Vec<String>,
    pub legacy_keys: Option<(i32, i32, i32)>,
    pub getkeys_api: bool,
    pub arity: Option<i32>,
}

#[derive(Clone)]
pub struct ModuleTimer {
    pub module_name: String,
    pub db_idx: u32,
    deadline: MonotonicInstant,
    pub callback: ValkeyModuleTimerProc,
    pub data: *mut c_void,
}

impl ModuleTimer {
    #[cfg(test)]
    pub(crate) fn for_test(
        module_name: &str,
        db_idx: u32,
        callback: ValkeyModuleTimerProc,
        data: *mut c_void,
    ) -> Self {
        Self {
            module_name: module_name.to_string(),
            db_idx,
            deadline: MonotonicInstant::now(),
            callback,
            data,
        }
    }
}

pub struct ModuleNotification {
    pub types: c_int,
    pub callback: super::ValkeyModuleNotificationFunc,
}

#[derive(Clone)]
pub struct ModuleServerEventListener {
    pub module_name: String,
    pub event: ValkeyModuleEvent,
    pub callback: ValkeyModuleEventCallback,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleKeySpec {
    pub flags: Vec<&'static str>,
    pub begin_search: ModuleKeyBeginSearch,
    pub find_keys: ModuleKeyFindKeys,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModuleKeyBeginSearch {
    Unknown,
    Index { pos: i32 },
    Keyword { keyword: String, start_from: i32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModuleKeyFindKeys {
    Unknown,
    Range {
        last_key: i32,
        key_step: i32,
        limit: i32,
    },
    KeyNum {
        key_num_idx: i32,
        first_key: i32,
        key_step: i32,
    },
}

// ============================================================================
// ============================================================================

#[derive(Clone, Debug, Default)]
pub struct ModuleOptions {
    value: std::sync::Arc<std::sync::atomic::AtomicI32>,
}

impl ModuleOptions {
    // Tests read the owning module's state; production currently only stores flags.
    #[cfg(test)]
    pub fn get(&self) -> c_int {
        self.value.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set(&self, options: c_int) {
        self.value
            .store(options, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct ModuleRef {
    pub handle: *mut c_void,
    pub name: String,
    pub ver: i32,
    pub is_static: bool,
    pub options: ModuleOptions,
    pub commands: Vec<String>,
    pub types: Vec<&'static ValkeyModuleType>,
    pub onunload: Option<ModuleUnloadFunc>,
    pub load_path: Option<(String, Vec<String>)>,
    pub notifications: Vec<ModuleNotification>,
    pub acl_categories: Vec<String>,
}

impl ModuleRef {
    pub fn new(name: &str, handle: *mut c_void, ver: i32, is_static: bool) -> Self {
        Self {
            handle,
            name: name.to_string(),
            ver,
            is_static,
            options: ModuleOptions::default(),
            commands: Vec::new(),
            types: Vec::new(),
            onunload: None,
            load_path: None,
            notifications: Vec::new(),
            acl_categories: Vec::new(),
        }
    }
}

// ============================================================================
// ============================================================================

#[derive(Debug)]
pub enum ModuleError {
    NotFound(String),
    NameBusy(String),
    LoadFailed(String),
    InitFailed(String),
    InvalidPath(String),
}

impl std::fmt::Display for ModuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModuleError::NotFound(name) => write!(f, "ERR Module '{}' not found", name),
            ModuleError::NameBusy(name) => write!(f, "ERR Module name '{}' is busy", name),
            ModuleError::LoadFailed(reason) => write!(f, "ERR Error loading module: {}", reason),
            ModuleError::InitFailed(reason) => {
                write!(f, "ERR Module initialization failed: {}", reason)
            }
            ModuleError::InvalidPath(path) => write!(f, "ERR Invalid module path: {}", path),
        }
    }
}

impl std::error::Error for ModuleError {}

// ============================================================================
// ============================================================================

struct InternalCtxPtr(*mut ModuleInternalCtx);
// SAFETY: InternalCtxPtr is only used within ModuleRegistry's internal_ctxs
// Vec, which is always accessed from a single-threaded context. The raw pointer
// is never dereferenced concurrently; it is only stored and later reclaimed
// during ModuleRegistry::drop (which is single-threaded). Therefore, sending
// InternalCtxPtr across threads is safe.
unsafe impl Send for InternalCtxPtr {}

// ============================================================================
// ============================================================================

pub struct ModuleRegistry {
    modules: HashMap<String, Box<ModuleRef>>,
    load_order: Vec<String>,
    types: HashMap<u64, &'static ValkeyModuleType>,
    internal_ctxs: Vec<InternalCtxPtr>,
    module_cmds: HashMap<String, ModuleCmdEntry>,
    /// Opaque command handles returned to C modules.
    command_handles: Vec<*mut ModuleCommandHandle>,
    timers: HashMap<u64, ModuleTimer>,
    server_event_listeners: Vec<ModuleServerEventListener>,
    next_timer_id: u64,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
            load_order: Vec::new(),
            types: HashMap::new(),
            internal_ctxs: Vec::new(),
            module_cmds: HashMap::new(),
            command_handles: Vec::new(),
            timers: HashMap::new(),
            server_event_listeners: Vec::new(),
            next_timer_id: 1,
        }
    }

    pub fn register(&mut self, module_ref: ModuleRef) -> Result<(), ModuleError> {
        let name = module_ref.name.clone();
        if self.modules.contains_key(&name) {
            return Err(ModuleError::NameBusy(name));
        }
        self.load_order.push(name.clone());
        self.modules.insert(name, Box::new(module_ref));
        Ok(())
    }

    /// Copy the callback without carrying a registry borrow across module code.
    pub fn prepare_unload(&self, name: &str) -> Result<Option<ModuleUnloadFunc>, ModuleError> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| ModuleError::NotFound(name.to_string()))?;
        if !module.types.is_empty() {
            return Err(ModuleError::InitFailed(
                "the module exports one or more module-side data types, can't unload".to_string(),
            ));
        }
        if self.timers.values().any(|timer| timer.module_name == name) {
            return Err(ModuleError::InitFailed(
                "the module holds timer that is not fired".to_string(),
            ));
        }
        Ok(module.onunload)
    }

    /// Remove registrations after OnUnload succeeds; the caller owns dlclose.
    pub fn finish_unload(&mut self, name: &str) -> Result<Box<ModuleRef>, ModuleError> {
        let module = self
            .modules
            .remove(name)
            .ok_or_else(|| ModuleError::NotFound(name.to_string()))?;
        self.load_order.retain(|entry| entry != name);
        for cmd_name in &module.commands {
            self.module_cmds.remove(&normalize_command_name(cmd_name));
            log::debug!("Module '{}' unregistered command: {}", name, cmd_name);
        }
        for category in &module.acl_categories {
            crate::engine::extension::module::api::acl::remove_module_acl_category_all(category);
        }
        self.server_event_listeners
            .retain(|listener| listener.module_name != name);
        self.timers.retain(|_, timer| timer.module_name != name);
        for mt in &module.types {
            self.types.retain(|_, v| !std::ptr::eq(*v, *mt));
        }
        let module_ptr = (&*module as *const ModuleRef as *mut ModuleRef).cast::<c_void>();
        self.internal_ctxs.retain(|ctx| {
            // SAFETY: internal_ctxs owns each context. The module box is still
            // live, and matching contexts are freed once before its library closes.
            unsafe {
                if (*ctx.0).module_ref == module_ptr {
                    super::free_module_ctx(Box::into_raw(super::create_module_ctx(ctx.0)));
                    false
                } else {
                    true
                }
            }
        });
        self.command_handles.retain(|handle| {
            // SAFETY: command_handles owns these Box allocations. No callback
            // is active; handles belonging to the removed module cannot escape.
            unsafe {
                if (**handle).module_name == name {
                    drop(Box::from_raw(*handle));
                    false
                } else {
                    true
                }
            }
        });
        Ok(module)
    }

    /// Roll back a module whose OnLoad returned an error. The loader owns the
    /// dlopen handle and closes it after this registry-only cleanup.
    pub fn abort_load(&mut self, name: &str) -> Result<(), ModuleError> {
        self.finish_unload(name)?;
        Ok(())
    }

    pub fn lookup(&self, name: &str) -> Option<&ModuleRef> {
        self.modules.get(name).map(Box::as_ref)
    }

    pub fn lookup_mut(&mut self, name: &str) -> Option<&mut ModuleRef> {
        self.modules.get_mut(name).map(Box::as_mut)
    }

    pub fn list(&self) -> impl Iterator<Item = &ModuleRef> {
        self.load_order
            .iter()
            .filter_map(|name| self.modules.get(name).map(Box::as_ref))
    }

    pub fn set_load_path(&mut self, module_name: &str, path: String, args: Vec<String>) -> bool {
        let Some(module) = self.modules.get_mut(module_name) else {
            return false;
        };
        module.load_path = Some((path, args));
        true
    }

    pub fn register_command(
        &mut self,
        module_name: &str,
        cmd_name: &str,
        func: ValkeyModuleCmdFunc,
    ) -> Result<(), ModuleError> {
        let module = self
            .modules
            .get_mut(module_name)
            .ok_or_else(|| ModuleError::NotFound(module_name.to_string()))?;
        module.commands.push(cmd_name.to_string());
        let lookup_name = normalize_command_name(cmd_name);
        if self.module_cmds.contains_key(&lookup_name) {
            return Err(ModuleError::NameBusy(cmd_name.to_string()));
        }

        self.module_cmds.insert(
            lookup_name,
            ModuleCmdEntry {
                func,
                module_name: module_name.to_string(),
                info: None,
            },
        );

        Ok(())
    }

    pub fn lookup_cmd_func(&self, name: &str) -> Option<&ModuleCmdEntry> {
        self.module_cmds.get(&normalize_command_name(name))
    }

    pub fn command_exists(&self, name: &str) -> bool {
        self.module_cmds.contains_key(&normalize_command_name(name))
    }

    pub fn set_command_info(&mut self, name: &str, info: ModuleCommandInfo) -> bool {
        let Some(entry) = self.module_cmds.get_mut(&normalize_command_name(name)) else {
            return false;
        };
        if let Some(existing) = &mut entry.info {
            if !info.key_specs.is_empty() && !existing.key_specs.is_empty() {
                return false;
            }
            if !info.key_specs.is_empty() {
                existing.key_specs = info.key_specs;
            }
            if !info.acl_categories.is_empty() {
                existing.acl_categories = info.acl_categories;
            }
            if info.legacy_keys.is_some() {
                existing.legacy_keys = info.legacy_keys;
            }
            existing.getkeys_api |= info.getkeys_api;
            if info.arity.is_some() {
                existing.arity = info.arity;
            }
            return true;
        }
        if info.key_specs.is_empty() && info.legacy_keys.is_none() && info.acl_categories.is_empty()
        {
            return false;
        }
        entry.info = Some(info);
        true
    }

    pub fn set_legacy_command_info(
        &mut self,
        name: &str,
        legacy_keys: (i32, i32, i32),
        getkeys_api: bool,
        arity: i32,
    ) -> bool {
        self.set_command_info(
            name,
            ModuleCommandInfo {
                key_specs: Vec::new(),
                acl_categories: Vec::new(),
                legacy_keys: Some(legacy_keys),
                getkeys_api,
                arity: Some(arity),
            },
        )
    }

    pub fn add_acl_category(&mut self, module_name: &str, category: &str) -> bool {
        let Some(module) = self.modules.get_mut(module_name) else {
            return false;
        };
        if !module.acl_categories.iter().any(|item| item == category) {
            module.acl_categories.push(category.to_string());
        }
        true
    }

    pub fn create_timer(
        &mut self,
        module_name: &str,
        db_idx: u32,
        period_ms: i64,
        callback: ValkeyModuleTimerProc,
        data: *mut c_void,
    ) -> Option<u64> {
        self.create_timer_at(
            module_name,
            db_idx,
            period_ms,
            callback,
            data,
            MonotonicInstant::now(),
        )
    }

    fn create_timer_at(
        &mut self,
        module_name: &str,
        db_idx: u32,
        period_ms: i64,
        callback: ValkeyModuleTimerProc,
        data: *mut c_void,
        now: MonotonicInstant,
    ) -> Option<u64> {
        if !self.modules.contains_key(module_name) {
            return None;
        }
        let id = self.next_timer_id;
        self.next_timer_id = self.next_timer_id.saturating_add(1).max(1);
        let period_ms = period_ms.max(0);
        self.timers.insert(
            id,
            ModuleTimer {
                module_name: module_name.to_string(),
                db_idx,
                deadline: now.saturating_add(Duration::from_millis(period_ms as u64)),
                callback,
                data,
            },
        );
        Some(id)
    }

    pub fn next_timer_timeout(&self, now: MonotonicInstant) -> Option<Duration> {
        self.timers
            .values()
            .map(|timer| timer.deadline.duration_until(now))
            .min()
    }

    pub fn subscribe_keyspace_events(
        &mut self,
        module_name: &str,
        types: c_int,
        callback: super::ValkeyModuleNotificationFunc,
    ) -> bool {
        let Some(module) = self.modules.get_mut(module_name) else {
            return false;
        };
        module
            .notifications
            .push(ModuleNotification { types, callback });
        true
    }

    pub fn subscribe_server_event(
        &mut self,
        module_name: &str,
        event: ValkeyModuleEvent,
        callback: Option<ValkeyModuleEventCallback>,
    ) -> bool {
        if event.id != VALKEYMODULE_EVENT_EVENTLOOP || !self.modules.contains_key(module_name) {
            return false;
        }
        let existing = self.server_event_listeners.iter().position(|listener| {
            listener.module_name == module_name && listener.event.id == event.id
        });
        match (existing, callback) {
            (Some(index), Some(callback)) => {
                self.server_event_listeners[index] = ModuleServerEventListener {
                    module_name: module_name.to_string(),
                    event,
                    callback,
                };
            }
            (Some(index), None) => {
                self.server_event_listeners.remove(index);
            }
            (None, Some(callback)) => {
                self.server_event_listeners.push(ModuleServerEventListener {
                    module_name: module_name.to_string(),
                    event,
                    callback,
                });
            }
            (None, None) => {}
        }
        true
    }

    pub fn server_event_listeners(&self, event_id: u64) -> Vec<ModuleServerEventListener> {
        self.server_event_listeners
            .iter()
            .filter(|listener| listener.event.id == event_id)
            .cloned()
            .collect()
    }

    pub(crate) fn keyspace_callbacks(
        &self,
        module_name: &str,
        event_type: c_int,
    ) -> Vec<(String, *mut c_void, super::ValkeyModuleNotificationFunc)> {
        self.modules
            .get(module_name)
            .into_iter()
            .flat_map(|module| {
                module
                    .notifications
                    .iter()
                    .filter(|notification| notification.types & event_type != 0)
                    .map(|notification| {
                        (
                            module.name.clone(),
                            module.as_ref() as *const ModuleRef as *mut c_void,
                            notification.callback,
                        )
                    })
            })
            .collect()
    }

    pub fn stop_timer(&mut self, module_name: &str, id: u64) -> Option<*mut c_void> {
        self.timers
            .get(&id)
            .filter(|timer| timer.module_name == module_name)?;
        self.timers.remove(&id).map(|timer| timer.data)
    }

    pub fn timer_info(&self, module_name: &str, id: u64) -> Option<(u64, *mut c_void)> {
        self.timer_info_at(module_name, id, MonotonicInstant::now())
    }

    fn timer_info_at(
        &self,
        module_name: &str,
        id: u64,
        now: MonotonicInstant,
    ) -> Option<(u64, *mut c_void)> {
        let timer = self.timers.get(&id)?;
        if timer.module_name != module_name {
            return None;
        }
        Some((
            timer.deadline.duration_until(now).as_millis() as u64,
            timer.data,
        ))
    }

    pub fn expired_timer_ids(&self, now: MonotonicInstant) -> Vec<u64> {
        let mut ids: Vec<_> = self
            .timers
            .iter()
            .filter_map(|(&id, timer)| (timer.deadline <= now).then_some((timer.deadline, id)))
            .collect();
        ids.sort_unstable();
        ids.into_iter().map(|(_, id)| id).collect()
    }

    pub fn timer_for_callback(&self, id: u64) -> Option<ModuleTimer> {
        self.timers.get(&id).cloned()
    }

    pub fn command_module_name(&self, name: &str) -> Option<&str> {
        self.lookup_cmd_func(name)
            .map(|entry| entry.module_name.as_str())
    }

    pub fn subcommand_names(&self, parent_name: &str) -> Vec<&str> {
        let prefix = format!("{}|", normalize_command_name(parent_name));
        let mut names: Vec<_> = self
            .module_cmds
            .keys()
            .filter(|name| name.starts_with(&prefix))
            .map(String::as_str)
            .collect();
        names.sort_unstable();
        names
    }

    pub fn new_command_handle(
        &mut self,
        module_name: String,
        command_name: String,
        runtime_handle: *mut c_void,
    ) -> *mut ModuleCommandHandle {
        let handle = Box::into_raw(Box::new(ModuleCommandHandle {
            module_name,
            command_name,
            runtime_handle,
        }));
        self.command_handles.push(handle);
        handle
    }

    pub fn register_type(
        &mut self,
        module_name: &str,
        type_: &'static ValkeyModuleType,
    ) -> Result<(), ModuleError> {
        let module = self
            .modules
            .get_mut(module_name)
            .ok_or_else(|| ModuleError::NotFound(module_name.to_string()))?;
        self.types.insert(type_.id, type_);
        module.types.push(type_);
        Ok(())
    }

    pub fn lookup_type_by_id(&self, id: u64) -> Option<&&ValkeyModuleType> {
        self.types.get(&id)
    }

    pub fn register_internal_ctx(&mut self, ctx: *mut ModuleInternalCtx) {
        self.internal_ctxs.push(InternalCtxPtr(ctx));
    }
}

fn normalize_command_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

impl Drop for ModuleRegistry {
    fn drop(&mut self) {
        for ctx_ptr in &self.internal_ctxs {
            let ptr = ctx_ptr.0;
            if !ptr.is_null() {
                // SAFETY: Each pointer in internal_ctxs was obtained from
                // Box::into_raw by register_internal_ctx. The module_name field
                // was allocated by CString::into_raw in create_module_internal_ctx
                // or create_module_cmd_ctx. Both are uniquely owned and have not
                // been freed before this point. Reconstructing the Box and CString
                // via from_raw is safe because we are the sole owner taking back
                // ownership during drop.
                unsafe {
                    let ctx = Box::into_raw(super::create_module_ctx(ptr));
                    super::free_module_ctx(ctx);
                }
            }
        }
        self.internal_ctxs.clear();
        for handle in self.command_handles.drain(..) {
            if !handle.is_null() {
                // SAFETY: command_handles only stores pointers produced by
                // Box::into_raw in new_command_handle and each is reclaimed once here.
                unsafe {
                    let _ = Box::from_raw(handle);
                }
            }
        }
    }
}

// SAFETY: ModuleRegistry is used exclusively within the single-threaded
// event loop (ServerWorkerState). The raw pointers it holds
// (internal_ctxs, cmd_registry, module handles) are only accessed from
// the single event-loop thread, never concurrently. The type is only sent
// between threads during construction before the event loop starts.
unsafe impl Send for ModuleRegistry {}

#[cfg(test)]
mod tests {
    use super::{ModuleRef, ModuleRegistry};
    use crate::engine::extension::module::{
        ValkeyModuleCtx, ValkeyModuleEvent, VALKEYMODULE_EVENT_EVENTLOOP,
    };
    use libc::c_void;
    use std::time::Duration;

    extern "C" fn timer_callback(
        _ctx: *mut super::super::ValkeyModuleCtx,
        _data: *mut libc::c_void,
    ) {
    }

    extern "C" fn first_event_callback(
        _ctx: *mut ValkeyModuleCtx,
        _event: ValkeyModuleEvent,
        _subevent: u64,
        _data: *mut c_void,
    ) {
    }

    extern "C" fn replacement_event_callback(
        _ctx: *mut ValkeyModuleCtx,
        _event: ValkeyModuleEvent,
        _subevent: u64,
        _data: *mut c_void,
    ) {
    }

    #[test]
    fn module_ref_address_survives_registry_growth() {
        let mut registry = ModuleRegistry::new();
        registry
            .register(ModuleRef::new("first", std::ptr::null_mut(), 1, true))
            .unwrap();
        let before = registry.lookup("first").unwrap() as *const ModuleRef;

        for index in 0..1024 {
            let name = format!("module-{index}");
            registry
                .register(ModuleRef::new(&name, std::ptr::null_mut(), 1, true))
                .unwrap();
        }

        let after = registry.lookup("first").unwrap() as *const ModuleRef;
        assert_eq!(before, after);
    }

    #[test]
    fn module_timer_deadline_uses_injected_monotonic_time() {
        let mut registry = ModuleRegistry::new();
        registry
            .register(ModuleRef::new("timer", std::ptr::null_mut(), 1, true))
            .unwrap();
        let origin = crate::foundation::io::sys::time::MonotonicInstant::from_millis(100);
        let id = registry
            .create_timer_at("timer", 0, 25, timer_callback, std::ptr::null_mut(), origin)
            .expect("create module timer");

        assert_eq!(
            registry.next_timer_timeout(origin),
            Some(Duration::from_millis(25))
        );
        assert_eq!(registry.timer_info_at("timer", id, origin).unwrap().0, 25);
        assert_eq!(
            registry
                .timer_info_at("timer", id, origin.saturating_add(Duration::from_micros(1)),)
                .unwrap()
                .0,
            24
        );
        assert!(registry
            .expired_timer_ids(origin.saturating_add(Duration::from_millis(24)))
            .is_empty());
        assert_eq!(
            registry.expired_timer_ids(origin.saturating_add(Duration::from_millis(25))),
            vec![id]
        );
    }

    #[test]
    fn event_loop_subscription_replaces_and_removes_one_module_callback() {
        let mut registry = ModuleRegistry::new();
        registry
            .register(ModuleRef::new("events", std::ptr::null_mut(), 1, true))
            .unwrap();
        let event = ValkeyModuleEvent {
            id: VALKEYMODULE_EVENT_EVENTLOOP,
            dataver: 1,
        };

        assert!(registry.subscribe_server_event("events", event, Some(first_event_callback)));
        assert!(registry.subscribe_server_event("events", event, Some(replacement_event_callback)));
        let listeners = registry.server_event_listeners(VALKEYMODULE_EVENT_EVENTLOOP);
        assert_eq!(listeners.len(), 1);
        assert!(std::ptr::fn_addr_eq(
            listeners[0].callback,
            replacement_event_callback
                as extern "C" fn(*mut ValkeyModuleCtx, ValkeyModuleEvent, u64, *mut c_void)
        ));

        assert!(registry.subscribe_server_event("events", event, None));
        assert!(registry
            .server_event_listeners(VALKEYMODULE_EVENT_EVENTLOOP)
            .is_empty());

        assert!(registry.subscribe_server_event("events", event, Some(first_event_callback)));
        assert!(registry.prepare_unload("events").unwrap().is_none());
        registry
            .finish_unload("events")
            .expect("unload event module");
        assert!(registry
            .server_event_listeners(VALKEYMODULE_EVENT_EVENTLOOP)
            .is_empty());
    }
}
