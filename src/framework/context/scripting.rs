use super::{MonitorEvent, ScriptExecutionState, ServerContext};
use crate::engine::command::{CmdFlags, CommandDescriptor, CMD_ADMIN, CMD_SKIP_MONITOR};
use crate::engine::extension::module::ModuleRegistry;
use crate::engine::extension::script::ScriptControlHandle;
use crate::framework::CommandRegistry;
use bytes::Bytes;

impl ServerContext {
    // ── Scripting and modules ──
    pub fn script(&self) -> &ScriptExecutionState {
        &self.script
    }
    pub(crate) fn script_mut(&mut self) -> &mut ScriptExecutionState {
        &mut self.script
    }
    pub fn module_registry(&self) -> &ModuleRegistry {
        &self.module_registry
    }
    pub(crate) fn module_registry_mut(&mut self) -> &mut ModuleRegistry {
        &mut self.module_registry
    }

    pub fn run_expired_module_timers(
        &mut self,
        now: crate::foundation::io::sys::time::MonotonicInstant,
    ) {
        let expired = self.module_registry.expired_timer_ids(now);
        for id in expired {
            let Some(timer) = self.module_registry.timer_for_callback(id) else {
                continue;
            };
            let module_name = timer.module_name.clone();
            crate::framework::capabilities::module::invoke_module_timer(timer, self);
            self.module_registry.stop_timer(&module_name, id);
        }
    }
    /// Whether a script is currently in BUSY state (timeout but still running).
    pub fn is_script_busy(&self) -> bool {
        self.script.is_busy()
    }
    /// Whether the busy script is a FUNCTION/FCALL (not EVAL).
    pub fn is_script_busy_function(&self) -> bool {
        self.script.is_busy_function()
    }
    /// Whether the busy script has performed writes.
    pub fn is_script_busy_write_dirty(&self) -> bool {
        self.script.is_busy_write_dirty()
    }
    /// Enter script BUSY state (called when a script times out).
    pub fn set_script_busy(&mut self, is_function: bool, write_dirty: bool) {
        self.script.set_busy(is_function, write_dirty);
    }
    /// Clear script BUSY state (script finished or was killed).
    pub fn clear_script_busy(&mut self) {
        self.script.clear_busy();
    }
    /// Whether a script kill is pending (waiting for busy script to acknowledge).
    pub fn pending_script_kill(&self) -> bool {
        self.script.pending_script_kill
    }
    /// Set the pending script kill flag.
    pub fn set_pending_script_kill(&mut self, val: bool) {
        self.script.pending_script_kill = val;
    }
    pub fn script_disable_deny_scripts(&self) -> bool {
        self.script.disable_deny_scripts()
    }
    pub fn set_script_disable_deny_scripts(&mut self, disabled: bool) {
        self.script.set_disable_deny_scripts(disabled);
    }
    pub fn interrupt_running_script_for_shutdown(&mut self) {
        if !self.is_script_busy() {
            return;
        }
        let Some(control) = self.script.running_script.as_ref() else {
            return;
        };
        if control.is_timed_out() {
            control.request_kill(crate::engine::extension::script::KillSource::Shutdown);
        }
    }
    /// Whether a script is currently executing.
    pub fn has_running_script(&self) -> bool {
        self.script.running_script.is_some()
    }
    pub(crate) fn schedule_script_eviction(&mut self) {
        self.script.scripts_eviction_pending = true;
    }

    pub(crate) fn drive_script_eviction(&mut self) {
        if self.has_running_script() {
            return;
        }
        let config = &self.config().server;
        let limit = crate::engine::extension::config::server::scripts_memory::effective_limit(
            config.maxmemory_scripts,
            config.maxmemory,
        );
        // Read the current limit even without a pending flag: startup registry
        // replay and non-command configuration updates use the same driver.
        self.script.scripts_eviction_pending = limit.is_some_and(|limit| {
            self.script
                .script_cache
                .evict_memory_slice(limit, crate::engine::extension::lua::release_eval_script)
        });
    }

    pub(crate) fn register_eval_script(&mut self, digest: String, body: String) {
        if !self.script.script_cache.exists(&digest) {
            self.drive_script_eviction();
        }
        for retired in self.script.script_cache.store(digest, body) {
            crate::engine::extension::lua::release_eval_script(&retired);
        }
        self.schedule_script_eviction();
    }
    pub(crate) fn command_registry(&self) -> &CommandRegistry {
        &self.script.command_registry
    }
    pub(crate) fn set_command_registry_snapshot(&mut self, registry: &CommandRegistry) {
        self.script.command_registry = registry.clone();
    }
    pub(crate) fn set_module_command_registry(&mut self, registry: *mut CommandRegistry) {
        self.module_command_registry = registry;
    }
    pub(crate) fn module_command_registry_ptr(&self) -> *mut CommandRegistry {
        self.module_command_registry
    }
    pub fn has_monitor_clients(&self) -> bool {
        !self.script.monitor_client_ids.is_empty()
    }
    pub fn register_monitor_client(&mut self, client_id: u64) {
        self.script.monitor_client_ids.insert(client_id);
    }
    pub fn unregister_monitor_client(&mut self, client_id: u64) {
        self.script.monitor_client_ids.remove(&client_id);
    }
    pub(crate) fn monitor_event(
        &self,
        db: u32,
        source: impl Into<String>,
        argv: Vec<Bytes>,
        command_flags: CmdFlags,
        explicit_script_outer: bool,
    ) -> Option<MonitorEvent> {
        if !self.has_monitor_clients() || self.is_loading() {
            return None;
        }
        if !explicit_script_outer && command_flags & (CMD_ADMIN | CMD_SKIP_MONITOR) != 0 {
            return None;
        }
        Some(MonitorEvent::capture(
            db,
            source,
            argv,
            self.script.monitor_client_ids.iter().copied().collect(),
        ))
    }
    pub(crate) fn record_internal_monitor(
        &mut self,
        command: CommandDescriptor,
        db: u32,
        source: impl Into<String>,
        argv: Vec<Bytes>,
    ) {
        let flags = argv
            .get(1)
            .and_then(|subcommand| {
                std::str::from_utf8(subcommand).ok().and_then(|subcommand| {
                    crate::framework::generated::commands::subcommand_meta(
                        command.name.as_str(),
                        subcommand,
                    )
                })
            })
            .map_or(command.flags, |meta| command.flags | meta.flags);
        if let Some(event) = self.monitor_event(db, source, argv, flags, false) {
            self.script.pending_internal_monitor.push(event);
        }
    }
    pub(crate) fn enter_script(&mut self, control: ScriptControlHandle) {
        self.script.running_script = Some(control);
    }
    pub(crate) fn leave_script(&mut self) {
        self.script.running_script = None;
        self.schedule_script_eviction();
    }
}

#[cfg(test)]
mod script_memory_tests {
    use super::*;
    use crate::engine::extension::lua::LuaEngine;
    use crate::engine::extension::script::ScriptRunCtx;
    use crate::framework::commands::scripting::script_cache::ScriptCache;

    #[test]
    fn script_eviction_waits_for_running_vm_and_resumes_after_exit() {
        crate::engine::extension::lua::reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.maxmemory_scripts = 1;
        let body = format!("--{}\nreturn redis.call('PING')", "x".repeat(200_000));
        let digest = ScriptCache::compute_sha1(&body);
        LuaEngine
            .prepare_cached_eval(&digest, &body, false)
            .unwrap();
        ctx.register_eval_script(digest.clone(), body.clone());
        assert_eq!(ctx.script().script_cache.len(), 1);

        let mut run = ScriptRunCtx::new_eval_detached(false, true, true, false);
        run.func_name = digest;
        ctx.enter_script(run.control.clone());
        let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
            crate::engine::host::HostRequest::ExecuteCommand { .. } => {
                ctx.drive_script_eviction();
                assert_eq!(ctx.script().script_cache.len(), 1);
                assert!(ctx.script().scripts_eviction_pending);
                Ok(crate::engine::host::HostReply::Command(
                    crate::foundation::io::protocol::Value::ok(),
                ))
            }
            _ => panic!("unexpected host request"),
        };
        assert_eq!(
            LuaEngine
                .eval_with_host(&body, &mut run, &mut host)
                .unwrap(),
            crate::foundation::io::protocol::Value::ok()
        );
        ctx.leave_script();
        ctx.drive_script_eviction();
        assert_eq!(ctx.script().script_cache.len(), 0);
        assert_eq!(ctx.script().script_cache.evicted_scripts(), 1);
        assert!(!ctx.script().scripts_eviction_pending);
        crate::engine::extension::lua::reset_lua_runtime();
    }
}
