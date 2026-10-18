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
        let expired = self.module_registry.take_expired_timers(now);
        for timer in expired {
            crate::framework::capabilities::module::invoke_module_timer(timer, self);
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
                    crate::framework::generated::commands::subcommand_meta(command.name, subcommand)
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
    }
}
