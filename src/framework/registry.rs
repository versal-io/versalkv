use crate::engine::command::ResolvedCommand;
use crate::framework::spec::{CommandExecutor, CommandSpec};
use slab::Slab;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct RegisteredCommand {
    spec: CommandSpec,
    execution_id: usize,
    generation: u64,
}

#[derive(Clone)]
struct RegisteredExecutor {
    executor: CommandExecutor,
    generation: u64,
}

pub struct CommandRegistry {
    /// Active command table — affected by rename-command.
    /// Key is String because renamed commands may have dynamic names.
    commands: HashMap<String, RegisteredCommand>,
    /// Original command table — written once at registration, never modified.
    /// Used for ACL permission checks (always based on original name).
    orig_commands: HashMap<&'static str, CommandSpec>,
    executors: Slab<RegisteredExecutor>,
    registry_id: u64,
    next_generation: u64,
}

impl Clone for CommandRegistry {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            orig_commands: self.orig_commands.clone(),
            executors: self.executors.clone(),
            registry_id: NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed),
            next_generation: self.next_generation,
        }
    }
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
            orig_commands: HashMap::new(),
            executors: Slab::new(),
            registry_id: NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed),
            next_generation: 1,
        }
    }

    pub fn register(&mut self, spec: CommandSpec) {
        // Automatically register in both tables: active table uses String key,
        // original table uses &'static str key.
        let name = spec.name.to_string();
        let orig_name = spec.name;
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let execution_id = self.executors.insert(RegisteredExecutor {
            executor: spec.executor,
            generation,
        });
        // Clone is cheap: CommandSpec only contains Copy fields + fn pointer
        self.orig_commands.insert(orig_name, spec.clone());
        if let Some(previous) = self.commands.insert(
            name,
            RegisteredCommand {
                spec,
                execution_id,
                generation,
            },
        ) {
            self.executors.remove(previous.execution_id);
        }
    }

    pub fn unregister(&mut self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        let removed_original = self.orig_commands.remove(lower.as_str()).is_some();
        let previous_len = self.commands.len();
        let removed_ids = self
            .commands
            .values()
            .filter(|command| command.spec.name.eq_ignore_ascii_case(&lower))
            .map(|command| command.execution_id)
            .collect::<Vec<_>>();
        self.commands
            .retain(|_, command| !command.spec.name.eq_ignore_ascii_case(&lower));
        for execution_id in removed_ids {
            self.executors.remove(execution_id);
        }
        removed_original || self.commands.len() != previous_len
    }

    pub fn update_module_command(
        &mut self,
        name: &str,
        arity: Option<i32>,
        first_key: i32,
        last_key: i32,
        key_step: i32,
    ) -> bool {
        let lower = name.to_ascii_lowercase();
        let mut updated = false;
        for spec in self
            .commands
            .values_mut()
            .map(|command| &mut command.spec)
            .filter(|spec| spec.name.eq_ignore_ascii_case(&lower))
        {
            if let Some(arity) = arity {
                spec.arity = arity;
            }
            spec.first_key = first_key;
            spec.last_key = last_key;
            spec.key_step = key_step;
            updated = true;
        }
        if let Some(spec) = self.orig_commands.get_mut(lower.as_str()) {
            if let Some(arity) = arity {
                spec.arity = arity;
            }
            spec.first_key = first_key;
            spec.last_key = last_key;
            spec.key_step = key_step;
            updated = true;
        }
        updated
    }

    /// Look up a command by name in the active table only.
    pub fn lookup(&self, name: &[u8]) -> Option<&CommandSpec> {
        let name = std::str::from_utf8(name).ok()?;
        self.lookup_entry(name).map(|command| &command.spec)
    }

    pub(crate) fn resolve(&self, name: &[u8]) -> Option<ResolvedCommand> {
        let name = std::str::from_utf8(name).ok()?;
        let command = self.lookup_entry(name)?;
        Some(ResolvedCommand::new(
            command.spec.descriptor(),
            self.registry_id,
            command.execution_id,
            command.generation,
        ))
    }

    pub(crate) fn executor(&self, command: ResolvedCommand) -> Option<CommandExecutor> {
        let (registry_id, execution_id, generation) = command.execution_token();
        if registry_id != self.registry_id {
            return None;
        }
        self.executors
            .get(execution_id)
            .filter(|executor| executor.generation == generation)
            .map(|executor| executor.executor)
    }

    /// Look up by &str (for rename-command processing).
    #[allow(dead_code)]
    // TODO(dead_code): reserved for rename-command runtime processing
    pub fn lookup_str(&self, name: &str) -> Option<&CommandSpec> {
        let lower = name.to_ascii_lowercase();
        self.commands.get(&lower).map(|command| &command.spec)
    }

    /// Look up the original command spec by original name (for ACL checks).
    #[allow(dead_code)]
    // TODO(dead_code): reserved for ACL permission checks (always based on original name)
    pub fn lookup_original(&self, name: &str) -> Option<&CommandSpec> {
        self.orig_commands.get(name)
    }

    /// Rename a command: remove old name from active table, insert new name.
    /// If new_name is empty, the command is disabled (removed from active table).
    pub fn rename(&mut self, old_name: &str, new_name: &str) {
        let old_lower = old_name.to_ascii_lowercase();
        // Remove from active table by any existing name
        if let Some(command) = self.commands.remove(&old_lower) {
            if new_name.is_empty() {
                self.executors.remove(command.execution_id);
                return;
            }
            let new_lower = new_name.to_ascii_lowercase();
            if let Some(previous) = self.commands.insert(new_lower, command) {
                self.executors.remove(previous.execution_id);
            }
        }
    }

    #[allow(dead_code)]
    // TODO(dead_code): reserved for future use (subcommand container check)
    pub fn has_subcommands(&self, name: &str) -> bool {
        self.commands
            .values()
            .any(|command| command.spec.container == Some(name))
    }

    /// Iterate over all registered command specs.
    /// Used via `#[command]` macro expansion in COMMAND handler.
    pub fn iter(&self) -> impl Iterator<Item = &CommandSpec> {
        self.commands.values().map(|command| &command.spec)
    }

    /// Get the number of registered commands.
    /// Used via `#[command]` macro expansion in COMMAND handler.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    fn lookup_entry(&self, name: &str) -> Option<&RegisteredCommand> {
        if name.bytes().all(|byte| !byte.is_ascii_uppercase()) {
            return self.commands.get(name);
        }
        self.commands.get(&name.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::DispatchGroup;
    use crate::foundation::io::protocol::Value;
    use crate::framework::CommandExecutor;

    fn test_handler(
        _conn: &mut dyn crate::engine::command::context::CommandConn,
        _args: &crate::engine::command::CommandArgs,
        _ctx: &mut crate::framework::context::ServerContext,
    ) -> Value {
        Value::ok()
    }

    fn test_spec(name: &'static str) -> CommandSpec {
        CommandSpec {
            name,
            arity: 0,
            flags: 0,
            dispatch_group: DispatchGroup::Default,
            first_key: 0,
            last_key: 0,
            key_step: 0,
            container: None,
            key_flags: &[],
            acl_categories: &[],
            executor: CommandExecutor::Immediate(test_handler),
            get_keys: None,
        }
    }

    #[test]
    fn ordinary_specs_keep_the_existing_immediate_handler() {
        let spec = test_spec("get");

        assert!(matches!(spec.executor, CommandExecutor::Immediate(_)));
    }

    #[test]
    fn unregister_removes_original_and_renamed_command_entries() {
        let mut registry = CommandRegistry::new();
        registry.register(test_spec("module.command"));
        registry.rename("module.command", "module.alias");

        assert!(registry.unregister("module.command"));
        assert!(registry.lookup(b"module.command").is_none());
        assert!(registry.lookup(b"module.alias").is_none());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn resolved_command_keeps_its_executor_after_alias_lookup() {
        let mut registry = CommandRegistry::new();
        registry.register(test_spec("get"));
        registry.rename("get", "fetch");

        let resolved = registry.resolve(b"fetch").unwrap();

        assert_eq!(resolved.descriptor().name, "get");
        assert!(registry.lookup(b"get").is_none());
        assert!(matches!(
            registry.executor(resolved).unwrap(),
            CommandExecutor::Immediate(_)
        ));
    }

    #[test]
    fn resolved_command_rejects_reused_executor_slot() {
        let mut registry = CommandRegistry::new();
        registry.register(test_spec("get"));
        let resolved = registry.resolve(b"get").unwrap();

        assert!(registry.unregister("get"));
        registry.register(test_spec("set"));

        assert!(registry.executor(resolved).is_none());
    }

    #[test]
    fn resolved_command_is_bound_to_its_registry_instance() {
        let mut registry = CommandRegistry::new();
        registry.register(test_spec("get"));
        let resolved = registry.resolve(b"get").unwrap();
        let cloned = registry.clone();

        assert!(cloned.executor(resolved).is_none());
    }

    #[test]
    fn rename_reclaims_disabled_and_replaced_executors() {
        let mut registry = CommandRegistry::new();
        registry.register(test_spec("get"));
        registry.rename("get", "");
        assert!(registry.executors.is_empty());

        registry.register(test_spec("get"));
        registry.register(test_spec("set"));
        registry.rename("get", "set");
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.executors.len(), 1);
    }
}
