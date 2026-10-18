use crate::framework::spec::CommandSpec;
use std::collections::HashMap;

#[derive(Clone)]
pub struct CommandRegistry {
    /// Active command table — affected by rename-command.
    /// Key is String because renamed commands may have dynamic names.
    commands: HashMap<String, CommandSpec>,
    /// Original command table — written once at registration, never modified.
    /// Used for ACL permission checks (always based on original name).
    orig_commands: HashMap<&'static str, CommandSpec>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
            orig_commands: HashMap::new(),
        }
    }

    pub fn register(&mut self, spec: CommandSpec) {
        // Automatically register in both tables: active table uses String key,
        // original table uses &'static str key.
        let name = spec.name.to_string();
        let orig_name = spec.name;
        // Clone is cheap: CommandSpec only contains Copy fields + fn pointer
        self.orig_commands.insert(orig_name, spec.clone());
        self.commands.insert(name, spec);
    }

    pub fn unregister(&mut self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        let removed_original = self.orig_commands.remove(lower.as_str()).is_some();
        let previous_len = self.commands.len();
        self.commands
            .retain(|_, spec| !spec.name.eq_ignore_ascii_case(&lower));
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
        if name.bytes().all(|byte| !byte.is_ascii_uppercase()) {
            return self.commands.get(name);
        }
        self.commands.get(&name.to_ascii_lowercase())
    }

    /// Look up by &str (for rename-command processing).
    #[allow(dead_code)]
    // TODO(dead_code): reserved for rename-command runtime processing
    pub fn lookup_str(&self, name: &str) -> Option<&CommandSpec> {
        let lower = name.to_ascii_lowercase();
        self.commands.get(&lower)
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
        if let Some(spec) = self.commands.remove(&old_lower) {
            if !new_name.is_empty() {
                let new_lower = new_name.to_ascii_lowercase();
                self.commands.insert(new_lower, spec);
            }
        }
    }

    #[allow(dead_code)]
    // TODO(dead_code): reserved for future use (subcommand container check)
    pub fn has_subcommands(&self, name: &str) -> bool {
        self.commands.values().any(|c| c.container == Some(name))
    }

    /// Iterate over all registered command specs.
    /// Used via `#[command]` macro expansion in COMMAND handler.
    pub fn iter(&self) -> impl Iterator<Item = &CommandSpec> {
        self.commands.values()
    }

    /// Get the number of registered commands.
    /// Used via `#[command]` macro expansion in COMMAND handler.
    pub fn len(&self) -> usize {
        self.commands.len()
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
}
