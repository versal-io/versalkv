use crate::engine::core::CommandOutcome;
use crate::framework::{CommandArgs, CommandError, CommandMeta, GetKeysFn};

use super::CommandRuntime;

pub type RuntimeCommandHandler = fn(&mut dyn CommandRuntime, &CommandArgs) -> CommandOutcome;

pub struct RuntimeCommandSpec {
    pub meta: &'static CommandMeta,
    pub handler: RuntimeCommandHandler,
    pub get_keys: Option<GetKeysFn>,
}

pub static RUNTIME_COMMANDS: &[RuntimeCommandSpec] =
    include!(concat!(env!("OUT_DIR"), "/runtime_command_catalog.rs"));

pub fn command_specs() -> &'static [RuntimeCommandSpec] {
    RUNTIME_COMMANDS
}

pub fn lookup_command(name: &[u8]) -> Option<&'static RuntimeCommandSpec> {
    command_specs()
        .iter()
        .find(|spec| name.eq_ignore_ascii_case(spec.meta.name.as_bytes()))
}

pub fn lookup_command_name(name: &str) -> Option<&'static RuntimeCommandSpec> {
    command_specs()
        .iter()
        .find(|spec| name.eq_ignore_ascii_case(spec.meta.name))
}

pub fn execute(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some(spec) = lookup_command(&args.name) else {
        if let Ok(name) = std::str::from_utf8(&args.name) {
            if let Some(meta) = crate::framework::generated::commands::acl_command_meta(name) {
                let message = if meta.flags & crate::engine::command::CMD_BLOCKING != 0 {
                    "ERR command requires a blocking capability"
                } else {
                    "ERR command requires a runtime capability unavailable in this host"
                };
                return CommandOutcome::reply(crate::engine::core::Value::error(message));
            }
        }
        return CommandOutcome::reply(CommandError::unknown(&args.name));
    };
    if spec.meta.dispatch_group != crate::engine::command::DispatchGroup::Default {
        return CommandOutcome::reply(crate::engine::core::Value::error(
            "ERR command requires a host dispatch capability",
        ));
    }
    (spec.handler)(runtime, args)
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "wasm"))]
    use super::command_specs;
    use super::execute;
    use crate::engine::command::CommandArgs;
    use crate::engine::core::Value;
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;

    #[cfg(not(feature = "wasm"))]
    #[test]
    fn runtime_catalog_matches_native_registry_definitions() {
        use crate::framework::{CommandExecutor, CommandRegistry};

        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);

        for runtime_spec in command_specs() {
            let native = registry
                .lookup(runtime_spec.meta.name.as_bytes())
                .unwrap_or_else(|| panic!("missing native command {}", runtime_spec.meta.name));
            assert_eq!(native.name, runtime_spec.meta.name);
            assert_eq!(native.arity, runtime_spec.meta.arity);
            assert_eq!(native.flags, runtime_spec.meta.flags);
            assert_eq!(native.dispatch_group, runtime_spec.meta.dispatch_group);
            assert_eq!(native.first_key, runtime_spec.meta.first_key);
            assert_eq!(native.last_key, runtime_spec.meta.last_key);
            assert_eq!(native.key_step, runtime_spec.meta.key_step);
            assert_eq!(native.container, runtime_spec.meta.container);
            assert_eq!(native.key_flags, runtime_spec.meta.key_flags);
            assert_eq!(native.acl_categories, runtime_spec.meta.acl_categories);
            assert_eq!(native.get_keys.is_some(), runtime_spec.get_keys.is_some());
            let CommandExecutor::Runtime(native_handler) = native.executor else {
                panic!("{} is not runtime-backed", native.name);
            };
            assert!(std::ptr::fn_addr_eq(native_handler, runtime_spec.handler));
        }
    }

    #[test]
    fn runtime_catalog_executes_the_same_shared_handler() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"missing"),
        ]);

        assert_eq!(execute(&mut runtime, &args).reply, Value::null());
    }
}
