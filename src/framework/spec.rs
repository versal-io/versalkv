use crate::engine::command::context::CommandConn;
use crate::engine::command::{CmdFlags, DispatchGroup};
use crate::engine::command::{CommandArgs, CommandName};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

use crate::framework::GetKeysFn;

pub type CommandHandler = fn(&mut dyn CommandConn, &CommandArgs, &mut ServerContext) -> Value;
pub type RuntimeCommandHandler = fn(
    &mut dyn crate::framework::commands::command_runtime::CommandRuntime,
    &CommandArgs,
) -> crate::engine::core::CommandOutcome;
pub type HostedCommandHandler = fn(
    &mut dyn CommandConn,
    &CommandArgs,
    &mut ServerContext,
)
    -> Result<crate::framework::host_tasks::PreparedHostTask, Value>;

#[derive(Clone, Copy)]
pub enum CommandExecutor {
    Immediate(CommandHandler),
    Runtime(RuntimeCommandHandler),
    Hosted {
        fallback: CommandHandler,
        prepare: HostedCommandHandler,
    },
}

impl CommandExecutor {
    pub fn runtime_handler(self) -> Option<RuntimeCommandHandler> {
        match self {
            Self::Runtime(handler) => Some(handler),
            Self::Immediate(_) | Self::Hosted { .. } => None,
        }
    }

    pub fn execute(
        self,
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
        ctx: &mut ServerContext,
    ) -> Value {
        match self {
            Self::Immediate(handler) => handler(conn, args, ctx),
            Self::Runtime(handler) => {
                crate::framework::commands::command_runtime::execute_server_command(
                    conn, args, ctx, handler,
                )
            }
            Self::Hosted { fallback, .. } => fallback(conn, args, ctx),
        }
    }

    pub fn hosted(self) -> Option<HostedCommandHandler> {
        match self {
            Self::Immediate(_) => None,
            Self::Runtime(_) => None,
            Self::Hosted { prepare, .. } => Some(prepare),
        }
    }
}

#[derive(Clone)]
pub struct CommandSpec {
    pub name: CommandName,
    pub arity: i32,
    pub flags: CmdFlags,
    pub dispatch_group: DispatchGroup,
    pub first_key: i32,
    pub last_key: i32,
    pub key_step: i32,
    #[allow(dead_code)]
    pub container: Option<&'static str>,
    /// Canonical ACL key-access tokens from the command's keyspecs (see
    /// `CommandMeta::key_flags`). Used by the generic key extractor when no
    /// `get_keys` function is mounted.
    pub key_flags: &'static [&'static str],
    /// ACL categories declared by the pinned Valkey command metadata.
    pub acl_categories: &'static [&'static str],
    pub executor: CommandExecutor,
    pub get_keys: Option<GetKeysFn>,
}

impl CommandSpec {
    pub fn execute(
        &self,
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
        ctx: &mut ServerContext,
    ) -> Value {
        self.executor.execute(conn, args, ctx)
    }

    pub fn hosted_handler(&self) -> Option<HostedCommandHandler> {
        self.executor.hosted()
    }

    pub fn descriptor(&self) -> crate::engine::command::CommandDescriptor {
        crate::engine::command::CommandDescriptor {
            name: self.name.clone(),
            arity: self.arity,
            flags: self.flags,
            first_key: self.first_key,
            last_key: self.last_key,
            key_step: self.key_step,
            container: self.container,
            acl_categories: self.acl_categories,
        }
    }
}

pub(crate) fn effective_acl_categories(
    name: &str,
    base_categories: &'static [&'static str],
    subcommand: Option<&[u8]>,
) -> &'static [&'static str] {
    let Some(subcommand) = subcommand.and_then(|value| std::str::from_utf8(value).ok()) else {
        return base_categories;
    };
    crate::framework::generated::commands::subcommand_meta(name, subcommand)
        .map(|meta| meta.acl_categories)
        .unwrap_or(base_categories)
}

#[cfg(test)]
mod tests {
    use crate::engine::core::CommandOutcome;
    use crate::framework::commands::command_runtime::CommandRuntime;
    use crate::framework::{CommandArgs, CommandExecutor};

    fn test_runtime_handler(
        _runtime: &mut dyn CommandRuntime,
        _args: &CommandArgs,
    ) -> CommandOutcome {
        CommandOutcome::reply(crate::engine::core::Value::ok())
    }

    #[test]
    fn runtime_executor_exposes_its_injected_handler() {
        let executor = CommandExecutor::Runtime(test_runtime_handler);
        let expected: super::RuntimeCommandHandler = test_runtime_handler;
        assert!(std::ptr::fn_addr_eq(
            executor.runtime_handler().unwrap(),
            expected
        ));
    }

    #[test]
    fn generated_acl_categories_reach_command_spec() {
        let expected = &["FAST", "READ", "STRING"];

        assert_eq!(
            crate::framework::generated::commands::GET_META.acl_categories,
            expected
        );
        assert_eq!(
            crate::framework::commands::string::get::GET_SPEC.acl_categories,
            expected
        );
    }

    #[test]
    fn generated_categories_match_engine_builtin_namespace() {
        let mut generated = crate::framework::generated::commands::BUILTIN_ACL_CATEGORIES.to_vec();
        let mut engine = crate::engine::extension::acl::BUILTIN_ACL_CATEGORY_NAMES.to_vec();
        generated.sort_unstable();
        engine.sort_unstable();

        assert_eq!(generated, engine);
    }
}
