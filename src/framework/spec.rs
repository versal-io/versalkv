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
    pub fn execute(
        self,
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
        ctx: &mut ServerContext,
    ) -> Value {
        let _scope = ctx.hotkeys_command_scope(args);
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
            key_flags: self.key_flags,
            acl_categories: self.acl_categories,
            get_keys: self.get_keys,
        }
    }
}

pub(crate) fn effective_command_flags_for_call(
    name: &str,
    base_flags: CmdFlags,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> CmdFlags {
    let subcommand_flags = args
        .args
        .first()
        .and_then(|subcommand| std::str::from_utf8(subcommand).ok())
        .map(|subcommand| subcommand.to_ascii_lowercase())
        .and_then(|subcommand| {
            crate::framework::generated::commands::SUBCOMMAND_GATE_FLAGS
                .iter()
                .find(|(container, candidate, _)| *container == name && *candidate == subcommand)
                .map(|(_, _, flags)| *flags)
        })
        .unwrap_or_default();
    effective_script_command_flags(name, base_flags | subcommand_flags, args, ctx)
}

fn effective_script_command_flags(
    name: &str,
    base_flags: CmdFlags,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> CmdFlags {
    use crate::engine::command::{CMD_DENYOOM, CMD_MAY_REPLICATE, CMD_STALE, CMD_WRITE};
    use crate::engine::extension::script::{
        SCRIPT_ALLOW_OOM, SCRIPT_ALLOW_STALE, SCRIPT_NO_WRITES, SCRIPT_READ_ONLY,
    };

    let name = name.to_ascii_lowercase();
    let script_flags = match name.as_str() {
        "eval" | "eval_ro" => args
            .args
            .first()
            .and_then(|body| std::str::from_utf8(body).ok())
            .and_then(extract_valid_shebang_flags),
        "evalsha" | "evalsha_ro" => args
            .args
            .first()
            .and_then(|sha| std::str::from_utf8(sha).ok())
            .and_then(|sha| ctx.script().script_cache.peek(sha))
            .and_then(extract_valid_shebang_flags),
        "fcall" | "fcall_ro" => args
            .args
            .first()
            .and_then(|func| std::str::from_utf8(func).ok())
            .and_then(|func| ctx.script().function_registry.find_function(func))
            .map(|func| func.flags),
        _ => None,
    };
    let Some(script_flags) = script_flags else {
        return base_flags;
    };

    let mut flags = base_flags & !(CMD_STALE | CMD_DENYOOM | CMD_WRITE | CMD_MAY_REPLICATE);
    let no_writes = script_flags & (SCRIPT_NO_WRITES | SCRIPT_READ_ONLY) != 0;
    if script_flags & SCRIPT_ALLOW_OOM == 0 && !no_writes {
        flags |= CMD_DENYOOM;
    }
    if !no_writes {
        flags |= CMD_WRITE;
    }
    if script_flags & SCRIPT_ALLOW_STALE != 0 {
        flags |= CMD_STALE;
    }
    flags
}

fn extract_valid_shebang_flags(
    body: &str,
) -> Option<crate::engine::extension::script::ScriptFlags> {
    let (_, flags, has_shebang) = crate::engine::extension::lua::parse_shebang(body, 0).ok()?;
    has_shebang.then_some(flags)
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

pub(crate) fn key_flags_to_acl_bits(flags: &[&'static str]) -> u8 {
    let mut bits = 0u8;
    for flag in flags {
        match *flag {
            "RO" | "access" => bits |= crate::engine::extension::acl::ACL_READ_PERMISSION,
            "RW" => {}
            "OW" | "insert" | "update" | "delete" => {
                bits |= crate::engine::extension::acl::ACL_WRITE_PERMISSION;
            }
            _ => {}
        }
    }
    bits
}

pub(crate) fn normalize_forkless_key_targets(
    name: &str,
    args: &CommandArgs,
    selected_db: u32,
    mut targets: Vec<(u32, Vec<u8>)>,
) -> Vec<(u32, Vec<u8>)> {
    let cross_db_target = match name {
        "move" => crate::framework::commands::generic::r#move::target_db_and_key(args),
        "copy" => crate::framework::commands::generic::copy::target_db_and_key(args, selected_db),
        _ => None,
    };
    let Some((target_db, key)) = cross_db_target else {
        return targets;
    };
    if name == "copy" && target_db != selected_db {
        if let Some(destination) = args.args.get(1) {
            targets.retain(|(db_id, key)| {
                *db_id != selected_db || key.as_slice() != destination.as_ref()
            });
        }
    }
    let target = (target_db, key.to_vec());
    if !targets.contains(&target) {
        targets.push(target);
    }
    targets
}

#[cfg(test)]
mod tests {
    use crate::framework::CommandArgs;
    use bytes::Bytes;

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

    #[test]
    fn key_flags_map_acl_read_and_write_permissions() {
        use crate::engine::extension::acl::{ACL_READ_PERMISSION, ACL_WRITE_PERMISSION};

        assert_eq!(
            super::key_flags_to_acl_bits(crate::framework::generated::commands::GET_META.key_flags),
            ACL_READ_PERMISSION
        );
        assert_eq!(
            super::key_flags_to_acl_bits(
                crate::framework::generated::commands::LPUSH_META.key_flags
            ),
            ACL_WRITE_PERMISSION
        );
        assert_eq!(
            super::key_flags_to_acl_bits(crate::framework::generated::commands::SET_META.key_flags),
            ACL_READ_PERMISSION | ACL_WRITE_PERMISSION
        );
    }

    #[test]
    fn command_descriptors_preserve_dynamic_key_extractors() {
        let sort_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SORT"),
            Bytes::from_static(b"source"),
            Bytes::from_static(b"STORE"),
            Bytes::from_static(b"destination"),
        ]);
        let xgroup_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"XGROUP"),
            Bytes::from_static(b"CREATE"),
            Bytes::from_static(b"stream"),
            Bytes::from_static(b"group"),
            Bytes::from_static(b"0"),
        ]);

        assert_eq!(
            crate::framework::commands::generic::sort::SORT_SPEC
                .descriptor()
                .extract_keys(&sort_args),
            Some(vec![
                Bytes::from_static(b"source"),
                Bytes::from_static(b"destination"),
            ])
        );
        assert_eq!(
            crate::framework::commands::stream::xgroup::XGROUP_SPEC
                .descriptor()
                .extract_keys(&xgroup_args),
            Some(vec![Bytes::from_static(b"stream")])
        );
        assert_eq!(
            crate::framework::commands::generic::sort::SORT_SPEC
                .descriptor()
                .extract_key_accesses(&sort_args),
            Some(vec![
                (Bytes::from_static(b"source"), vec!["RO", "access"],),
                (Bytes::from_static(b"destination"), vec!["OW", "update"],),
            ])
        );
    }
}
