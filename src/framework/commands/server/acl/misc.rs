use bytes::Bytes;

use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

fn gen_password(bits: u32) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut rng = seed;
    let chars = b"0123456789abcdef";
    let len = (bits / 4).max(1) as usize;
    let mut pass = String::with_capacity(len);
    for _ in 0..len {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        pass.push(chars[(rng >> 33) as usize % 16] as char);
    }
    pass
}

pub(super) fn acl_cat(args: &CommandArgs) -> Value {
    if args.args.len() > 2 {
        return Value::error("ERR unknown subcommand or wrong number of arguments for 'CAT'");
    }
    if args.args.len() == 2 {
        let category_raw = std::str::from_utf8(&args.args[1]).unwrap_or("");
        let is_builtin = crate::framework::generated::commands::BUILTIN_ACL_CATEGORIES
            .iter()
            .any(|category| category.eq_ignore_ascii_case(category_raw));
        let module_commands =
            crate::engine::extension::acl::module_acl_category_commands(category_raw);
        if !is_builtin && module_commands.is_none() {
            return Value::error(format!("ERR Unknown category '{}'", category_raw));
        }
        let mut commands = crate::framework::generated::commands::ALL_COMMAND_META
            .iter()
            .filter(|meta| {
                meta.acl_categories
                    .iter()
                    .any(|category| category.eq_ignore_ascii_case(category_raw))
            })
            .map(|meta| acl_meta_name(meta))
            .collect::<Vec<_>>();
        commands.extend(module_commands.unwrap_or_default());
        commands.sort();
        commands.dedup();
        Value::Array(
            commands
                .into_iter()
                .map(|command| Value::bulk(Bytes::from(command)))
                .collect(),
        )
    } else {
        let mut names: Vec<String> = crate::framework::generated::commands::BUILTIN_ACL_CATEGORIES
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        names.extend(crate::engine::extension::acl::module_acl_categories());
        names.sort();
        names.dedup();
        Value::Array(
            names
                .into_iter()
                .map(|name| Value::bulk(Bytes::from(name)))
                .collect(),
        )
    }
}

fn acl_meta_name(meta: &crate::framework::CommandMeta) -> String {
    let Some(container) = meta.container else {
        return meta.name.to_ascii_lowercase();
    };
    let subcommand = meta
        .name
        .strip_prefix(container)
        .and_then(|name| name.strip_prefix('-'))
        .unwrap_or(meta.name);
    format!("{}|{}", container.to_ascii_lowercase(), subcommand)
}

pub(super) fn acl_dryrun(args: &CommandArgs, ctx: &ServerContext) -> Value {
    use crate::engine::extension::acl::{ACL_READ_PERMISSION, ACL_WRITE_PERMISSION};

    if args.args.len() < 3 {
        return CommandError::wrong_arity("acl|dryrun");
    }
    let username = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return CommandError::syntax_error(),
    };
    let cmd_name = match std::str::from_utf8(&args.args[2]) {
        Ok(s) => s,
        Err(_) => return CommandError::syntax_error(),
    };
    let users = crate::engine::extension::acl::acl_users();
    let users = users.lock().unwrap();
    let user = match users.get(username) {
        Some(u) => u,
        None => return Value::error(format!("ERR User '{}' not found", username)),
    };

    let registry = ctx.command_registry();
    let spec = match registry.lookup(cmd_name.as_bytes()) {
        Some(s) => s,
        None => return Value::error(format!("ERR Command '{}' not found", cmd_name)),
    };

    let argc = (args.args.len() - 2) as i32;
    let arity_ok = if spec.arity > 0 {
        argc == spec.arity
    } else {
        argc >= -spec.arity
    };
    if !arity_ok {
        return CommandError::wrong_arity(&cmd_name.to_ascii_lowercase());
    }

    let cmd_args = &args.args[3..];
    let cmd_lower = cmd_name.to_ascii_lowercase();
    let extracted = crate::framework::commands::server::command::extract_command_keys(
        &cmd_lower, cmd_args, ctx,
    );
    let keys: Vec<(&[u8], u8)> = extracted
        .iter()
        .map(|(key, flags)| {
            let mut required = 0u8;
            for f in flags {
                match *f {
                    "access" => required |= ACL_READ_PERMISSION,
                    "insert" | "update" | "delete" => required |= ACL_WRITE_PERMISSION,
                    _ => {}
                }
            }
            (*key, required)
        })
        .collect();

    let dbid = 0i64;
    let num_dbs = ctx.num_dbs();
    let acl_categories = crate::framework::spec::effective_acl_categories(
        spec.name.as_str(),
        spec.acl_categories,
        cmd_args.first().map(|arg| arg.as_ref()),
    );
    let has_keys = spec.first_key != 0 || spec.get_keys.is_some();
    let db_req = crate::engine::extension::acl::compute_db_requirement(
        &cmd_lower,
        cmd_args,
        (spec.flags & crate::engine::command::CMD_ALL_DBS) != 0,
        crate::engine::extension::acl::should_restrict_cmd(&cmd_lower, acl_categories, has_keys),
        dbid,
        num_dbs,
    );
    let command =
        crate::engine::extension::acl::AclCommand::new(&cmd_lower, cmd_args, acl_categories);
    if let Some(denial) = user.check_command_perm(command, &db_req, num_dbs, &keys) {
        use crate::engine::extension::acl::AclDenial;
        return match denial {
            AclDenial::Db { dbid, arg_index } => {
                let object = arg_index
                    .and_then(|i| cmd_args.get(i))
                    .and_then(|raw| std::str::from_utf8(raw).ok())
                    .map_or_else(|| dbid.to_string(), str::to_string);
                Value::bulk(Bytes::from(format!(
                    "User {} has no permissions to access database {}",
                    username, object
                )))
            }
            AclDenial::Command => Value::bulk(Bytes::from(format!(
                "User {} has no permissions to run the '{}' command",
                username, cmd_lower
            ))),
            AclDenial::Key(i) => {
                let key_name = std::str::from_utf8(keys[i].0).unwrap_or("<binary>");
                Value::bulk(Bytes::from(format!(
                    "User {} has no permissions to access the '{}' key",
                    username, key_name
                )))
            }
            AclDenial::Channel(i) => {
                let channel = cmd_args
                    .get(i)
                    .map(|channel| String::from_utf8_lossy(channel))
                    .unwrap_or_default();
                Value::bulk(Bytes::from(format!(
                    "User {username} has no permissions to access the '{channel}' channel"
                )))
            }
        };
    }

    if (cmd_lower == "publish"
        || cmd_lower == "spublish"
        || cmd_lower == "subscribe"
        || cmd_lower == "ssubscribe"
        || cmd_lower == "psubscribe")
        && !user.can_access_channel_for_command(&cmd_lower, cmd_args)
    {
        let channel_name = cmd_args
            .first()
            .and_then(|c| std::str::from_utf8(c).ok())
            .unwrap_or("<binary>");
        return Value::bulk(Bytes::from(format!(
            "User {} has no permissions to access the '{}' channel",
            username, channel_name
        )));
    }

    Value::bulk(Bytes::from_static(b"OK"))
}

pub(super) fn acl_genpass(args: &CommandArgs) -> Value {
    let bits = if args.args.len() >= 2 {
        match args.parse::<u32>(1) {
            Ok(b) if (1..=4096).contains(&b) => b,
            _ => {
                return Value::error(
                    "ERR ACL GENPASS argument must be the number of bits (1 to 4096)",
                );
            }
        }
    } else {
        256
    };
    Value::bulk(Bytes::from(gen_password(bits)))
}

#[cfg(test)]
mod tests {
    use super::acl_cat;
    use crate::foundation::io::protocol::Value;
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        CommandArgs::from_parts(parts.iter().copied().map(Bytes::from_static).collect())
    }

    fn bulk_items(value: Value) -> Vec<String> {
        let Value::Array(values) = value else {
            panic!("expected ACL CAT array");
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::Bulk(value) => String::from_utf8(value.to_vec()).unwrap(),
                _ => panic!("expected ACL CAT bulk item"),
            })
            .collect()
    }

    #[test]
    fn acl_cat_uses_generated_top_level_and_subcommand_membership() {
        let string_commands = bulk_items(acl_cat(&args(&[b"ACL", b"CAT", b"STRING"])));
        assert!(string_commands.contains(&"get".to_string()));
        assert!(string_commands.contains(&"set".to_string()));

        let admin_commands = bulk_items(acl_cat(&args(&[b"ACL", b"CAT", b"ADMIN"])));
        assert!(admin_commands.contains(&"config|get".to_string()));
    }

    #[test]
    fn acl_cat_names_match_generated_category_catalog() {
        let names = bulk_items(acl_cat(&args(&[b"ACL", b"CAT"])));
        for category in crate::framework::generated::commands::BUILTIN_ACL_CATEGORIES {
            assert!(names.contains(&category.to_ascii_lowercase()));
        }
    }
}
