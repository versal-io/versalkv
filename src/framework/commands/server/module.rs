use crate::engine::extension::config::server::ProtectedAction;
use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

#[cfg(test)]
mod tests {
    use crate::engine::extension::config::server::ProtectedAction;

    #[test]
    fn module_gate_honors_no_yes_and_local_modes() {
        assert!(!super::module_allowed(ProtectedAction::No, true));
        assert!(super::module_allowed(ProtectedAction::Yes, false));
        assert!(super::module_allowed(ProtectedAction::Local, true));
        assert!(!super::module_allowed(ProtectedAction::Local, false));
    }
}

#[command("MODULE")]
pub fn module_cmd(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if !module_allowed(ctx.config().server.enable_module_command, conn.is_local()) {
        return Value::error(
            "ERR MODULE command not allowed. Please check the server configuration.",
        );
    }
    if args.args.is_empty() {
        return CommandError::wrong_arity("module");
    }
    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args.args[0]),
    };
    match subcmd.as_str() {
        "list" => cmd_list(ctx),
        "help" => cmd_help(),
        "load" => cmd_load(args, ctx, false),
        "loadex" => cmd_load(args, ctx, true),
        "unload" => cmd_unload(args, ctx),
        _ => CommandError::syntax_error(),
    }
}

fn module_allowed(action: ProtectedAction, is_local: bool) -> bool {
    match action {
        ProtectedAction::Yes => true,
        ProtectedAction::Local => is_local,
        ProtectedAction::No => false,
    }
}

fn cmd_list(ctx: &mut ServerContext) -> Value {
    let modules: Vec<Value> = ctx
        .module_registry()
        .list()
        .map(|m| {
            let ver = format!("{}", m.ver);
            let cmds = m.commands.join(" ");
            Value::Array(vec![
                Value::bulk(Bytes::from_static(b"name")),
                Value::bulk(Bytes::from(m.name.clone())),
                Value::bulk(Bytes::from_static(b"ver")),
                Value::bulk(Bytes::from(ver)),
                Value::bulk(Bytes::from_static(b"is_static")),
                Value::bulk(Bytes::from(if m.is_static { "1" } else { "0" })),
                Value::bulk(Bytes::from_static(b"commands")),
                Value::bulk(Bytes::from(cmds)),
            ])
        })
        .collect();
    Value::Array(modules)
}

fn cmd_help() -> Value {
    let help = [
        "MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands:",
        "LIST",
        "LOAD <path> [<arg> [value] [opt] ...]",
        "LOADEX <path> [CONFIG <name> <value> ...] [ARGS <name> <value> ...]",
        "UNLOAD <name>",
        "HELP",
    ]
    .join("\r\n");
    Value::bulk(Bytes::from(help))
}

fn cmd_load(args: &CommandArgs, ctx: &mut ServerContext, _is_loadex: bool) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("module|load");
    }

    let path = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Invalid module path"),
    };

    let module_args: Vec<&[u8]> = args.args[2..].iter().map(|b| b.as_ref()).collect();
    let result = crate::framework::capabilities::module::load_module(path, &module_args, ctx);
    match result {
        Ok(name) => {
            let load_args = args.args[2..]
                .iter()
                .map(|arg| String::from_utf8_lossy(arg).into_owned())
                .collect();
            ctx.module_registry_mut()
                .set_load_path(&name, path.to_string(), load_args);
            Value::ok()
        }
        Err(e) => {
            log::warn!("Error loading extension '{}': {}", path, e);
            Value::error(e.to_string())
        }
    }
}

fn cmd_unload(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("module|unload");
    }
    let name = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Invalid module name"),
    };
    let users = crate::engine::extension::acl::acl_users().lock().unwrap();
    let command_names = ctx
        .module_registry()
        .lookup(name)
        .map(|module| module.commands.clone())
        .unwrap_or_default();
    if command_names.iter().any(|command| {
        users
            .values()
            .any(|user| user.references_command(command))
    }) {
        return Value::error(
            "ERR Can't unload module: one or more ACL users reference commands from this module",
        );
    }
    drop(users);

    match crate::framework::capabilities::module::unload_module(name, ctx) {
        Ok(()) => Value::ok(),
        Err(e) => Value::error(format!("ERR {}", e)),
    }
}
