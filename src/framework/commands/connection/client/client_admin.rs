use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;

pub fn client_setinfo(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 3 {
        return CommandError::wrong_arity("client");
    }
    let key = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    let val = String::from_utf8_lossy(&args.args[2]).to_string();
    if val.contains(' ') {
        return Value::error("ERR LIB-NAME/LIB-VER cannot contain spaces");
    }
    if val.contains('\n') || val.contains('\r') {
        return Value::error(
            "ERR LIB-NAME/LIB-VER cannot contain newlines or special chars",
        );
    }
    match key.as_str() {
        "lib-name" => {
            conn.set_lib_name(val);
            if let Some(ci) = ctx.client_info_mut(conn.token().0) {
                ci.lib_name = conn.lib_name().to_string();
            }
            Value::ok()
        }
        "lib-ver" => {
            conn.set_lib_ver(val);
            if let Some(ci) = ctx.client_info_mut(conn.token().0) {
                ci.lib_ver = conn.lib_ver().to_string();
            }
            Value::ok()
        }
        _ => Value::error("ERR Unrecognized CLIENT SETINFO option"),
    }
}

pub fn client_no_touch(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return Value::error("ERR unknown subcommand or wrong number of arguments. Try CLIENT HELP");
    }
    let val = String::from_utf8_lossy(&args.args[1]);
    match val.as_ref() {
        "on" | "yes" | "1" => conn.set_no_touch(true),
        "off" | "no" | "0" => conn.set_no_touch(false),
        _ => return Value::error("ERR CLIENT NO-TOUCH must be ON or OFF"),
    }
    Value::ok()
}

pub fn client_import_source(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    let val = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    match val.as_ref() {
        "on" | "yes" | "1" => {
            if !ctx.config().server.import_mode {
                return Value::error("ERR Server is not in import mode");
            }
            conn.set_import_source(true);
            if let Some(ci) = ctx.client_info_mut(conn.token().0) {
                ci.flags.retain(|c| c != 'N');
                if !ci.flags.contains('I') {
                    ci.flags.push('I');
                }
            }
        }
        "off" | "no" | "0" => {
            conn.set_import_source(false);
            if let Some(ci) = ctx.client_info_mut(conn.token().0) {
                ci.flags.retain(|c| c != 'I');
                if ci.flags.is_empty() {
                    ci.flags.push('N');
                }
            }
        }
        _ => return Value::error("ERR CLIENT IMPORT-SOURCE must be ON or OFF"),
    }
    Value::ok()
}

pub fn client_reply(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    let val = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    match val.as_ref() {
        "on" => {
            conn.set_reply_off(false);
            conn.set_reply_skip(false);
            conn.set_reply_skip_next(false);
            Value::ok()
        }
        "off" => {
            conn.set_reply_off(true);
            conn.set_reply_skip(false);
            conn.set_reply_skip_next(false);
            conn.suppress_reply();
            Value::Null
        }
        "skip" => {
            if !conn.reply_off() {
                conn.set_reply_skip_next(true);
            }
            conn.suppress_reply();
            Value::Null
        }
        _ => Value::error("ERR syntax error"),
    }
}

pub fn client_capa(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    let capa = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    let mut flags = conn.capa_flags().to_vec();
    if !flags.contains(&capa) {
        flags.push(capa);
    }
    conn.set_capa_flags(flags);
    Value::ok()
}

pub fn client_caching(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    if !conn.tracking() {
        Value::error(
            "ERR CLIENT CACHING is only available when the client is in tracking mode",
        )
    } else {
        let val = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
        match val.as_ref() {
            "yes" | "no" => {
                if (conn.tracking_optin() && val == "yes")
                    || (conn.tracking_optout() && val == "no")
                {
                    conn.set_tracking_caching(true);
                    Value::ok()
                } else {
                    Value::error("ERR syntax error")
                }
            }
            _ => Value::error("ERR syntax error"),
        }
    }
}
