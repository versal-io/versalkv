mod file;
mod log;
mod misc;
mod setuser;
mod user;

use self::file::{acl_load, acl_save};
use self::log::acl_log_list;
use self::misc::{acl_cat, acl_dryrun, acl_genpass};
use self::setuser::acl_setuser;
use self::user::{acl_deluser, acl_getuser, acl_list, acl_users_list};
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("ACL")]
pub fn acl_cmd(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("acl");
    }
    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args.args[0]),
    };
    match subcmd.as_str() {
        "list" => acl_list(),
        "digest" => {
            if args.args.len() != 1 {
                return Value::error("ERR wrong number of arguments for 'acl|digest' command");
            }
            match crate::engine::extension::acl::digest() {
                Ok(digest) => Value::bulk(Bytes::from(digest)),
                Err(error) => Value::error(format!("ERR {error}")),
            }
        }
        "whoami" => Value::bulk(Bytes::from(conn.username().as_bytes().to_vec())),
        "users" => acl_users_list(),
        "getuser" => {
            if args.args.len() < 2 {
                return CommandError::wrong_arity("acl getuser");
            }
            let username = std::str::from_utf8(&args.args[1]).unwrap_or("");
            acl_getuser(username)
        }
        "setuser" => acl_setuser(args, ctx),
        "deluser" => acl_deluser(conn, args),
        "cat" => acl_cat(args),
        "genpass" => acl_genpass(args),
        "log" => acl_log_list(args),
        "load" => acl_load(ctx),
        "save" => acl_save(ctx),
        "dryrun" => acl_dryrun(args, ctx),
        "help" => {
            if args.args.len() > 1 {
                return Value::error("ERR wrong number of arguments for 'acl|help' command");
            }
            let help = vec![
                "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands:",
                "LIST",
                "WHOAMI",
                "USERS",
                "GETUSER <username>",
                "SETUSER <username> [rule [rule ...]]",
                "DELUSER <username> [<username> ...]",
                "DIGEST",
                "CAT",
                "CAT <category>",
                "GENPASS [<bits>]",
                "LOG [<count>]",
                "LOAD",
                "SAVE",
                "DRYRUN <username> <command> [<arg> [arg] ...]",
                "HELP",
            ]
            .join("\r\n");
            Value::bulk(Bytes::from(help))
        }
        _ => CommandError::syntax_error(),
    }
}
