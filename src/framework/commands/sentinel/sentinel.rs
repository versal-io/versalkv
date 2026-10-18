use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("SENTINEL")]
pub fn sentinel(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("sentinel");
    }
    let subcmd = String::from_utf8_lossy(&args.args[0]).to_lowercase();
    match subcmd.as_str() {
        "masters" | "primaries" => super::info_cmds::masters(conn, args, ctx),
        "master" | "primary" => super::info_cmds::master(conn, args, ctx),
        "replicas" | "slaves" => super::info_cmds::replicas(conn, args, ctx),
        "sentinels" => super::info_cmds::sentinels(conn, args, ctx),
        "get-master-addr-by-name" | "get-primary-addr-by-name" => {
            super::info_cmds::get_master_addr_by_name(conn, args, ctx)
        }
        "info-cache" => super::info_cmds::info_cache(conn, args, ctx),
        "pending-scripts" => super::info_cmds::pending_scripts(conn, args, ctx),

        "monitor" => super::control_cmds::monitor(conn, args, ctx),
        "remove" => super::control_cmds::remove(conn, args, ctx),
        "set" => super::control_cmds::set(conn, args, ctx),
        "failover" => super::control_cmds::failover(conn, args, ctx),
        "ckquorum" => super::info_cmds::ckquorum(conn, args, ctx),
        "is-master-down-by-addr" | "is-primary-down-by-addr" => {
            super::control_cmds::is_master_down_by_addr(conn, args, ctx)
        }
        "simulate-failure" => super::control_cmds::simulate_failure(conn, args, ctx),

        "flushconfig" => super::admin_cmds::flushconfig(conn, args, ctx),
        "reset" => super::admin_cmds::reset(conn, args, ctx),
        "myid" => super::admin_cmds::myid(conn, args, ctx),
        "config" => super::admin_cmds::config(conn, args, ctx),
        "debug" => super::admin_cmds::debug(conn, args, ctx),
        "help" => super::info_cmds::help(conn, args, ctx),

        _ => Value::error(format!("ERR Unknown subcommand '{}'", subcmd)),
    }
}
