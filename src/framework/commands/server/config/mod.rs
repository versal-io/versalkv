use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

mod get;
mod help;
mod meta;
mod reset;
mod rewrite;
mod set;
mod support;
mod restore_asking;

pub use get::config_get;
pub use help::config_help;
pub use reset::config_resetstat;
pub use rewrite::config_rewrite;
pub use set::config_set;
pub(crate) use set::apply_config_set_one;
pub(crate) use set::set_cluster::apply_cluster_config_snapshot;
pub use support::{config_registry, set_oom_score_adj, set_oom_score_adj_child};
use meta::config_info;
use support::read_oom_score_adj;
pub(crate) use support::write_oom_score_adj;
pub(in crate::framework::commands::server::config) use support::{
    bool_to_yesno, parse_bool_strict, parse_mem_unit_unsigned,
};

pub fn register_all(reg: &mut crate::framework::CommandRegistry) {
    reg.register(CONFIG_SPEC);
    reg.register(restore_asking::RESTORE_ASKING_SPEC);
}

#[command("CONFIG")]
pub fn config_cmd(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("config");
    }
    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args.args[0]),
    };
    match subcmd.as_str() {
        "set" => config_set(conn, args, ctx),
        "get" => config_get(args, ctx),
        "info" => config_info(args),
        "resetstat" => config_resetstat(ctx),
        "rewrite" => config_rewrite(ctx),
        "help" => config_help(),
        _ => Value::error(format!(
            "ERR unknown subcommand '{}'. Try CONFIG HELP.",
            subcmd
        )),
    }
}
