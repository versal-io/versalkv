//! SHUTDOWN command — thin wrapper over the event-loop shutdown pipeline.

use crate::engine::distributed::shutdown::{
    self, SHUTDOWN_FAILOVER, SHUTDOWN_FORCE, SHUTDOWN_NOFLAGS, SHUTDOWN_NOSAVE, SHUTDOWN_NOW,
    SHUTDOWN_SAFE, SHUTDOWN_SAVE,
};
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use command_macros::command;

/// Parse argv into (flags, abort_flag). Returns Err(reply) on syntax error.
pub(crate) fn parse_argv(args: &CommandArgs) -> Result<(u32, bool), Value> {
    let mut flags: u32 = SHUTDOWN_NOFLAGS;
    let mut abort = false;
    for arg in &args.args {
        let s = String::from_utf8_lossy(arg);
        match s.to_ascii_lowercase().as_str() {
            "save" => flags |= SHUTDOWN_SAVE,
            "nosave" => flags |= SHUTDOWN_NOSAVE,
            "now" => flags |= SHUTDOWN_NOW,
            "force" => flags |= SHUTDOWN_FORCE,
            "safe" => flags |= SHUTDOWN_SAFE,
            "failover" => flags |= SHUTDOWN_FAILOVER,
            "abort" => abort = true,
            _ => return Err(Value::error("ERR syntax error")),
        }
    }
    if flags & SHUTDOWN_SAVE != 0 && flags & SHUTDOWN_NOSAVE != 0 {
        return Err(Value::error("ERR syntax error"));
    }
    if abort && flags != SHUTDOWN_NOFLAGS {
        return Err(Value::error("ERR syntax error"));
    }
    Ok((flags, abort))
}

#[command("SHUTDOWN")]
pub fn shutdown(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let (flags, abort) = match parse_argv(args) {
        Ok(t) => t,
        Err(e) => return e,
    };

    if abort {
        let aborted = shutdown::abort_shutdown(ctx.shutdown_state_mut());
        crate::foundation::io::sys::signal::take_pending_signal();
        let _ = crate::foundation::io::sys::signal::take_second_sigint();
        return if aborted {
            crate::server_log!(notice, "{}", shutdown::messages::MANUALLY_ABORTED);
            Value::ok()
        } else {
            Value::error("ERR No shutdown in progress")
        };
    }

    let has_initial_aof_rewrite = ctx.persistence().aof_rewrite_child_pid().is_some();
    match shutdown::request_shutdown(
        ctx.shutdown_state_mut(),
        flags,
        has_initial_aof_rewrite,
    ) {
        Ok(()) => Value::ok(),
        Err(msg) => {
            crate::server_log!(warning, "{}", msg);
            Value::error(format!("ERR {msg}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_argv;
    use crate::engine::distributed::shutdown::{SHUTDOWN_FAILOVER, SHUTDOWN_NOSAVE};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn parses_nosave_failover_flags() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SHUTDOWN"),
            Bytes::from_static(b"NOSAVE"),
            Bytes::from_static(b"FAILOVER"),
        ]);

        assert_eq!(
            parse_argv(&args),
            Ok((SHUTDOWN_NOSAVE | SHUTDOWN_FAILOVER, false))
        );
    }
}
