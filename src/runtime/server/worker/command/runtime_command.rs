use crate::engine::command::context::CommandConn;
use crate::engine::command::{CommandArgs, SessionId};
use crate::engine::core::Value;
use crate::framework::commands::command_runtime::{
    apply_server_command_effects, prepare_server_command,
};
use crate::framework::context::ServerContext;
use crate::framework::spec::RuntimeCommandHandler;
use crate::runtime::driver::{CommandEffectSink, CommandTransition};

pub(super) fn execute_runtime_command(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    handler: RuntimeCommandHandler,
) -> Value {
    let session = conn.session_id();
    let (db_idx, outcome) = prepare_server_command(conn, args, ctx, handler);
    let mut sink = ServerEffectSink { conn, ctx, db_idx };
    sink.commit(session, CommandTransition::Complete(outcome))
}

struct ServerEffectSink<'a> {
    conn: &'a mut dyn CommandConn,
    ctx: &'a mut ServerContext,
    db_idx: u32,
}

impl CommandEffectSink for ServerEffectSink<'_> {
    type Output = Value;

    fn commit(&mut self, _session: SessionId, transition: CommandTransition) -> Self::Output {
        let mut outcome = match transition {
            CommandTransition::Complete(outcome) | CommandTransition::Close(outcome) => outcome,
            CommandTransition::Suspend(_) => {
                return Value::error("ERR server continuation was not registered");
            }
        };
        apply_server_command_effects(self.conn, self.ctx, self.db_idx, &mut outcome.effects);
        outcome.reply
    }
}
