use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::{CommandRuntime, FlushMode};
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("FLUSHALL", runtime)]
pub fn flushall(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mode = match args.args.as_slice() {
        [] => FlushMode::Default,
        [mode] if mode.eq_ignore_ascii_case(b"sync") => FlushMode::Sync,
        [mode] if mode.eq_ignore_ascii_case(b"async") => FlushMode::Async,
        _ => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    runtime.flushall(mode)
}

#[cfg(test)]
mod tests {
    use super::flushall;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn flushall_clears_every_database() {
        let mut runtime = TestCommandRuntime::new(2, 0);
        for db_idx in 0..2 {
            runtime
                .db_mut_at(db_idx)
                .unwrap()
                .set(format!("key-{db_idx}").into_bytes(), Robj::string("value"));
        }
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"FLUSHALL")]);

        assert_eq!(flushall(&mut runtime, &args).reply, Value::ok());
        assert_eq!(runtime.dbs.db_size(0), 0);
        assert_eq!(runtime.dbs.db_size(1), 0);
    }
}
