use super::geosearch::{geosearch_generic, GeoSearchInvocation};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("GEOSEARCHSTORE", runtime)]
pub fn geosearchstore(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    if args.args.len() < 4 {
        return CommandOutcome::reply(CommandError::wrong_arity("geosearchstore"));
    }
    let dest = args.args[0].clone();
    let db_index = runtime.selected_db();
    CommandOutcome::reply(geosearch_generic(
        args,
        runtime,
        GeoSearchInvocation {
            db_index,
            key_idx: 1,
            opt_start: 2,
            is_store: true,
            dest_key: Some(dest),
        },
    ))
}
