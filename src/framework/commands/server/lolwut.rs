use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::memory::util_version as version;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LOLWUT", runtime)]
pub fn lolwut(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() > 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("lolwut"));
    }
    if args.args.len() == 2 {
        let ver = std::str::from_utf8(&args.args[0]).unwrap_or("");
        let _arg = std::str::from_utf8(&args.args[1]).unwrap_or("");
        if ver.eq_ignore_ascii_case("version") {
        }
    }
    CommandOutcome::reply(if runtime.extended_redis_compatibility() {
        Value::bulk(Bytes::from_static(b"Redis ver. 7.2.0"))
    } else {
        Value::bulk(Bytes::from(format!("Valkey ver. {}", version::REDIS_VERSION)))
    })
}
