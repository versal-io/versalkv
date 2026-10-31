//! Scoped requests from long-running guest work into the server host.
//! This protocol deliberately contains no event-loop types.  The L5 runtime
//! supplies a stack-borrowed dispatcher while Lua, RDB, or AOF work is active.

use crate::engine::command::CommandArgs;
use crate::engine::extension::script::ScriptRunCtx;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::DbSet;

pub type HostDispatcher<'host> =
    dyn for<'request> FnMut(HostRequest<'request>) -> Result<HostReply, HostError> + 'host;

pub enum HostRequest<'a> {
    ExecuteCommand {
        args: &'a CommandArgs,
        origin: HostCommandOrigin<'a>,
    },
    CheckAcl {
        args: &'a CommandArgs,
    },
    Yield(HostYield<'a>),
}

pub enum HostCommandOrigin<'a> {
    Script {
        run: &'a mut ScriptRunCtx,
        mode: ScriptCallMode,
    },
    AofReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptCallMode {
    Call,
    ProtectedCall,
}

pub enum HostYield<'a> {
    Module {
        flags: i32,
        busy_reply: Option<&'a str>,
        elapsed_ms: u64,
    },
    ScriptCheckpoint {
        run: &'a mut ScriptRunCtx,
    },
    RdbProgress {
        progress: LoadProgress,
        visible_dataset: Option<&'a mut DbSet>,
    },
    AofProgress {
        loaded_bytes: u64,
        command_count: u64,
    },
    ReplicationReplay {
        processed_bytes: u64,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadProgress {
    pub loaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub loaded_keys: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostAbort {
    Shutdown,
}

#[derive(Debug, PartialEq)]
pub enum HostReply {
    Command(Value),
    Allowed(bool),
    Continue,
    Abort(HostAbort),
}

#[derive(Debug, PartialEq, Eq)]
pub enum HostControl {
    Continue,
    Abort(HostAbort),
}

#[derive(Debug, PartialEq, Eq)]
pub enum HostError {
    Invariant(&'static str),
    Runtime(String),
}

impl std::fmt::Display for HostError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invariant(message) => write!(formatter, "host invariant violated: {message}"),
            Self::Runtime(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for HostError {}

pub fn dispatch_command(
    dispatcher: &mut HostDispatcher<'_>,
    args: &CommandArgs,
    origin: HostCommandOrigin<'_>,
) -> Result<Value, HostError> {
    match dispatcher(HostRequest::ExecuteCommand { args, origin })? {
        HostReply::Command(value) => Ok(value),
        HostReply::Allowed(_) | HostReply::Continue | HostReply::Abort(_) => Err(
            HostError::Invariant("execute-command request returned a control reply"),
        ),
    }
}

pub fn yield_script(
    dispatcher: &mut HostDispatcher<'_>,
    run: &mut ScriptRunCtx,
) -> Result<HostControl, HostError> {
    control_reply(dispatcher(HostRequest::Yield(
        HostYield::ScriptCheckpoint { run },
    ))?)
}

pub fn yield_rdb(
    dispatcher: &mut HostDispatcher<'_>,
    progress: LoadProgress,
    visible_dataset: Option<&mut DbSet>,
) -> Result<HostControl, HostError> {
    control_reply(dispatcher(HostRequest::Yield(HostYield::RdbProgress {
        progress,
        visible_dataset,
    }))?)
}

pub fn yield_aof(
    dispatcher: &mut HostDispatcher<'_>,
    loaded_bytes: u64,
    command_count: u64,
) -> Result<HostControl, HostError> {
    control_reply(dispatcher(HostRequest::Yield(HostYield::AofProgress {
        loaded_bytes,
        command_count,
    }))?)
}

pub fn yield_replication_replay(
    dispatcher: &mut HostDispatcher<'_>,
    processed_bytes: u64,
) -> Result<HostControl, HostError> {
    control_reply(dispatcher(HostRequest::Yield(
        HostYield::ReplicationReplay { processed_bytes },
    ))?)
}

fn control_reply(reply: HostReply) -> Result<HostControl, HostError> {
    match reply {
        HostReply::Continue => Ok(HostControl::Continue),
        HostReply::Abort(reason) => Ok(HostControl::Abort(reason)),
        HostReply::Command(_) | HostReply::Allowed(_) => Err(HostError::Invariant(
            "yield request returned a command reply",
        )),
    }
}

pub fn check_acl(
    dispatcher: &mut HostDispatcher<'_>,
    args: &CommandArgs,
) -> Result<bool, HostError> {
    match dispatcher(HostRequest::CheckAcl { args })? {
        HostReply::Allowed(allowed) => Ok(allowed),
        HostReply::Command(_) | HostReply::Continue | HostReply::Abort(_) => Err(
            HostError::Invariant("ACL request returned a non-boolean reply"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;

    use super::{
        dispatch_command, yield_script, HostCommandOrigin, HostError, HostReply, HostRequest,
    };

    fn continue_for_any_request(_request: HostRequest<'_>) -> Result<HostReply, HostError> {
        Ok(HostReply::Continue)
    }

    fn command_for_any_request(_request: HostRequest<'_>) -> Result<HostReply, HostError> {
        Ok(HostReply::Command(Value::ok()))
    }

    #[test]
    fn command_wrapper_rejects_control_reply() {
        let mut host = continue_for_any_request;
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"PING")]);

        assert!(matches!(
            dispatch_command(&mut host, &args, HostCommandOrigin::AofReplay),
            Err(HostError::Invariant(_))
        ));
    }

    #[test]
    fn yield_wrapper_rejects_command_reply() {
        let mut host = command_for_any_request;
        let mut run = crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(
            crate::engine::extension::script::SCRIPT_EVAL_MODE,
        );

        assert!(matches!(
            yield_script(&mut host, &mut run),
            Err(HostError::Invariant(_))
        ));
    }
}
