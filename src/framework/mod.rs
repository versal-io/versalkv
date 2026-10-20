#[cfg(not(feature = "wasm"))]
pub(crate) mod aof_replay;
#[cfg(not(feature = "wasm"))]
pub mod capabilities;
#[cfg_attr(feature = "wasm", path = "commands/portable.rs")]
pub mod commands;
#[cfg(not(feature = "wasm"))]
pub mod config;
#[cfg(not(feature = "wasm"))]
pub mod context;
#[cfg(not(feature = "wasm"))]
pub mod distributed;
#[cfg(not(feature = "wasm"))]
mod flag_names;
pub mod generated;
#[cfg(not(feature = "wasm"))]
pub(crate) mod host_tasks;
#[cfg(not(feature = "wasm"))]
pub mod observability;
#[cfg(not(feature = "wasm"))]
pub mod registry;
#[cfg(not(feature = "wasm"))]
pub mod spec;

mod command_metadata;
pub use command_metadata::{CommandMeta, GetKeysFn};

#[cfg(not(feature = "wasm"))]
pub use registry::CommandRegistry;

#[cfg(not(feature = "wasm"))]
pub use crate::engine::command::parse_hex_or_float;
#[allow(unused_imports)]
pub use crate::engine::command::{
    CmdFlags, CommandArgs, CommandError, DispatchGroup, CMD_ADMIN, CMD_ALLOW_BUSY, CMD_ALL_DBS,
    CMD_ASKING, CMD_BLOCKING, CMD_DENYOOM, CMD_FAST, CMD_LOADING, CMD_MAY_REPLICATE,
    CMD_MOVABLE_KEYS, CMD_NOSCRIPT, CMD_NO_ASYNC_LOADING, CMD_NO_AUTH, CMD_NO_MANDATORY_KEYS,
    CMD_NO_MULTI, CMD_ONLY_SENTINEL, CMD_PROTECTED, CMD_PUBSUB, CMD_READONLY, CMD_SENTINEL,
    CMD_SKIP_COMMANDLOG, CMD_SKIP_MONITOR, CMD_SKIP_SLOWLOG, CMD_SLOW, CMD_STALE,
    CMD_TOUCHES_ARBITRARY_KEYS, CMD_WRITE,
};

/// ```ignore
/// subcommand_dispatch!(sub, conn, args, ctx, {
///     "list" => handler_list,
///     "id"   => handler_id,
/// })
/// ```
#[macro_export]
macro_rules! subcommand_dispatch {
    ($sub:expr, $conn:expr, $args:expr, $ctx:expr, { $($name:literal => $handler:path),* $(,)? }) => {
        match $sub {
            $(
                $name => $handler($conn, $args, $ctx),
            )*
            _ => $crate::framework::CommandError::syntax_error(),
        }
    };
}
#[allow(unused_imports)]
#[cfg(not(feature = "wasm"))]
pub use spec::{CommandExecutor, CommandSpec, HostedCommandHandler};
