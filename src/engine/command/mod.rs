mod args;
#[cfg(not(feature = "wasm"))]
pub mod context;
#[cfg(not(feature = "wasm"))]
mod descriptor;
mod metadata;
#[cfg(not(feature = "wasm"))]
mod name;
mod session;

#[cfg(not(feature = "wasm"))]
pub use args::parse_hex_or_float;
pub use args::{CommandArgs, CommandError};
#[cfg(not(feature = "wasm"))]
pub(crate) use descriptor::command_may_mutate_dataset;
#[cfg(not(feature = "wasm"))]
pub use descriptor::{CommandDescriptor, CommandKeyIndices, ResolvedCommand};
pub use metadata::*;
#[cfg(not(feature = "wasm"))]
pub use name::CommandName;
pub use session::SessionId;

/// Commands that dispatch to subcommands (e.g. `CLIENT KILL`). Nested script
/// calls need this to resolve stats names and per-subcommand `noscript`
/// restrictions. This is a static property of the command table, so callers
/// avoid a per-command runtime query.
pub(crate) fn is_subcommand_container(name: &str) -> bool {
    matches!(
        name,
        "acl"
            | "client"
            | "cluster"
            | "command"
            | "commandlog"
            | "config"
            | "debug"
            | "function"
            | "latency"
            | "memory"
            | "module"
            | "object"
            | "pubsub"
            | "script"
            | "sentinel"
            | "slowlog"
            | "xgroup"
            | "xinfo"
    )
}

#[cfg(all(test, not(feature = "wasm")))]
mod tests;
