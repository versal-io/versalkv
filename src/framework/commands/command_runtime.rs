#[path = "command_runtime/shared.rs"]
mod shared;
pub use shared::{CommandRuntime, FlushMode};

#[cfg(any(feature = "wasm", test))]
#[path = "command_runtime/catalog.rs"]
mod catalog;
#[cfg(any(feature = "wasm", test))]
pub use catalog::execute;
#[cfg(any(feature = "wasm", test))]
pub use catalog::{command_specs, lookup_command_name, RuntimeCommandSpec};

#[cfg(not(feature = "wasm"))]
#[path = "command_runtime/server.rs"]
mod server_adapter;
#[cfg(not(feature = "wasm"))]
pub(crate) use server_adapter::{
    apply_server_command_effects, execute_server_command, prepare_server_command,
};

#[cfg(any(feature = "wasm", test))]
#[path = "command_runtime/embedded.rs"]
mod embedded_adapter;
#[cfg(any(feature = "wasm", test))]
pub(crate) use embedded_adapter::EmbeddedCommandRuntime;

#[cfg(test)]
#[path = "command_runtime/test_support.rs"]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::TestCommandRuntime;
