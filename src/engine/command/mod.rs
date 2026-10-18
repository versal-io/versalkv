mod args;
#[cfg(not(feature = "wasm"))]
pub mod context;
#[cfg(not(feature = "wasm"))]
mod descriptor;
mod metadata;
mod session;

#[cfg(not(feature = "wasm"))]
pub use args::parse_hex_or_float;
pub use args::{CommandArgs, CommandError};
#[cfg(not(feature = "wasm"))]
pub use descriptor::{CommandDescriptor, CommandKeyIndices};
pub use metadata::*;
pub use session::SessionId;

#[cfg(all(test, not(feature = "wasm")))]
mod tests;
