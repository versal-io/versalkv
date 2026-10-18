//! L2 semantic engine layer.

pub mod command;
pub mod core;
#[cfg(not(feature = "wasm"))]
pub mod distributed;
#[cfg(not(feature = "wasm"))]
pub mod extension;
#[cfg(not(feature = "wasm"))]
pub mod host;
#[cfg(not(feature = "wasm"))]
pub mod persistence;
#[cfg(not(feature = "wasm"))]
pub mod state;
