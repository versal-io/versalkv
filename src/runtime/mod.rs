#![forbid(unsafe_code)]

pub mod driver;

#[cfg(any(feature = "wasm", test))]
pub mod embedded;

#[cfg(not(feature = "wasm"))]
pub(crate) mod server;
