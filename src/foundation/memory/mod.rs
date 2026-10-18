//! Pure in-memory foundation modules.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod metrics;
#[cfg_attr(feature = "wasm", path = "util_alloc_wasm.rs")]
pub mod util_alloc;
pub mod util_assert;
pub mod util_version;
pub mod value;
