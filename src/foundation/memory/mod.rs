//! Pure in-memory foundation modules.

#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(any(not(feature = "wasm"), test))]
pub(crate) mod client_id;
pub mod external;
pub mod key_access;
pub mod metrics;
pub mod numeric;
pub mod tls;
#[cfg_attr(feature = "wasm", path = "util_alloc_wasm.rs")]
pub mod util_alloc;
pub mod util_assert;
pub mod util_version;
pub mod value;
