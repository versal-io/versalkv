// SPDX-License-Identifier: BSD-3-Clause

//! Per-type operation traits implemented by each encoding.

/// Signal that a mutating operation cannot complete in the current encoding
/// and the caller should upgrade or downgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingChange {
    Upgrade,
    Downgrade,
}

pub mod hash_ops;
pub mod list_ops;
pub mod set_ops;
pub mod stream_ops;
pub mod zset_ops;

pub use hash_ops::HashOps;
pub use list_ops::ListOps;
pub use set_ops::SetOps;
pub use stream_ops::StreamOps;
pub use zset_ops::ZSetOps;
