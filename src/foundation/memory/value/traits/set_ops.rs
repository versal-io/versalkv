// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange};

/// Unified interface for set-like data structures across encodings.
pub trait SetOps {
    /// Add a member. Returns Ok(true) if added, Ok(false) if already present.
    /// Returns Err(EncodingChange::Upgrade) if the value cannot be stored in
    /// the current encoding (e.g. non-integer added to intset).
    fn add(&mut self, member: &[u8]) -> Result<bool, EncodingChange>;

    /// Remove a member. Returns Ok(true) if removed, Ok(false) if not found.
    /// Returns Err(EncodingChange::Downgrade) if shrinkage makes a smaller
    /// encoding viable (caller decides whether to actually downgrade).
    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange>;

    fn contains(&self, member: &[u8]) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    /// Return all members as owned byte vectors. Order is encoding-dependent.
    fn members(&self) -> Vec<Vec<u8>>;

    /// Current encoding identifier.
    fn encoding(&self) -> Encoding;
}
