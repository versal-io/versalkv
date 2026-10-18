// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange};

/// Unified interface for sorted set data structures across encodings.
pub trait ZSetOps {
    /// Add a member with a score. Returns Ok(true) if added, Ok(false) if updated.
    fn add(&mut self, member: &[u8], score: f64) -> Result<bool, EncodingChange>;

    /// Remove a member. Returns Ok(true) if removed, Ok(false) if not found.
    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange>;

    /// Get the score of a member. Returns None if the member doesn't exist.
    fn score(&self, member: &[u8]) -> Option<f64>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    /// Current encoding identifier.
    fn encoding(&self) -> Encoding;
}
