// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange};

/// Unified interface for hash-like data structures across encodings.
pub trait HashOps {
    /// Set a field-value pair. Returns Ok(true) if the field is new,
    /// Ok(false) if it was updated.
    fn set(&mut self, field: &[u8], value: &[u8]) -> Result<bool, EncodingChange>;

    /// Get the value for a field. Returns None if the field doesn't exist.
    fn get(&self, field: &[u8]) -> Option<Vec<u8>>;

    /// Delete a field. Returns Ok(true) if deleted, Ok(false) if not found.
    fn del(&mut self, field: &[u8]) -> Result<bool, EncodingChange>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, field: &[u8]) -> bool;

    /// Current encoding identifier.
    fn encoding(&self) -> Encoding;
}
