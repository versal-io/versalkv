// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange};
use bytes::Bytes;

/// Unified interface for list-like data structures across encodings.
pub trait ListOps {
    fn push_back(&mut self, value: &[u8]) -> Result<usize, EncodingChange>;
    fn push_front(&mut self, value: &[u8]) -> Result<usize, EncodingChange>;
    fn pop_back(&mut self) -> Result<Option<Bytes>, EncodingChange>;
    fn pop_front(&mut self) -> Result<Option<Bytes>, EncodingChange>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get(&self, index: isize) -> Option<Vec<u8>>;
    fn encoding(&self) -> Encoding;
}
