// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::Encoding;

/// Unified interface for stream data structures across encodings.
pub trait StreamOps {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn encoding(&self) -> Encoding;
}
