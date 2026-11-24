use bytes::{Buf, Bytes};
use std::ops::Deref;

use crate::foundation::memory::shared_output::SharedOutputAllocation;

#[cfg(test)]
mod owner_accounting_tests;

struct SharedOutputOwner {
    // Drop the backing store before removing its physical reservation.
    data: Vec<u8>,
    _allocation: SharedOutputAllocation,
}

impl AsRef<[u8]> for SharedOutputOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Immutable payload and its original capacity projection, retained across
/// partial writes and thread handoffs. The allocator and the maxmemory
/// exclusion both count its backing allocation once.
#[derive(Clone, Debug)]
pub(crate) struct SharedOutputBlock {
    pub(super) bytes: Bytes,
    pub(super) allocation_bytes: usize,
}

impl From<Vec<u8>> for SharedOutputBlock {
    fn from(data: Vec<u8>) -> Self {
        let allocation_bytes = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
        Self {
            bytes: Bytes::from_owner(SharedOutputOwner {
                data,
                _allocation: SharedOutputAllocation::new(allocation_bytes),
            }),
            allocation_bytes,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OutputFrame {
    Owned(Vec<u8>),
    Shared(SharedOutputBlock),
}

impl OutputFrame {
    pub(crate) fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(data) => data,
            Self::Shared(block) => &block.bytes,
        }
    }

    /// Client limits retain the full frame capacity until it is consumed.
    pub(crate) fn logical_allocation_bytes(&self) -> usize {
        match self {
            Self::Owned(_) => self.owned_allocation_bytes(),
            Self::Shared(block) => block.allocation_bytes,
        }
    }

    /// Shared payloads have no per-connection physical ownership and must not
    /// be subtracted once for every replica in maxmemory accounting.
    pub(crate) fn owned_allocation_bytes(&self) -> usize {
        match self {
            Self::Owned(data) => crate::foundation::memory::util_alloc::vec_allocation_bytes(data),
            Self::Shared(_) => 0,
        }
    }

    pub(crate) fn advance(&mut self, count: usize) {
        match self {
            Self::Owned(data) => {
                data.drain(..count);
            }
            Self::Shared(block) => block.bytes.advance(count),
        }
    }

    /// CommandConn and hosted module replies still use owned vectors.
    pub(crate) fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(data) => data,
            Self::Shared(block) => block.bytes.to_vec(),
        }
    }
}

impl From<Vec<u8>> for OutputFrame {
    fn from(data: Vec<u8>) -> Self {
        Self::Owned(data)
    }
}

impl AsRef<[u8]> for OutputFrame {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for OutputFrame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for OutputFrame {
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl Eq for OutputFrame {}

impl<'a> IntoIterator for &'a OutputFrame {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}
