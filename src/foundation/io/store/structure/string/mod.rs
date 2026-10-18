pub mod encoding;
pub mod inlinestr;

use self::inlinestr::InlineStr;
use crate::foundation::memory::value::Encoding;
use bytes::Bytes;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Key representation used by the database dictionaries. Most user keys are
/// short; keeping those bytes inline avoids a separate allocator block per key
/// while preserving byte-slice lookup semantics.
#[derive(Clone, Copy, Debug)]
pub struct InlineKey {
    data: [u8; 8],
    len: u8,
}

impl InlineKey {
    const MAX: usize = 8;

    fn new(value: &[u8]) -> Self {
        debug_assert!(value.len() <= Self::MAX);
        let mut data = [0; Self::MAX];
        data[..value.len()].copy_from_slice(value);
        Self {
            data,
            len: value.len() as u8,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

#[derive(Clone, Debug)]
pub enum StoredKey {
    Inline(InlineKey),
    Heap(Box<Vec<u8>>),
}

impl StoredKey {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline(value) => value.as_bytes(),
            Self::Heap(value) => value,
        }
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Inline(value) => value.as_bytes().to_vec(),
            Self::Heap(value) => *value,
        }
    }
}

impl From<Vec<u8>> for StoredKey {
    fn from(value: Vec<u8>) -> Self {
        if value.len() <= InlineKey::MAX {
            Self::Inline(InlineKey::new(&value))
        } else {
            Self::Heap(Box::new(value))
        }
    }
}

impl From<&[u8]> for StoredKey {
    fn from(value: &[u8]) -> Self {
        if value.len() <= InlineKey::MAX {
            Self::Inline(InlineKey::new(value))
        } else {
            Self::Heap(Box::new(value.to_vec()))
        }
    }
}

impl Borrow<[u8]> for StoredKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for StoredKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl PartialEq for StoredKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for StoredKey {}

impl Hash for StoredKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrInt {
    pub value: i64,
    raw: InlineStr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrEmbstr(StrEmbstrRepr);

#[derive(Debug, Clone, PartialEq, Eq)]
enum StrEmbstrRepr {
    Inline(InlineStr),
    Heap(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrRaw(pub Bytes);

impl StrInt {
    pub fn new(value: i64, raw: Bytes) -> Self {
        // Canonical i64 text is at most 20 bytes (i64::MIN), so retaining the
        // original RESP spelling never needs a heap-backed Bytes handle.
        debug_assert!(raw.len() <= InlineStr::MAX);
        Self {
            value,
            raw: InlineStr::new(&raw),
        }
    }
    pub fn encoding(&self) -> Encoding {
        Encoding::Int
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.raw.as_bytes()
    }
    pub fn len(&self) -> usize {
        self.raw.len()
    }
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl StrEmbstr {
    pub fn new(data: Bytes) -> Self {
        if data.len() <= InlineStr::MAX {
            Self(StrEmbstrRepr::Inline(InlineStr::new(&data)))
        } else {
            Self(StrEmbstrRepr::Heap(data))
        }
    }
    pub fn from_slice(data: &[u8]) -> Self {
        if data.len() <= InlineStr::MAX {
            Self(StrEmbstrRepr::Inline(InlineStr::new(data)))
        } else {
            Self(StrEmbstrRepr::Heap(Bytes::copy_from_slice(data)))
        }
    }
    pub fn into_bytes(self) -> Bytes {
        match self.0 {
            StrEmbstrRepr::Inline(s) => Bytes::copy_from_slice(s.as_bytes()),
            StrEmbstrRepr::Heap(b) => b,
        }
    }
    pub fn encoding(&self) -> Encoding {
        Encoding::Embstr
    }
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            StrEmbstrRepr::Inline(s) => s.as_bytes(),
            StrEmbstrRepr::Heap(b) => b.as_ref(),
        }
    }
    pub fn len(&self) -> usize {
        match &self.0 {
            StrEmbstrRepr::Inline(s) => s.len(),
            StrEmbstrRepr::Heap(b) => b.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<Bytes> for StrEmbstr {
    fn from(b: Bytes) -> Self {
        StrEmbstr::new(b)
    }
}
impl StrRaw {
    pub fn encoding(&self) -> Encoding {
        Encoding::Raw
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

const _: () = assert!(std::mem::size_of::<InlineStr>() == 24);
const _: () = assert!(std::mem::size_of::<StrEmbstr>() <= 40);
