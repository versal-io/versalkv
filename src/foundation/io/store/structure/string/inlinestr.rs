#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InlineStr {
    data: [u8; 23],
    len: u8,
}

impl InlineStr {
    pub const MAX: usize = 22;

    #[inline]
    pub fn new(src: &[u8]) -> Self {
        debug_assert!(src.len() <= Self::MAX);
        let mut data = [0u8; 23];
        data[..src.len()].copy_from_slice(src);
        Self {
            data,
            len: src.len() as u8,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::string::StoredKey;

    #[test]
    fn inline_str_fits_short() {
        let s = InlineStr::new(b"hello");
        assert_eq!(s.as_bytes(), b"hello");
        assert_eq!(s.len(), 5);
        assert!(!s.is_empty());
    }

    #[test]
    fn inline_str_empty() {
        let s = InlineStr::new(b"");
        assert_eq!(s.as_bytes(), b"");
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn inline_str_max() {
        let data = [b'x'; InlineStr::MAX];
        let s = InlineStr::new(&data);
        assert_eq!(s.as_bytes(), &data[..]);
        assert_eq!(s.len(), InlineStr::MAX);
    }

    #[test]
    fn stored_key_inlines_short_bytes_and_borrows_as_a_slice() {
        assert!(std::mem::size_of::<StoredKey>() <= 16);
        let key = StoredKey::from(b"key:1234".as_slice());

        assert!(matches!(key, StoredKey::Inline(_)));
        assert_eq!(
            <StoredKey as std::borrow::Borrow<[u8]>>::borrow(&key),
            b"key:1234"
        );
    }
}
