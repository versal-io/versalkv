use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};

use crate::foundation::memory::value::{Encoding, EncodingChange, ListOps};

#[derive(Debug, Clone, PartialEq)]
pub struct ListValue {
    #[allow(clippy::box_collection)]
    pub(crate) inner: Box<VecDeque<Bytes>>,
    pub(crate) encoding_override: Option<&'static str>,
}

impl ListValue {
    pub fn new() -> Self {
        Self {
            inner: Box::new(VecDeque::new()),
            encoding_override: None,
        }
    }

    pub fn from_vecdeque(v: VecDeque<Bytes>) -> Self {
        Self {
            inner: Box::new(v),
            encoding_override: None,
        }
    }

    pub fn encoding_name(&self, max_listpack_size: i64) -> &'static str {
        if let Some(enc) = self.encoding_override {
            return enc;
        }
        infer_list_encoding(&self.inner, max_listpack_size)
    }

    pub fn set_encoding_override(&mut self, enc: &'static str) {
        self.encoding_override = Some(enc);
    }

    pub fn clear_encoding_override(&mut self) {
        self.encoding_override = None;
    }

    pub fn note_encoding_before_shrink(&mut self, max_listpack_size: i64) {
        if self.encoding_name(max_listpack_size) == "quicklist" {
            self.encoding_override = Some("quicklist");
        }
    }

    pub fn update_encoding_after_grow(&mut self, max_listpack_size: i64) {
        if self.encoding_override == Some("quicklist") {
            return;
        }
        let inferred = infer_list_encoding(&self.inner, max_listpack_size);
        if inferred == "quicklist" {
            self.encoding_override = Some("quicklist");
        }
    }

    pub fn update_encoding_after_shrink(&mut self, max_listpack_size: i64) {
        let inferred = infer_list_encoding(&self.inner, max_listpack_size);
        if inferred == "quicklist" {
            self.encoding_override = Some("quicklist");
            return;
        }
        if would_shrink_fit_limit(&self.inner, max_listpack_size) {
            self.encoding_override = None;
        }
    }
}

impl ListOps for ListValue {
    fn push_back(&mut self, value: &[u8]) -> Result<usize, EncodingChange> {
        self.inner.push_back(Bytes::copy_from_slice(value));
        Ok(self.inner.len())
    }

    fn push_front(&mut self, value: &[u8]) -> Result<usize, EncodingChange> {
        self.inner.push_front(Bytes::copy_from_slice(value));
        Ok(self.inner.len())
    }

    fn pop_back(&mut self) -> Result<Option<Bytes>, EncodingChange> {
        Ok(self.inner.pop_back())
    }

    fn pop_front(&mut self) -> Result<Option<Bytes>, EncodingChange> {
        Ok(self.inner.pop_front())
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn get(&self, index: isize) -> Option<Vec<u8>> {
        let idx = if index < 0 {
            let len = self.inner.len() as isize;
            let adjusted = len + index;
            if adjusted < 0 {
                return None;
            }
            adjusted as usize
        } else {
            index as usize
        };
        self.inner.get(idx).map(|b| b.to_vec())
    }

    fn encoding(&self) -> Encoding {
        Encoding::Quicklist
    }
}

impl Default for ListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ListValue {
    type Target = VecDeque<Bytes>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ListValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub fn would_grow_exceed_limit(
    list: &VecDeque<Bytes>,
    new_elem: &[u8],
    max_listpack_size: i64,
) -> bool {
    let clamped = if max_listpack_size < -5 {
        -5
    } else if max_listpack_size == 0 {
        1
    } else {
        max_listpack_size
    };
    if clamped >= 0 {
        // count-based: adding +1 element
        list.len() + 1 > clamped as usize
    } else {
        // size-based: estimate size with the new element
        let elem_bytes: usize = list.iter().map(|b| b.len()).sum::<usize>() + new_elem.len();
        let overhead: usize = 7 + (list.len() + 1) * 4;
        let total_bytes = elem_bytes + overhead;
        let limit_bytes = (1 << ((-clamped) as usize + 11)) as usize;
        total_bytes > limit_bytes
    }
}

pub fn would_shrink_fit_limit(list: &VecDeque<Bytes>, max_listpack_size: i64) -> bool {
    let clamped = if max_listpack_size < -5 {
        -5
    } else if max_listpack_size == 0 {
        1
    } else {
        max_listpack_size
    };
    if clamped >= 0 {
        list.len() <= clamped as usize / 2
    } else {
        let elem_bytes: usize = list.iter().map(|b| b.len()).sum();
        let overhead: usize = 7 + list.len() * 4;
        let total_bytes = elem_bytes + overhead;
        let limit_bytes = (1 << ((-clamped) as usize + 11)) as usize;
        total_bytes <= limit_bytes / 2
    }
}

pub fn infer_list_encoding(list: &VecDeque<Bytes>, max_listpack_size: i64) -> &'static str {
    let clamped = if max_listpack_size < -5 {
        -5
    } else if max_listpack_size == 0 {
        1
    } else {
        max_listpack_size
    };
    if clamped >= 0 {
        let max_entries = clamped as usize;
        if list.len() > max_entries {
            return "quicklist";
        }
        return "listpack";
    }
    let elem_bytes: usize = list.iter().map(|b| b.len()).sum();
    let overhead: usize = 7 + list.len() * 4; // header(6) + end(1) + per-element(4)
    let total_bytes = elem_bytes + overhead;
    let limit_bytes = (1 << ((-clamped) as usize + 11)) as usize; // -1=4KB, -2=8KB, -3=16KB, -4=32KB, -5=64KB
    if total_bytes > limit_bytes {
        return "quicklist";
    }
    "listpack"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_with_lengths(lengths: &[usize]) -> VecDeque<Bytes> {
        lengths
            .iter()
            .map(|len| Bytes::from(vec![b'x'; *len]))
            .collect()
    }

    #[test]
    fn positive_listpack_size_uses_element_count_limit() {
        let under_limit = list_with_lengths(&[1, 1]);
        let at_limit = list_with_lengths(&[1, 1, 1]);
        let over_limit = list_with_lengths(&[1, 1, 1, 1]);

        assert_eq!(infer_list_encoding(&under_limit, 3), "listpack");
        assert_eq!(infer_list_encoding(&at_limit, 3), "listpack");
        assert_eq!(infer_list_encoding(&over_limit, 3), "quicklist");
    }

    #[test]
    fn negative_and_zero_listpack_size_follow_c_quicklist_limits() {
        assert_eq!(
            infer_list_encoding(&list_with_lengths(&[4085]), -1),
            "listpack"
        );
        assert_eq!(
            infer_list_encoding(&list_with_lengths(&[4086]), -1),
            "quicklist"
        );
        assert_eq!(infer_list_encoding(&list_with_lengths(&[1]), 0), "listpack");
        assert_eq!(
            infer_list_encoding(&list_with_lengths(&[1, 1]), 0),
            "quicklist"
        );
        assert_eq!(
            infer_list_encoding(&list_with_lengths(&[60000]), -6),
            "listpack"
        );
        assert_eq!(
            infer_list_encoding(&list_with_lengths(&[60000, 5537]), -6),
            "quicklist"
        );

        let many_small =
            std::iter::repeat_n(Bytes::from_static(b"x"), 501).collect::<VecDeque<_>>();
        assert_eq!(infer_list_encoding(&many_small, -5), "listpack");
    }

    #[test]
    fn growing_quicklist_keeps_encoding_until_shrink_threshold() {
        let mut list = ListValue::from_vecdeque(list_with_lengths(&[3000, 2000]));
        list.update_encoding_after_grow(-1);
        assert_eq!(list.encoding_name(-1), "quicklist");

        list.inner.push_back(Bytes::from_static(b"x"));
        list.update_encoding_after_grow(-1);
        assert_eq!(list.encoding_name(-1), "quicklist");

        list.note_encoding_before_shrink(-1);
        list.inner.pop_front();
        list.update_encoding_after_shrink(-1);
        assert_eq!(list.encoding_name(-1), "listpack");
    }

    #[test]
    fn shrink_hysteresis_uses_pre_shrink_quicklist_state_after_reload() {
        let mut list = ListValue::from_vecdeque(VecDeque::from([
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"d"),
        ]));

        assert_eq!(list.encoding_name(3), "quicklist");
        list.note_encoding_before_shrink(3);
        list.inner.pop_back();
        list.update_encoding_after_shrink(3);

        assert_eq!(list.encoding_name(3), "quicklist");
    }
}
