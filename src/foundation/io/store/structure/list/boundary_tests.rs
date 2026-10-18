use super::would_shrink_fit_limit;
use bytes::Bytes;
use std::collections::VecDeque;

#[test]
fn shrink_converts_at_half_of_count_limit() {
    let list = VecDeque::from([Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    assert!(would_shrink_fit_limit(&list, 4));
}
