use super::*;
use crate::foundation::memory::shared_output::{allocation_bytes, isolate_for_test};

#[test]
fn shared_owner_counts_capacity_once_until_the_last_bytes_view() {
    let _scope = isolate_for_test();
    let mut data = Vec::with_capacity(64 * 1024);
    data.resize(32 * 1024, b'r');
    let payload = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let pointer = data.as_ptr();
    let block = SharedOutputBlock::from(data);
    let raw_view = block.bytes.clone();
    let mut frames: [Option<OutputFrame>; 8] =
        std::array::from_fn(|_| Some(OutputFrame::Shared(block.clone())));

    assert_eq!(allocation_bytes(), payload);
    assert_eq!(block.bytes.as_ptr(), pointer);
    drop(block);
    for frame in &mut frames {
        let mut frame = frame.take().unwrap();
        frame.advance(17);
        assert_eq!(frame.logical_allocation_bytes(), payload);
        assert_eq!(frame.owned_allocation_bytes(), 0);
        assert_eq!(allocation_bytes(), payload);
        drop(frame);
        assert_eq!(allocation_bytes(), payload);
    }
    assert_eq!(raw_view.as_ptr(), pointer);
    drop(raw_view);
    assert_eq!(allocation_bytes(), 0);
}

#[test]
fn independent_shared_owners_are_added_and_removed_separately() {
    let _scope = isolate_for_test();
    let first = SharedOutputBlock::from(vec![b'a'; 4096]);
    let second = SharedOutputBlock::from(vec![b'b'; 8192]);
    assert_eq!(allocation_bytes(), 4096 + 8192);
    drop(first);
    assert_eq!(allocation_bytes(), 8192);
    let copied = OutputFrame::Shared(second).into_vec();
    assert_eq!(copied, vec![b'b'; 8192]);
    assert_eq!(allocation_bytes(), 0);
}

#[test]
fn empty_shared_owner_retains_its_allocated_capacity() {
    let _scope = isolate_for_test();
    let block = SharedOutputBlock::from(Vec::with_capacity(4096));
    assert!(block.bytes.is_empty());
    assert_eq!(allocation_bytes(), 4096);
    drop(block);
    assert_eq!(allocation_bytes(), 0);

    let block = SharedOutputBlock::from(Vec::new());
    assert_eq!(allocation_bytes(), 0);
    drop(block);
    assert_eq!(allocation_bytes(), 0);
}

#[test]
#[cfg(all(not(miri), not(feature = "wasm")))]
fn shared_owner_metadata_stays_reported_without_becoming_a_payload_exclusion() {
    use crate::foundation::memory::util_alloc::current_thread_allocated;

    let _scope = isolate_for_test();
    let before = current_thread_allocated();
    let block = SharedOutputBlock::from(vec![b'r'; 16 * 1024]);
    let reported = current_thread_allocated() - before;
    let payload = block.allocation_bytes;

    // Bytes owns one metadata allocation in addition to the Vec backing store.
    // Only the payload is excluded; no private Bytes layout is assumed.
    assert!(reported > payload);
    assert_eq!(allocation_bytes(), payload);
    let frames: [OutputFrame; 8] = std::array::from_fn(|_| OutputFrame::Shared(block.clone()));
    assert_eq!(current_thread_allocated() - before, reported);
    drop(block);
    drop(frames);
    assert_eq!(allocation_bytes(), 0);
    assert_eq!(current_thread_allocated(), before);
}
