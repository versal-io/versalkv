use super::*;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct TrackedOwner {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for TrackedOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for TrackedOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn shared_output_preserves_capacity_and_moves_only_the_view() {
    let mut data = Vec::with_capacity(64 * 1024);
    data.resize(32 * 1024, b'r');
    let allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let pointer = data.as_ptr();
    let block = SharedOutputBlock::from(data);
    let mut frame = OutputFrame::Shared(block.clone());

    assert_eq!(frame.as_ptr(), pointer);
    assert_eq!(frame.logical_allocation_bytes(), allocation);
    assert_eq!(frame.owned_allocation_bytes(), 0);
    frame.advance(7);
    assert_eq!(frame.as_ptr(), pointer.wrapping_add(7));
    assert_eq!(frame.len(), 32 * 1024 - 7);
    assert_eq!(frame.logical_allocation_bytes(), allocation);
    assert_eq!(block.bytes.as_ptr(), pointer);
    assert_eq!(block.bytes.len(), 32 * 1024);
}

#[test]
fn shared_output_owner_is_released_only_after_the_last_frame() {
    let drops = Arc::new(AtomicUsize::new(0));
    let block = SharedOutputBlock {
        bytes: Bytes::from_owner(TrackedOwner {
            data: b"rdb-body".to_vec(),
            drops: Arc::clone(&drops),
        }),
        allocation_bytes: 8,
    };
    let mut first = OutputFrame::Shared(block.clone());
    let second = OutputFrame::Shared(block.clone());
    drop(block);
    first.advance(3);
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(second.as_slice(), b"rdb-body");
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_output_keeps_its_allocation_for_pool_reuse() {
    let mut data = Vec::with_capacity(4096);
    data.extend_from_slice(b"ordinary");
    let pointer = data.as_ptr();
    let allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let mut frame = OutputFrame::from(data);
    frame.advance(3);

    assert_eq!(frame.as_slice(), b"inary");
    assert_eq!(frame.owned_allocation_bytes(), allocation);
    let data = frame.into_vec();
    assert_eq!(data.as_ptr(), pointer);
    assert!(data.capacity() >= 4096);
}

#[test]
#[cfg(all(not(miri), not(feature = "wasm")))]
fn shared_output_allocator_counts_one_payload_and_zero_clone_allocations() {
    use crate::foundation::memory::util_alloc::{current_thread_allocated, vec_allocation_bytes};

    // Keep every allocation and release on this thread; other tests cannot
    // perturb the allocator delta as they could with total_allocated().
    let baseline = current_thread_allocated();
    let data = vec![b'r'; 16 * 1024];
    let payload_allocation = vec_allocation_bytes(&data);
    assert_eq!(current_thread_allocated() - baseline, payload_allocation);
    let block = SharedOutputBlock::from(data);
    let shared_allocation = current_thread_allocated();
    assert!(shared_allocation - baseline >= payload_allocation);
    let mut frames: [Option<OutputFrame>; 8] =
        std::array::from_fn(|_| Some(OutputFrame::Shared(block.clone())));
    assert_eq!(current_thread_allocated(), shared_allocation);
    drop(block);
    for frame in &mut frames[..7] {
        drop(frame.take());
    }
    assert_eq!(current_thread_allocated(), shared_allocation);
    drop(frames[7].take());
    assert_eq!(current_thread_allocated(), baseline);

    let data = vec![b'r'; 16 * 1024];
    let before_clones = current_thread_allocated();
    let owned_frames: [OutputFrame; 8] = std::array::from_fn(|_| OutputFrame::from(data.clone()));
    assert_eq!(
        current_thread_allocated() - before_clones,
        8 * payload_allocation
    );
    drop(owned_frames);
    drop(data);
    assert_eq!(current_thread_allocated(), baseline);
}
