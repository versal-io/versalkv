use super::*;
use crate::framework::capabilities::connection::SharedOutputBlock;
use std::io::{IoSlice, Write};

struct PrefixWriter(usize);

impl Write for PrefixWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        Ok(data.len().min(self.0))
    }

    fn write_vectored(&mut self, data: &[IoSlice<'_>]) -> std::io::Result<usize> {
        Ok(data
            .iter()
            .map(|slice| slice.len())
            .sum::<usize>()
            .min(self.0))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn shared_partial_write_advances_the_view_without_releasing_capacity() {
    let block = SharedOutputBlock::from(b"rdb-body".to_vec());
    let mut first = Connection::new_fake();
    let mut second = Connection::new_fake();
    first.queue_shared_reply(block.clone());
    second.queue_shared_reply(block);
    let pointer = first.output.queue[0].as_ptr();
    let mut queue = std::mem::take(&mut first.output.queue);
    let mut pool = Vec::new();
    let (outcome, released) =
        flush_queue_vectored(&mut PrefixWriter(3), &mut queue, None, Some(&mut pool));

    assert!(matches!(outcome, WriteOutcome::Backpressured));
    assert_eq!(released, 0);
    assert!(pool.is_empty());
    assert_eq!(queue[0].as_slice(), b"-body");
    assert_eq!(queue[0].as_ptr(), pointer.wrapping_add(3));
    assert_eq!(second.output.queue[0].as_slice(), b"rdb-body");
    assert_eq!(second.output.queue[0].as_ptr(), pointer);
    assert_eq!(first.consume_written_bytes(3, 8), Ok(0));
    first.restore_threaded_write_snapshot(queue);
    assert_eq!(first.pending_reply_bytes(), 5);
}

#[test]
fn mixed_shared_and_owned_output_recycles_only_owned_buffers() {
    let data = b"+OK\r\n".to_vec();
    let pointer = data.as_ptr();
    let allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let mut queue = VecDeque::from([
        OutputFrame::from(data),
        OutputFrame::Shared(SharedOutputBlock::from(b"rdb-body".to_vec())),
    ]);
    let mut pool = Vec::new();
    let mut writer = Vec::new();
    let (outcome, released) = flush_queue_vectored(&mut writer, &mut queue, None, Some(&mut pool));

    assert!(matches!(outcome, WriteOutcome::Complete));
    assert_eq!(writer, b"+OK\r\nrdb-body");
    assert_eq!(released, allocation);
    assert_eq!(pool.len(), 1);
    assert!(pool[0].is_empty());
    assert_eq!(pool[0].as_ptr(), pointer);
}

#[test]
fn shared_output_write_zero_keeps_the_backpressure_gate_closed() {
    let mut queue = VecDeque::from([OutputFrame::Shared(SharedOutputBlock::from(
        b"rdb-body".to_vec(),
    ))]);
    let (outcome, released) = flush_queue_vectored(&mut PrefixWriter(0), &mut queue, None, None);
    assert!(matches!(
        outcome,
        WriteOutcome::Failed(error) if error.kind() == std::io::ErrorKind::WriteZero
    ));
    assert_eq!(released, 0);
    assert_eq!(queue[0].as_slice(), b"rdb-body");
}
