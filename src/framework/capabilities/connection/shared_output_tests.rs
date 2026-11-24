use super::*;

#[test]
fn shared_rdb_output_is_ordinary_and_has_per_connection_logical_memory() {
    let mut data = Vec::with_capacity(64 * 1024);
    data.resize(32 * 1024, b'r');
    let allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let block = SharedOutputBlock::from(data);
    let mut first = Connection::new_fake();
    let mut second = Connection::new_fake();
    first.queue_shared_reply(block.clone());
    second.queue_shared_reply(block);
    let logical_memory = allocation
        + std::mem::size_of::<OutputFrame>()
        + std::mem::size_of::<OutputFrameAccounting>();

    assert_eq!(
        first.output.queue[0].as_ptr(),
        second.output.queue[0].as_ptr()
    );
    assert_eq!(first.pending_reply_bytes(), 32 * 1024);
    assert_eq!(second.pending_reply_bytes(), 32 * 1024);
    assert_eq!(first.pending_output_memory_usage(), logical_memory);
    assert_eq!(second.pending_output_memory_usage(), logical_memory);
    assert!(first.output_buffer_allocation_bytes() < allocation);
    assert_eq!(first.replication_output_bytes(), 0);
    assert_eq!(first.reply_bytes_current_command(), 0);
    first.output.queue.front_mut().unwrap().advance(17);
    assert_eq!(first.consume_written_bytes(17, 32 * 1024), Ok(0));
    assert_eq!(first.pending_output_memory_usage(), logical_memory);
    assert_eq!(second.output.queue[0].len(), 32 * 1024);
    first.clear_output_queue();
    assert_eq!(first.pending_reply_bytes(), 0);
    assert_eq!(first.pending_output_memory_usage(), 0);
    assert_eq!(second.pending_reply_bytes(), 32 * 1024);
}

#[test]
fn shared_snapshot_restores_partial_prefix_before_later_owned_output() {
    let mut conn = Connection::new_fake();
    conn.queue_shared_reply(SharedOutputBlock::from(b"rdb-body".to_vec()));
    let snapshot = conn.take_threaded_write_snapshot();
    let mut frames = snapshot.into_frames();
    conn.queue_reply(b"later".to_vec());
    frames.front_mut().unwrap().advance(3);
    assert_eq!(conn.consume_written_bytes(3, 8), Ok(0));
    conn.restore_threaded_write_snapshot(frames);
    assert_eq!(conn.output.queue[0].as_slice(), b"-body");
    assert_eq!(conn.output.queue[1].as_slice(), b"later");
    assert_eq!(conn.pending_reply_bytes(), 10);
    assert_eq!(conn.replication_output_bytes(), 0);
}

#[test]
fn shared_rdb_prefix_does_not_report_command_replication_progress() {
    let mut conn = Connection::new_fake();
    conn.queue_shared_reply(SharedOutputBlock::from(b"rdb".to_vec()));
    conn.queue_replication_reply(b"tail".to_vec());
    assert_eq!(conn.consume_written_bytes(2, 7), Ok(0));
    assert_eq!(conn.consume_written_bytes(3, 5), Ok(2));
    assert_eq!(conn.replication_output_bytes(), 2);
    assert_eq!(conn.consume_written_bytes(2, 2), Ok(2));
    assert_eq!(conn.pending_reply_bytes(), 0);
}

#[test]
fn shared_snapshot_merge_counts_only_the_surviving_queue_container() {
    let mut conn = Connection::new_fake();
    conn.queue_shared_reply(SharedOutputBlock::from(vec![b'r'; 32 * 1024]));
    let snapshot = conn.take_threaded_write_snapshot();
    conn.queue_reply(b"later".to_vec());
    conn.restore_threaded_write_snapshot(snapshot.into_frames());
    let owned_payload = conn
        .output
        .queue
        .iter()
        .map(OutputFrame::owned_allocation_bytes)
        .sum::<usize>();
    let expected = owned_payload
        + conn.output.queue.capacity() * std::mem::size_of::<OutputFrame>()
        + conn.output.frame_accounting.capacity() * std::mem::size_of::<OutputFrameAccounting>();
    assert_eq!(conn.output_buffer_allocation_bytes(), expected);
}
