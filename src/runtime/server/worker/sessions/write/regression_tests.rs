use super::*;
use std::io::Read;
use std::os::fd::AsRawFd;

#[test]
fn completed_write_releases_the_recorded_allocator_usable_size() {
    let mut connection = Connection::new_fake();
    let mut frame = Vec::with_capacity(144);
    frame.resize(144, b'x');
    let frame_allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&frame);
    connection.queue_replication_reply(frame);
    let retained_container_allocation = connection
        .output_buffer_allocation_bytes()
        .saturating_sub(frame_allocation);
    let mut queue = std::mem::take(&mut connection.output.queue);
    let mut writer = Vec::new();

    let (_, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);
    connection.consume_written_bytes(144, 144).unwrap();
    connection.release_output_frame_allocations(released_allocation_bytes);

    assert_eq!(
        connection.output_buffer_allocation_bytes(),
        retained_container_allocation
    );
}

#[test]
fn partial_dual_channel_sync_flush_does_not_start_rdb_wait_deadline() {
    let (mut backend, replica_stream, token_value, replica_id) =
        super::tests::connected_primary_link();
    let token = Token(token_value);
    let writer_fd = backend
        .transport
        .raw_fd(token)
        .expect("replica transport fd");
    // Buffers must stay well above the loopback GSO segment size (up to 64 KiB
    // on Linux). A queued segment larger than the peer's advertised window is
    // never split by the kernel, so 8 KiB buffers against a 15 KiB window stall
    // the connection permanently instead of producing backpressure.
    const TEST_SEND_BUFFER: i32 = 512 * 1024;
    const TEST_RECEIVE_BUFFER: i32 = 512 * 1024;
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        writer_fd,
        crate::foundation::io::sys::socket::SocketBuffer::Send,
        TEST_SEND_BUFFER,
    )
    .expect("set replica send buffer");
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        replica_stream.as_raw_fd(),
        crate::foundation::io::sys::socket::SocketBuffer::Receive,
        TEST_RECEIVE_BUFFER,
    )
    .expect("set replica receive buffer");
    // Replica writes are unbudgeted, so exceed both socket buffers while the
    // peer stays unread rather than relying on a transient WouldBlock result.
    let padding = vec![0x5a; 8 * 1024 * 1024];
    let reply = crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY;
    backend.connection_registry[token].queue_reply(padding.clone());
    backend.connection_registry[token].queue_reply(reply.to_vec());

    backend.handle_writable(token);

    assert!(backend.connection_registry[token].has_pending_output());
    assert_eq!(
        backend.connection_registry[token]
            .output
            .queue
            .back()
            .map(Vec::as_slice),
        Some(reply),
        "the terminal dual-channel reply must remain intact behind pending padding"
    );
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .expect("registered replica link")
            .rdb_channel_wait_deadline_ms,
        None,
        "the RDB-channel wait deadline must not start before the reply fully drains"
    );

    let mut replica_stream = replica_stream;
    replica_stream
        .set_nonblocking(true)
        .expect("make replica reads nonblocking for the drain barrier");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut received = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    while backend.connection_registry[token].has_pending_output()
        || received.len() < padding.len() + reply.len()
    {
        match replica_stream.read(&mut buffer) {
            Ok(0) => panic!("replica closed before receiving the complete dual-channel reply"),
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read replica dual-channel reply: {error}"),
        }
        backend.handle_writable(token);
        assert!(
            std::time::Instant::now() < deadline,
            "replica output did not drain before the deadline"
        );
        std::thread::yield_now();
    }
    assert_eq!(&received[..padding.len()], padding);
    assert_eq!(&received[padding.len()..], reply);
    assert!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .expect("registered replica link")
            .rdb_channel_wait_deadline_ms
            .is_some(),
        "the RDB-channel wait deadline must start once the unchanged reply fully drains"
    );
}
