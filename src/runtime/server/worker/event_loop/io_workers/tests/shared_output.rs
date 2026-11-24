use super::*;
use crate::framework::capabilities::connection::{Connection, SharedOutputBlock};

#[test]
fn shared_worker_write_retains_original_capacity_across_a_budget_split() {
    let mut data = Vec::with_capacity(64 * 1024);
    data.resize(32 * 1024, b'r');
    let block = SharedOutputBlock::from(data);
    let peer_frame = OutputFrame::Shared(block.clone());
    let pointer = peer_frame.as_ptr();
    let mut frames = VecDeque::from([OutputFrame::Shared(block)]);
    let allocation = frames[0].logical_allocation_bytes();
    let (written, released, outcome) = write_with_budget(&mut Vec::new(), &mut frames, 17);

    assert_eq!(written, 17);
    assert_eq!(released, 0);
    assert!(matches!(outcome, WriteOutcome::BudgetExhausted));
    assert_eq!(frames[0].as_ptr(), pointer.wrapping_add(17));
    assert_eq!(frames[0].logical_allocation_bytes(), allocation);
    assert_eq!(peer_frame.len(), 32 * 1024);
    let (written, released, outcome) = write_with_budget(&mut Vec::new(), &mut frames, usize::MAX);
    assert_eq!(written, 32 * 1024 - 17);
    assert_eq!(released, 0);
    assert!(matches!(outcome, WriteOutcome::Complete));
    assert!(frames.is_empty());
    assert_eq!(peer_frame.as_ptr(), pointer);
}

#[test]
fn shared_worker_completion_restores_cancelled_output_before_later_replies() {
    let mut io = IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
    let token = Token(8);
    let (read, _read_peer) = thread_handle_pair();
    let (write, _write_peer) = thread_handle_pair();
    let generation = io.register_connection(token, read, write);
    let id = IoOperationId {
        token,
        generation,
        sequence: 1,
    };
    let mut connection = Connection::new_fake();
    connection.queue_shared_reply(SharedOutputBlock::from(b"rdb-body".to_vec()));
    let snapshot = connection.take_threaded_write_snapshot();
    connection.queue_reply(b"later".to_vec());
    let state = io.connections.get_mut(&token).unwrap();
    state.write_inflight = Some(id);
    let handle = state.write_handle.take().unwrap();
    let action = io.complete_write(WriteCompletion {
        id,
        handle,
        frames: snapshot.into_frames(),
        written: 0,
        released_allocation_bytes: 0,
        outcome: WriteOutcome::Backpressured,
        cancelled: true,
    });
    let WriteCompletionAction::Apply {
        frames,
        written,
        released_allocation_bytes,
        ..
    } = action
    else {
        panic!("cancelled output must return its snapshot");
    };
    assert_eq!(written, 0);
    assert_eq!(released_allocation_bytes, 0);
    connection.restore_threaded_write_snapshot(frames);
    assert_eq!(connection.output.queue[0].as_slice(), b"rdb-body");
    assert_eq!(connection.output.queue[1].as_slice(), b"later");
    assert!(!io.is_write_inflight(token));
}

#[test]
fn shared_stale_and_disconnected_completions_do_not_touch_other_replicas() {
    for should_close in [false, true] {
        let mut io =
            IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
        let token = Token(8);
        let (read, _read_peer) = thread_handle_pair();
        let (write, _write_peer) = thread_handle_pair();
        let generation = io.register_connection(token, read, write);
        let id = IoOperationId {
            token,
            generation,
            sequence: 1,
        };
        let block = SharedOutputBlock::from(b"rdb-body".to_vec());
        let peer_frame = OutputFrame::Shared(block.clone());
        let state = io.connections.get_mut(&token).unwrap();
        state.write_inflight = Some(id);
        let handle = state.write_handle.take().unwrap();
        if should_close {
            assert!(io.begin_close(token));
        } else {
            let (read, _peer) = thread_handle_pair();
            let (write, _peer) = thread_handle_pair();
            let new_generation = io.register_connection(token, read, write);
            assert_ne!(generation, new_generation);
        }
        let action = io.complete_write(WriteCompletion {
            id,
            handle,
            frames: VecDeque::from([OutputFrame::Shared(block)]),
            written: 0,
            released_allocation_bytes: 0,
            outcome: WriteOutcome::Backpressured,
            cancelled: false,
        });
        if should_close {
            assert!(
                matches!(action, WriteCompletionAction::CloseTransport(closed) if closed == token)
            );
            assert!(!io.has_connection(token));
        } else {
            assert!(matches!(action, WriteCompletionAction::Stale));
            assert!(io.has_connection(token));
            assert!(!io.is_write_inflight(token));
        }
        assert_eq!(peer_frame.as_slice(), b"rdb-body");
    }
}
