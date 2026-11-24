use super::*;
use crate::foundation::memory::shared_output::{allocation_bytes, isolate_for_test};
use crate::framework::capabilities::connection::SharedOutputBlock;
use crate::framework::distributed::replication::ReplicaConnectionInfo;
use std::sync::mpsc;

fn add_primary(backend: &mut ServerWorkerState, token: usize) {
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token));
    backend.connection_registry.insert(connection);
    backend.replication_runtime.register_primary(
        ConnectionId(token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: RdbFilterMode::All,
        },
    );
}

fn owned_output_max(backend: &ServerWorkerState) -> usize {
    backend
        .replication_runtime
        .primary_links()
        .values()
        .filter_map(|link| {
            backend
                .connection_registry
                .get(Token(link.connection_id().0))
        })
        .map(|connection| connection.output_buffer_allocation_bytes())
        .max()
        .unwrap_or(0)
}

fn ordinary_replication_exclusion(backend: &ServerWorkerState) -> usize {
    backend
        .ctx
        .replication()
        .backlog
        .allocation_bytes()
        .saturating_add(owned_output_max(backend))
        .saturating_sub(backend.ctx.config().server.repl_backlog_size.max(0) as usize)
}

#[test]
fn actual_maxmemory_projection_excludes_one_shared_payload_and_preserves_owned_max() {
    let _scope = isolate_for_test();
    let mut backend = ServerWorkerState::new(1).unwrap();
    let backlog_allocation = backend.ctx.replication().backlog.allocation_bytes();
    backend.ctx.config_mut().server.repl_backlog_size = (backlog_allocation + 1024 * 1024) as i64;
    backend.ctx.config_mut().server.maxmemory = 1;
    let tokens = [9101, 9102, 9103];
    for token in tokens {
        add_primary(&mut backend, token);
        backend.connection_registry[Token(token)].queue_reply(vec![b'o'; token - 9000]);
    }
    backend.refresh_replica_output_buffer_bytes();
    let before = backend.ctx.maxmemory_state();
    assert_eq!(before.not_counted, 0);

    let mut data = Vec::with_capacity(64 * 1024);
    data.resize(32 * 1024, b'r');
    let payload = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let block = SharedOutputBlock::from(data);
    for token in tokens {
        backend.connection_registry[Token(token)].queue_shared_reply(block.clone());
    }
    drop(block);
    backend.refresh_replica_output_buffer_bytes();

    let owned_max = owned_output_max(&backend);
    let owned_sum: usize = tokens
        .iter()
        .map(|token| backend.connection_registry[Token(*token)].output_buffer_allocation_bytes())
        .sum();
    assert!(owned_sum > owned_max);
    assert_eq!(
        backend
            .ctx
            .replication()
            .replica_output_buffer_allocation_bytes,
        owned_max
    );
    assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 0);
    assert_eq!(ordinary_replication_exclusion(&backend), 0);
    assert_eq!(allocation_bytes(), payload);
    let state = backend.ctx.maxmemory_state();
    assert_eq!(state.not_counted, before.not_counted + payload);
    assert_eq!(state.logical, state.reported.saturating_sub(payload));

    for token in &tokens[..2] {
        backend.connection_registry[Token(*token)].clear_output_queue();
        backend.refresh_replica_output_buffer_bytes();
        assert_eq!(allocation_bytes(), payload);
        assert_eq!(backend.ctx.maxmemory_state().not_counted, payload);
    }
    backend.connection_registry[Token(tokens[2])].clear_output_queue();
    assert_eq!(allocation_bytes(), 0);
    // Shared accounting is live, not a cached replication-view snapshot.
    assert_eq!(backend.ctx.maxmemory_state().not_counted, 0);
}

#[test]
fn actual_maxmemory_projection_counts_an_io_thread_only_holder_and_releases_it_live() {
    let _scope = isolate_for_test();
    let mut backend = ServerWorkerState::new(1).unwrap();
    let backlog_allocation = backend.ctx.replication().backlog.allocation_bytes();
    backend.ctx.config_mut().server.repl_backlog_size = (backlog_allocation + 1024 * 1024) as i64;
    add_primary(&mut backend, 9201);
    backend.refresh_replica_output_buffer_bytes();
    let before = backend.ctx.maxmemory_state().not_counted;
    assert_eq!(before, ordinary_replication_exclusion(&backend));
    assert_eq!(before, 0);
    let data = vec![b'r'; 32 * 1024];
    let payload = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let block = SharedOutputBlock::from(data);
    backend.connection_registry[Token(9201)].queue_shared_reply(block);
    let snapshot = backend.connection_registry[Token(9201)].take_threaded_write_snapshot();
    let mut frames = snapshot.into_frames();
    assert!(backend.connection_registry[Token(9201)]
        .output
        .queue
        .is_empty());
    backend.refresh_replica_output_buffer_bytes();

    let (holding_tx, holding_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("rdb-output-accounting-io".to_string())
        .spawn(move || {
            frames.front_mut().unwrap().advance(17);
            holding_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(frames);
        })
        .unwrap();
    holding_rx.recv().unwrap();
    assert_eq!(ordinary_replication_exclusion(&backend), 0);
    assert_eq!(allocation_bytes(), payload);
    assert_eq!(backend.ctx.maxmemory_state().not_counted, before + payload);
    release_tx.send(()).unwrap();
    worker.join().unwrap();

    assert_eq!(allocation_bytes(), 0);
    assert_eq!(backend.ctx.maxmemory_state().not_counted, before);
}

#[test]
fn shared_payload_exclusion_is_separate_from_the_existing_backlog_allowance() {
    let _scope = isolate_for_test();
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().server.repl_backlog_size = 1024;
    backend.ctx.replication_mut().backlog.set_capacity(8192);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'b'; 4096])
        .unwrap();
    add_primary(&mut backend, 9301);
    backend.connection_registry[Token(9301)].queue_replication_reply(vec![b'o'; 2048]);
    backend.refresh_replica_output_buffer_bytes();
    let before = backend.ctx.maxmemory_state().not_counted;
    assert_eq!(before, ordinary_replication_exclusion(&backend));
    assert!(before > 0);

    let data = vec![b'r'; 32 * 1024];
    let payload = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
    let block = SharedOutputBlock::from(data);
    backend.connection_registry[Token(9301)].queue_shared_reply(block);
    backend.refresh_replica_output_buffer_bytes();
    assert_eq!(backend.ctx.maxmemory_state().not_counted, before + payload);
    assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 4096);
    backend.connection_registry[Token(9301)].clear_output_queue();
    backend.refresh_replica_output_buffer_bytes();
    assert_eq!(allocation_bytes(), 0);
    assert_eq!(
        backend.ctx.maxmemory_state().not_counted,
        ordinary_replication_exclusion(&backend)
    );
}
