use super::*;

#[test]
fn replicated_transactions_finish_tracking_imports_before_later_increments() {
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::engine::distributed::replication::ReplicationRole;

    const JOB: &[u8] = b"dddddddddddddddddddddddddddddddddddddddd";
    const SOURCE: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const KEY: &[u8] = b"{atomic}list";
    for transaction_count in 1..=2 {
        let mut backend =
            ServerWorkerState::new_with_slot_router(2, 14, |key| key_hash_slot(key) as usize)
                .unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        *backend.ctx.cluster_state_mut() =
            Some(Box::new(ClusterState::new("127.0.0.1", 6380, 16_380)));
        backend.ctx.replication_mut().role = ReplicationRole::Replica;
        backend.ctx.replication_mut().backlog.activate();
        let start = backend.ctx.replication().backlog.next_stream_cursor();
        let (token, mut primary) = add_runtime_connection(&mut backend);
        backend
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(token),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
                backend.ctx.replication().backlog.replid().to_string(),
                offset(0),
            ));
        let slot = key_hash_slot(KEY);
        assert_ne!(slot, key_hash_slot(b"{serving}one"));
        let slot_text = slot.to_string();
        let initial = [
            protocol::encode_command(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"ESTABLISH",
                b"SOURCE",
                SOURCE,
                b"NAME",
                JOB,
                b"SLOTSRANGE",
                slot_text.as_bytes(),
                slot_text.as_bytes(),
            ]),
            protocol::encode_command(&[b"SELECT", b"1"]),
            protocol::encode_command(&[b"RPUSH", KEY, b"seed"]),
        ]
        .concat();
        primary.write_all(&initial).unwrap();
        backend.transport.wait_readable_for_test(Token(token));
        backend.recv_replication_stream(token, token);
        assert_eq!(backend.ctx.slot_import_key_count(1, slot as usize), 1);
        assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 0);
        let mut stream = initial;

        for index in 0..transaction_count {
            let value = index.to_string();
            let transaction = [
                protocol::encode_command(&[b"MULTI"]),
                protocol::encode_command(&[b"SELECT", b"0"]),
                protocol::encode_command(&[b"SET", b"{serving}one", value.as_bytes()]),
                protocol::encode_command(&[b"SET", b"{serving}two", value.as_bytes()]),
                protocol::encode_command(&[b"EXEC"]),
                protocol::encode_command(&[b"SELECT", b"1"]),
                protocol::encode_command(&[b"RPUSH", KEY, value.as_bytes()]),
            ]
            .concat();
            primary.write_all(&transaction).unwrap();
            backend.transport.wait_readable_for_test(Token(token));
            backend.recv_replication_stream(token, token);
            assert!(
                backend.replication_runtime.replica_link().is_some(),
                "a later MULTI must not encounter an unfinished hidden import transaction"
            );
            assert_eq!(backend.ctx.db(0).unwrap().dbsize(), 2);
            assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 0);
            stream.extend_from_slice(&transaction);
        }
        let finish = [
            protocol::encode_command(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"FINISH",
                b"STATE",
                b"SUCCESS",
                b"NAME",
                JOB,
            ]),
            protocol::encode_command(&[b"SELECT", b"0"]),
            protocol::encode_command(&[b"SET", b"{serving}after", b"done"]),
        ]
        .concat();
        primary.write_all(&finish).unwrap();
        backend.transport.wait_readable_for_test(Token(token));
        backend.recv_replication_stream(token, token);
        stream.extend_from_slice(&finish);
        assert_eq!(backend.ctx.slot_import_key_count(1, slot as usize), 0);
        assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 1);
        assert_eq!(backend.ctx.db(0).unwrap().dbsize(), 3);
        let list = backend.ctx.db(1).unwrap().get_no_expire(KEY).unwrap();
        assert_eq!(
            list.list_len(),
            1 + transaction_count,
            "increments received after EXEC must be committed exactly once at FINISH"
        );
        assert_eq!(list.list_get(0).unwrap(), b"seed");
        for index in 0..transaction_count {
            assert_eq!(
                list.list_get((index + 1) as isize).unwrap(),
                index.to_string().as_bytes()
            );
        }
        assert_eq!(
            backend.ctx.replication().backlog.read_from(start),
            Some(stream)
        );
    }
}
