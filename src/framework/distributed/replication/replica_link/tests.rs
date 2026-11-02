use super::{ProvisionalPrimary, ReplicaLink};
use crate::engine::distributed::replication::protocol;
use crate::engine::distributed::replication::ReplicationOffset;
use crate::framework::distributed::replication::{
    ConnectionId, LinkError, MasterInfo, ReplicaAction, ReplicaEvent, ReplicaLinkState,
    ReplicaRdbChannelState, ReplicationApply,
};

fn link() -> ReplicaLink {
    ReplicaLink::new(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    )
}

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("test offset must be non-negative")
}

fn boundary_waiting_endoff_link() -> ReplicaLink {
    let mut link = link();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    let (events, actions) = link.on_rdb_bytes(b"+OK\r\n").unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::WaitingForEndOffset
    );
    link
}

#[test]
fn boundary_endoff_unselected_database_sends_dual_psync_and_selects_later() {
    let replid = "0123456789abcdef0123456789abcdef01234567";
    for db in ["-2", "-2147483648", "4294967296"] {
        let mut link = boundary_waiting_endoff_link();
        assert!(matches!(
            link.on_rdb_bytes(format!("$ENDOFF:123 {replid} {db} 91\r\n").as_bytes()),
            Err(LinkError::Protocol)
        ));
        assert_eq!(
            link.rdb_channel_state(),
            ReplicaRdbChannelState::WaitingForEndOffset
        );
        assert!(link.provisional_primary().is_none());
        assert_eq!(link.received_position(), offset(0));
        assert_eq!(link.applied_position(), offset(0));
    }
    for cached_offset in [0, 42] {
        let mut link = boundary_waiting_endoff_link();
        if cached_offset != 0 {
            link.set_resume_position("previous".to_string(), offset(cached_offset), 9);
        }
        let frame = format!("$ENDOFF:123 {replid} -1 91\r\n");
        let (events, actions) = link
            .on_rdb_bytes(frame.as_bytes())
            .expect("C's unselected database is a valid snapshot boundary");
        assert!(events.is_empty());
        assert_eq!(
            actions,
            vec![ReplicaAction::Send(
                protocol::build_replconf_set_rdb_client_id(91)
            )]
        );
        assert_eq!(link.state(), ReplicaLinkState::WaitDualRdbClientIdReply);
        assert_eq!(
            link.rdb_channel_state(),
            ReplicaRdbChannelState::ReceivingRdb
        );
        assert_eq!(link.provisional_primary().unwrap().db_id, 0);
        assert_eq!(link.provisional_primary().unwrap().end_offset, offset(123));
        assert_eq!(link.selected_db(), 0);
        assert_eq!(
            (link.received_position(), link.decoded_offset),
            (offset(123), offset(123))
        );
        assert_eq!(
            (link.applied_position(), link.acknowledged_position()),
            (offset(cached_offset), offset(cached_offset))
        );

        let (events, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
        assert!(events.is_empty());
        assert_eq!(
            actions,
            vec![ReplicaAction::SendDualPsync(protocol::build_psync(
                replid, 124
            ))]
        );
        let select = protocol::encode_command(&[b"SELECT", b"2"]);
        let set = protocol::encode_command(&[b"SET", b"key", b"value"]);
        let mut stream = b"+CONTINUE\r\n".to_vec();
        stream.extend_from_slice(&select);
        stream.extend_from_slice(&set);
        let (events, actions) = link.on_bytes(&stream, 6380).unwrap();
        assert!(actions.is_empty());
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            ReplicaEvent::Apply {
                apply: ReplicationApply::Select(2),
                ..
            }
        ));
        assert!(matches!(
            events[1],
            ReplicaEvent::Apply {
                apply: ReplicationApply::Command(_),
                ..
            }
        ));
        // The decoded SELECT waits for application; decoding does not change the selected DB.
        assert_eq!(link.selected_db(), 0);
        let end = offset(123 + select.len() as i64 + set.len() as i64);
        assert_eq!((link.received_position(), link.decoded_offset), (end, end));
        assert_eq!(
            (link.applied_position(), link.acknowledged_position()),
            (offset(cached_offset), offset(cached_offset))
        );
        assert_eq!(link.prevalidate_rdb_apply_position(), Ok(offset(123)));
    }
}

#[test]
fn boundary_endoff_keepalives_are_consumed_across_text_fragments_once() {
    let cases: &[(&str, &[&[u8]])] = &[
        ("LF", &[b"\n"]),
        ("CRLF", &[b"\r\n"]),
        ("repeated", &[b"\n\r\n\n"]),
        ("split CR", &[b"\r", b"\n"]),
        ("mixed fragments", &[b"\n\r", b"\n\n"]),
        ("coalesced", &[b"\n\r\n"]),
    ];
    let endoff = protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        91,
    );
    let mut results = Vec::new();
    for (label, chunks) in cases {
        let mut link = boundary_waiting_endoff_link();
        let mut actions = Vec::new();
        let mut error = None;
        for (index, chunk) in chunks.iter().enumerate() {
            let mut input = chunk.to_vec();
            if *label == "coalesced" {
                input.extend_from_slice(&endoff);
            }
            match link.on_rdb_bytes(&input) {
                Ok((events, emitted)) => {
                    assert!(events.is_empty(), "{label}/{index}");
                    actions.extend(emitted);
                    if input == b"\r" {
                        assert_eq!(link.rdb_input, b"\r", "fragmented CR must remain buffered");
                    }
                }
                Err(cause) => {
                    error = Some(cause);
                    break;
                }
            }
        }
        if error.is_none() && *label != "coalesced" {
            match link.on_rdb_bytes(&endoff) {
                Ok((events, emitted)) => {
                    assert!(events.is_empty());
                    actions.extend(emitted);
                }
                Err(cause) => error = Some(cause),
            }
        }
        if error.is_none() {
            let (events, actions) = link.on_rdb_bytes(b"$6\r\n\nx\r\ny\n").unwrap();
            assert!(events.is_empty());
            assert!(actions.is_empty());
            assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"\nx\r\ny\n");
        }
        results.push((*label, error, actions, link.rdb_channel_state()));
    }
    for (label, error, actions, state) in results {
        assert_eq!(
            error, None,
            "complete empty keepalives must be consumed: {label}"
        );
        assert_eq!(
            actions,
            vec![ReplicaAction::Send(
                protocol::build_replconf_set_rdb_client_id(91)
            )],
            "{label}"
        );
        assert_eq!(state, ReplicaRdbChannelState::ReceivingRdb, "{label}");
    }
}

#[test]
fn boundary_endoff_rejects_nonempty_lines_and_preserves_binary_newlines() {
    use crate::foundation::io::persistence::rdb::{
        load_rdb_bytes_checked, serialize_all_with_options, RdbSerializeOptions,
    };
    use crate::foundation::io::store::{kv_engine::DbSet, Robj};

    for input in [
        b"unknown\r\n".as_slice(),
        b"+OK\r\n".as_slice(),
        b"unknown\n$ENDOFF:123 0123456789abcdef0123456789abcdef01234567 0 91\r\n".as_slice(),
    ] {
        let mut link = boundary_waiting_endoff_link();
        assert!(
            matches!(link.on_rdb_bytes(input), Err(LinkError::Protocol)),
            "{input:?}"
        );
        assert_eq!(
            link.rdb_channel_state(),
            ReplicaRdbChannelState::WaitingForEndOffset
        );
        assert!(link.provisional_primary().is_none());
    }
    let mut source = DbSet::new(1);
    let value = b"\nline\r\nend\n";
    source
        .get_mut(0)
        .unwrap()
        .set(b"newlines".to_vec(), Robj::string(value.as_slice()));
    let body = serialize_all_with_options(
        &source,
        0,
        RdbSerializeOptions {
            compression: false,
            checksum: true,
        },
    );
    assert!(body.windows(value.len()).any(|bytes| bytes == value));
    let marker = b"0123456789abcdef0123456789abcdef01234567";
    let mut link = boundary_waiting_endoff_link();
    link.on_rdb_bytes(&protocol::encode_end_offset(
        offset(123),
        std::str::from_utf8(marker).unwrap(),
        0,
        91,
    ))
    .unwrap();
    let mut header = b"$EOF:".to_vec();
    header.extend_from_slice(marker);
    header.extend_from_slice(b"\r\n");
    let (events, actions) = link.on_rdb_bytes(&header).unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    for chunk in body.chunks(3) {
        let (events, actions) = link.on_rdb_bytes(chunk).unwrap();
        assert!(events.is_empty());
        assert!(actions.is_empty());
    }
    let (events, actions) = link.on_rdb_bytes(marker).unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    let received = link.rdb_receive_mut().unwrap().buffered().to_vec();
    assert_eq!(
        received, body,
        "text keepalive rules must not alter any RDB body byte"
    );
    let mut loaded = DbSet::new(1);
    assert_eq!(
        load_rdb_bytes_checked(&received, &mut loaded, true).unwrap(),
        (1, received.len())
    );
    assert_eq!(
        loaded.get_mut(0).unwrap().get_string(b"newlines").unwrap(),
        Some(value.as_slice())
    );
}

#[test]
fn replica_rdb_state_rejects_illegal_jumps() {
    assert_eq!(
        ReplicaRdbChannelState::None.transition(ReplicaRdbChannelState::Connecting),
        Ok(ReplicaRdbChannelState::Connecting)
    );
    assert!(ReplicaRdbChannelState::None
        .transition(ReplicaRdbChannelState::ReceivingRdb)
        .is_err());
    assert!(ReplicaRdbChannelState::Handshake
        .transition(ReplicaRdbChannelState::RdbLoaded)
        .is_err());
    assert_eq!(
        ReplicaRdbChannelState::WaitingForEndOffset
            .transition(ReplicaRdbChannelState::ReceivingRdb),
        Ok(ReplicaRdbChannelState::ReceivingRdb)
    );
    assert_eq!(
        ReplicaRdbChannelState::ReceivingRdb.transition(ReplicaRdbChannelState::RdbLoaded),
        Ok(ReplicaRdbChannelState::RdbLoaded)
    );
}

#[test]
fn dual_rdb_handshake_reply_precedes_endoff() {
    let mut link = link();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let (events, actions) = link.on_rdb_bytes(b"+OK\r\n").unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::WaitingForEndOffset
    );

    let replid = "0123456789abcdef0123456789abcdef01234567";
    let endoff = protocol::encode_end_offset(offset(123), replid, 0, 91);
    let (events, actions) = link.on_rdb_bytes(&endoff).unwrap();
    assert!(events.is_empty());
    assert_eq!(
        actions,
        vec![ReplicaAction::Send(
            protocol::build_replconf_set_rdb_client_id(91)
        )]
    );
}

#[test]
fn fullresync_keeps_empty_applied_position_until_rdb_commit() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(
        b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 1563\r\n",
        6380,
    )
    .unwrap();

    assert!(link.is_receiving_rdb());
    assert_eq!(link.applied_position(), offset(0));
    assert_eq!(link.acknowledged_position(), offset(0));
    assert_eq!(link.received_position(), offset(1563));
    let snapshot_position = link.prevalidate_rdb_apply_position().unwrap();
    assert_eq!(snapshot_position, offset(1563));
    link.finish_rdb_apply(snapshot_position);
    assert_eq!(link.applied_position(), offset(1563));
}

#[test]
fn fullresync_keeps_cached_applied_position_until_rdb_commit() {
    let mut link = link();
    link.set_resume_position("cached-replid".to_string(), offset(42), 0);
    link.connected(None, None);
    link.on_bytes(
        b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 1563\r\n$100\r\n",
        6380,
    )
    .unwrap();

    assert!(link.is_receiving_rdb());
    assert!(link.has_pending_rdb_receive());
    assert_eq!(link.applied_position(), offset(42));
    assert_eq!(link.acknowledged_position(), offset(42));
    let snapshot_position = link.prevalidate_rdb_apply_position().unwrap();
    assert_eq!(snapshot_position, offset(1563));
    link.finish_rdb_apply(snapshot_position);
    assert_eq!(link.applied_position(), offset(1563));
}

#[test]
fn dual_endoff_keeps_applied_position_until_rdb_commit() {
    for cached_offset in [0, 42] {
        let mut link = link();
        if cached_offset != 0 {
            link.set_resume_position("cached-replid".to_string(), offset(cached_offset), 0);
        }
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.on_rdb_bytes(b"+OK\r\n").unwrap();
        let endoff = protocol::encode_end_offset(
            offset(1563),
            "0123456789abcdef0123456789abcdef01234567",
            0,
            91,
        );

        link.on_rdb_bytes(&endoff).unwrap();

        assert_eq!(link.applied_position(), offset(cached_offset));
        assert_eq!(link.acknowledged_position(), offset(cached_offset));
        let snapshot_position = link.prevalidate_rdb_apply_position().unwrap();
        assert_eq!(snapshot_position, offset(1563));
        link.finish_rdb_apply(snapshot_position);
        assert_eq!(link.applied_position(), offset(1563));
    }
}

#[test]
fn classic_replica_link_keeps_rdb_channel_disabled() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n+OK\r\n", 6380).unwrap();
    link.on_bytes(
        b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n$0\r\n",
        6380,
    )
    .unwrap();

    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
}

#[test]
fn fragmented_handshake_produces_ordered_frames() {
    let mut link = link();
    assert_eq!(
        link.connected(None, None),
        vec![ReplicaAction::Send(protocol::build_ping())]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitPong);

    let (events, actions) = link.on_bytes(b"+PO", 6380).unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    let (_, actions) = link.on_bytes(b"NG\r\n", 6380).unwrap();
    assert_eq!(
        actions,
        vec![
            ReplicaAction::Send(protocol::build_replconf_port(6380)),
            ReplicaAction::Send(protocol::build_replconf_capa_with_dual_channel(true)),
        ]
    );
    assert_eq!(
        link.state(),
        ReplicaLinkState::WaitReplconfReplies { remaining: 2 }
    );

    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert!(actions.is_empty());
    assert_eq!(
        link.state(),
        ReplicaLinkState::WaitReplconfReplies { remaining: 1 }
    );
    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert_eq!(
        actions,
        vec![ReplicaAction::Send(protocol::build_psync_full())]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitPsyncReply);
}

#[test]
fn cluster_replica_handshake_advertises_version_and_node_id_before_psync() {
    let mut link = link();
    let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    link.set_cluster_node_id(Some(node_id.to_string()));
    link.connected(None, None);

    let (_, actions) = link.on_bytes(b"+PONG\r\n", 6380).unwrap();

    assert_eq!(
        actions,
        vec![
            ReplicaAction::Send(protocol::build_replconf_port(6380)),
            ReplicaAction::Send(protocol::build_replconf_capa_with_dual_channel(true)),
            ReplicaAction::Send(protocol::build_replconf_version()),
            ReplicaAction::Send(protocol::build_replconf_cluster_node_id(node_id)),
        ]
    );
    assert_eq!(
        link.state(),
        ReplicaLinkState::WaitReplconfReplies { remaining: 4 }
    );
}

#[test]
fn transient_psync_replies_are_retryable_not_protocol_errors() {
    for reply in [
        b"-NOMASTERLINK Can't SYNC while not connected with my master\r\n".as_slice(),
        b"-LOADING primary is loading the dataset in memory\r\n".as_slice(),
        b"-BUSY Valkey is busy running a script\r\n".as_slice(),
    ] {
        let mut link = link();
        advance_to_waitpsyncreply(&mut link, 6380);
        assert!(matches!(
            link.on_bytes(reply, 6380),
            Err(crate::framework::distributed::replication::LinkError::TryLater)
        ));
        assert!(!link.can_resume_after_disconnect());
    }
}

#[test]
fn continue_records_one_shot_partial_resync_acceptance() {
    let mut link = link();
    link.set_resume_position("cached-replid".to_string(), offset(42), 0);
    advance_to_waitpsyncreply(&mut link, 6380);

    link.on_bytes(b"+CONTINUE\r\n", 6380).unwrap();

    assert!(link.take_partial_resync_accepted());
    assert!(!link.take_partial_resync_accepted());
}

#[test]
fn continue_without_cached_primary_rejects_partial_resync() {
    for reply in [
        b"+CONTINUE\r\n".as_slice(),
        b"+CONTINUE 0123456789abcdef0123456789abcdef01234567\r\n",
    ] {
        let mut link = link();
        advance_to_waitpsyncreply(&mut link, 6380);
        assert!(matches!(
            link.on_bytes(reply, 6380),
            Err(LinkError::TryLater)
        ));
        assert!(!link.is_streaming());
        assert!(!link.take_partial_resync_accepted());
        assert!(!link.can_resume_after_disconnect());
    }
}

#[test]
fn dual_continue_before_end_offset_rejects_partial_resync() {
    let mut link = link();
    advance_to_waitpsyncreply(&mut link, 6380);
    link.on_bytes(b"+DUALCHANNELSYNC\r\n", 6380)
        .expect("dual-channel negotiation must succeed");

    assert!(matches!(
        link.on_bytes(b"+CONTINUE\r\n", 6380),
        Err(LinkError::TryLater)
    ));
    assert!(!link.is_streaming());
    assert!(!link.take_partial_resync_accepted());
}

#[test]
fn dual_channel_negotiation_closes_ack_gate_before_rdb_header() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n+OK\r\n", 6380).unwrap();

    let (events, actions) = link
        .on_bytes(b"+DUALCHANNELSYNC\r\n", 6380)
        .expect("dual-channel negotiation must succeed");

    assert!(events.is_empty());
    assert!(matches!(
        actions.as_slice(),
        [ReplicaAction::OpenRdbChannel { .. }]
    ));
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::Connecting);
    assert!(
        !link.can_ack_primary(),
        "the main stream must not ACK before the dual RDB commit"
    );
}

#[test]
fn dual_rdb_commit_keeps_periodic_ack_gated_until_terminal_handoff() {
    let mut link = link();
    advance_to_waitpsyncreply(&mut link, 6380);
    link.on_bytes(b"+DUALCHANNELSYNC\r\n", 6380)
        .expect("dual-channel negotiation must succeed");
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    link.set_resume_position(replid.clone(), offset(20), 0);
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(20),
        db_id: 0,
        rdb_client_id: 7,
    });
    link.on_bytes(b"+CONTINUE\r\n", 6380)
        .expect("dual main PSYNC must enter streaming");

    let apply_position = link.prevalidate_rdb_apply_position().unwrap();
    link.finish_rdb_apply(apply_position);

    assert!(
        link.take_periodic_ack(5_000, None).is_none(),
        "C has no primary client to ACK until pending dual replay finishes"
    );

    let terminal_ack = link.acknowledge_applied(None);
    assert_eq!(
        terminal_ack,
        ReplicaAction::Send(protocol::build_replconf_ack(link.applied_position(), None,))
    );
    assert!(
        link.can_ack_primary(),
        "the explicit terminal ACK completes the dual handoff gate"
    );
}

#[test]
fn handshake_announces_configured_replica_ip_before_capabilities() {
    let mut link = link();
    link.connected(None, None);

    let (_, actions) = link
        .on_bytes_with_announce_ip(b"+PONG\r\n", 6380, "localhost")
        .unwrap();
    assert_eq!(
        actions,
        vec![
            ReplicaAction::Send(protocol::build_replconf_port(6380)),
            ReplicaAction::Send(protocol::build_replconf_ip_address("localhost")),
            ReplicaAction::Send(protocol::build_replconf_capa_with_dual_channel(true)),
        ]
    );
    assert_eq!(
        link.state(),
        ReplicaLinkState::WaitReplconfReplies { remaining: 3 }
    );
    let (_, actions) = link
        .on_bytes_with_announce_ip(b"+OK\r\n", 6380, "localhost")
        .unwrap();
    assert!(actions.is_empty());
    assert_eq!(
        link.state(),
        ReplicaLinkState::WaitReplconfReplies { remaining: 2 }
    );
    let (_, actions) = link
        .on_bytes_with_announce_ip(b"+OK\r\n+OK\r\n", 6380, "localhost")
        .unwrap();
    assert_eq!(
        actions,
        vec![ReplicaAction::Send(protocol::build_psync_full())]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitPsyncReply);
}

#[test]
fn authentication_precedes_ping_and_preserves_binary_credentials() {
    let mut link = link();

    assert_eq!(
        link.connected(Some(b"replica-user"), Some(b"abc\x00def")),
        vec![ReplicaAction::Send(protocol::build_auth(
            Some(b"replica-user"),
            b"abc\x00def"
        ))]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitAuthReply);

    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert_eq!(actions, vec![ReplicaAction::Send(protocol::build_ping())]);
    assert_eq!(link.state(), ReplicaLinkState::WaitPong);
}

#[test]
fn fullresync_and_fragmented_rdb_forward_body_to_decoder() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();

    let (events, _) = link
        .on_bytes(
            b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 9\r\n$5\r\nab",
            6380,
        )
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(
        link.state(),
        ReplicaLinkState::ReceivingRdb { remaining: 3 }
    );
    assert!(link.is_receiving_rdb());
    // Body bytes seen so far are forwarded to the decoder, not buffered as an
    // event payload.
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"ab");

    // Completing the length-framed body emits no event: the backend drives the
    // decoder to EOF and commits (rdb_recv_path.rs). Framing switches to the
    // command stream and the whole body sits in the decoder buffer.
    let (events, _) = link.on_bytes(b"cde", 6380).unwrap();
    assert!(events.is_empty());
    assert!(!link.is_receiving_rdb());
    assert!(link.has_pending_rdb_receive());
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"abcde");
    assert_eq!(
        link.master_replid(),
        Some("0123456789abcdef0123456789abcdef01234567")
    );
    assert_eq!(link.received_position(), offset(9));
}

#[test]
fn fragmented_rdb_header_is_not_treated_as_body() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();

    let (events, _) = link
        .on_bytes(
            b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n$",
            6380,
        )
        .unwrap();
    assert!(events.is_empty());

    let (events, _) = link.on_bytes(b"3\r\nabc", 6380).unwrap();
    assert!(events.is_empty());
    // The `$3\r\n` header framing is consumed; only the 3-byte body reaches the
    // decoder.
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"abc");
}

#[test]
fn diskless_eof_marker_is_removed_from_decoder_body() {
    let mut link = link();
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    let marker = b"0123456789abcdef0123456789abcdef01234567";

    let (events, _) = link
        .on_bytes(
            b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n\
              $EOF:0123456789abcdef0123456789abcdef01234567\r\nabc0123",
            6380,
        )
        .unwrap();
    assert!(events.is_empty());
    let (events, _) = link.on_bytes(&marker[4..], 6380).unwrap();

    // The diskless EOF marker never reaches the decoder: only the pre-marker
    // body (`abc`) is forwarded, and framing switches to the command stream.
    assert!(events.is_empty());
    assert!(!link.is_receiving_rdb());
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"abc");
}

#[test]
fn disk_backed_load_retains_diskless_rdb_body_without_the_eof_marker() {
    let mut link = link();
    link.pin_rdb_load_mode(crate::framework::distributed::replication::RdbLoadMode::DiskBacked);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    let marker = b"0123456789abcdef0123456789abcdef01234567";
    let mut input = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n\
                      $EOF:0123456789abcdef0123456789abcdef01234567\r\nbody"
        .to_vec();
    input.extend_from_slice(marker);

    link.on_bytes(&input, 6380).unwrap();

    let commit = link.rdb_receive.take().unwrap().finish();
    assert_eq!(commit.raw.as_deref(), Some(b"body".as_slice()));
}

#[test]
fn rdb_payload_bytes_count_only_framed_rdb_body() {
    let mut classic_link = link();
    classic_link.connected(None, None);
    classic_link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    classic_link.on_bytes(b"+OK\r\n", 6380).unwrap();
    classic_link.on_bytes(b"+OK\r\n", 6380).unwrap();

    let (events, _) = classic_link
        .on_bytes(
            b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 9\r\n$3\r\nabc*1\r\n$4\r\nPING\r\n",
            6380,
        )
        .unwrap();

    assert_eq!(classic_link.rdb_payload_bytes(), 3);
    assert_eq!(
        events.len(),
        1,
        "command tail must remain command stream data"
    );

    let mut eof_link = link();
    eof_link.connected(None, None);
    eof_link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    eof_link.on_bytes(b"+OK\r\n", 6380).unwrap();
    eof_link.on_bytes(b"+OK\r\n", 6380).unwrap();
    let marker = b"0123456789abcdef0123456789abcdef01234567";
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 9\r\n$EOF:".to_vec();
    frame.extend_from_slice(marker);
    frame.extend_from_slice(b"\r\nabc");
    frame.extend_from_slice(marker);
    frame.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");

    let (events, _) = eof_link.on_bytes(&frame, 6380).unwrap();

    assert_eq!(eof_link.rdb_payload_bytes(), 3);
    assert_eq!(
        events.len(),
        1,
        "EOF marker and command tail must not be counted as RDB payload"
    );
}

#[test]
fn continue_enters_streaming_without_rdb() {
    let mut link = link();
    link.set_resume_position("replid".to_string(), offset(42), 0);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert_eq!(
        actions,
        vec![ReplicaAction::Send(protocol::build_psync("replid", 43))]
    );

    let (events, _) = link.on_bytes(b"+CONTINUE\r\n", 6380).unwrap();
    assert!(events.is_empty());
    assert_eq!(link.state(), ReplicaLinkState::Streaming);
    assert_eq!(link.received_position(), offset(42));
}

#[test]
fn failover_reconnect_emits_psync_failover() {
    let mut link = link();
    let replid = "0123456789abcdef0123456789abcdef01234567";
    link.set_resume_position(replid.to_string(), offset(42), 0);
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();

    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();

    assert_eq!(
        actions,
        vec![ReplicaAction::Send(protocol::build_psync_failover(
            replid, 43
        ))]
    );
}

#[test]
fn overflowing_cached_position_is_a_protocol_error_for_normal_and_failover_psync() {
    for is_failover in [false, true] {
        let mut link = link();
        link.set_resume_position("replid".to_string(), offset(i64::MAX), 0);
        link.set_failover_request(is_failover);
        link.connected(None, None);

        assert!(matches!(
            link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380),
            Err(LinkError::Protocol)
        ));
    }
}

#[test]
fn dual_provisional_psync_rejects_an_overflowing_snapshot_offset() {
    let mut link = link();
    link.state = ReplicaLinkState::WaitDualRdbClientIdReply;
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: "replid".to_string(),
        end_offset: offset(i64::MAX),
        db_id: 0,
        rdb_client_id: 7,
    });

    assert!(matches!(
        link.on_bytes(b"+OK\r\n", 6380),
        Err(LinkError::Protocol)
    ));
}

#[test]
fn accepted_failover_psync_is_reported_once() {
    let mut link = link();
    let replid = "0123456789abcdef0123456789abcdef01234567";
    link.set_resume_position(replid.to_string(), offset(42), 0);
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380).unwrap();

    link.on_bytes(
        b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n",
        6380,
    )
    .unwrap();

    assert!(link.take_failover_accepted());
    assert!(!link.take_failover_accepted());
}

#[test]
fn rejected_failover_psync_has_a_distinct_error() {
    let mut link = link();
    let replid = "0123456789abcdef0123456789abcdef01234567";
    link.set_resume_position(replid.to_string(), offset(42), 0);
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380).unwrap();

    assert!(matches!(
        link.on_bytes(b"-NOPERM PSYNC is disabled\r\n", 6380),
        Err(LinkError::FailoverRejected)
    ));
}

#[test]
fn resume_position_restores_selected_db_for_partial_reconnect() {
    // A partial resync replays the backlog tail as raw bytes; if the last
    // SELECT preceded the resume offset the tail carries no SELECT, so the
    // link must resume on the DB the previous link had reached (§5.6).
    let mut link = link();
    link.set_resume_position("replid".to_string(), offset(4), 9);
    assert_eq!(link.selected_db(), 9);

    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();

    let (_, _) = link.on_bytes(b"+CONTINUE\r\n", 6380).unwrap();
    assert_eq!(link.state(), ReplicaLinkState::Streaming);
    assert_eq!(link.selected_db(), 9);
}

#[test]
fn fullresync_after_resume_drops_cached_selected_db() {
    // A full resync starts a brand-new stream whose serializer re-emits SELECT
    // and reloads from RDB, so any cached selected DB must reset to 0 (§5.6).
    let mut link = link();
    link.set_resume_position("replid".to_string(), offset(4), 9);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();

    link.on_bytes(
        b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 100\r\n",
        6380,
    )
    .unwrap();
    assert_eq!(link.selected_db(), 0);
}

#[test]
fn streaming_classifies_control_and_data_commands() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(0),
    );
    let select = protocol::encode_command(&[b"SELECT", b"2"]);
    let set = protocol::encode_command(&[b"SET", b"k", b"v"]);
    let getack = protocol::encode_command(&[b"REPLCONF", b"GETACK", b"*"]);
    let mut stream = select.clone();
    stream.extend_from_slice(&set);
    stream.extend_from_slice(&getack);

    let (events, _) = link.on_bytes(&stream, 6380).unwrap();
    assert_eq!(events.len(), 3);
    assert!(matches!(
        events[0],
        ReplicaEvent::Apply {
            apply: ReplicationApply::Select(2),
            ..
        }
    ));
    assert!(matches!(
        events[1],
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(_),
            ..
        }
    ));
    assert!(matches!(
        events[2],
        ReplicaEvent::Apply {
            apply: ReplicationApply::GetAck,
            ..
        }
    ));
    assert_eq!(link.received_position(), offset(stream.len() as i64));
    assert_eq!(link.applied_position(), offset(0));
}

#[test]
fn fragmented_large_bulk_advances_exact_stream_positions_and_ack() {
    const PAYLOAD_LEN: usize = 4 * 1024 * 1024;
    const LARGE_CHUNK: usize = 128 * 1024;

    let start = offset(73);
    let payload: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|index| b'a' + (index % 23) as u8)
        .collect();
    let command = protocol::encode_command(&[b"SET", b"replica-stream-key", &payload]);
    let expected_end = start.checked_advance(command.len()).unwrap();
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        start,
    );

    let chunk_sizes = [1, 3, 17, 257, LARGE_CHUNK];
    let mut cursor = 0;
    let mut chunk_index = 0;
    let mut all_events = Vec::new();
    while cursor < command.len() {
        let end = (cursor + chunk_sizes[chunk_index % chunk_sizes.len()]).min(command.len());
        let (events, actions) = link.on_bytes(&command[cursor..end], 6380).unwrap();
        assert!(actions.is_empty());
        all_events.extend(events);
        cursor = end;
        chunk_index += 1;
    }

    let [ReplicaEvent::Apply { apply, end_offset }] = all_events.as_slice() else {
        panic!("fragmented command must produce exactly one apply event");
    };
    let ReplicationApply::Command(args) = apply else {
        panic!("SET must remain a regular replication command");
    };
    assert_eq!(args.name.as_ref(), b"SET");
    assert_eq!(args.args.len(), 2);
    assert_eq!(args.args[0].as_ref(), b"replica-stream-key");
    assert_eq!(args.args[1].as_ref(), payload.as_slice());
    assert_eq!(*end_offset, expected_end);
    assert_eq!(link.received_position(), expected_end);
    assert_eq!(link.decoded_offset, expected_end);
    assert_eq!(link.applied_position(), start);

    link.finish_command_apply(*end_offset).unwrap();
    let ack = link.acknowledge_applied(None);
    assert_eq!(link.applied_position(), expected_end);
    assert_eq!(link.acknowledged_position(), expected_end);
    assert_eq!(
        ack,
        ReplicaAction::Send(protocol::build_replconf_ack(expected_end, None))
    );
}

#[test]
fn applied_and_acked_offsets_never_exceed_received() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(10),
    );
    let ping = protocol::encode_command(&[b"PING"]);
    let (events, _) = link.on_bytes(&ping, 6380).unwrap();
    let ReplicaEvent::Apply { end_offset, .. } = events[0];

    link.finish_command_apply(end_offset).unwrap();
    let ack = link.acknowledge_applied(None);
    assert_eq!(link.received_position(), offset(10 + ping.len() as i64));
    assert_eq!(link.applied_position(), link.received_position());
    assert_eq!(link.acknowledged_position(), link.applied_position());
    assert_eq!(
        ack,
        ReplicaAction::Send(protocol::build_replconf_ack(link.applied_position(), None,))
    );
}

#[test]
fn fullresync_41_then_real_command_acks_its_applied_end() {
    let mut link = link();
    advance_to_waitpsyncreply(&mut link, 6380);
    link.on_bytes(
        b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 41\r\n$0\r\n",
        6380,
    )
    .unwrap();
    let apply_position = link.prevalidate_rdb_apply_position().unwrap();
    link.finish_rdb_apply(apply_position);

    let ping = protocol::encode_command(&[b"PING"]);
    let (events, _) = link.on_bytes(&ping, 6380).unwrap();
    let [ReplicaEvent::Apply { end_offset, .. }] = events.as_slice() else {
        panic!("one RESP command must produce one apply event");
    };
    link.finish_command_apply(*end_offset).unwrap();
    let ack = link.acknowledge_applied(None);
    let expected = offset(41 + ping.len() as i64);

    assert_eq!(link.applied_position(), expected);
    assert_eq!(link.acknowledged_position(), expected);
    assert_eq!(
        ack,
        ReplicaAction::Send(protocol::build_replconf_ack(expected, None))
    );
}

#[test]
fn replica_link_positions_use_replication_offset_types() {
    let link = link();
    let expected =
        std::any::type_name::<crate::engine::distributed::replication::ReplicationOffset>();

    assert_eq!(std::any::type_name_of_val(&link.received_offset), expected);
    assert_eq!(std::any::type_name_of_val(&link.decoded_offset), expected);
    assert_eq!(std::any::type_name_of_val(&link.applied_offset), expected);
    assert_eq!(std::any::type_name_of_val(&link.acked_offset), expected);
    assert_eq!(
        std::any::type_name_of_val(&link.requested_offset),
        std::any::type_name::<Option<crate::engine::distributed::replication::ReplicationOffset>>()
    );
}

#[test]
fn finishing_rdb_preserves_snapshot_offset_before_buffered_tail() {
    let mut link = link();
    link.set_resume_position("replid".to_string(), offset(20), 0);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6380).unwrap();
    link.on_bytes(b"+OK\r\n+OK\r\n", 6380).unwrap();
    link.on_bytes(b"+CONTINUE\r\n*1\r\n$4\r\nPING\r\n", 6380)
        .unwrap();
    assert!(link.received_position() > offset(20));
    let apply_position = link.prevalidate_rdb_apply_position().unwrap();
    link.finish_rdb_apply(apply_position);
    assert_eq!(link.applied_position(), offset(20));
}

#[test]
fn finish_rdb_apply_arms_immediate_periodic_ack() {
    // A freshly-online replica must emit a REPLCONF ACK on the next cron tick
    // rather than waiting a full second. Mirrors C's repl_ack_time stamp in
    // replicaPutOnline: without this early ACK the primary's "(streaming sync)"
    // timeout spuriously fires for a healthy replica after a tiny/empty full
    // resync that never produced a post-sync frame to piggyback on.
    let mut link = ReplicaLink::streaming(
        ConnectionId(4),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(20),
    );
    let apply_position = link.prevalidate_rdb_apply_position().unwrap();
    link.finish_rdb_apply(apply_position);

    use crate::framework::distributed::replication::ReplicaAction;
    let first = link.take_periodic_ack(5_000, None);
    assert!(
        matches!(first, Some(ReplicaAction::Send(_))),
        "first cron tick after RDB commit must ACK"
    );

    // The very next tick (same second) must be throttled.
    let second = link.take_periodic_ack(5_500, None);
    assert!(second.is_none(), "periodic ACK throttled to one per second");

    // A later tick (>=1s) emits again.
    let third = link.take_periodic_ack(6_100, None);
    assert!(
        matches!(third, Some(ReplicaAction::Send(_))),
        "ACK resumes after 1s interval"
    );
}

#[test]
fn outbound_replication_activity_refreshes_io_time() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(4),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(10),
    );
    link.set_last_io_time_ms(1);
    link.mark_io_activity();
    assert!(link.last_io_time_ms() > 1);
}

fn advance_to_waitpsyncreply(link: &mut ReplicaLink, port: u16) {
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", port).unwrap();
    link.on_bytes(b"+OK\r\n", port).unwrap();
    link.on_bytes(b"+OK\r\n", port).unwrap();
    assert_eq!(link.state(), ReplicaLinkState::WaitPsyncReply);
}

fn position_snapshot(link: &ReplicaLink) -> (Option<i64>, i64, i64, i64, i64) {
    (
        link.requested_offset.map(ReplicationOffset::get),
        link.received_offset.get(),
        link.decoded_offset.get(),
        link.applied_offset.get(),
        link.acked_offset.get(),
    )
}

#[test]
fn fullresync_rejects_invalid_offsets_before_mutating_link_state() {
    let replid = "0123456789abcdef0123456789abcdef01234567";
    for offset in [-1, i64::MAX] {
        let mut link = link();
        advance_to_waitpsyncreply(&mut link, 6380);
        let positions = position_snapshot(&link);
        let state = link.state();
        let reply = format!("+FULLRESYNC {replid} {offset}\r\n");

        assert!(matches!(
            link.on_bytes(reply.as_bytes(), 6380),
            Err(LinkError::Protocol)
        ));
        assert_eq!(position_snapshot(&link), positions);
        assert_eq!(link.state(), state);
        assert!(link.master_replid().is_none());
        assert!(link.provisional_primary().is_none());
    }
}

#[test]
fn malformed_fullresync_identity_and_shape_do_not_mutate_link_state() {
    let valid_replid = "0123456789abcdef0123456789abcdef01234567";
    for reply in [
        "+FULLRESYNC short 12\r\n".to_string(),
        "+FULLRESYNC 0123456789abcdef0123456789abcdef0123456g 12\r\n".to_string(),
        format!("+FULLRESYNC {valid_replid}\r\n"),
        format!("+FULLRESYNC {valid_replid} 12 trailing\r\n"),
        format!("+FULLRESYNC  {valid_replid} 12\r\n"),
        format!("+FULLRESYNC {valid_replid}  12\r\n"),
        format!("+FULLRESYNC {valid_replid}\t12\r\n"),
        format!("+FULLRESYNC {valid_replid} 12 \r\n"),
        format!("+FULLRESYNC {valid_replid} 12\t\r\n"),
    ] {
        let mut link = link();
        advance_to_waitpsyncreply(&mut link, 6380);
        link.set_resume_position("existing-replid".to_string(), offset(23), 4);
        let master_replid = link.master_replid().map(str::to_string);
        let positions = position_snapshot(&link);
        let state = link.state();
        let rdb_state = link.rdb_channel_state();
        let has_rdb_receive = link.has_pending_rdb_receive();
        let provisional = link.provisional_primary().cloned();

        assert!(matches!(
            link.on_bytes(reply.as_bytes(), 6380),
            Err(LinkError::Protocol)
        ));
        assert_eq!(link.master_replid(), master_replid.as_deref(), "{reply:?}");
        assert_eq!(position_snapshot(&link), positions, "{reply:?}");
        assert_eq!(link.state(), state, "{reply:?}");
        assert_eq!(link.rdb_channel_state(), rdb_state, "{reply:?}");
        assert_eq!(link.has_pending_rdb_receive(), has_rdb_receive, "{reply:?}");
        assert_eq!(
            link.provisional_primary(),
            provisional.as_ref(),
            "{reply:?}"
        );
    }
}

#[test]
fn endoff_rejects_invalid_offsets_before_mutating_provisional_state() {
    let replid = "0123456789abcdef0123456789abcdef01234567";
    for offset in [-1, i64::MAX] {
        let mut link = link();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.on_rdb_bytes(b"+OK\r\n").unwrap();
        let positions = position_snapshot(&link);
        let state = link.state();
        let rdb_state = link.rdb_channel_state();
        let frame = format!("$ENDOFF:{offset} {replid} 0 7\r\n");

        assert!(matches!(
            link.on_rdb_bytes(frame.as_bytes()),
            Err(LinkError::Protocol)
        ));
        assert_eq!(position_snapshot(&link), positions);
        assert_eq!(link.state(), state);
        assert_eq!(link.rdb_channel_state(), rdb_state);
        assert!(link.master_replid().is_none());
        assert!(link.provisional_primary().is_none());
    }
}

#[test]
fn receive_offset_overflow_is_protocol_error_without_position_mutation() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(i64::MAX),
    );
    link.decoded_offset = offset(0);
    let positions = position_snapshot(&link);
    let input = link.input.clone();
    let undecoded_len = link.parser.undecoded_len();

    assert!(matches!(
        link.on_bytes(b"x", 6380),
        Err(LinkError::Protocol)
    ));
    assert_eq!(position_snapshot(&link), positions);
    assert_eq!(link.input, input);
    assert_eq!(link.parser.undecoded_len(), undecoded_len);
}

#[test]
fn decoded_offset_overflow_preflight_preserves_partial_parser_and_positions() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(0),
    );
    let partial = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nva";
    let (events, actions) = link.on_bytes(partial, 6380).unwrap();
    assert!(events.is_empty());
    assert!(actions.is_empty());
    assert_eq!(link.parser.undecoded_len(), partial.len());
    assert!(link.parser.undecoded_len() > link.parser.pending_len());

    link.decoded_offset = offset(i64::MAX - 4);
    let positions = position_snapshot(&link);
    let input = link.input.clone();
    let undecoded_len = link.parser.undecoded_len();

    assert!(matches!(
        link.on_bytes(b"lue\r\n", 6380),
        Err(LinkError::Protocol)
    ));
    assert_eq!(position_snapshot(&link), positions);
    assert_eq!(link.input, input);
    assert_eq!(link.parser.undecoded_len(), undecoded_len);
}

#[test]
fn streaming_protocol_error_does_not_publish_prior_decodes_or_offsets() {
    let mut link = ReplicaLink::streaming(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(19),
    );
    let positions = position_snapshot(&link);
    let mut input = protocol::encode_command(&[b"PING"]);
    input.extend_from_slice(b"*1\r\n!4\r\nPONG\r\n");

    assert!(matches!(
        link.on_bytes(&input, 6380),
        Err(LinkError::Protocol)
    ));
    assert_eq!(position_snapshot(&link), positions);
}

#[test]
fn main_channel_receives_dualchannelsync_opens_rdb_handshake() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_failover_request(true);
    advance_to_waitpsyncreply(&mut link, 6380);
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert!(link.master_replid().is_none());

    let (events, actions) = link
        .on_bytes_with_announce_ip(b"+DUALCHANNELSYNC\r\n", 6380, "")
        .unwrap();

    assert!(events.is_empty());
    let open_action = actions
        .iter()
        .find(|a| matches!(a, ReplicaAction::OpenRdbChannel { .. }))
        .expect("OpenRdbChannel action not found");
    match open_action {
        ReplicaAction::OpenRdbChannel { initial_frames } => {
            assert_eq!(initial_frames, &[protocol::build_replconf_rdb_channel()]);
            assert!(
                initial_frames.iter().all(|frame| {
                    !frame
                        .windows(b"set-rdb-client-id".len())
                        .any(|window| window == b"set-rdb-client-id")
                }),
                "RDB setup must not associate a local main token"
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::Connecting);
    assert_eq!(link.state(), ReplicaLinkState::WaitPsyncReply);
    assert!(link.master_replid().is_none());
    assert!(
        link.take_failover_accepted(),
        "C clears a coordinated failover as soon as DUALCHANNELSYNC is accepted"
    );
}

#[test]
fn classic_fullresync_unchanged_when_no_dualchannelsync() {
    let mut link = link();
    advance_to_waitpsyncreply(&mut link, 6380);
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);

    let replid = "0123456789abcdef0123456789abcdef01234567";
    let input = format!("+FULLRESYNC {} 12345\r\n", replid);
    let (events, actions) = link
        .on_bytes_with_announce_ip(input.as_bytes(), 6380, "")
        .unwrap();

    assert!(events.is_empty());
    let has_open = actions
        .iter()
        .any(|a| matches!(a, ReplicaAction::OpenRdbChannel { .. }));
    assert!(!has_open, "should not have OpenRdbChannel action");
    assert!(matches!(
        link.state(),
        ReplicaLinkState::ReceivingRdbHeader
            | ReplicaLinkState::ReceivingRdb { .. }
            | ReplicaLinkState::ReceivingRdbEof { .. }
    ));
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert_eq!(link.master_replid(), Some(replid));
}

#[test]
fn capa_advertisement_includes_dual_channel() {
    let mut link = link();
    link.connected(None, None);
    assert_eq!(link.state(), ReplicaLinkState::WaitPong);

    let (_, actions) = link
        .on_bytes_with_announce_ip(b"+PONG\r\n", 6380, "")
        .unwrap();

    let capa_frame = actions.iter().find_map(|a| match a {
        ReplicaAction::Send(bytes) => {
            if bytes.windows(b"REPLCONF".len()).any(|w| w == b"REPLCONF")
                && bytes.windows(b"capa".len()).any(|w| w == b"capa")
            {
                Some(bytes.clone())
            } else {
                None
            }
        }
        _ => None,
    });
    let capa_frame = capa_frame.expect("capa frame not found in actions");
    assert!(
        capa_frame
            .windows(b"dual-channel".len())
            .any(|w| w == b"dual-channel"),
        "capa frame should contain dual-channel, got: {:?}",
        String::from_utf8_lossy(&capa_frame)
    );
    assert_eq!(
        capa_frame,
        protocol::build_replconf_capa_with_dual_channel(true)
    );
}

#[test]
fn disabled_dual_channel_omits_dual_capability() {
    let mut link = link();
    link.set_dual_channel_enabled(false);
    link.connected(None, None);

    let (_, actions) = link
        .on_bytes_with_announce_ip(b"+PONG\r\n", 6380, "")
        .unwrap();
    let capa_frame = actions.iter().find_map(|action| match action {
        ReplicaAction::Send(bytes)
            if bytes.windows(b"REPLCONF".len()).any(|w| w == b"REPLCONF")
                && bytes.windows(b"capa".len()).any(|w| w == b"capa") =>
        {
            Some(bytes.clone())
        }
        _ => None,
    });

    assert_eq!(
        capa_frame,
        Some(protocol::build_replconf_capa_with_dual_channel(false))
    );
}

#[test]
fn endoff_queues_main_replconf_then_snapshot_psync() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::Handshake);

    let replid = "0123456789abcdef0123456789abcdef01234567";
    let endoff_bytes = protocol::encode_end_offset(offset(123), replid, 0, 91u64);
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    let (_events, actions) = link.on_rdb_bytes(&endoff_bytes).unwrap();

    let pp = link.provisional_primary().unwrap();
    assert_eq!(pp.replid, replid);
    assert_eq!(pp.end_offset.get(), 123);
    assert_eq!(pp.db_id, 0);
    assert_eq!(pp.rdb_client_id, 91);

    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        ReplicaAction::Send(protocol::build_replconf_set_rdb_client_id(91))
    );
    assert!(
        !actions.iter().any(|action| matches!(
            action,
            ReplicaAction::Send(frame) if frame == &protocol::build_psync(replid, 124)
        )),
        "PSYNC must wait for the main-channel REPLCONF reply"
    );

    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::ReceivingRdb
    );

    let (_events, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert_eq!(
        actions,
        vec![ReplicaAction::SendDualPsync(protocol::build_psync(
            replid, 124
        ))]
    );
    assert_eq!(link.selected_db(), 0);
}

#[test]
fn coalesced_endoff_continues_into_rdb_framing_without_advancing_main_reply_state() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        91,
    ));
    bytes.extend_from_slice(b"$3\r\nabc");
    let (_, actions) = link.on_rdb_bytes(&bytes).unwrap();

    assert_eq!(
        actions,
        vec![ReplicaAction::Send(
            protocol::build_replconf_set_rdb_client_id(91)
        )]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitDualRdbClientIdReply);
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"abc");
}

#[test]
fn split_endoff_then_rdb_framing_stays_in_the_rdb_phase() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let endoff = protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        91,
    );
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&endoff).unwrap();
    let (_, actions) = link.on_rdb_bytes(b"$3\r\nabc").unwrap();

    assert!(actions.is_empty());
    assert_eq!(link.state(), ReplicaLinkState::WaitDualRdbClientIdReply);
    assert_eq!(link.rdb_receive_mut().unwrap().buffered(), b"abc");
}

#[test]
fn dual_rdb_eof_header_uses_diskless_receive() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let marker = b"0123456789abcdef0123456789abcdef01234567";
    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        91,
    ));
    bytes.extend_from_slice(b"$EOF:");
    bytes.extend_from_slice(marker);
    bytes.extend_from_slice(b"\r\n");
    link.on_rdb_bytes(&bytes).unwrap();

    let commit = link
        .take_rdb_receive()
        .expect("EOF RDB framing must allocate a receive")
        .finish();
    assert!(commit.diskless, "EOF RDB framing must use diskless receive");
    assert!(
        commit.raw.is_none(),
        "diskless receive must not retain an AOF RDB body"
    );
}

#[test]
fn eof_marker_streaming_state_does_not_resume_before_rdb_commit() {
    let mut link = link();
    advance_to_waitpsyncreply(&mut link, 6380);
    let marker = "0123456789abcdef0123456789abcdef01234567";
    let input = format!(
        "+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 123\r\n$EOF:{marker}\r\n{marker}"
    );
    link.on_bytes(input.as_bytes(), 6380)
        .expect("EOF-framed full sync must parse");

    assert!(link.is_streaming());
    assert!(link.has_pending_rdb_receive());
    assert!(
        !link.can_resume_after_disconnect(),
        "resume is not valid until EOF-framed RDB data is committed"
    );
    let _failed_commit = link.take_rdb_receive().unwrap();
    assert!(
        !link.can_resume_after_disconnect(),
        "dropping an uncommitted RDB must not make its advertised offset resumable"
    );
}

#[test]
fn endoff_main_replconf_reply_gates_snapshot_psync() {
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let replid = "0123456789abcdef0123456789abcdef01234567";
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&protocol::encode_end_offset(offset(123), replid, 0, 91))
        .unwrap();

    let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();
    assert_eq!(
        actions,
        vec![ReplicaAction::SendDualPsync(protocol::build_psync(
            replid, 124
        ))]
    );
    assert_eq!(link.state(), ReplicaLinkState::WaitPsyncReply);

    let mut rejected = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    rejected
        .transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    rejected
        .transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    rejected.on_rdb_bytes(b"+OK\r\n").unwrap();
    rejected
        .on_rdb_bytes(&protocol::encode_end_offset(offset(123), replid, 0, 91))
        .unwrap();
    assert!(matches!(
        rejected.on_bytes(b"-ERR no such RDB client\r\n", 6380),
        Err(crate::framework::distributed::replication::LinkError::Protocol)
    ));
}

#[test]
fn dual_endoff_seeds_main_stream_offsets_before_continue() {
    let mut link = link();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    let replid = "0123456789abcdef0123456789abcdef01234567";
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&protocol::encode_end_offset(offset(123), replid, 0, 91))
        .unwrap();
    link.on_bytes(b"+OK\r\n", 6380).unwrap();
    let frame = b"*1\r\n$4\r\nPING\r\n";
    let (events, _) = link
        .on_bytes(b"+CONTINUE\r\n*1\r\n$4\r\nPING\r\n", 6380)
        .unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ReplicaEvent::Apply { end_offset, .. } => {
            assert_eq!(end_offset.get(), 123 + frame.len() as i64);
        }
    }
    let mut link = ReplicaLink::new(
        ConnectionId(7),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();

    let malformed_cases: &[&[u8]] = &[
        b"$ENDOFF:BAD_INT zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz 0 0\r\n",
        b"$ENDOFF:1 nonhexshort 0 0\r\n",
        b"junk-line\r\n",
        b"$ENDOFF:1 0123456789abcdef0123456789abcdef01234567 0 0 extra\r\n",
    ];

    for case in malformed_cases {
        let mut link_clone = ReplicaLink::new(
            ConnectionId(7),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link_clone
            .transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link_clone
            .transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            link_clone.on_rdb_bytes(case)
        }));
        assert!(result.is_ok(), "malformed input must not panic");

        let inner = result.unwrap();
        assert!(inner.is_err(), "malformed input should return Err, got Ok");
        assert!(matches!(
            inner,
            Err(crate::framework::distributed::replication::LinkError::Protocol)
        ));
    }
}

#[test]
fn dual_mode_main_channel_entering_receiving_rdb_is_rejected_or_impossible() {
    // Case 1: rdb_channel_state = ReceivingRdb (dual mode) → begin_rdb rejected
    let mut link_dual = link();
    link_dual
        .transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link_dual
        .transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link_dual
        .transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link_dual
        .transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link_dual.provisional_primary = Some(super::ProvisionalPrimary {
        replid: "0123456789abcdef0123456789abcdef01234567".to_string(),
        end_offset: offset(1000),
        db_id: 0,
        rdb_client_id: 42,
    });
    advance_to_waitpsyncreply(&mut link_dual, 6380);

    let result = link_dual.on_bytes_with_announce_ip(
        b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n$EOF:0123456789abcdef0123456789abcdef01234567\r\n",
        6380,
        "",
    );
    assert!(
        result.is_err(),
        "dual mode main channel must reject FULLRESYNC"
    );
    assert!(matches!(
        result,
        Err(crate::framework::distributed::replication::LinkError::Protocol)
    ));
    assert!(!link_dual.is_receiving_rdb(), " state  ReceivingRdb*");
    assert!(link_dual.take_rdb_receive().is_none(), " rdb_receive  None");

    // Case 2: Control group - classic single-channel (no dual) works normally
    let mut link_classic = link();
    assert_eq!(
        link_classic.rdb_channel_state(),
        ReplicaRdbChannelState::None
    );
    assert!(link_classic.provisional_primary().is_none());
    advance_to_waitpsyncreply(&mut link_classic, 6380);

    let result_classic = link_classic.on_bytes_with_announce_ip(
        b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n$0\r\n",
        6380,
        "",
    );
    assert!(
        result_classic.is_ok(),
        "classic mode should work, got err: {:?}",
        result_classic.err()
    );
    assert!(
        link_classic.is_receiving_rdb() || link_classic.has_pending_rdb_receive(),
        "classic  RDB "
    );
}

#[test]
fn reconnect_with_incomplete_snapshot_uses_psync_question_mark_minus_one() {
    let make_link_at_state = |rdb_state: ReplicaRdbChannelState, has_provisional: bool| {
        let mut link = ReplicaLink::new(
            ConnectionId(7),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        if rdb_state != ReplicaRdbChannelState::None {
            link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
                .unwrap();
            if rdb_state != ReplicaRdbChannelState::Connecting {
                link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
                    .unwrap();
                if rdb_state == ReplicaRdbChannelState::WaitingForEndOffset {
                    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
                        .unwrap();
                }
            }
        }
        if has_provisional {
            link.provisional_primary = Some(ProvisionalPrimary {
                replid: "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF".to_string(),
                end_offset: offset(9999),
                db_id: 0,
                rdb_client_id: 7,
            });
        }
        link
    };

    let scenarios = [
        (
            ReplicaRdbChannelState::Connecting,
            false,
            "Connecting, no provisional",
        ),
        (
            ReplicaRdbChannelState::Handshake,
            false,
            "Handshake, no provisional",
        ),
        (
            ReplicaRdbChannelState::WaitingForEndOffset,
            false,
            "WaitingForEndOffset, no provisional",
        ),
        (
            ReplicaRdbChannelState::WaitingForEndOffset,
            true,
            "WaitingForEndOffset, WITH provisional (not yet adopted)",
        ),
    ];

    for (rdb_state, has_provisional, label) in scenarios {
        eprintln!("--- scenario: {label} ---");
        let mut link = make_link_at_state(rdb_state, has_provisional);

        assert_eq!(link.rdb_channel_state(), rdb_state);
        assert_eq!(
            link.provisional_primary().is_some(),
            has_provisional,
            "scenario setup sanity"
        );
        assert!(
            link.master_replid().is_none(),
            "{label}: master_replid must be None before reconnect (not adopted)"
        );

        let _actions = link.connected(None, None);
        let (_, actions_after_pong) = link.on_bytes(b"+PONG\r\n", 6380).unwrap();

        let find_capa = actions_after_pong.iter().find_map(|a| match a {
            ReplicaAction::Send(bytes) => {
                let needle = b"REPLCONF";
                if bytes.windows(needle.len()).any(|w| w == needle) {
                    Some(bytes.clone())
                } else {
                    None
                }
            }
            _ => None,
        });
        assert!(find_capa.is_some(), "{label}: capa REPLCONF must be sent");

        let (_, actions_after_two_ok) = link
            .on_bytes(b"+OK\r\n+OK\r\n", 6380)
            .expect("two REPLCONF OK replies should succeed");

        let psync_action = actions_after_two_ok.iter().find_map(|a| match a {
            ReplicaAction::Send(bytes) => {
                let needle = b"PSYNC";
                if bytes.windows(needle.len()).any(|w| w == needle) {
                    Some(bytes.clone())
                } else {
                    None
                }
            }
            _ => None,
        });
        let psync_action =
            psync_action.unwrap_or_else(|| panic!("{label}: a PSYNC action must be issued"));
        assert_eq!(
            psync_action,
            protocol::build_psync_full(),
            "{label}: reconnect during incomplete snapshot must issue PSYNC ? -1, got: {:?}",
            String::from_utf8_lossy(&psync_action)
        );
        assert!(
            link.master_replid().is_none(),
            "{label}: master_replid should remain None after handshake rebuild"
        );
        assert!(
            !psync_action
                .windows(b"DEADBEEF".len())
                .any(|w| w == b"DEADBEEF"),
            "{label}: PSYNC must NOT carry the provisional replid, got: {:?}",
            String::from_utf8_lossy(&psync_action)
        );
    }
}
