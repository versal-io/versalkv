use super::{
    ExportPhase, ImportPhase, MigrationAction, MigrationError, MigrationJob, MigrationJobId,
    MigrationOperation, MigrationPhase, MigrationRegistry, SlotRanges, SlotRangesError,
    TerminalPhase,
};
use crate::engine::distributed::cluster::{NodeId, SlotRange};

fn node_id(byte: u8) -> NodeId {
    NodeId::parse(&[byte; 40]).expect("test node ID is valid")
}

fn job_id(byte: u8) -> MigrationJobId {
    MigrationJobId::parse(&[byte; 40]).expect("test job ID is valid")
}

fn ranges(values: &[(u16, u16)]) -> SlotRanges {
    SlotRanges::new(
        values
            .iter()
            .map(|&(start, end)| SlotRange::new(start, end).expect("test range is valid"))
            .collect(),
    )
    .expect("test ranges do not overlap")
}

fn export_job(id: u8, values: &[(u16, u16)], now: i64) -> MigrationJob {
    MigrationJob::new(
        job_id(id),
        node_id(b'a'),
        node_id(b'b'),
        ranges(values),
        MigrationOperation::Export,
        now,
    )
}

#[test]
fn raw_ranges_reject_missing_invalid_and_reversed_endpoints() {
    assert_eq!(
        SlotRanges::parse_pairs(&[b"0".as_slice()]),
        Err(SlotRangesError::MissingEnd)
    );
    assert_eq!(
        SlotRanges::parse_pairs(&[b"16384".as_slice(), b"16384".as_slice()]),
        Err(SlotRangesError::InvalidSlot)
    );
    assert_eq!(
        SlotRanges::parse_pairs(&[b"3".as_slice(), b"2".as_slice()]),
        Err(SlotRangesError::Reversed { start: 3, end: 2 })
    );
}

#[test]
fn ranges_reject_overlap_in_either_order() {
    let first = SlotRanges::parse_pairs(&[
        b"0".as_slice(),
        b"5".as_slice(),
        b"3".as_slice(),
        b"6".as_slice(),
    ]);
    assert_eq!(
        first,
        Err(SlotRangesError::Overlap {
            current: SlotRange::new(3, 6).unwrap(),
            previous: SlotRange::new(0, 5).unwrap(),
        })
    );

    let reverse = SlotRanges::parse_pairs(&[
        b"3".as_slice(),
        b"6".as_slice(),
        b"0".as_slice(),
        b"5".as_slice(),
    ]);
    assert_eq!(
        reverse,
        Err(SlotRangesError::Overlap {
            current: SlotRange::new(0, 5).unwrap(),
            previous: SlotRange::new(3, 6).unwrap(),
        })
    );
}

#[test]
fn ranges_preserve_input_order_and_render_canonically() {
    let ranges = SlotRanges::parse_pairs(&[
        b"9".as_slice(),
        b"12".as_slice(),
        b"1".as_slice(),
        b"1".as_slice(),
    ])
    .unwrap();

    assert_eq!(ranges.to_string(), "9-12 1-1");
    assert!(ranges.contains(10));
    assert!(ranges.contains(1));
    assert!(!ranges.contains(8));
}

#[test]
fn phases_expose_every_upstream_state_name() {
    let imports = [
        (ImportPhase::WaitingForAck, "waiting-for-ack"),
        (ImportPhase::ReceivingSnapshot, "receiving-snapshot"),
        (ImportPhase::WaitingForPaused, "waiting-for-paused"),
        (ImportPhase::FailoverRequested, "failover-requested"),
        (ImportPhase::FailoverGranted, "failover-granted"),
        (ImportPhase::OccurringOnPrimary, "occurring-on-primary"),
    ];
    let exports = [
        (ExportPhase::Connecting, "connecting"),
        (ExportPhase::SendingAuthCommand, "sending-auth-command"),
        (ExportPhase::ReadingAuthResponse, "reading-auth-response"),
        (
            ExportPhase::SendingEstablishCommand,
            "sending-establish-command",
        ),
        (
            ExportPhase::ReadingEstablishResponse,
            "reading-establish-response",
        ),
        (ExportPhase::WaitingToSnapshot, "waiting-to-snapshot"),
        (ExportPhase::Snapshotting, "snapshotting"),
        (ExportPhase::Streaming, "replicating"),
        (ExportPhase::WaitingToPause, "waiting-to-pause"),
        (ExportPhase::FailoverPaused, "failover-paused"),
        (ExportPhase::FailoverGranted, "failover-granted"),
    ];
    let terminals = [
        (TerminalPhase::Success, "success"),
        (TerminalPhase::Cancelled, "cancelled"),
        (TerminalPhase::Failed, "failed"),
    ];

    for (phase, name) in imports {
        assert_eq!(phase.as_str(), name);
    }
    for (phase, name) in exports {
        assert_eq!(phase.as_str(), name);
    }
    for (phase, name) in terminals {
        assert_eq!(phase.as_str(), name);
    }
}

#[test]
fn registry_rejects_active_overlap_and_allows_finished_ranges() {
    let mut registry = MigrationRegistry::default();
    registry.start(export_job(b'1', &[(10, 20)], 100)).unwrap();

    assert_eq!(
        registry.start(export_job(b'2', &[(20, 30)], 101)),
        Err(MigrationError::ActiveSlotOverlap { slot: 20 })
    );

    registry
        .finish(&job_id(b'1'), TerminalPhase::Success, None, 102)
        .unwrap();
    registry.start(export_job(b'2', &[(20, 30)], 103)).unwrap();
}

#[test]
fn registry_allows_reusing_a_finished_job_id() {
    let mut registry = MigrationRegistry::default();
    let id = job_id(b'1');
    registry.start(export_job(b'1', &[(10, 10)], 100)).unwrap();
    registry
        .finish(&id, TerminalPhase::Failed, Some("first".into()), 101)
        .unwrap();

    registry.start(export_job(b'1', &[(11, 11)], 102)).unwrap();

    assert_eq!(registry.views().len(), 2);
    assert!(registry.get(&id).unwrap().is_active());
}

#[test]
fn registry_allows_opposite_direction_overlap_for_handoff() {
    let mut registry = MigrationRegistry::default();
    registry.start(export_job(b'1', &[(10, 20)], 100)).unwrap();
    let import = MigrationJob::new(
        job_id(b'2'),
        node_id(b'b'),
        node_id(b'a'),
        ranges(&[(20, 30)]),
        MigrationOperation::Import,
        101,
    );

    assert!(registry.start(import).is_ok());
}

#[test]
fn finish_and_cancel_are_idempotent() {
    let mut registry = MigrationRegistry::default();
    registry.start(export_job(b'1', &[(1, 1)], 10)).unwrap();
    registry.set_remaining_repl_size(&job_id(b'1'), 51);

    assert_eq!(
        registry
            .finish(
                &job_id(b'1'),
                TerminalPhase::Failed,
                Some("first".into()),
                11
            )
            .unwrap(),
        vec![MigrationAction::Cleanup(job_id(b'1'))]
    );
    assert!(registry
        .finish(
            &job_id(b'1'),
            TerminalPhase::Success,
            Some("second".into()),
            12
        )
        .unwrap()
        .is_empty());
    let view = registry.views().into_iter().next().unwrap();
    assert_eq!(view.phase, MigrationPhase::Terminal(TerminalPhase::Failed));
    assert_eq!(view.message.as_deref(), Some("first"));
    assert_eq!(view.last_update_time, 11);
    assert_eq!(view.remaining_repl_size, 0);
}

#[test]
fn flush_failure_terminates_all_active_jobs() {
    let mut registry = MigrationRegistry::default();
    registry.start(export_job(b'1', &[(1, 1)], 10)).unwrap();
    let import = MigrationJob::new(
        job_id(b'2'),
        node_id(b'b'),
        node_id(b'a'),
        ranges(&[(2, 2)]),
        MigrationOperation::Import,
        10,
    );
    registry.start(import).unwrap();

    let actions = registry.fail_all("Data was flushed", 11);

    assert_eq!(actions.len(), 2);
    assert!(registry.views().iter().all(|job| {
        job.phase == MigrationPhase::Terminal(TerminalPhase::Failed)
            && job.message.as_deref() == Some("Data was flushed")
    }));
}

#[test]
fn demotion_turns_live_imports_into_tracking_jobs() {
    let mut registry = MigrationRegistry::default();
    let id = job_id(b'2');
    let import = MigrationJob::new(
        id.clone(),
        node_id(b'b'),
        node_id(b'a'),
        ranges(&[(2, 2)]),
        MigrationOperation::Import,
        10,
    );
    registry.start(import).unwrap();

    assert_eq!(registry.track_imports_after_demotion(11), vec![id.clone()]);
    assert_eq!(
        registry.get(&id).unwrap().phase,
        MigrationPhase::Import(ImportPhase::OccurringOnPrimary)
    );
}

#[test]
fn bounded_history_removes_only_oldest_terminal_jobs() {
    let mut registry = MigrationRegistry::default();
    for (id, slot) in [(b'1', 1), (b'2', 2), (b'3', 3)] {
        registry
            .start(export_job(id, &[(slot, slot)], slot as i64))
            .unwrap();
    }
    registry
        .finish(&job_id(b'1'), TerminalPhase::Success, None, 10)
        .unwrap();
    registry
        .finish(&job_id(b'2'), TerminalPhase::Cancelled, None, 11)
        .unwrap();

    registry.trim_history(1);
    let ids: Vec<_> = registry.views().into_iter().map(|view| view.id).collect();
    assert_eq!(ids, vec![job_id(b'3'), job_id(b'2')]);
    assert!(registry.get(&job_id(b'3')).unwrap().is_active());
}

#[test]
fn export_and_import_follow_legal_protocol_transitions() {
    use super::MigrationEvent as Event;

    let mut registry = MigrationRegistry::default();
    let export_id = job_id(b'1');
    registry.start(export_job(b'1', &[(1, 1)], 1)).unwrap();
    assert_eq!(
        registry
            .transition(
                &export_id,
                Event::Connected {
                    auth_required: false
                },
                2
            )
            .unwrap(),
        vec![MigrationAction::SendEstablish(export_id.clone())]
    );
    registry
        .transition(&export_id, Event::EstablishCommandSent, 3)
        .unwrap();
    assert_eq!(
        registry
            .transition(&export_id, Event::Established, 4)
            .unwrap(),
        vec![
            MigrationAction::Send(export_id.clone(), super::MigrationProtocolMessage::Ack),
            MigrationAction::StartSnapshot(export_id.clone()),
        ]
    );
    assert!(matches!(
        registry.transition(&export_id, Event::SnapshotEof, 5),
        Err(MigrationError::InvalidTransition { .. })
    ));

    let import_id = job_id(b'2');
    let import = MigrationJob::new(
        import_id.clone(),
        node_id(b'a'),
        node_id(b'b'),
        ranges(&[(2, 2)]),
        MigrationOperation::Import,
        1,
    );
    assert_eq!(
        registry.start(import).unwrap(),
        vec![MigrationAction::CreateImport(import_id.clone())]
    );
    registry.record_ack(&import_id, 2).unwrap();
    assert_eq!(
        registry
            .transition(&import_id, Event::SnapshotEof, 3)
            .unwrap(),
        vec![MigrationAction::Send(
            import_id.clone(),
            super::MigrationProtocolMessage::RequestPause,
        )]
    );
    registry.transition(&import_id, Event::Paused, 4).unwrap();
    assert_eq!(
        registry
            .transition(&import_id, Event::FailoverGranted, 5)
            .unwrap(),
        Vec::<MigrationAction>::new()
    );
    assert_eq!(
        registry.get(&import_id).unwrap().phase,
        MigrationPhase::Import(ImportPhase::FailoverGranted)
    );
}
