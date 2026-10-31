use super::*;
use crate::framework::distributed::replication::ReplicaId;
use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
use crate::runtime::server::worker::distributed::replication::diskless_child::{
    DirectDualCompletionState, DirectDualFact, DirectDualRdbDropOutcome,
};
use std::fs::File;
#[cfg(target_os = "linux")]
use std::process::{Child, Command};
#[cfg(target_os = "linux")]
use std::thread::sleep;
#[cfg(target_os = "linux")]
use std::time::Duration;

fn install_fake_child(
    backend: &mut ServerWorkerState,
    pid: i32,
    rdb_connections: Vec<ConnectionId>,
    targets: Vec<ReplicaId>,
) {
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let handle = backend.install_test_child(
        crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
        pid,
    );
    backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
        pid,
        child: handle,
        rdb_connections,
        targets,
        shared_replication_buffer_bytes: 0,
        reaped_successfully: false,
        metric_read_end: File::open("/dev/null").unwrap(),
        phase_read_end: File::open("/dev/null").unwrap(),
        phase_source: None,
        phases: Vec::new(),
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
    });
}

#[cfg(target_os = "linux")]
fn install_child(
    backend: &mut ServerWorkerState,
    rdb_connections: Vec<ConnectionId>,
    targets: Vec<ReplicaId>,
) -> Child {
    let child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn child");
    let pid = child.id() as i32;
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let handle = backend.install_test_child(
        crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
        pid,
    );
    backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
        pid,
        child: handle,
        rdb_connections,
        targets,
        shared_replication_buffer_bytes: 0,
        reaped_successfully: false,
        metric_read_end: File::open("/dev/null").unwrap(),
        phase_read_end: File::open("/dev/null").unwrap(),
        phase_source: None,
        phases: Vec::new(),
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
    });
    child
}

fn register_primary(backend: &mut ServerWorkerState, main: ConnectionId) -> ReplicaId {
    backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: dual_channel_caps(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    )
}

#[test]
fn direct_dual_completion_facts_gate_publication_and_target_hup_fails() {
    let success_facts = [
        DirectDualFact::ChildSucceeded,
        DirectDualFact::MarkerPhase,
        DirectDualFact::MetricsDrained,
    ];
    for order in [
        success_facts,
        [
            DirectDualFact::ChildSucceeded,
            DirectDualFact::MetricsDrained,
            DirectDualFact::MarkerPhase,
        ],
        [
            DirectDualFact::MarkerPhase,
            DirectDualFact::ChildSucceeded,
            DirectDualFact::MetricsDrained,
        ],
        [
            DirectDualFact::MarkerPhase,
            DirectDualFact::MetricsDrained,
            DirectDualFact::ChildSucceeded,
        ],
        [
            DirectDualFact::MetricsDrained,
            DirectDualFact::ChildSucceeded,
            DirectDualFact::MarkerPhase,
        ],
        [
            DirectDualFact::MetricsDrained,
            DirectDualFact::MarkerPhase,
            DirectDualFact::ChildSucceeded,
        ],
    ] {
        let mut state = DirectDualCompletionState::default();
        for (index, fact) in order.into_iter().enumerate() {
            assert_eq!(state.apply_fact(fact), index == 2);
        }
    }

    let mut failed = DirectDualCompletionState::default();
    failed.apply_fact(DirectDualFact::ChildSucceeded);
    failed.apply_fact(DirectDualFact::TargetHup);
    failed.apply_fact(DirectDualFact::MarkerPhase);
    assert!(!failed.apply_fact(DirectDualFact::MetricsDrained));
}

#[test]
#[cfg(target_os = "linux")]
fn successful_direct_child_with_no_replica_targets_reaps_normally() {
    use crate::framework::commands::server::save::DirectDualRdbChildPhase;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let mut child_process = Command::new("true").spawn().expect("spawn test child");
    let pid = child_process.id() as i32;
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let child = backend.install_test_child(
        crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
        pid,
    );
    backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
        pid,
        child,
        rdb_connections: Vec::new(),
        targets: Vec::new(),
        shared_replication_buffer_bytes: 0,
        reaped_successfully: false,
        metric_read_end: File::open("/dev/null").unwrap(),
        phase_read_end: File::open("/dev/null").unwrap(),
        phase_source: None,
        phases: vec![DirectDualRdbChildPhase::MarkerSent],
        completion_state: DirectDualCompletionState::default(),
    });

    for _ in 0..100 {
        backend.reap_direct_dual_rdb_child();
        if !backend.ctx.persistence().has_active_child() {
            break;
        }
        sleep(Duration::from_millis(10));
    }

    assert!(!backend.ctx.persistence().has_active_child());
    assert!(backend.full_sync.direct_dual_rdb_child.is_none());
    let _ = child_process.wait();
}

#[test]
#[cfg(target_os = "linux")]
fn abnormal_reap_closes_unpaired_candidates_and_associated_main_once() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let unpaired_rdb = ConnectionId(50);
    let main = ConnectionId(51);
    let paired_rdb = ConnectionId(52);
    for connection in [unpaired_rdb, main, paired_rdb] {
        add_connection(&mut backend, connection.0);
    }
    backend
        .replication_runtime
        .register_rdb_candidate(unpaired_rdb)
        .unwrap();
    let replica_id = register_primary(&mut backend, main);
    backend.associate_waiting_rdb_for_test(main, paired_rdb);
    let mut child = install_child(
        &mut backend,
        vec![unpaired_rdb, paired_rdb],
        vec![replica_id],
    );
    let pid = child.id() as i32;

    crate::foundation::io::sys::process::send_signal(
        pid,
        crate::foundation::io::sys::process::ProcessSignal::Kill,
    )
    .expect("kill test child");
    for _ in 0..100 {
        backend.reap_direct_dual_rdb_child();
        if !backend.ctx.persistence().has_active_child() {
            break;
        }
        sleep(Duration::from_millis(10));
    }

    assert!(backend.full_sync.direct_dual_rdb_child.is_none());
    assert!(!backend.ctx.persistence().has_active_child());
    for connection in [unpaired_rdb, main, paired_rdb] {
        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection.0)));
        assert_eq!(backend.replication_runtime.role(connection), None);
    }
    assert_eq!(
        backend
            .replication_runtime
            .rebuild_view()
            .replicas_waiting_psync,
        0
    );
    let _ = child.wait();
}

#[test]
#[cfg(target_os = "linux")]
fn target_drop_before_publication_aborts_direct_child_cohort() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(53);
    let paired_rdb = ConnectionId(54);
    let remaining_candidate = ConnectionId(55);
    for connection in [main, paired_rdb, remaining_candidate] {
        add_connection(&mut backend, connection.0);
    }
    let replica_id = register_primary(&mut backend, main);
    backend.associate_waiting_rdb_for_test(main, paired_rdb);
    backend
        .replication_runtime
        .register_rdb_candidate(remaining_candidate)
        .unwrap();
    let mut child = install_child(
        &mut backend,
        vec![paired_rdb, remaining_candidate],
        vec![replica_id],
    );

    backend.remove_connection(main.0);

    assert!(backend.full_sync.direct_dual_rdb_child.is_none());
    assert!(!backend.ctx.persistence().has_active_child());
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(
            remaining_candidate.0
        )));
    assert_eq!(backend.replication_runtime.role(remaining_candidate), None);
    let _ = child.wait();
}

fn waiting_direct_child() -> (ServerWorkerState, ConnectionId, ConnectionId, ReplicaId) {
    use crate::framework::distributed::replication::PrimaryDualChannelState;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(56);
    let rdb = ConnectionId(57);
    for connection in [main, rdb] {
        add_connection(&mut backend, connection.0);
    }
    let replica_id = register_primary(&mut backend, main);
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link exists");
    link.transition_to_waiting_for_rdb_channel().unwrap();
    backend.associate_waiting_rdb_for_test(main, rdb);
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link exists");
    link.transition_to_waiting_for_main_psync().unwrap();
    link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
        .unwrap();
    install_fake_child(&mut backend, 123_456, vec![rdb], vec![replica_id]);
    (backend, main, rdb, replica_id)
}

#[test]
fn completed_rdb_socket_close_after_serialize_done_keeps_direct_child() {
    use crate::framework::commands::server::save::DirectDualRdbChildPhase;

    let (mut backend, _, rdb, _) = waiting_direct_child();
    backend
        .full_sync
        .direct_dual_rdb_child
        .as_mut()
        .expect("direct child installed")
        .phases
        .push(DirectDualRdbChildPhase::SerializeDone);

    let outcome = backend.drop_direct_dual_rdb_child_connection(rdb);

    assert_eq!(outcome, DirectDualRdbDropOutcome::CompletedPaired);
    assert!(backend.full_sync.direct_dual_rdb_child.is_some());
    assert!(backend.ctx.persistence().has_active_child());
}

#[test]
fn completed_rdb_close_observes_serialize_done_pending_in_phase_pipe() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    use crate::framework::commands::server::save::DirectDualRdbChildPhase;
    use std::io::Write;

    let (mut backend, _, rdb, _) = waiting_direct_child();
    let (read_end, mut write_end) = pipe(PipeFlags::CLOEXEC_NONBLOCK).unwrap().into_parts();
    write_end
        .write_all(&[DirectDualRdbChildPhase::SerializeDone.as_byte()])
        .unwrap();
    backend
        .full_sync
        .direct_dual_rdb_child
        .as_mut()
        .unwrap()
        .phase_read_end = read_end;

    assert_eq!(
        backend.drop_direct_dual_rdb_child_connection(rdb),
        DirectDualRdbDropOutcome::CompletedPaired
    );
    assert!(backend.ctx.persistence().has_active_child());
}

#[test]
fn completed_rdb_teardown_retains_main_target_until_child_reap() {
    use crate::framework::commands::server::save::DirectDualRdbChildPhase;
    use crate::runtime::server::worker::distributed::replication::ReplicationConnection;

    let (mut backend, main, rdb, replica_id) = waiting_direct_child();
    backend
        .full_sync
        .direct_dual_rdb_child
        .as_mut()
        .unwrap()
        .phases
        .push(DirectDualRdbChildPhase::SerializeDone);

    backend.remove_connection(rdb.0);

    let child = backend.full_sync.direct_dual_rdb_child.as_ref().unwrap();
    assert_eq!(child.targets, vec![replica_id]);
    assert!(child.rdb_connections.is_empty());
    assert!(backend.ctx.persistence().has_active_child());
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main.0)));
    assert_eq!(
        backend.replication_runtime.role(main),
        Some(ReplicationConnection::Primary(replica_id))
    );
}
