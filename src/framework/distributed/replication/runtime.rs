use std::collections::{HashMap, HashSet};

use crate::engine::distributed::replication::{
    backlog::ReplicationBacklog, BacklogCursor, ReplicationOffset, SnapshotBoundary,
};
use crate::foundation::io::sys::time::mstime;

use super::{
    ConnectionId, LinkError, MasterInfo, MasterReconnectCause, OutputBufferSoftLimitReachedAt,
    PendingReplicationBuffer, PrimaryDualChannelState, PrimaryLink, PrimaryLinkState,
    ReplicaConnClass, ReplicaConnectionInfo, ReplicaId, ReplicaLink, ReplicaRdbChannelState,
    ReplicationConnection,
};

#[cfg(test)]
use super::RdbFilterMode;

mod channels;
mod connection;
mod psync_wait;
mod replica;
mod view;

use connection::{apply_pending_metadata, ChannelEndpoint, ChannelRecord};
use psync_wait::RdbPsyncWait;

#[cfg(test)]
use view::main_channel_state_name;

pub(crate) struct ReplicaRdbLink {
    pub(crate) connection_id: ConnectionId,
    #[allow(dead_code)]
    pub(crate) state: ReplicaRdbChannelState,
    #[allow(dead_code)]
    pub(crate) input: Vec<u8>,
}

struct ReplicaPendingBuffer {
    session_id: u64,
    buffer: PendingReplicationBuffer,
}

#[derive(Debug)]
struct GracePsyncWait {
    deadline_ms: i64,
    pin_cursor: Option<BacklogCursor>,
}

#[derive(Debug)]
struct RetainedRdbPsyncWait {
    wait: RdbPsyncWait,
    deadline_ms: i64,
    soft_limit_reached_at: Option<OutputBufferSoftLimitReachedAt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnassociatedRetainedRdbOwner {
    pub public_client_id: u64,
    pub pin_cursor: Option<BacklogCursor>,
}

impl ReplicaRdbLink {
    pub fn new(connection_id: ConnectionId) -> Self {
        Self {
            connection_id,
            state: ReplicaRdbChannelState::None,
            input: Vec::new(),
        }
    }
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }
    #[allow(dead_code)]
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.input.extend_from_slice(bytes);
    }
}

pub struct ReplicationRuntime {
    primary_links: HashMap<ReplicaId, PrimaryLink>,
    replica_link: Option<ReplicaLink>,
    replica_rdb_link: Option<ReplicaRdbLink>,
    replica_pending_buffer: Option<ReplicaPendingBuffer>,
    replica_main_read_paused: bool,
    replica_resume: Option<(MasterInfo, String, ReplicationOffset, u32)>,
    force_full_sync: bool,
    rejected_auth: Option<AuthenticationAttempt>,
    /// Single source of truth for metadata whose lifetime is bound to one
    /// replication connection. Pairing and grace waits remain separate because
    /// they describe relationships or may outlive the connection itself.
    channel_records: HashMap<ConnectionId, ChannelRecord>,
    channel_pairs: HashMap<ConnectionId, ConnectionId>,
    /// ENDOFF and backlog-pin state keyed by the RDB connection that owns it.
    rdb_psync_waits: HashMap<ConnectionId, RdbPsyncWait>,
    /// Completed RDB waits whose transport closed before the main channel
    /// supplied the advertised public client id. These records own only
    /// logical ENDOFF and backlog state, never a socket or connection role.
    retained_rdb_psync_waits: HashMap<u64, RetainedRdbPsyncWait>,
    pub(crate) repl_buffer_tail_exists: bool,
    /// Pins whose main connection closed while the paired RDB client is still
    /// protected by C's delayed-free grace period.
    grace_psync_pin_by_main: HashMap<ConnectionId, GracePsyncWait>,
    next_replica_id: u64,
    next_master_connect_ms: i64,
    dual_channel_failed_replicas: HashSet<(String, u16)>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthenticationAttempt {
    pub host: String,
    pub port: u16,
    pub user: Option<Vec<u8>>,
    pub password: Option<Vec<u8>>,
}

impl ReplicationRuntime {
    pub fn new() -> Self {
        Self {
            primary_links: HashMap::new(),
            replica_link: None,
            replica_rdb_link: None,
            replica_pending_buffer: None,
            replica_main_read_paused: false,
            replica_resume: None,
            force_full_sync: false,
            rejected_auth: None,
            channel_records: HashMap::new(),
            channel_pairs: HashMap::new(),
            rdb_psync_waits: HashMap::new(),
            retained_rdb_psync_waits: HashMap::new(),
            repl_buffer_tail_exists: false,
            grace_psync_pin_by_main: HashMap::new(),
            next_replica_id: 1,
            next_master_connect_ms: 0,
            dual_channel_failed_replicas: HashSet::new(),
        }
    }
}

impl Default for ReplicationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::replication::PsyncRequestOffset;
    use crate::framework::distributed::replication::{
        MasterInfo, PendingReplicationBuffer, ProvisionalPrimary, ReplicaEvent, ReplicationApply,
    };

    fn replica_info(port: u16) -> ReplicaConnectionInfo {
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port,
            capability: Vec::new(),
            rdb_filter_mode: RdbFilterMode::All,
        }
    }

    fn request(value: i64) -> PsyncRequestOffset {
        PsyncRequestOffset::try_from_raw(value).expect("test request must be non-negative")
    }

    fn cursor(value: i64) -> BacklogCursor {
        BacklogCursor::try_from(value).expect("test cursor must be non-negative")
    }

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn boundary(value: usize) -> SnapshotBoundary {
        let mut backlog = ReplicationBacklog::new("boundary".to_string(), value.max(1));
        backlog.append(&vec![b'x'; value]).unwrap();
        backlog.snapshot_boundary()
    }

    fn upstream(connection_id: usize) -> ReplicaLink {
        ReplicaLink::new(
            ConnectionId(connection_id),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        )
    }

    fn register_waiting_dual_main(runtime: &mut ReplicationRuntime, main: ConnectionId) {
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(replica_id)
            .expect("registered primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("primary link must enter the dual RDB wait state");
    }

    fn retain_unpaired_wait(
        runtime: &mut ReplicationRuntime,
        rdb: ConnectionId,
        public_client_id: u64,
        backlog: &mut ReplicationBacklog,
        deadline_ms: i64,
    ) {
        runtime.register_rdb_candidate(rdb).unwrap();
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, backlog)
            .is_some());
        assert!(runtime.retain_completed_unpaired_rdb_wait(rdb, public_client_id, deadline_ms,));
        runtime.remove_connection(rdb);
    }

    #[test]
    fn rdb_psync_wait_tracks_existing_backlog_history_tail() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(30);
        let rdb = ConnectionId(31);
        register_waiting_dual_main(&mut runtime, main);
        runtime.register_rdb_candidate(rdb).unwrap();
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog.activate();
        backlog.append(b"tail").unwrap();
        let tail = backlog.tail_block_start_cursor();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog)
            .is_some());

        assert_eq!(runtime.min_protected_backlog_cursor(), Some(tail));
    }

    #[test]
    fn restored_rdb_endoff_carries_boundary_through_association() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(40);
        let rdb = ConnectionId(41);
        let id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        runtime.register_rdb_candidate(rdb).unwrap();
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let replid = backlog.replid().to_string();

        let frame = runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog)
            .unwrap();
        let endoff = crate::engine::distributed::replication::protocol::parse_end_offset(&frame)
            .expect("ENDOFF must parse");
        assert_eq!(endoff.offset.get(), 41);
        runtime.associate_rdb_channel(main, rdb).unwrap();
        runtime
            .finalize_dual_channel_rdb_association(main, rdb)
            .unwrap();
        assert_eq!(
            runtime.primary_link(id).unwrap().dual_endoff_boundary(),
            Some(boundary)
        );

        backlog.append(b"tail").unwrap();
        let link = runtime.primary_link_mut(id).unwrap();
        assert!(link.complete_dual_channel_main_psync(
            &replid,
            PsyncRequestOffset::try_from_raw(42).unwrap(),
            &backlog,
        ));
        assert_eq!(link.next_send_cursor().get(), 42);
        assert_eq!(link.sent_cursor().get(), 42);
        assert_eq!(link.pump(&backlog), Ok(b"tail".to_vec()));
    }

    #[test]
    fn registers_and_removes_primary_link_role() {
        let mut runtime = ReplicationRuntime::new();
        let id = runtime.register_primary(ConnectionId(4), replica_info(6380));
        assert_eq!(
            runtime.role(ConnectionId(4)),
            Some(ReplicationConnection::Primary(id))
        );
        assert!(runtime.primary_link(id).is_some());

        runtime.remove_connection(ConnectionId(4));
        assert_eq!(runtime.role(ConnectionId(4)), None);
        assert!(runtime.primary_link(id).is_none());
    }

    #[test]
    fn teardown_clears_unpaired_rdb_only_before_connection_id_reuse() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(5);

        runtime.set_rdb_only(connection_id, true);
        assert!(runtime.rdb_only(connection_id));

        runtime.remove_connection(connection_id);
        assert!(!runtime.rdb_only(connection_id));

        runtime.register_primary(connection_id, replica_info(6380));
        assert!(!runtime.rdb_only(connection_id));
    }

    #[test]
    fn register_primary_transfers_staged_replconf_metadata_before_cleanup() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(6);
        runtime.stage_replconf_ip(connection_id, "192.0.2.6".to_string());
        runtime.stage_replconf_port(connection_id, 6386);
        runtime.stage_replconf_capability(connection_id, "eof".to_string());
        runtime.stage_replconf_capability(connection_id, "psync2".to_string());

        let id = runtime.register_primary(connection_id, replica_info(6380));
        let link = runtime.primary_link(id).expect("primary link should exist");

        assert_eq!(link.replica().ip, "192.0.2.6");
        assert_eq!(link.replica().port, 6386);
        assert_eq!(
            link.replica().capability,
            vec!["eof".to_string(), "psync2".to_string()]
        );
    }

    #[test]
    fn register_primary_transfers_auxiliary_metadata_before_cleanup() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(7);
        let node_id = "0123456789abcdef0123456789abcdef01234567".to_string();
        runtime.set_rdb_only(connection_id, true);
        runtime.set_replica_node_id(connection_id, node_id.clone());
        runtime.set_replica_version(connection_id, 0x080001);

        let id = runtime.register_primary(connection_id, replica_info(6380));

        assert!(runtime.rdb_only(connection_id));
        assert_eq!(
            runtime.replica_node_id(connection_id),
            Some(node_id.as_str())
        );
        assert_eq!(
            runtime.primary_link(id).unwrap().replica_version(),
            0x080001
        );
    }

    #[test]
    fn main_channel_owns_at_most_one_rdb_channel() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(10);
        runtime.register_primary(main, replica_info(6380));

        runtime
            .register_rdb_channel(ConnectionId(11), main)
            .expect("first RDB channel should be accepted");
        assert_eq!(runtime.rdb_channel_for_main(main), Some(ConnectionId(11)));
        assert!(runtime
            .register_rdb_channel(ConnectionId(12), main)
            .is_err());
    }

    #[test]
    fn rdb_candidate_associates_by_client_id_and_removal_is_bidirectional() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(20);
        let rdb = ConnectionId(21);
        runtime.register_primary(main, replica_info(6380));

        runtime.register_rdb_candidate(rdb).unwrap();
        assert_eq!(
            runtime.role(rdb),
            Some(ReplicationConnection::RdbChannelCandidate)
        );
        runtime.associate_rdb_channel(main, rdb).unwrap();
        assert_eq!(
            runtime.role(rdb),
            Some(ReplicationConnection::RdbChannel { main })
        );
        assert_eq!(runtime.rdb_channel_for_main(main), Some(rdb));

        let removed = runtime.remove_channel_pair(rdb);
        assert_eq!(removed, vec![main]);
        assert_eq!(runtime.role(main), None);
        assert_eq!(runtime.role(rdb), None);
        assert_eq!(runtime.rdb_channel_for_main(main), None);
    }

    #[test]
    fn removing_unpaired_rdb_candidate_does_not_mutate_a_main_snapshot() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(24);
        let rdb = ConnectionId(25);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main link must await an RDB channel");
        runtime.register_rdb_candidate(rdb).unwrap();

        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog)
            .is_some());
        runtime.activate_waiting_psync_pins_for_rdb_connections(&[rdb], &mut backlog);
        assert_eq!(
            runtime
                .primary_link(replica_id)
                .expect("main link must remain registered")
                .dual_endoff_boundary(),
            None
        );

        runtime.remove_connection(rdb);

        assert_eq!(
            runtime
                .primary_link(replica_id)
                .expect("main link must survive candidate teardown")
                .dual_endoff_boundary(),
            None
        );
    }

    #[test]
    fn waiting_psync_pin_backfills_empty_backlog_and_releases_after_main_psync() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(26);
        let rdb = ConnectionId(27);

        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(0),
                main: Some(main),
                pin_cursor: None,
                pin_from_existing_tail: false,
            },
        );

        assert_eq!(runtime.min_protected_backlog_cursor(), None);

        assert_eq!(runtime.backfill_waiting_psync_pins(cursor(0)), vec![rdb]);
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(0)));

        assert!(
            runtime.backfill_waiting_psync_pins(cursor(0)).is_empty(),
            "the C-compatible attach event must be emitted only once"
        );

        runtime.clear_psync_wait_for_main(main);
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
    }

    #[test]
    fn psync_wait_owner_query_counts_unpinned_rdb_and_grace_entries() {
        let mut runtime = ReplicationRuntime::new();
        let rdb = ConnectionId(27);
        let main = ConnectionId(28);
        assert!(!runtime.has_psync_wait_owners());

        runtime.insert_unpaired_rdb_psync_wait_for_test(rdb, boundary(0));

        assert!(runtime.has_psync_wait_owners());
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 1);

        runtime.remove_connection(rdb);
        runtime.insert_grace_psync_wait_for_test(main, i64::MAX);

        assert!(runtime.has_psync_wait_owners());
        assert_eq!(runtime.psync_wait_pin_for_main(main), None);
        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 1);

        assert_eq!(runtime.take_expired_grace_psync_waits(i64::MAX), vec![main]);
        assert!(!runtime.has_psync_wait_owners());
        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 0);
    }

    #[test]
    fn completed_unpaired_wait_survives_transport_removal_with_one_owner_and_pin() {
        let mut runtime = ReplicationRuntime::new();
        let rdb = ConnectionId(29);
        let public_client_id = 7_029;
        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(41),
                main: None,
                pin_cursor: Some(cursor(42)),
                pin_from_existing_tail: true,
            },
        );

        assert!(runtime.retain_completed_unpaired_rdb_wait(rdb, public_client_id, 2_000,));
        runtime.remove_connection(rdb);

        assert_eq!(runtime.role(rdb), None);
        assert_eq!(runtime.psync_wait_owner_count(), 1);
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(42)));
        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 1);
        assert!(runtime
            .take_expired_retained_rdb_psync_waits(1_999)
            .is_empty());
    }

    #[test]
    fn retained_waits_associate_by_public_id_in_reverse_order_and_reject_duplicates() {
        let mut runtime = ReplicationRuntime::new();
        let first_main = ConnectionId(30);
        let second_main = ConnectionId(31);
        let first_rdb = ConnectionId(40);
        let second_rdb = ConnectionId(41);
        let first_public_id = 8_040;
        let second_public_id = 8_041;

        for main in [first_main, second_main] {
            let id = runtime.register_primary(main, replica_info(6380));
            runtime
                .primary_link_mut(id)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
        }
        for (rdb, public_id, wait_boundary, pin) in [
            (first_rdb, first_public_id, boundary(11), cursor(12)),
            (second_rdb, second_public_id, boundary(21), cursor(22)),
        ] {
            runtime.register_rdb_candidate(rdb).unwrap();
            runtime.rdb_psync_waits.insert(
                rdb,
                RdbPsyncWait {
                    boundary: wait_boundary,
                    main: None,
                    pin_cursor: Some(pin),
                    pin_from_existing_tail: true,
                },
            );
            assert!(runtime.retain_completed_unpaired_rdb_wait(rdb, public_id, 5_000));
            runtime.remove_connection(rdb);
        }

        assert!(runtime
            .try_associate_retained_rdb_wait(second_main, second_public_id, 1_000)
            .is_ok());
        assert!(runtime
            .try_associate_retained_rdb_wait(first_main, first_public_id, 1_000)
            .is_ok());

        let first_link = runtime.primary_for_connection(first_main).unwrap();
        assert_eq!(
            first_link.dual_state(),
            PrimaryDualChannelState::WaitingForMainPsync
        );
        assert_eq!(first_link.dual_endoff_boundary(), Some(boundary(11)));
        assert!(first_link.dual_rdb_complete());
        let second_link = runtime.primary_for_connection(second_main).unwrap();
        assert_eq!(second_link.dual_endoff_boundary(), Some(boundary(21)));
        assert!(second_link.dual_rdb_complete());
        assert_eq!(runtime.psync_wait_owner_count(), 2);
        assert_eq!(
            runtime.psync_wait_pin_for_main(first_main),
            Some(cursor(12))
        );
        assert_eq!(
            runtime.psync_wait_pin_for_main(second_main),
            Some(cursor(22))
        );
        assert!(runtime
            .try_associate_retained_rdb_wait(second_main, first_public_id, 1_000)
            .is_err());
    }

    #[test]
    fn retained_wait_rejects_wrong_main_and_expires_before_late_association() {
        let mut runtime = ReplicationRuntime::new();
        let wrong_main = ConnectionId(50);
        let waiting_main = ConnectionId(51);
        let rdb = ConnectionId(52);
        let public_client_id = 9_052;
        runtime.register_primary(wrong_main, replica_info(6380));
        let waiting_id = runtime.register_primary(waiting_main, replica_info(6381));
        runtime
            .primary_link_mut(waiting_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(31),
                main: None,
                pin_cursor: Some(cursor(32)),
                pin_from_existing_tail: false,
            },
        );
        assert!(runtime.retain_completed_unpaired_rdb_wait(rdb, public_client_id, 2_000));
        runtime.remove_connection(rdb);

        assert!(runtime
            .try_associate_retained_rdb_wait(wrong_main, public_client_id, 1_000)
            .is_err());
        assert_eq!(runtime.psync_wait_owner_count(), 1);
        assert_eq!(
            runtime.take_expired_retained_rdb_psync_waits(2_000),
            vec![(public_client_id, None)]
        );
        assert_eq!(runtime.psync_wait_owner_count(), 0);
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
        assert!(runtime
            .try_associate_retained_rdb_wait(waiting_main, public_client_id, 2_001)
            .is_err());
    }

    #[test]
    fn retained_empty_wait_backfills_once_and_projects_to_associated_main() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(60);
        let rdb = ConnectionId(61);
        let public_client_id = 10_061;
        let id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.insert_unpaired_rdb_psync_wait_for_test(rdb, boundary(0));
        assert!(runtime.retain_completed_unpaired_rdb_wait(rdb, public_client_id, 2_000));
        runtime.remove_connection(rdb);

        assert!(runtime.backfill_waiting_psync_pins(cursor(100)).is_empty());
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(100)));
        assert!(runtime
            .try_associate_retained_rdb_wait(main, public_client_id, 1_000)
            .is_ok());
        assert_eq!(runtime.psync_wait_pin_for_main(main), Some(cursor(100)));

        runtime.abort_psync_wait_for_main(main);
        assert_eq!(runtime.psync_wait_owner_count(), 0);
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
    }

    #[test]
    fn remove_channel_pair_cleans_an_unpaired_primary_with_retained_wait() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(98);
        let retained_rdb = ConnectionId(99);
        let public_client_id = 10_099;
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        register_waiting_dual_main(&mut runtime, main);
        retain_unpaired_wait(
            &mut runtime,
            retained_rdb,
            public_client_id,
            &mut backlog,
            5_000,
        );
        runtime.backfill_waiting_psync_pins(backlog.next_stream_cursor());
        runtime
            .try_associate_retained_rdb_wait(main, public_client_id, 1_000)
            .unwrap();

        assert!(runtime.remove_channel_pair(main).is_empty());

        assert_eq!(runtime.role(main), None);
        assert!(runtime.primary_links().is_empty());
        assert_eq!(runtime.psync_wait_owner_count(), 0);
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
        assert!(runtime
            .take_expired_retained_rdb_psync_waits(i64::MAX)
            .is_empty());
    }

    #[test]
    fn retained_associated_main_does_not_admit_an_extra_rdb_candidate() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(62);
        let retained_rdb = ConnectionId(63);
        let extra_rdb = ConnectionId(64);
        let public_client_id = 10_063;
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        register_waiting_dual_main(&mut runtime, main);
        retain_unpaired_wait(
            &mut runtime,
            retained_rdb,
            public_client_id,
            &mut backlog,
            5_000,
        );
        runtime
            .try_associate_retained_rdb_wait(main, public_client_id, 1_000)
            .unwrap();
        runtime.register_rdb_candidate(extra_rdb).unwrap();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(extra_rdb, true, &mut backlog)
            .is_none());
    }

    #[test]
    fn two_waiting_mains_admit_exactly_one_candidate_after_retained_association() {
        let mut runtime = ReplicationRuntime::new();
        let first_main = ConnectionId(65);
        let second_main = ConnectionId(66);
        let retained_rdb = ConnectionId(67);
        let admitted_rdb = ConnectionId(68);
        let extra_rdb = ConnectionId(69);
        let public_client_id = 10_067;
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        register_waiting_dual_main(&mut runtime, first_main);
        register_waiting_dual_main(&mut runtime, second_main);
        retain_unpaired_wait(
            &mut runtime,
            retained_rdb,
            public_client_id,
            &mut backlog,
            5_000,
        );
        runtime
            .try_associate_retained_rdb_wait(first_main, public_client_id, 1_000)
            .unwrap();
        runtime.register_rdb_candidate(admitted_rdb).unwrap();
        runtime.register_rdb_candidate(extra_rdb).unwrap();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(admitted_rdb, true, &mut backlog)
            .is_some());
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(extra_rdb, true, &mut backlog)
            .is_none());
    }

    #[test]
    fn retained_unassociated_wait_counts_as_assigned_candidate_capacity() {
        let mut runtime = ReplicationRuntime::new();
        let mains = [ConnectionId(90), ConnectionId(91)];
        let retained_rdb = ConnectionId(92);
        let admitted_rdb = ConnectionId(93);
        let extra_rdb = ConnectionId(94);
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        for main in mains {
            register_waiting_dual_main(&mut runtime, main);
        }
        retain_unpaired_wait(&mut runtime, retained_rdb, 10_092, &mut backlog, 5_000);
        runtime.register_rdb_candidate(admitted_rdb).unwrap();
        runtime.register_rdb_candidate(extra_rdb).unwrap();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(admitted_rdb, true, &mut backlog)
            .is_some());
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(extra_rdb, true, &mut backlog)
            .is_none());
    }

    #[test]
    fn retained_unassociated_cleanup_reopens_candidate_capacity() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(95);
        let retained_rdb = ConnectionId(96);
        let pending_rdb = ConnectionId(97);
        let public_client_id = 10_096;
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        register_waiting_dual_main(&mut runtime, main);
        retain_unpaired_wait(
            &mut runtime,
            retained_rdb,
            public_client_id,
            &mut backlog,
            2_000,
        );
        runtime.register_rdb_candidate(pending_rdb).unwrap();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(pending_rdb, true, &mut backlog)
            .is_none());
        assert_eq!(
            runtime.take_expired_retained_rdb_psync_waits(2_000),
            vec![(public_client_id, None)]
        );
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(pending_rdb, true, &mut backlog)
            .is_some());
    }

    #[test]
    fn direct_cohort_pins_unassociated_rdb_candidates_before_first_tail_frame() {
        let mut runtime = ReplicationRuntime::new();
        let mains = [ConnectionId(70), ConnectionId(71), ConnectionId(72)];
        let rdbs = [ConnectionId(80), ConnectionId(81), ConnectionId(82)];
        for main in mains {
            let id = runtime.register_primary(main, replica_info(6380));
            runtime
                .primary_link_mut(id)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
        }
        for rdb in rdbs {
            runtime.register_rdb_candidate(rdb).unwrap();
        }
        let mut backlog = ReplicationBacklog::new("current".to_string(), 1);
        backlog.append(&[b'x'; 42]).unwrap();
        backlog.clear_history();
        assert_eq!(
            runtime
                .take_endoff_for_rdb_cohort(&rdbs, |rdb| Some(rdb.0 as u64), &mut backlog, 0,)
                .len(),
            3
        );
        assert_eq!(runtime.min_protected_backlog_cursor(), None);

        runtime.activate_waiting_psync_pins_for_rdb_connections(&rdbs, &mut backlog);
        backlog.append(b"tail").unwrap();

        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(42)));
        assert_eq!(backlog.read_from(cursor(42)), Some(b"tail".to_vec()));
    }

    #[test]
    fn multiple_waiting_mains_each_receive_one_rdb_endoff() {
        let mut runtime = ReplicationRuntime::new();
        let mains = [ConnectionId(10), ConnectionId(11), ConnectionId(12)];
        let rdbs = [ConnectionId(20), ConnectionId(21), ConnectionId(22)];
        for main in mains {
            let id = runtime.register_primary(main, replica_info(6380));
            let link = runtime.primary_link_mut(id).unwrap();
            link.transition_to_waiting_for_rdb_channel().unwrap();
        }
        for rdb in rdbs {
            runtime.register_rdb_candidate(rdb).unwrap();
        }
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);

        for rdb in rdbs {
            assert!(runtime
                .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog,)
                .is_some());
        }

        assert!(runtime
            .rdb_psync_waits
            .values()
            .all(|wait| wait.main.is_none()));
        assert_eq!(runtime.rdb_psync_waits.len(), 3);
    }

    #[test]
    fn dual_cohort_assigns_one_authoritative_endoff_at_snapshot_start() {
        let mut runtime = ReplicationRuntime::new();
        let mains = [ConnectionId(70), ConnectionId(71), ConnectionId(72)];
        let rdbs = [ConnectionId(80), ConnectionId(81), ConnectionId(82)];
        for main in mains {
            let id = runtime.register_primary(main, replica_info(6380));
            runtime
                .primary_link_mut(id)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
        }
        for rdb in rdbs {
            runtime.register_rdb_candidate(rdb).unwrap();
        }
        let mut backlog =
            ReplicationBacklog::new("0123456789abcdef0123456789abcdef01234567".to_string(), 32);
        backlog.append(b"abcde").unwrap();

        let public_client_id = |rdb: ConnectionId| 10_000 + rdb.0 as u64;
        let frames = runtime.take_endoff_for_rdb_cohort(
            &rdbs,
            |rdb| Some(public_client_id(rdb)),
            &mut backlog,
            7,
        );

        assert_eq!(frames.len(), 3);
        for (rdb, frame) in frames {
            let endoff = crate::engine::distributed::replication::protocol::parse_end_offset(
                &frame[..frame.len() - 2],
            )
            .expect("cohort frame must contain ENDOFF");
            assert_eq!(endoff.offset.get(), 5);
            assert_eq!(endoff.db_id, 7);
            assert_eq!(endoff.client_id, public_client_id(rdb));
            assert_eq!(runtime.rdb_psync_waits[&rdb].boundary, boundary(5));
        }
    }

    #[test]
    fn explicit_client_ids_can_pair_rdb_candidates_in_reverse_arrival_order() {
        let mut runtime = ReplicationRuntime::new();
        let first_main = ConnectionId(30);
        let second_main = ConnectionId(31);
        let first_rdb = ConnectionId(40);
        let second_rdb = ConnectionId(41);
        let first_replica = runtime.register_primary(first_main, replica_info(6380));
        let second_replica = runtime.register_primary(second_main, replica_info(6381));
        for replica_id in [first_replica, second_replica] {
            runtime
                .primary_link_mut(replica_id)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
        }
        for rdb in [first_rdb, second_rdb] {
            runtime.register_rdb_candidate(rdb).unwrap();
        }
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abc").unwrap();
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(first_rdb, true, &mut backlog,)
            .is_some());
        backlog.append(b"de").unwrap();
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(second_rdb, true, &mut backlog,)
            .is_some());

        runtime
            .associate_rdb_channel(second_main, first_rdb)
            .unwrap();
        runtime
            .finalize_dual_channel_rdb_association(second_main, first_rdb)
            .unwrap();
        runtime
            .associate_rdb_channel(first_main, second_rdb)
            .unwrap();
        runtime
            .finalize_dual_channel_rdb_association(first_main, second_rdb)
            .unwrap();

        assert_eq!(runtime.rdb_channel_for_main(first_main), Some(second_rdb));
        assert_eq!(runtime.rdb_channel_for_main(second_main), Some(first_rdb));
        assert_eq!(
            runtime
                .primary_link(first_replica)
                .unwrap()
                .dual_endoff_boundary(),
            Some(boundary(5))
        );
        assert_eq!(
            runtime
                .primary_link(second_replica)
                .unwrap()
                .dual_endoff_boundary(),
            Some(boundary(3))
        );
    }

    #[test]
    fn later_rdb_candidate_gets_endoff_after_first_pair_is_associated() {
        let mut runtime = ReplicationRuntime::new();
        let first_main = ConnectionId(50);
        let second_main = ConnectionId(51);
        let first_rdb = ConnectionId(60);
        let second_rdb = ConnectionId(61);
        let mut replicas = Vec::new();
        for main in [first_main, second_main] {
            let replica = runtime.register_primary(main, replica_info(6380));
            replicas.push(replica);
            runtime
                .primary_link_mut(replica)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
        }
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);

        runtime.register_rdb_candidate(first_rdb).unwrap();
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(first_rdb, true, &mut backlog,)
            .is_some());
        runtime
            .associate_rdb_channel(first_main, first_rdb)
            .unwrap();
        runtime
            .finalize_dual_channel_rdb_association(first_main, first_rdb)
            .unwrap();
        let replid = backlog.replid().to_string();
        assert!(runtime
            .primary_link_mut(replicas[0])
            .unwrap()
            .complete_dual_channel_main_psync(&replid, request(1), &backlog));
        runtime.complete_psync_wait_for_main(first_main);
        assert_eq!(
            runtime.rebuild_view().replicas_waiting_psync,
            0,
            "a completed main PSYNC is no longer waiting even while its RDB channel remains"
        );

        runtime.register_rdb_candidate(second_rdb).unwrap();
        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(second_rdb, true, &mut backlog,)
            .is_some());
    }

    #[test]
    fn online_dual_ack_protects_primary_backlog_cursor() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(28);
        let id = runtime.register_primary(main, replica_info(6380));
        let backlog = ReplicationBacklog::new("current".to_string(), 1);
        {
            let link = runtime.primary_link_mut(id).unwrap();
            link.set_state(PrimaryLinkState::Streaming);
            link.transition_to_waiting_for_rdb_channel().unwrap();
            link.transition_to_waiting_for_main_psync().unwrap();
            link.record_dual_channel_endoff_boundary(backlog.snapshot_boundary());
            assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
            link.mark_dual_rdb_complete();
            assert_eq!(
                link.dual_main_backlog_pin_cursor()
                    .map(|cursor| cursor.get()),
                None
            );
        }

        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            None,
            "an empty backlog has no main block reference before a real tail is attached"
        );
        runtime
            .primary_link_mut(id)
            .unwrap()
            .acknowledge(offset(1), crate::foundation::io::sys::time::mstime());
        assert!(runtime
            .primary_link_mut(id)
            .unwrap()
            .promote_dual_replica_on_ack());
        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(0)),
            "once online, a dual-channel replica protects its normal stream cursor like a C online replica"
        );
    }

    #[test]
    fn waiting_dual_ack_with_queued_main_stream_protects_backlog_cursor() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(28);
        let id = runtime.register_primary(main, replica_info(6380));
        let mut backlog = ReplicationBacklog::new("current".to_string(), 1);
        {
            let link = runtime.primary_link_mut(id).unwrap();
            link.set_state(PrimaryLinkState::Streaming);
            link.transition_to_waiting_for_rdb_channel().unwrap();
            link.transition_to_waiting_for_main_psync().unwrap();
            link.record_dual_channel_endoff_boundary(backlog.snapshot_boundary());
            assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
            let appended = backlog.append(&[b'x'; 128]).unwrap();
            assert_eq!(
                link.advance_terminal_dual_main_stream(appended.end, backlog.next_stream_cursor(),),
                Ok(true)
            );
            link.mark_replication_queued(128);
        }

        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(0)),
            "BG_RDB_LOAD dual main output must retain backlog until the terminal ACK"
        );
    }

    #[test]
    fn completing_main_psync_transfers_rdb_wait_pin_to_main_link() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(28);
        let rdb = ConnectionId(29);
        let id = runtime.register_primary(main, replica_info(6380));
        let mut backlog = ReplicationBacklog::new("current".to_string(), 1);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let replid = backlog.replid().to_string();
        let boundary = backlog.snapshot_boundary();
        {
            let link = runtime.primary_link_mut(id).unwrap();
            link.set_state(PrimaryLinkState::Streaming);
            link.transition_to_waiting_for_rdb_channel().unwrap();
            link.transition_to_waiting_for_main_psync().unwrap();
            link.record_dual_channel_endoff_boundary(boundary);
        }
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary,
                main: Some(main),
                pin_cursor: Some(cursor(42)),
                pin_from_existing_tail: false,
            },
        );
        assert!(runtime
            .primary_link_mut(id)
            .unwrap()
            .complete_dual_channel_main_psync(&replid, request(42), &backlog));

        runtime.complete_psync_wait_for_main(main);

        let link = runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_main_backlog_pin_cursor()
                .map(|cursor| cursor.get()),
            Some(42)
        );
        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(42)),
            "main link must retain the RDB wait pin until terminal ACK releases the first tail"
        );
        runtime
            .primary_link_mut(id)
            .unwrap()
            .mark_dual_rdb_complete();
        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(42)),
            "direct child completion must not release the main backlog pin before terminal ACK"
        );
        {
            let link = runtime.primary_link_mut(id).unwrap();
            link.acknowledge(offset(42), crate::foundation::io::sys::time::mstime());
            assert!(link.promote_dual_replica_on_ack());
        }
        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(42)),
            "terminal ACK promotion keeps the restored half-open main cursor"
        );
    }

    #[test]
    fn deferred_rdb_release_keeps_pin_until_grace_deadline() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(28);
        let rdb = ConnectionId(29);
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(41),
                main: Some(main),
                pin_cursor: Some(cursor(42)),
                pin_from_existing_tail: false,
            },
        );

        assert!(runtime.defer_psync_wait_for_main(main, 2_000));
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(42)));
        assert_eq!(
            runtime.rebuild_view().replicas_waiting_psync,
            1,
            "a grace-held PSYNC pin remains a wait-PSYNC member until its deadline"
        );
        assert!(runtime.take_expired_grace_psync_waits(1_999).is_empty());
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(42)));
        assert_eq!(runtime.take_expired_grace_psync_waits(2_000), vec![main]);
        assert_eq!(runtime.min_protected_backlog_cursor(), None);
        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 0);
    }

    #[test]
    fn grace_psync_pin_backfills_when_the_first_replication_frame_arrives() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(30);
        let rdb = ConnectionId(31);
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(0),
                main: Some(main),
                pin_cursor: None,
                pin_from_existing_tail: false,
            },
        );

        assert!(runtime.defer_psync_wait_for_main(main, 2_000));
        runtime.backfill_waiting_psync_pins(cursor(100));

        assert_eq!(
            runtime.min_protected_backlog_cursor(),
            Some(cursor(100)),
            "a grace-held protected RDB client must retain the first repl-buffer block"
        );
        assert_eq!(runtime.psync_wait_pin_for_main(main), Some(cursor(100)));
    }

    #[test]
    fn endoff_with_retained_backlog_pins_existing_replication_tail() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(28);
        let rdb = ConnectionId(29);
        runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_for_connection_mut(main)
            .expect("main link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main link must await an RDB channel");
        runtime.register_rdb_candidate(rdb).unwrap();

        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        // This fixture represents an already-created shared replication-buffer
        // tail. Backlog history alone is intentionally not enough to imply the
        // C block list still exists (see the companion no-tail test below).
        backlog.append(&[b'x'; 64]).unwrap();
        let first_retained_cursor = backlog.tail_block_start_cursor();

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog)
            .is_some());
        runtime
            .rdb_psync_waits
            .get_mut(&rdb)
            .expect("ENDOFF must create a PSYNC wait")
            .pin_cursor = Some(first_retained_cursor);
        backlog.set_min_protected_cursor(runtime.min_protected_backlog_cursor());
        assert_eq!(
            runtime
                .rdb_psync_waits
                .get(&rdb)
                .and_then(|wait| wait.pin_cursor),
            Some(first_retained_cursor),
            "C addRdbReplicaToPsyncWait retains the current repl-buffer tail block"
        );
        assert_eq!(
            backlog.min_protected_cursor(),
            Some(first_retained_cursor),
            "the byte backlog must protect the current tail reference immediately"
        );
    }

    #[test]
    fn endoff_ignores_backlog_history_without_a_replication_buffer_tail() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(40);
        let rdb = ConnectionId(41);
        runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_for_connection_mut(main)
            .expect("main link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main link must await an RDB channel");
        runtime.register_rdb_candidate(rdb).unwrap();

        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);

        assert!(runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(rdb, true, &mut backlog)
            .is_some());
        assert_eq!(
            runtime
                .rdb_psync_waits
                .get(&rdb)
                .and_then(|wait| wait.pin_cursor),
            None,
            "backlog history alone must not be treated as a C replication-buffer tail"
        );
    }

    #[test]
    fn rdb_filter_metadata_survives_candidate_registration_and_association() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(22);
        let rdb = ConnectionId(23);
        runtime.register_primary(main, replica_info(6380));
        runtime.stage_replconf_rdb_filter_only(rdb, RdbFilterMode::Functions);

        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.associate_rdb_channel(main, rdb).unwrap();

        assert!(
            runtime
                .primary_for_connection(main)
                .expect("main channel should remain registered")
                .replica()
                .rdb_filter_mode
                == RdbFilterMode::Functions,
            "rdb-filter-only must survive candidate registration and association"
        );
    }

    #[test]
    fn rebuild_view_reports_paired_channel_ownership_and_state() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(50);
        let rdb = ConnectionId(51);
        runtime.register_primary(main, replica_info(6380));
        runtime.register_rdb_channel(rdb, main).unwrap();
        runtime.set_channel_endpoint(rdb, "192.0.2.51".to_string(), 6381);
        runtime.set_channel_state(rdb, "sending_rdb".to_string());

        let view = runtime.rebuild_view();
        assert!(view.replica_info.is_empty());
        assert_eq!(view.replica_order, vec![main.0]);
        assert_eq!(view.info_order, vec![rdb.0, main.0]);
        assert_eq!(view.channel_info.len(), 2);
        assert_eq!(view.channel_info[&main.0].channel_type, "main-channel");
        assert_eq!(view.channel_info[&main.0].state, "wait_bgsave");
        assert_eq!(view.channel_info[&rdb.0].channel_type, "rdb-channel");
        assert_eq!(view.channel_info[&rdb.0].state, "sending_rdb");
        assert_eq!(view.channel_info[&rdb.0].ip, "192.0.2.51");
        assert_eq!(view.channel_info[&rdb.0].port, 6381);
    }

    #[test]
    fn rebuild_view_projects_waiting_dual_sync_as_two_channels() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(53);
        let rdb = ConnectionId(54);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        let link = runtime
            .primary_link_mut(replica_id)
            .expect("main channel must be registered");
        link.set_state(PrimaryLinkState::Streaming);
        link.transition_to_waiting_for_rdb_channel()
            .expect("main channel must enter the dual-channel wait state");
        runtime.register_rdb_channel(rdb, main).unwrap();

        let view = runtime.rebuild_view();

        assert!(view.replica_info.is_empty(), "{:?}", view.replica_info);
        assert_eq!(view.replica_order, vec![main.0]);
        assert_eq!(view.info_order, vec![rdb.0, main.0]);
        assert_eq!(view.channel_info.len(), 2);
        assert_eq!(view.channel_info[&main.0].channel_type, "main-channel");
        assert_eq!(view.channel_info[&main.0].state, "bg_transfer");
        assert_eq!(view.channel_info[&rdb.0].channel_type, "rdb-channel");
        assert_eq!(view.channel_info[&rdb.0].state, "wait_bgsave");
    }

    #[test]
    fn rebuild_view_preserves_registration_order_for_mixed_replicas() {
        let mut runtime = ReplicationRuntime::new();
        let classic = ConnectionId(55);
        let dual_main = ConnectionId(56);
        let dual_rdb = ConnectionId(57);

        let mut classic_info = replica_info(6381);
        classic_info.ip = "127.0.0.1".to_string();
        let classic_id = runtime.register_primary(classic, classic_info);
        runtime
            .primary_link_mut(classic_id)
            .expect("classic link must exist")
            .set_state(PrimaryLinkState::Streaming);

        let mut dual_info = replica_info(6380);
        dual_info.ip = "127.0.0.1".to_string();
        let dual_id = runtime.register_primary(dual_main, dual_info);
        let dual_link = runtime
            .primary_link_mut(dual_id)
            .expect("dual main link must exist");
        dual_link.set_state(PrimaryLinkState::Streaming);
        dual_link
            .transition_to_waiting_for_rdb_channel()
            .expect("dual main must wait for its RDB channel");
        runtime
            .register_rdb_channel(dual_rdb, dual_main)
            .expect("dual channel pair must register");

        let view = runtime.rebuild_view();

        assert_eq!(view.replica_order, vec![classic.0, dual_main.0]);
        assert_eq!(view.info_order, vec![classic.0, dual_rdb.0, dual_main.0]);
    }

    #[test]
    fn rebuild_view_keeps_dual_main_in_bg_transfer_until_ack() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(59);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        let backlog = ReplicationBacklog::new("replid".to_string(), 32);
        let link = runtime
            .primary_link_mut(replica_id)
            .expect("dual main link must be registered");
        link.set_state(PrimaryLinkState::Streaming);
        link.transition_to_waiting_for_rdb_channel()
            .expect("main link must enter dual wait state");
        link.transition_to_waiting_for_main_psync()
            .expect("main link must await main PSYNC");
        link.record_dual_channel_endoff_boundary(backlog.snapshot_boundary());
        assert!(link.complete_dual_channel_main_psync("replid", request(1), &backlog));
        link.mark_dual_rdb_complete();

        assert_eq!(main_channel_state_name(link), "bg_transfer");
    }

    #[test]
    fn dual_replica_waiting_for_terminal_ack_is_not_live() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(60);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        let link = runtime
            .primary_link_mut(replica_id)
            .expect("dual main link must be registered");
        link.set_state(PrimaryLinkState::Streaming);
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.transition_to_waiting_for_main_psync().unwrap();
        link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
            .unwrap();

        assert_eq!(runtime.live_replica_count(), 0);

        runtime
            .primary_link_mut(replica_id)
            .expect("dual main link must be registered")
            .mark_dual_rdb_complete();
        assert_eq!(
            runtime.live_replica_count(),
            0,
            "RDB completion must not bypass the terminal ACK gate"
        );
    }

    #[test]
    fn view_keeps_completed_dual_rdb_in_bg_transfer_until_terminal_ack() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(61);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        let link = runtime
            .primary_link_mut(replica_id)
            .expect("dual main link must be registered");
        link.set_state(PrimaryLinkState::Streaming);
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.transition_to_waiting_for_main_psync().unwrap();
        link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
            .unwrap();
        link.acknowledge(offset(0), 1);
        link.mark_dual_rdb_complete();

        let view = runtime.rebuild_view();

        assert_eq!(view.live_replica_count, 0);
        assert!(!view.replica_info.contains_key(&main.0));
        assert_eq!(view.channel_info[&main.0].channel_type, "main-channel");
        assert_eq!(view.channel_info[&main.0].state, "bg_transfer");
    }

    #[test]
    fn rebuild_view_keeps_normal_full_sync_in_wait_bgsave() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(58);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(replica_id)
            .expect("normal replica link must be registered")
            .set_state(PrimaryLinkState::PreparingFullResync);

        let view = runtime.rebuild_view();

        assert_eq!(view.replica_info[&main.0].state, "wait_bgsave");
    }

    #[test]
    fn rebuild_view_keeps_unpaired_candidate_separate_from_waiting_main_channels() {
        let mut runtime = ReplicationRuntime::new();
        let main_one = ConnectionId(55);
        let main_two = ConnectionId(56);
        let candidate = ConnectionId(57);
        let main_one_id = runtime.register_primary(main_one, replica_info(6380));
        let main_two_id = runtime.register_primary(main_two, replica_info(6381));

        for replica_id in [main_one_id, main_two_id] {
            let link = runtime
                .primary_link_mut(replica_id)
                .expect("main channel must be registered");
            link.set_state(PrimaryLinkState::Streaming);
            link.transition_to_waiting_for_rdb_channel()
                .expect("main channel must enter the dual-channel wait state");
        }
        runtime.register_rdb_candidate(candidate).unwrap();

        let view = runtime.rebuild_view();

        assert!(!view.replica_info.contains_key(&main_one.0));
        assert!(!view.replica_info.contains_key(&main_two.0));
        assert_eq!(view.channel_info[&main_one.0].channel_type, "main-channel");
        assert_eq!(view.channel_info[&main_one.0].state, "bg_transfer");
        assert_eq!(view.channel_info[&main_two.0].channel_type, "main-channel");
        assert_eq!(view.channel_info[&main_two.0].state, "bg_transfer");
        assert_eq!(view.channel_info.len(), 3);
        assert_eq!(view.channel_info[&candidate.0].channel_type, "rdb-channel");
        assert_eq!(runtime.channel_pair_for(main_one), None);
        assert_eq!(runtime.channel_pair_for(main_two), None);
    }

    #[test]
    fn rebuild_view_reports_unpaired_rdb_channel_ownership_and_state() {
        let mut runtime = ReplicationRuntime::new();
        let rdb = ConnectionId(52);
        runtime.register_rdb_candidate(rdb).unwrap();

        let view = runtime.rebuild_view();
        assert!(!view.replica_info.contains_key(&rdb.0));
        assert_eq!(view.channel_info[&rdb.0].channel_type, "rdb-channel");
        assert_eq!(view.channel_info[&rdb.0].state, "wait_bgsave");
        assert_eq!(view.channel_info[&rdb.0].ip, "");
        assert_eq!(view.channel_info[&rdb.0].port, 0);
    }

    #[test]
    fn stale_channel_token_cannot_inherit_a_previous_association() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(30);
        let old_rdb = ConnectionId(31);
        runtime.register_primary(main, replica_info(6380));
        runtime
            .register_rdb_channel(old_rdb, main)
            .expect("initial pair");

        runtime.remove_connection(old_rdb);
        let replacement = runtime.register_primary(old_rdb, replica_info(6381));

        assert_eq!(
            runtime.role(old_rdb),
            Some(ReplicationConnection::Primary(replacement))
        );
        assert_eq!(runtime.rdb_channel_for_main(main), None);
    }

    #[test]
    fn clearing_rdb_channel_preserves_the_main_link() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(40);
        let rdb = ConnectionId(41);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime.register_rdb_channel(rdb, main).unwrap();
        runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .record_dual_channel_endoff_boundary(boundary(17));

        assert_eq!(runtime.clear_rdb_channel(rdb), vec![rdb]);
        let main_link = runtime
            .primary_link(replica_id)
            .expect("main link must remain registered");
        assert_eq!(
            runtime.role(main),
            Some(ReplicationConnection::Primary(replica_id))
        );
        assert_eq!(main_link.dual_endoff_boundary(), None);
        assert_eq!(runtime.role(rdb), None);
        assert_eq!(runtime.rdb_channel_for_main(main), None);
    }

    #[test]
    fn closing_rdb_after_dual_replica_is_online_releases_wait_pin() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(42);
        let rdb = ConnectionId(43);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime.register_rdb_channel(rdb, main).unwrap();
        let link = runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist");
        link.set_state(PrimaryLinkState::Streaming);
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(17));
        link.transition_to_waiting_for_main_psync().unwrap();
        link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
            .unwrap();
        link.transition_dual_state(PrimaryDualChannelState::Online)
            .unwrap();
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(17),
                main: Some(main),
                pin_cursor: Some(cursor(17)),
                pin_from_existing_tail: false,
            },
        );

        assert_eq!(runtime.detach_rdb_channel_preserving_main(rdb), Some(main));

        assert_eq!(runtime.rebuild_view().replicas_waiting_psync, 0);
        assert!(!runtime.grace_psync_pin_by_main.contains_key(&main));
    }

    #[test]
    fn dual_rdb_waiting_still_pins_the_replication_backlog() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(44);
        let replica_id = runtime.register_primary(main, replica_info(6380));
        runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("dual main link must await an RDB channel");
        let rdb = ConnectionId(46);
        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(0),
                main: Some(main),
                pin_cursor: Some(cursor(0)),
                pin_from_existing_tail: false,
            },
        );
        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(0)));
        let mut classic = ReplicationRuntime::new();
        let classic_id = classic.register_primary(ConnectionId(45), replica_info(6381));
        classic
            .primary_link_mut(classic_id)
            .expect("classic link must exist")
            .set_state(PrimaryLinkState::PreparingFullResync);
        assert_eq!(classic.min_protected_backlog_cursor(), None);
    }

    #[test]
    fn replacing_upstream_cleans_old_mapping() {
        let mut runtime = ReplicationRuntime::new();
        runtime.set_replica_link(upstream(5));
        runtime.set_replica_main_read_paused(true);
        runtime.set_replica_link(upstream(6));
        assert_eq!(runtime.role(ConnectionId(5)), None);
        assert_eq!(
            runtime.role(ConnectionId(6)),
            Some(ReplicationConnection::Replica)
        );
        assert_eq!(
            runtime.replica_link().map(ReplicaLink::connection_id),
            Some(ConnectionId(6))
        );
        assert!(
            !runtime.replica_main_read_paused(),
            "a replacement upstream must not inherit the old session's pause"
        );
    }

    #[test]
    fn removing_active_replica_main_or_rdb_clears_session_pause() {
        let mut main_removed = ReplicationRuntime::new();
        main_removed.set_replica_link(upstream(11));
        main_removed.set_replica_main_read_paused(true);

        main_removed.remove_connection(ConnectionId(11));

        assert!(!main_removed.replica_main_read_paused());
        main_removed.set_replica_link(upstream(12));
        assert!(!main_removed.replica_main_read_paused());

        let mut rdb_removed = ReplicationRuntime::new();
        rdb_removed.set_replica_link(upstream(21));
        rdb_removed
            .set_replica_rdb_link(ConnectionId(22), ConnectionId(21))
            .unwrap();
        rdb_removed.set_replica_main_read_paused(true);

        rdb_removed.remove_connection(ConnectionId(22));

        assert!(!rdb_removed.replica_main_read_paused());
    }

    #[test]
    fn unrelated_removal_and_same_session_reinstall_preserve_pause() {
        let mut runtime = ReplicationRuntime::new();
        runtime.set_replica_link(upstream(31));
        runtime.set_replica_main_read_paused(true);
        let downstream = runtime.register_primary(ConnectionId(32), replica_info(6380));

        runtime.remove_connection(ConnectionId(32));

        assert!(runtime.primary_link(downstream).is_none());
        assert!(runtime.replica_main_read_paused());

        let active = runtime.take_replica_link().unwrap();
        runtime.set_replica_link(active);
        assert!(runtime.replica_main_read_paused());
    }

    #[test]
    fn role_transition_removes_stale_primary_link() {
        let mut runtime = ReplicationRuntime::new();
        let id = runtime.register_primary(ConnectionId(8), replica_info(6380));
        runtime.set_replica_link(upstream(8));
        assert!(runtime.primary_link(id).is_none());
        assert_eq!(
            runtime.role(ConnectionId(8)),
            Some(ReplicationConnection::Replica)
        );
    }

    #[test]
    fn removing_synced_upstream_preserves_resume_position() {
        let mut runtime = ReplicationRuntime::new();
        runtime.set_replica_link(ReplicaLink::streaming(
            ConnectionId(9),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "replid".to_string(),
            offset(42),
        ));
        runtime.remove_connection(ConnectionId(9));
        let resume = runtime
            .replica_resume_for(&MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            })
            .expect("streaming disconnect must preserve cached resume");
        assert_eq!(resume, ("replid", offset(42), 0));
        assert_eq!(
            std::any::type_name_of_val(&resume.1),
            std::any::type_name::<crate::engine::distributed::replication::ReplicationOffset>()
        );
    }

    #[test]
    fn removing_synced_upstream_captures_selected_db() {
        // A dropped master link must remember the DB its stream had reached so a
        // partial-resync reconnect can resume on the right DB (§5.6).
        let mut runtime = ReplicationRuntime::new();
        let mut link = ReplicaLink::streaming(
            ConnectionId(9),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "replid".to_string(),
            offset(42),
        );
        link.set_selected_db(9);
        runtime.set_replica_link(link);
        runtime.remove_connection(ConnectionId(9));
        assert_eq!(
            runtime.replica_resume_for(&MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            }),
            Some(("replid", offset(42), 9))
        );
    }

    #[test]
    fn clearing_replica_session_preserves_captured_resume_position() {
        let mut runtime = ReplicationRuntime::new();
        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let mut link = ReplicaLink::streaming(
            ConnectionId(9),
            master.clone(),
            "replid".to_string(),
            offset(42),
        );
        link.set_selected_db(9);
        runtime.set_replica_link(link);

        runtime.remove_connection(ConnectionId(9));
        runtime.clear_replica_session();

        assert_eq!(
            runtime.replica_resume_for(&master),
            Some(("replid", offset(42), 9)),
            "session cleanup must not discard the cached-primary resume cursor"
        );
    }

    #[test]
    fn clear_replica_session_returns_pending_buffer_for_l5_disposal() {
        let mut runtime = ReplicationRuntime::new();
        let link = upstream(9);
        let session_id = link.session_id();
        runtime.set_replica_link(link);
        let mut pending = PendingReplicationBuffer::new(usize::MAX, offset(100), 0);
        pending
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Ping,
                end_offset: offset(101),
            })
            .unwrap();
        let _ = runtime.set_replica_pending_buffer(session_id, pending);

        let pending = runtime.clear_replica_session();

        assert_eq!(pending.as_ref().map(|buffer| buffer.len()), Some(1));
        assert!(runtime.replica_pending_buffer().is_none());
        assert!(runtime.clear_replica_session().is_none());
    }

    #[test]
    fn pending_replication_buffers_are_scoped_to_replica_sessions() {
        let mut runtime = ReplicationRuntime::new();
        let original = upstream(31);
        let original_session_id = original.session_id();
        runtime.set_replica_link(original);
        let mut original_pending = PendingReplicationBuffer::new(usize::MAX, offset(100), 0);
        original_pending
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Ping,
                end_offset: offset(101),
            })
            .unwrap();
        assert!(runtime
            .set_replica_pending_buffer(original_session_id, original_pending)
            .is_none());

        let replacement = upstream(31);
        let replacement_session_id = replacement.session_id();
        runtime.set_replica_link(replacement);
        let mut replacement_pending = PendingReplicationBuffer::new(usize::MAX, offset(200), 0);
        replacement_pending
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Ping,
                end_offset: offset(201),
            })
            .unwrap();
        let mut displaced = runtime
            .set_replica_pending_buffer(replacement_session_id, replacement_pending)
            .expect("replacement installation returns the old-session buffer");

        assert_eq!(displaced.pop_front().unwrap().end_offset().get(), 101);
        assert!(runtime
            .take_replica_pending_buffer_for(original_session_id)
            .is_none());
        assert_eq!(
            runtime
                .take_replica_pending_buffer_for(replacement_session_id)
                .unwrap()
                .pop_front()
                .unwrap()
                .end_offset()
                .get(),
            201
        );
    }

    #[test]
    fn dropping_link_before_streaming_does_not_cache_resume_position() {
        // A mid-RDB short read tears the link down while it still knows the
        // master's replid (from +FULLRESYNC) but has not reached the streaming
        // phase. Caching a resume position here would drive a bogus +CONTINUE
        // partial resync against an unloaded dataset; the reconnect must instead
        // issue a fresh full sync (`PSYNC ? -1`).
        let mut runtime = ReplicationRuntime::new();
        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let mut link = ReplicaLink::new(ConnectionId(9), master.clone());
        // Knows the replid/offset (+FULLRESYNC parsed) but is still receiving the
        // RDB, so `is_streaming()` is false.
        link.set_resume_position("replid".to_string(), offset(42), 0);
        assert!(!link.is_streaming());
        runtime.set_replica_link(link);
        runtime.remove_connection(ConnectionId(9));
        assert_eq!(runtime.replica_resume_for(&master), None);
    }

    #[test]
    fn dropping_uncommitted_eof_snapshot_does_not_cache_resume_position() {
        let mut runtime = ReplicationRuntime::new();
        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let mut link = ReplicaLink::new(ConnectionId(9), master.clone());
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380).unwrap();
        let marker = "0123456789abcdef0123456789abcdef01234567";
        let full_sync = format!("+FULLRESYNC {marker} 42\r\n$EOF:{marker}\r\n{marker}");
        link.on_bytes(full_sync.as_bytes(), 6380).unwrap();
        let _failed_commit = link.take_rdb_receive().unwrap();
        assert!(link.is_streaming());

        runtime.set_replica_link(link);
        runtime.remove_connection(ConnectionId(9));

        assert_eq!(runtime.replica_resume_for(&master), None);
    }

    #[test]
    fn dropping_dual_main_before_rdb_load_preserves_previous_master_resume() {
        let mut runtime = ReplicationRuntime::new();
        let previous_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let next_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        };
        let mut link = ReplicaLink::new(ConnectionId(9), next_master.clone());
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
            .unwrap();
        link.provisional_primary = Some(ProvisionalPrimary {
            replid: "replid".to_string(),
            end_offset: offset(42),
            db_id: 3,
            rdb_client_id: 7,
        });
        runtime.set_replica_resume(
            previous_master.clone(),
            "old-committed".to_string(),
            offset(7),
            0,
        );
        runtime.set_replica_link(link);

        runtime.remove_connection(ConnectionId(9));

        assert_eq!(
            runtime.replica_resume_for(&previous_master),
            Some(("old-committed", offset(7), 0)),
            "C keeps the cached primary until the replacement RDB starts loading"
        );
        assert_eq!(runtime.replica_resume_for(&next_master), None);
        assert!(!runtime.take_force_full_sync());
    }

    #[test]
    fn dropping_main_after_dual_rdb_load_preserves_psync_resume_position() {
        let mut runtime = ReplicationRuntime::new();
        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let mut link = ReplicaLink::new(ConnectionId(10), master.clone());
        link.set_resume_position("replid".to_string(), offset(42), 3);
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
            .unwrap();
        runtime.set_replica_link(link);

        runtime.remove_connection(ConnectionId(10));

        assert_eq!(
            runtime.replica_resume_for(&master),
            Some(("replid", offset(42), 3)),
            "a committed dual RDB must retry PSYNC with its ENDOFF resume position"
        );
    }

    #[test]
    fn authentication_rejection_is_scoped_to_exact_credentials() {
        let mut runtime = ReplicationRuntime::new();
        let rejected = AuthenticationAttempt {
            host: "127.0.0.1".to_string(),
            port: 6379,
            user: None,
            password: Some(b"wrong".to_vec()),
        };
        runtime.reject_authentication(rejected.clone());

        assert!(runtime.authentication_is_rejected(&rejected));

        let changed = AuthenticationAttempt {
            password: Some(b"correct".to_vec()),
            ..rejected
        };
        assert!(!runtime.authentication_is_rejected(&changed));
    }

    #[test]
    fn backoff_reconnect_causes_delay_then_allow_retry() {
        let mut runtime = ReplicationRuntime::new();
        assert!(runtime.master_connect_allowed(10_000));

        for cause in [
            MasterReconnectCause::HandshakeFailed,
            MasterReconnectCause::TryLater,
            MasterReconnectCause::ConnectFailed,
            MasterReconnectCause::ResolveFailed,
            MasterReconnectCause::ClusterPrimaryUnavailable,
            MasterReconnectCause::AdministrativeClientKill,
        ] {
            runtime.schedule_master_reconnect(10_000, cause);
            assert!(!runtime.master_connect_allowed(10_999), "{cause:?}");
            assert!(runtime.master_connect_allowed(11_000), "{cause:?}");
        }
        runtime.reset_master_reconnect_backoff();
        assert!(runtime.master_connect_allowed(10_001));
    }

    #[test]
    fn immediate_reconnect_causes_allow_retry_without_delay() {
        let mut runtime = ReplicationRuntime::new();

        for cause in [
            MasterReconnectCause::RdbTransferFailed,
            MasterReconnectCause::EstablishedLinkLost,
            MasterReconnectCause::ReplicationTimeout,
            MasterReconnectCause::ExplicitTargetChange,
        ] {
            runtime.schedule_master_reconnect(10_000, cause);
            assert!(runtime.master_connect_allowed(10_000), "{cause:?}");
        }
    }

    #[test]
    fn immediate_reconnect_replaces_an_existing_backoff() {
        let mut runtime = ReplicationRuntime::new();
        runtime.schedule_master_reconnect(10_000, MasterReconnectCause::ConnectFailed);

        runtime.schedule_master_reconnect(10_100, MasterReconnectCause::RdbTransferFailed);

        assert!(runtime.master_connect_allowed(10_100));
    }

    #[test]
    fn live_replica_count_only_counts_streaming_links() {
        let mut runtime = ReplicationRuntime::new();
        let id1 = runtime.register_primary(ConnectionId(1), replica_info(6380));
        let id2 = runtime.register_primary(ConnectionId(2), replica_info(6381));

        assert_eq!(runtime.live_replica_count(), 0);

        if let Some(link) = runtime.primary_link_mut(id1) {
            link.set_state(PrimaryLinkState::Streaming);
        }
        assert_eq!(runtime.live_replica_count(), 1);

        if let Some(link) = runtime.primary_link_mut(id2) {
            link.set_state(PrimaryLinkState::PreparingFullResync);
        }
        assert_eq!(runtime.live_replica_count(), 1);
    }

    #[test]
    fn connected_replica_count_includes_dual_replication_channels() {
        let mut runtime = ReplicationRuntime::new();
        let online = ConnectionId(1);
        let syncing = ConnectionId(2);
        let syncing_rdb = ConnectionId(3);
        let online_id = runtime.register_primary(online, replica_info(6380));
        runtime
            .primary_link_mut(online_id)
            .unwrap()
            .set_state(PrimaryLinkState::Streaming);
        let syncing_id = runtime.register_primary(syncing, replica_info(6381));
        runtime
            .primary_link_mut(syncing_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        runtime.register_rdb_channel(syncing_rdb, syncing).unwrap();

        assert_eq!(runtime.rebuild_view().connected_replica_count, 3);
    }

    #[test]
    fn connected_replica_count_keeps_main_after_rdb_channel_closes() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(4);
        let id = runtime.register_primary(main, replica_info(6380));
        let link = runtime.primary_link_mut(id).expect("main link must exist");
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.transition_to_waiting_for_main_psync().unwrap();
        link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
            .unwrap();

        assert_eq!(runtime.rebuild_view().connected_replica_count, 1);
    }

    #[test]
    fn main_first_grace_pin_survives_runtime_connection_removal() {
        let mut runtime = ReplicationRuntime::new();
        let main = ConnectionId(10);
        let rdb = ConnectionId(11);
        let id = runtime.register_primary(main, replica_info(6380));
        runtime.register_rdb_candidate(rdb).unwrap();
        runtime.rdb_psync_waits.insert(
            rdb,
            RdbPsyncWait {
                boundary: boundary(41),
                main: Some(main),
                pin_cursor: Some(cursor(42)),
                pin_from_existing_tail: false,
            },
        );
        runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();

        assert!(runtime.defer_psync_wait_for_main(main, 2_000));
        runtime.remove_connection(main);

        assert_eq!(runtime.psync_wait_pin_for_main(main), Some(cursor(42)));
    }

    #[test]
    fn replica_resume_is_scoped_to_the_same_master() {
        let mut runtime = ReplicationRuntime::new();
        let original_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        runtime.set_replica_link(ReplicaLink::streaming(
            ConnectionId(20),
            original_master.clone(),
            "replid".to_string(),
            offset(42),
        ));
        runtime.remove_connection(ConnectionId(20));

        assert_eq!(
            runtime.replica_resume_for(&original_master),
            Some(("replid", offset(42), 0))
        );
        assert_eq!(
            runtime.replica_resume_for(&MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6380,
            }),
            None
        );
    }

    #[test]
    fn explicit_resume_position_is_scoped_to_the_new_master() {
        let mut runtime = ReplicationRuntime::new();
        let new_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        };

        runtime.set_replica_resume(new_master.clone(), "new-replid".to_string(), offset(84), 0);

        assert_eq!(
            runtime.replica_resume_for(&new_master),
            Some(("new-replid", offset(84), 0))
        );
        assert_eq!(
            runtime.replica_resume_for(&MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            }),
            None
        );
    }

    #[test]
    fn clearing_resume_position_forces_full_sync_for_new_master() {
        let mut runtime = ReplicationRuntime::new();
        let new_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        };
        runtime.set_replica_resume(
            new_master.clone(),
            "stale-replid".to_string(),
            offset(84),
            0,
        );

        runtime.clear_replica_resume();

        assert_eq!(runtime.replica_resume_for(&new_master), None);
    }

    #[test]
    fn count_acked_replicas_filters_by_offset_state_and_lag() {
        let mut runtime = ReplicationRuntime::new();
        let id1 = runtime.register_primary(ConnectionId(10), replica_info(6380));
        let id2 = runtime.register_primary(ConnectionId(11), replica_info(6381));
        let id3 = runtime.register_primary(ConnectionId(12), replica_info(6382));

        // All three streaming; id1 acked to 100 (t=1000), id2 acked to 50 (t=5000),
        // id3 still in handshake.
        for &(id, state) in &[
            (id1, PrimaryLinkState::Streaming),
            (id2, PrimaryLinkState::Streaming),
            (id3, PrimaryLinkState::PreparingFullResync),
        ] {
            if let Some(l) = runtime.primary_link_mut(id) {
                l.set_state(state);
            }
        }
        if let Some(l) = runtime.primary_link_mut(id1) {
            l.acknowledge(offset(100), 1000);
        }
        if let Some(l) = runtime.primary_link_mut(id2) {
            l.acknowledge(offset(50), 5000);
        }

        // No lag filter: both streaming replicas with offset >= 50
        assert_eq!(runtime.count_acked_replicas(offset(50), 0, 9999), 2);
        // min_offset 80 excludes id2
        assert_eq!(runtime.count_acked_replicas(offset(80), 0, 9999), 1);
        // With max_lag_ms=1000 at now=6000: id1 acked at 1000 (lag=5000, excluded);
        // id2 acked at 5000 (lag=1000, included, lag <= 1000)
        assert_eq!(runtime.count_acked_replicas(offset(50), 1000, 6000), 1);
        // max_lag_ms=0 disables lag check
        assert_eq!(runtime.count_acked_replicas(offset(50), 0, 6000), 2);
    }
}
