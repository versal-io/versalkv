use super::ServerWorkerState;
use crate::engine::extension::config::persistence::BgsaveMethod;
use crate::foundation::io::persistence::rdb::compression::{RdbCompression, RdbSnapshotOptions};
use crate::foundation::io::persistence::rdb::frozen::FrozenRdb;
use crate::foundation::io::persistence::RdbSerializeOptions;
use crate::framework::distributed::replication::{ConnectionId, RdbFilterMode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FullSyncTarget {
    File,
    Socket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FullSyncChannel {
    Classic,
    Dual,
}

impl FullSyncChannel {
    fn label(self) -> &'static str {
        match self {
            Self::Classic => "normal sync",
            Self::Dual => "dual-channel",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForklessFallback {
    Module,
    SlotImport,
    ExcludeData,
    MissingInband,
    Compression,
}

impl ForklessFallback {
    fn reason(self) -> &'static str {
        match self {
            Self::Module => "a loaded module has not declared VALKEYMODULE_OPTIONS_HANDLE_FORKLESS",
            Self::SlotImport => "an active slot import is in progress",
            Self::ExcludeData => "cohort requested an RDB without keyspace data",
            Self::MissingInband => "replica does not support inline replication",
            Self::Compression => "cohort needs stream compression",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FullSyncSavePlan {
    pub(crate) target: FullSyncTarget,
    pub(crate) channel: FullSyncChannel,
    pub(crate) method: BgsaveMethod,
    pub(crate) fallback: Option<ForklessFallback>,
}

impl ServerWorkerState {
    fn full_sync_channel(&self, connection: ConnectionId) -> FullSyncChannel {
        match self.replication_runtime.role(connection) {
            Some(
                crate::framework::distributed::replication::ReplicationConnection::RdbChannelCandidate
                | crate::framework::distributed::replication::ReplicationConnection::RdbChannel { .. },
            ) => FullSyncChannel::Dual,
            _ => FullSyncChannel::Classic,
        }
    }

    pub(crate) fn full_sync_target(
        &self,
        connection: ConnectionId,
        snapshot: RdbSnapshotOptions,
    ) -> FullSyncTarget {
        let (filter, supports_eof) = self
            .replication_runtime
            .replica_sync_preferences(connection);
        let version = self
            .replication_runtime
            .replica_snapshot_version(connection);
        // RDB 81 was introduced in Valkey 9.2. Non-EOF peers always use the
        // current on-disk format, as in replicaRdbVersion in the reference.
        let needs_wire_format = self.full_sync_channel(connection) == FullSyncChannel::Dual
            || filter != RdbFilterMode::All
            || (supports_eof && version < 0x090200)
            || snapshot.compression != self.ctx.config().persistence.rdbcompression;
        if needs_wire_format || (supports_eof && self.ctx.config().replication.repl_diskless_sync) {
            FullSyncTarget::Socket
        } else {
            FullSyncTarget::File
        }
    }

    pub(crate) fn full_sync_save_plan(
        &self,
        connections: &[ConnectionId],
        snapshot: RdbSnapshotOptions,
    ) -> FullSyncSavePlan {
        let target = connections
            .first()
            .map_or(FullSyncTarget::File, |connection| {
                self.full_sync_target(*connection, snapshot)
            });
        let channel = connections
            .first()
            .map_or(FullSyncChannel::Classic, |connection| {
                self.full_sync_channel(*connection)
            });
        let requested = self.ctx.config().persistence.bgsave_default_method;
        let fallback = if requested != BgsaveMethod::Forkless {
            None
        } else if !self
            .ctx
            .module_registry()
            .list()
            .all(|module| module.options.get() & (1 << 6) != 0)
        {
            Some(ForklessFallback::Module)
        } else if !self.ctx.slot_import_dbs().is_empty() {
            Some(ForklessFallback::SlotImport)
        } else if target == FullSyncTarget::Socket
            && connections.iter().any(|connection| {
                self.replication_runtime
                    .replica_sync_preferences(*connection)
                    .0
                    != RdbFilterMode::All
            })
        {
            Some(ForklessFallback::ExcludeData)
        } else if target == FullSyncTarget::Socket
            && connections.iter().any(|connection| {
                let capabilities = self
                    .replication_runtime
                    .primary_for_connection(*connection)
                    .map(|link| link.replica().capability.as_slice())
                    .or_else(|| self.replication_runtime.channel_capabilities(*connection));
                capabilities.is_none_or(|capabilities| {
                    !super::replication::peer_supports_capability(capabilities, "inband-repl")
                })
            })
        {
            Some(ForklessFallback::MissingInband)
        } else if snapshot.compression == RdbCompression::Lz4 {
            Some(ForklessFallback::Compression)
        } else {
            None
        };
        FullSyncSavePlan {
            target,
            channel,
            method: if fallback.is_some() {
                BgsaveMethod::Fork
            } else {
                requested
            },
            fallback,
        }
    }

    pub(crate) fn record_full_sync_plan(
        &mut self,
        plan: FullSyncSavePlan,
        snapshot: RdbSnapshotOptions,
    ) {
        if let Some(fallback) = plan.fallback {
            let stats = self.ctx.stats_mut();
            match fallback {
                ForklessFallback::Module => stats.forkless_fallbacks_module += 1,
                ForklessFallback::SlotImport => stats.forkless_fallbacks_slot_import += 1,
                ForklessFallback::ExcludeData => stats.forkless_fallbacks_exclude_data += 1,
                ForklessFallback::MissingInband => {
                    stats.forkless_fallbacks_missing_inband_repl += 1
                }
                ForklessFallback::Compression => stats.forkless_fallbacks_compression += 1,
            }
            crate::server_log!(
                notice,
                "Falling back to fork-based save for SYNC: {}",
                fallback.reason()
            );
        }
        crate::server_log!(
            notice,
            "Starting BGSAVE for SYNC with target: {} using: {}, compression: {}, method: {}",
            match plan.target {
                FullSyncTarget::File => "disk",
                FullSyncTarget::Socket => "replicas sockets",
            },
            plan.channel.label(),
            if snapshot.compression == RdbCompression::Lz4 {
                "lz4"
            } else {
                "none"
            },
            plan.method.as_str(),
        );
    }

    pub(crate) fn full_sync_replication_info(
        &self,
    ) -> crate::foundation::io::persistence::RdbReplicationInfo {
        let mut replication = self.ctx.rdb_full_sync_replication_info();
        let boundary = self.ctx.replication().backlog.snapshot_boundary();
        replication.repl_id = self.ctx.replication().backlog.replid().to_string();
        replication.repl_offset = boundary.reported_offset().get();
        if self.ctx.replication().is_replica() {
            // A chained replica relays the upstream stream without adding SELECT.
            // Downstream PSYNC resets repl_seldb, not the upstream client's DB.
            // The connection role survives while replica apply owns the link.
            let selected_db = self
                .replication_runtime
                .replica_link()
                .map(|link| link.selected_db())
                .or_else(|| {
                    self.connection_registry.iter().find_map(|(_, connection)| {
                        matches!(
                            self.replication_runtime
                                .role(ConnectionId(connection.token().0)),
                            Some(crate::framework::distributed::replication::ReplicationConnection::Replica)
                        )
                        .then_some(connection.selected_db)
                    })
                })
                .or_else(|| {
                    let config = &self.ctx.config().replication;
                    let master = crate::framework::distributed::replication::MasterInfo {
                        host: config.master_host.as_ref()?.clone(),
                        port: config.master_port,
                    };
                    self.replication_runtime
                        .replica_resume_for(&master)
                        .map(|(_, _, selected_db)| selected_db)
                });
            if let Some(selected_db) = selected_db {
                replication.repl_stream_db = i64::from(selected_db);
            }
        }
        replication
    }

    pub(crate) fn capture_full_sync(
        &self,
        libraries: &[Vec<u8>],
        snapshot: RdbSnapshotOptions,
    ) -> FrozenRdb {
        let replication = self.full_sync_replication_info();
        FrozenRdb::capture(
            self.ctx.dbs(),
            &[],
            libraries,
            Some(&replication),
            RdbSerializeOptions {
                compression: snapshot.compression.uses_lzf(),
                checksum: snapshot.checksum,
            },
            crate::foundation::io::sys::time::mstime().max(0) as u64,
            true,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::distributed::replication::ReplicaConnectionInfo;

    #[test]
    fn chained_snapshot_uses_upstream_connection_while_link_is_taken() {
        use crate::engine::distributed::replication::ReplicationOffset;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;
        use crate::framework::distributed::replication::{MasterInfo, ReplicaLink};

        let mut worker = ServerWorkerState::new(16).unwrap();
        worker.ctx.replication_mut().switch_to_replica();
        let connection = ConnectionId(40);
        let mut client = Connection::new_fake();
        client.set_token_for_test(Token(connection.0));
        client.selected_db = 9;
        worker.connection_registry.insert(client);
        let mut link = ReplicaLink::streaming(
            connection,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            worker.ctx.replication().backlog.replid().to_string(),
            ReplicationOffset::ZERO,
        );
        link.set_selected_db(9);
        worker.replication_runtime.set_replica_link(link);
        worker.ctx.reset_repl_seldb();
        let expected = worker.full_sync_replication_info();
        let _link = worker.replication_runtime.take_replica_link().unwrap();

        assert_eq!(expected.repl_stream_db, 9);
        assert_eq!(worker.full_sync_replication_info(), expected);
        assert_eq!(worker.ctx.repl_seldb(), -1);
    }

    #[test]
    fn primary_snapshot_preserves_outgoing_stream_database() {
        let mut worker = ServerWorkerState::new(16).unwrap();
        worker.ctx.set_repl_seldb(9);
        assert_eq!(worker.full_sync_replication_info().repl_stream_db, 9);
        worker.ctx.reset_repl_seldb();
        assert_eq!(worker.full_sync_replication_info().repl_stream_db, 0);
    }

    #[test]
    fn chained_snapshot_uses_matching_cached_upstream_database() {
        use crate::engine::distributed::replication::ReplicationOffset;
        use crate::framework::distributed::replication::MasterInfo;

        let mut worker = ServerWorkerState::new(16).unwrap();
        worker.ctx.replication_mut().switch_to_replica();
        worker.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        worker.ctx.config_mut().replication.master_port = 6379;
        worker.replication_runtime.set_replica_resume(
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            worker.ctx.replication().backlog.replid().to_string(),
            ReplicationOffset::ZERO,
            9,
        );
        worker.ctx.reset_repl_seldb();

        assert_eq!(worker.full_sync_replication_info().repl_stream_db, 9);
        worker.ctx.config_mut().replication.master_port = 6380;
        assert_eq!(worker.full_sync_replication_info().repl_stream_db, 0);
        worker.ctx.config_mut().replication.master_port = 6379;
        worker.replication_runtime.clear_replica_resume();
        assert_eq!(worker.full_sync_replication_info().repl_stream_db, 0);
    }

    fn fixture(
        filter: RdbFilterMode,
        inband: bool,
    ) -> (ServerWorkerState, ConnectionId, RdbSnapshotOptions) {
        let mut worker = ServerWorkerState::new(1).unwrap();
        worker.ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
        worker.ctx.config_mut().persistence.rdbcompression = RdbCompression::No;
        let connection = ConnectionId(42);
        let mut capability = vec!["eof".to_string()];
        if inband {
            capability.push("inband-repl".to_string());
        }
        let id = worker.replication_runtime.register_primary(
            connection,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability,
                rdb_filter_mode: filter,
            },
        );
        worker
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .set_replica_version(0x090200);
        (
            worker,
            connection,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
    }

    #[test]
    fn disk_sync_does_not_require_inband_and_lzf_is_not_stream_compression() {
        let (mut worker, connection, mut snapshot) = fixture(RdbFilterMode::All, false);
        worker.ctx.config_mut().replication.repl_diskless_sync = false;
        worker.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lzf;
        snapshot.compression = RdbCompression::Lzf;
        let plan = worker.full_sync_save_plan(&[connection], snapshot);
        assert_eq!(plan.target, FullSyncTarget::File);
        assert_eq!(plan.method, BgsaveMethod::Forkless);
        assert_eq!(plan.fallback, None);
    }

    #[test]
    fn socket_fallback_counts_only_the_first_applicable_reason() {
        let (mut worker, connection, mut snapshot) = fixture(RdbFilterMode::Functions, false);
        worker.ctx.config_mut().replication.repl_diskless_sync = true;
        snapshot.compression = RdbCompression::Lz4;
        let plan = worker.full_sync_save_plan(&[connection], snapshot);
        assert_eq!(plan.fallback, Some(ForklessFallback::ExcludeData));
        worker.record_full_sync_plan(plan, snapshot);
        assert_eq!(worker.ctx.stats().forkless_fallbacks_exclude_data, 1);
        assert_eq!(worker.ctx.stats().forkless_fallbacks_missing_inband_repl, 0);
        assert_eq!(worker.ctx.stats().forkless_fallbacks_compression, 0);
        worker.ctx.stats_mut().reset();
        assert_eq!(worker.ctx.stats().forkless_fallbacks_exclude_data, 0);
    }

    #[test]
    fn old_rdb_version_forces_socket_and_a_real_fork_fallback() {
        let (mut worker, connection, snapshot) = fixture(RdbFilterMode::All, false);
        worker.ctx.config_mut().replication.repl_diskless_sync = false;
        worker
            .replication_runtime
            .primary_for_connection_mut(connection)
            .unwrap()
            .set_replica_version(0x080000);
        let plan = worker.full_sync_save_plan(&[connection], snapshot);
        assert_eq!(plan.target, FullSyncTarget::Socket);
        assert_eq!(plan.method, BgsaveMethod::Fork);
        assert_eq!(plan.fallback, Some(ForklessFallback::MissingInband));
    }

    #[test]
    fn dedicated_rdb_channel_forces_socket_and_uses_its_own_capabilities() {
        let (mut worker, _, snapshot) = fixture(RdbFilterMode::All, false);
        worker.ctx.config_mut().replication.repl_diskless_sync = false;
        let connection = ConnectionId(43);
        worker
            .replication_runtime
            .register_rdb_candidate(connection)
            .unwrap();
        worker
            .replication_runtime
            .set_channel_endpoint(connection, "127.0.0.1".to_string(), 6381);
        worker
            .replication_runtime
            .add_channel_capability(connection, "eof".to_string());
        let plan = worker.full_sync_save_plan(&[connection], snapshot);
        assert_eq!(plan.target, FullSyncTarget::Socket);
        assert_eq!(plan.method, BgsaveMethod::Fork);
        assert_eq!(plan.fallback, Some(ForklessFallback::MissingInband));

        worker
            .replication_runtime
            .add_channel_capability(connection, "inband-repl".to_string());
        let plan = worker.full_sync_save_plan(&[connection], snapshot);
        assert_eq!(plan.target, FullSyncTarget::Socket);
        assert_eq!(plan.method, BgsaveMethod::Forkless);
        assert_eq!(plan.fallback, None);
    }
}
