use super::{resolve_master_addr, ServerWorkerState};
use crate::foundation::io::transport::Interest;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, LinkError, ReplicaRdbChannelState};
use crate::runtime::server::worker::event_loop::sources::SourceOwner;
use crate::runtime::server::worker::PendingRemovalOwner;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplicaRdbOpenStage {
    MissingMainLink,
    Resolve,
    Connect,
    Pair,
    MissingSlab,
    State,
    Interest,
}

impl fmt::Display for ReplicaRdbOpenStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::MissingMainLink => "missing-main-link",
            Self::Resolve => "resolve",
            Self::Connect => "connect",
            Self::Pair => "pair",
            Self::MissingSlab => "missing-slab",
            Self::State => "state",
            Self::Interest => "interest",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug)]
pub(super) enum ReplicaRdbOpenError {
    MissingMainLink,
    Resolve { host: String, port: u16 },
    Connect(std::io::Error),
    Pair(LinkError),
    MissingSlab(ConnectionId),
    State(LinkError),
    Interest(std::io::Error),
}

impl ReplicaRdbOpenError {
    pub(super) fn stage(&self) -> ReplicaRdbOpenStage {
        match self {
            Self::MissingMainLink => ReplicaRdbOpenStage::MissingMainLink,
            Self::Resolve { .. } => ReplicaRdbOpenStage::Resolve,
            Self::Connect(_) => ReplicaRdbOpenStage::Connect,
            Self::Pair(_) => ReplicaRdbOpenStage::Pair,
            Self::MissingSlab(_) => ReplicaRdbOpenStage::MissingSlab,
            Self::State(_) => ReplicaRdbOpenStage::State,
            Self::Interest(_) => ReplicaRdbOpenStage::Interest,
        }
    }
}

impl fmt::Display for ReplicaRdbOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMainLink => formatter.write_str("main replica link is absent"),
            Self::Resolve { host, port } => write!(formatter, "unable to resolve {host}:{port}"),
            Self::Connect(error) => write!(formatter, "{error}"),
            Self::Pair(error) => write!(formatter, "{error:?}"),
            Self::MissingSlab(connection_id) => {
                write!(formatter, "missing slab for connection {}", connection_id.0)
            }
            Self::State(error) => write!(formatter, "{error:?}"),
            Self::Interest(error) => write!(formatter, "{error}"),
        }
    }
}

impl ServerWorkerState {
    pub(super) fn open_replica_rdb_channel(
        &mut self,
        initial_frames: Vec<Vec<u8>>,
    ) -> Result<(), ReplicaRdbOpenError> {
        let (main_connection_id, host, port) = self
            .replication_runtime
            .replica_link()
            .map(|link| {
                (
                    link.connection_id(),
                    link.master().host.clone(),
                    link.master().port,
                )
            })
            .ok_or(ReplicaRdbOpenError::MissingMainLink)?;
        let resolved =
            resolve_master_addr(&host, port).ok_or(ReplicaRdbOpenError::Resolve { host, port })?;
        crate::server_log!(
            debug,
            "dual-rdb open: main={} target={}:{} initial_frames={}",
            main_connection_id.0,
            resolved.ip(),
            resolved.port(),
            initial_frames.len()
        );
        let handle = self
            .transport
            .connect_tcp(resolved)
            .map_err(ReplicaRdbOpenError::Connect)?;
        self.route_runtime_source(handle.token, SourceOwner::Connection)
            .map_err(ReplicaRdbOpenError::Connect)?;
        let rdb_connection_id = ConnectionId(handle.token.0);
        self.connection_registry
            .register(Connection::new(handle.token, resolved, false))
            .expect("a newly connected RDB transport token must be unique");
        self.prioritize_connection(handle.token);
        crate::server_log!(
            debug,
            "dual-rdb open: connected transport rdb={} main={} slab={}",
            rdb_connection_id.0,
            main_connection_id.0,
            handle.token.0
        );

        self.complete_replica_rdb_open(main_connection_id, rdb_connection_id, initial_frames)
    }

    fn complete_replica_rdb_open(
        &mut self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
        initial_frames: Vec<Vec<u8>>,
    ) -> Result<(), ReplicaRdbOpenError> {
        let result = (|| {
            let connection_token = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(
                    rdb_connection_id.0,
                ))
                .then_some(rdb_connection_id.0)
                .ok_or(ReplicaRdbOpenError::MissingSlab(rdb_connection_id))?;
            self.replication_runtime
                .set_replica_rdb_link(rdb_connection_id, main_connection_id)
                .map_err(ReplicaRdbOpenError::Pair)?;
            for frame in initial_frames {
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(frame);
            }
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            let queued_bytes: usize = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
            .iter()
            .map(Vec::len)
            .sum();
            self.replication_runtime
                .replica_link_mut()
                .ok_or(ReplicaRdbOpenError::MissingMainLink)?
                .transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
                .map_err(ReplicaRdbOpenError::State)?;
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .map_err(ReplicaRdbOpenError::Interest)?;
            crate::server_log!(
                debug,
                "dual-rdb open: armed rdb={} main={} queued_bytes={}",
                rdb_connection_id.0,
                main_connection_id.0,
                queued_bytes
            );
            Ok(())
        })();
        if result.is_err() {
            self.rollback_replica_rdb_open(rdb_connection_id);
        }
        result
    }

    fn rollback_replica_rdb_open(&mut self, rdb_connection_id: ConnectionId) {
        if self
            .replication_runtime
            .replica_rdb_link()
            .is_some_and(|link| link.connection_id() == rdb_connection_id)
        {
            self.replication_runtime
                .clear_rdb_channel(rdb_connection_id);
            self.replication_runtime.take_replica_rdb_link();
        }
        self.retire_runtime_source(Token(rdb_connection_id.0));
        self.connection_registry.remove(Token(rdb_connection_id.0));
        self.deregister_or_defer(Token(rdb_connection_id.0), PendingRemovalOwner::ReplicaRdb);
    }

    #[cfg(test)]
    pub(super) fn open_replica_rdb_channel_for_test(
        &mut self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
    ) -> Result<(), ReplicaRdbOpenError> {
        if self
            .replication_runtime
            .replica_link()
            .map(|link| link.connection_id())
            != Some(main_connection_id)
        {
            return Err(ReplicaRdbOpenError::MissingMainLink);
        }
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(rdb_connection_id.0));
        self.connection_registry.register_for_test(connection);
        self.complete_replica_rdb_open(main_connection_id, rdb_connection_id, Vec::new())
    }
}
