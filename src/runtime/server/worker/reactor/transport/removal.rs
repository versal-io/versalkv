use super::RuntimeTransport;
use crate::engine::command::context::Token;
use crate::foundation::io::transport::TransportRuntime;
use crate::runtime::server::worker::reactor::sources::{
    RuntimeSourceHandle, RuntimeSourceRegistry, SourceOwner,
};
use std::collections::HashMap;
use std::fmt::Display;
use std::io;

/// Which owner requested a transport teardown that failed its L1 deregister.
/// Retained purely so the minimal retry registry can log the owner: all L4/L5
/// business isolation already happened at teardown time, so the retry only has
/// to redo the L1 deregister.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingRemovalOwner {
    Client,
    RejectedClient,
    Listener,
    ClusterBus,
    Sentinel,
    ReplicaRdb,
    RuntimeSource,
    FailedRegistration,
}

impl PendingRemovalOwner {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::RejectedClient => "rejected client",
            Self::Listener => "listener",
            Self::ClusterBus => "cluster bus",
            Self::Sentinel => "sentinel",
            Self::ReplicaRdb => "replica RDB",
            Self::RuntimeSource => "runtime source",
            Self::FailedRegistration => "failed registration",
        }
    }
}

/// A single retained teardown intent. The connection is already isolated from
/// every business index (connection registry, cluster_bus connections,
/// pubsub, blocking, replication); only its L1 registration outlives the failed
/// deregister and must be retried from a later background tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PendingTransportRemoval {
    pub(crate) token: Token,
    pub(crate) owner_kind: PendingRemovalOwner,
}

pub(crate) struct SourceInstallRollback<'a, T: TransportRuntime> {
    transport: &'a mut RuntimeTransport<T>,
    sources: &'a mut RuntimeSourceRegistry,
    pending_transport_removals: &'a mut HashMap<Token, PendingTransportRemoval>,
}

impl<'a, T: TransportRuntime> SourceInstallRollback<'a, T> {
    pub(crate) fn new(
        transport: &'a mut RuntimeTransport<T>,
        sources: &'a mut RuntimeSourceRegistry,
        pending_transport_removals: &'a mut HashMap<Token, PendingTransportRemoval>,
    ) -> Self {
        Self {
            transport,
            sources,
            pending_transport_removals,
        }
    }

    pub(crate) fn quarantine_failed_registration(
        &mut self,
        token: Token,
        owner: SourceOwner,
        install_error: &dyn Display,
    ) -> io::Result<()> {
        retire_route_for_teardown(
            self.sources,
            self.pending_transport_removals,
            token,
            owner.label(),
        );
        match self.transport.remove_registered(token) {
            Ok(()) => {
                self.pending_transport_removals.remove(&token);
                Ok(())
            }
            Err(remove_error) => {
                crate::server_log!(
                    warning,
                    "failed to roll back {} registration for token {} after route install error ({}): {}",
                    owner.label(),
                    token.0,
                    install_error,
                    remove_error
                );
                self.pending_transport_removals.insert(
                    token,
                    PendingTransportRemoval {
                        token,
                        owner_kind: PendingRemovalOwner::FailedRegistration,
                    },
                );
                Err(remove_error)
            }
        }
    }
}

pub(crate) fn deregister_or_defer<T: TransportRuntime>(
    transport: &mut RuntimeTransport<T>,
    pending_transport_removals: &mut HashMap<Token, PendingTransportRemoval>,
    token: Token,
    owner_kind: PendingRemovalOwner,
) -> io::Result<()> {
    if !transport.is_registered(token) {
        pending_transport_removals.remove(&token);
        return Ok(());
    }
    match transport.remove_registered(token) {
        Ok(()) => {
            pending_transport_removals.remove(&token);
            Ok(())
        }
        Err(error) => {
            crate::server_log!(
                warning,
                "Retaining {} {} after transport deregistration failure: {}",
                owner_kind.label(),
                token.0,
                error
            );
            pending_transport_removals.insert(token, PendingTransportRemoval { token, owner_kind });
            Err(error)
        }
    }
}

pub(crate) fn retry_pending_transport_removals<T: TransportRuntime>(
    transport: &mut RuntimeTransport<T>,
    pending_transport_removals: &mut HashMap<Token, PendingTransportRemoval>,
) {
    let pending: Vec<PendingTransportRemoval> =
        pending_transport_removals.values().copied().collect();
    for removal in pending {
        if !transport.is_registered(removal.token) {
            pending_transport_removals.remove(&removal.token);
            continue;
        }
        match transport.remove_registered(removal.token) {
            Ok(()) => {
                pending_transport_removals.remove(&removal.token);
            }
            Err(error) => {
                crate::server_log!(
                    warning,
                    "Retaining {} {} after transport deregistration failure: {}",
                    removal.owner_kind.label(),
                    removal.token.0,
                    error
                );
            }
        }
    }
}

pub(crate) fn retire_route_for_teardown(
    sources: &mut RuntimeSourceRegistry,
    pending_transport_removals: &HashMap<Token, PendingTransportRemoval>,
    token: Token,
    owner_label: &str,
) {
    if sources.retire(RuntimeSourceHandle { token }).is_err() {
        if pending_transport_removals.contains_key(&token) {
            crate::server_log!(
                debug,
                "{} for token {} was already stale while transport removal remained pending",
                owner_label,
                token.0
            );
        } else {
            crate::server_log!(
                warning,
                "{} for token {} was already stale during idempotent teardown",
                owner_label,
                token.0
            );
        }
    }
}
