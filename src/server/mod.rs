#![forbid(unsafe_code)]

use crate::framework::config::Config;
use crate::runtime::server::worker::ServerWorkerState;
use std::net::SocketAddr;
use std::process;
use std::thread;
use std::time::Duration;

mod assembly;
mod bootstrap;
mod env;
mod startup;

pub use env::run_from_env;

pub struct Server {
    config: Config,
    systemd_supervised: bool,
    startup_args: Vec<std::ffi::OsString>,
}

pub(super) fn cluster_slot_router(key: &[u8]) -> usize {
    crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
}

pub(super) fn bind_listener_with_restart_grace(
    backend: &mut ServerWorkerState,
    addr: SocketAddr,
    bind: fn(&mut ServerWorkerState, SocketAddr) -> std::io::Result<()>,
) -> std::io::Result<()> {
    // Bounded AddrInUse retry to absorb the test harness's port-release lag
    // after a previous instance shutdown (the prior LISTEN socket may not be
    // dropped by the kernel the instant the process is signalled). The parallel
    // Tcl harness can keep a just-selected port busy for more than five seconds
    // under load. Give that transient owner up to 30 seconds to exit so the
    // same server process survives; a harness-level retry truncates its reused
    // stdout and corrupts cumulative startup-log assertions.
    //
    // Critically the loop must honour a shutdown signal. `install_signals()`
    // runs BEFORE we bind, so a SIGTERM/SIGINT delivered while startup is still
    // waiting for the port fires the async handler (setting the pending-signal
    // slot) but is otherwise invisible here. Without this check the process
    // would keep sleeping in `clock_nanosleep`, ignore the signal for the full
    // budget, and retain the port — the root cause of sentinel port-retention
    // flakiness (harness SIGTERMs a slow-binding instance, it stays stuck in
    // nanosleep, port 20000 never returns available).
    const SERVER_RETRIES: usize = 1_500; // ~30s at the 20ms retry cadence
                                         // The Sentinel harness intentionally starts a second process on an
                                         // occupied port to verify rewritten configuration. C exits that duplicate
                                         // immediately. Keeping it in the server grace window lets it steal the
                                         // listener after a later failover crash, outside the harness PID registry.
    let retries = if backend.ctx.sentinel_state().sentinel_mode {
        0
    } else {
        SERVER_RETRIES
    };
    for attempt in 0..=retries {
        // The harness gave up on us and asked us to exit: release everything by
        // exiting cleanly instead of sleeping out the remaining budget.
        if crate::foundation::io::sys::signal::take_pending_signal().is_some() {
            crate::server_log!(
                warning,
                "Shutdown signal received while binding listeners; exiting."
            );
            process::exit(0);
        }
        match bind(backend, addr) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse && attempt < retries => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("listener bind loop returns on success or final error")
}

/// Whether a failed *optional* TCP bind should be tolerated. C valkey
/// (`listenToPort` in server.c) skips optional addresses on `EADDRNOTAVAIL`
/// (address family present but the wildcard/interface is unusable) and on the
/// no-protocol/family errors; anything else is fatal. We mirror that set so a
/// host without an IPv6 stack still starts under the default `bind * -::*`.
pub(crate) fn is_optional_bind_soft_error(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EADDRNOTAVAIL)
            | Some(libc::EAFNOSUPPORT)
            | Some(libc::EPROTONOSUPPORT)
            | Some(libc::ESOCKTNOSUPPORT)
            | Some(libc::EPFNOSUPPORT)
            | Some(libc::ENOPROTOOPT)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn listener_bind_grace_outlasts_harness_port_release_lag() {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = occupied.local_addr().unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5_500));
            drop(occupied);
        });
        let mut runtime = ServerWorkerState::new(1).unwrap();

        let result = bind_listener_with_restart_grace(&mut runtime, addr, ServerWorkerState::bind);

        release.join().unwrap();
        assert!(
            result.is_ok(),
            "listener did not survive port-release lag: {result:?}"
        );
    }

    #[test]
    fn sentinel_listener_does_not_wait_for_an_occupied_port() {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = occupied.local_addr().unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.sentinel_state_mut().sentinel_mode = true;
        let started = std::time::Instant::now();

        let error = bind_listener_with_restart_grace(&mut runtime, addr, ServerWorkerState::bind)
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "sentinel bind unexpectedly entered the server restart grace"
        );
    }

    #[test]
    fn sentinel_identity_uses_reachable_default_for_wildcard_bind() {
        let mut config = Config::parse_from(["versal-sentinel"]);
        config.sentinel = true;
        config.port = 26_379;
        config.bind = "* -::*".to_string();
        let mut server = Server::new(config);
        let mut runtime = ServerWorkerState::new(1).unwrap();

        server.init_sentinel_identity(&mut runtime);

        assert!(runtime.ctx.sentinel_state().sentinel_mode);
        assert_eq!(
            runtime.ctx.sentinel_state().announce_addr,
            "127.0.0.1:26379"
        );
    }

    #[test]
    fn primary_cluster_config_overrides_stale_replicaof_startup_config() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        runtime.ctx.config_mut().replication.master_port = 6380;
        runtime.ctx.config_mut().replication.master_link_status = "down".to_string();
        runtime.ctx.replication_mut().role =
            crate::engine::distributed::replication::ReplicationRole::Replica;
        let state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);

        Server::restore_cluster_replication_role(&mut runtime, &state);

        assert_eq!(
            runtime.ctx.replication().role,
            crate::engine::distributed::replication::ReplicationRole::Primary
        );
        assert_eq!(runtime.ctx.config().replication.master_host, None);
        assert_eq!(runtime.ctx.config().replication.master_port, 0);
    }

    #[test]
    fn replica_cluster_config_restores_replica_runtime_role_before_rdb_load() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = primary_id.to_string();
        state.nodes.insert(
            primary_id.to_string(),
            ClusterNode::new(
                primary_id.to_string(),
                "192.0.2.1".to_string(),
                6380,
                16_380,
                true,
            ),
        );
        let mut runtime = ServerWorkerState::new(1).unwrap();

        Server::restore_cluster_replication_role(&mut runtime, &state);

        assert_eq!(
            runtime.ctx.replication().role,
            crate::engine::distributed::replication::ReplicationRole::Replica
        );
        assert_eq!(
            runtime.ctx.config().replication.master_host.as_deref(),
            Some("192.0.2.1")
        );
        assert_eq!(runtime.ctx.config().replication.master_port, 6380);
    }
}
