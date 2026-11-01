use super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::distributed::replication::{
    ConnectionId, PrimarySyncPlan, RdbFilterMode, ReplicaConnectionInfo, ReplicationConnection,
};

mod psync;
pub(crate) mod replconf;

impl ServerWorkerState {
    fn transport_acl_denial(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
    ) -> Option<Vec<u8>> {
        let username = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .username
        .clone();
        let acl_categories = crate::framework::generated::commands::top_level_meta(name_lower)
            .map(|meta| meta.acl_categories)
            .unwrap_or(&[]);
        let command =
            crate::engine::extension::acl::AclCommand::new(name_lower, &args.args, acl_categories);
        let denied = crate::engine::extension::acl::acl_users()
            .lock()
            .unwrap()
            .get(&username)
            .is_some_and(|user| !user.can_execute_command_with_args(command));
        if !denied {
            return None;
        }
        crate::engine::extension::acl::add_acl_log_entry(
            crate::engine::extension::acl::ACL_DENIED_CMD,
            crate::engine::extension::acl::ACL_LOG_CTX_TOPLEVEL,
            name_lower.to_string(),
            username.clone(),
            format!(
                "addr={} cmd={}",
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .peer_addr,
                name_lower
            ),
        );
        self.ctx.end_cmd(name_lower, true);
        let error = Value::error(format!(
            "NOPERM User {} has no permissions to run the '{}' command",
            username, name_lower
        ));
        Some(if resp3 {
            serialize_to_vec_resp3(&error)
        } else {
            serialize_to_vec_resp2(&error)
        })
    }

    pub(crate) fn try_handle_transport_cmd(
        &mut self,
        connection_token: usize,
        token_value: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
        _pending_unblock: &mut Vec<(usize, bool)>,
    ) -> Option<bool> {
        if !matches!(name_lower, "replconf" | "psync" | "sync") {
            return None;
        }
        if let Some(reply) = self.transport_acl_denial(connection_token, args, name_lower, resp3) {
            replies.push(reply);
            return Some(false);
        }
        let allows_busy = crate::framework::generated::commands::top_level_meta(name_lower)
            .is_some_and(|meta| meta.flags & crate::engine::command::CMD_ALLOW_BUSY != 0);
        if self.ctx.is_script_busy() && !self.ctx.is_loading() && !allows_busy {
            self.ctx.end_cmd(name_lower, true);
            let reply = Value::error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            );
            crate::runtime::server::worker::flows::request::dispatch::record_error_reply(
                &mut self.ctx,
                &reply,
                None,
            );
            replies.push(if resp3 {
                serialize_to_vec_resp3(&reply)
            } else {
                serialize_to_vec_resp2(&reply)
            });
            return Some(false);
        }
        match name_lower {
            "replconf" => {
                Some(self.handle_replconf(connection_token, token_value, args, resp3, replies))
            }
            "psync" | "sync" => {
                Some(self.handle_psync(connection_token, args, name_lower, replies))
            }
            _ => unreachable!("transport command name checked above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::{BlockedCmd, BlockedState};
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::replication::{ReplicationOffset, ReplicationRole};
    use crate::engine::extension::acl::{acl_users, AclUser};
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::PrimaryLinkState;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use bytes::Bytes;
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn connect_runtime_peer(backend: &mut ServerWorkerState) -> (RuntimeConnectedPeer, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind runtime peer");
        let peer_addr = listener.local_addr().expect("read runtime peer address");
        let peer = backend
            .transport
            .connect_tcp(peer_addr)
            .expect("connect runtime peer");
        let (target, _) = listener.accept().expect("accept runtime peer");
        (peer, target)
    }

    fn backend_with_primary_link() -> (ServerWorkerState, usize, ConnectionId) {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (peer, _target) = connect_runtime_peer(&mut backend);
        let token = peer.token;
        backend.add_connection(peer);
        let connection_id = ConnectionId(token.0);
        backend.replication_runtime.register_primary(
            connection_id,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        (backend, token.0, connection_id)
    }

    #[test]
    fn transport_busy_rejections_preserve_replication_and_count_errors() {
        for (name, resp3) in [("psync", false), ("sync", true)] {
            let (mut backend, token, connection_id) = backend_with_primary_link();
            backend.ctx.set_script_busy(false, false);
            let args = CommandArgs {
                name: Bytes::copy_from_slice(name.as_bytes()),
                args: if name == "psync" {
                    vec![Bytes::from_static(b"?"), Bytes::from_static(b"-1")]
                } else {
                    Vec::new()
                },
            };
            let mut replies = Vec::new();
            let mut unblocks = Vec::new();
            backend.ctx.begin_cmd();

            assert_eq!(
                backend.try_handle_transport_cmd(
                    token,
                    token,
                    &args,
                    name,
                    resp3,
                    &mut replies,
                    &mut unblocks,
                ),
                Some(false)
            );
            assert!(matches!(replies.as_slice(), [reply] if reply.starts_with(b"-BUSY ")));
            assert_eq!(backend.ctx.stats().total_error_replies, 1);
            assert_eq!(backend.ctx.stats().error_stats.get("BUSY"), Some(&1));
            assert_eq!(backend.ctx.stats().sync_full, 0);
            assert_eq!(
                backend
                    .replication_runtime
                    .primary_for_connection(connection_id)
                    .unwrap()
                    .state(),
                PrimaryLinkState::AwaitingPsync
            );
            replies.clear();
            let replconf = CommandArgs {
                name: Bytes::from_static(b"REPLCONF"),
                args: vec![
                    Bytes::from_static(b"listening-port"),
                    Bytes::from_static(b"6380"),
                ],
            };
            backend.ctx.begin_cmd();
            assert_eq!(
                backend.try_handle_transport_cmd(
                    token,
                    token,
                    &replconf,
                    "replconf",
                    resp3,
                    &mut replies,
                    &mut unblocks,
                ),
                Some(false)
            );
            assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
            assert_eq!(backend.ctx.stats().total_error_replies, 1);
        }
    }

    fn malformed_ack_with_trailing_option() -> CommandArgs {
        CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"ACK"),
                Bytes::from_static(b"9"),
                Bytes::from_static(b"FACK"),
                Bytes::from_static(b"8"),
                Bytes::from_static(b"trailing"),
            ],
        }
    }

    fn add_blocked_wait_client(
        backend: &mut ServerWorkerState,
        target_offset: ReplicationOffset,
    ) -> Token {
        let token = Token(10_001);
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(token);
        connection.blocked = Some(BlockedState {
            cmd: BlockedCmd::Wait {
                required: 1,
                target_offset,
            },
            keys: Vec::new(),
            timeout_ms: 60_000,
            start_time_ms: crate::foundation::io::sys::time::mstime(),
            orig_args: CommandArgs::from_parts(vec![
                Bytes::from_static(b"WAIT"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"60000"),
            ]),
            req_bytes: 0,
            orig_wrapped: None,
        });
        backend.connection_registry.register_for_test(connection);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_keyless_client(session);
        token
    }

    #[test]
    fn malformed_ack_does_not_mutate_classic_promotion_or_wait_state() {
        let (mut backend, connection_token, connection_id) = backend_with_primary_link();
        {
            let backlog = &backend.ctx.replication().backlog;
            let link = backend
                .replication_runtime
                .primary_for_connection_mut(connection_id)
                .unwrap();
            link.plan_full_resync(backlog);
            link.finish_full_resync();
        }
        let wait_token = add_blocked_wait_client(&mut backend, offset(9));
        let before_ack_time = backend
            .replication_runtime
            .primary_for_connection(connection_id)
            .unwrap()
            .last_ack_time_ms();
        let before_replica_output = backend.connection_registry[Token(connection_token)]
            .output
            .queue
            .clone();
        let before_wait_output = backend.connection_registry[wait_token].output.queue.clone();
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(
            connection_token,
            connection_id.0,
            &malformed_ack_with_trailing_option(),
            false,
            &mut replies,
        ));

        assert_eq!(replies, vec![b"-ERR syntax error\r\n".to_vec()]);
        let link = backend
            .replication_runtime
            .primary_for_connection(connection_id)
            .unwrap();
        assert_eq!(link.last_ack_position(), None);
        assert_eq!(link.last_aof_ack_position(), None);
        assert_eq!(link.last_ack_time_ms(), before_ack_time);
        assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
        assert_eq!(
            backend.connection_registry[Token(connection_token)]
                .output
                .queue,
            before_replica_output
        );
        assert!(backend.connection_registry[wait_token].blocked.is_some());
        assert_eq!(
            backend.connection_registry[wait_token].output.queue,
            before_wait_output
        );
    }

    #[test]
    fn malformed_ack_does_not_mutate_dual_promotion_state() {
        let (mut backend, connection_token, connection_id) = backend_with_primary_link();
        {
            let backlog = &backend.ctx.replication().backlog;
            let boundary = backlog.snapshot_boundary();
            let replid = backlog.replid().to_string();
            let requested = boundary.reported_offset().checked_next_request().unwrap();
            let link = backend
                .replication_runtime
                .primary_for_connection_mut(connection_id)
                .unwrap();
            link.transition_to_waiting_for_rdb_channel().unwrap();
            link.record_dual_channel_endoff_boundary(boundary);
            link.transition_to_waiting_for_main_psync().unwrap();
            assert!(link.complete_dual_channel_main_psync(&replid, requested, backlog));
            link.mark_dual_rdb_complete();
        }
        let before_ack_time = backend
            .replication_runtime
            .primary_for_connection(connection_id)
            .unwrap()
            .last_ack_time_ms();
        let before_output = backend.connection_registry[Token(connection_token)]
            .output
            .queue
            .clone();
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(
            connection_token,
            connection_id.0,
            &malformed_ack_with_trailing_option(),
            false,
            &mut replies,
        ));

        assert_eq!(replies, vec![b"-ERR syntax error\r\n".to_vec()]);
        let link = backend
            .replication_runtime
            .primary_for_connection(connection_id)
            .unwrap();
        assert_eq!(link.last_ack_position(), None);
        assert_eq!(link.last_aof_ack_position(), None);
        assert_eq!(link.last_ack_time_ms(), before_ack_time);
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
        );
        assert_eq!(
            backend.connection_registry[Token(connection_token)]
                .output
                .queue,
            before_output
        );
    }

    #[test]
    fn replconf_ack_rejects_non_live_offsets_without_mutating_link() {
        for raw_offset in ["-1", "9223372036854775807"] {
            let (mut backend, connection_token, connection_id) = backend_with_primary_link();
            let ack = CommandArgs {
                name: Bytes::from_static(b"REPLCONF"),
                args: vec![
                    Bytes::from_static(b"ACK"),
                    Bytes::copy_from_slice(raw_offset.as_bytes()),
                ],
            };
            let mut replies = Vec::new();

            assert!(!backend.handle_replconf(
                connection_token,
                connection_id.0,
                &ack,
                false,
                &mut replies,
            ));

            let link = backend
                .replication_runtime
                .primary_for_connection(connection_id)
                .expect("invalid ACK must retain the primary link");
            assert_eq!(link.last_ack_position(), None);
            assert_eq!(link.last_ack_time_ms(), 0);
            assert!(replies.is_empty());
        }
    }

    #[test]
    fn invalid_replconf_fack_does_not_partially_apply_valid_ack() {
        let (mut backend, connection_token, connection_id) = backend_with_primary_link();
        let ack = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"ACK"),
                Bytes::from_static(b"9"),
                Bytes::from_static(b"FACK"),
                Bytes::from_static(b"-1"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(
            connection_token,
            connection_id.0,
            &ack,
            false,
            &mut replies,
        ));

        let link = backend
            .replication_runtime
            .primary_for_connection(connection_id)
            .expect("invalid FACK must retain the primary link");
        assert_eq!(link.last_ack_position(), None);
        assert_eq!(link.last_aof_ack_position(), None);
        assert_eq!(link.last_ack_time_ms(), 0);
        assert!(replies.is_empty());
    }

    #[test]
    fn transport_psync_honors_acl_command_denial() {
        let username = "transport-psync-denied-test";
        let mut user = AclUser::new_disabled();
        user.apply_rule("on").unwrap();
        user.apply_rule("nopass").unwrap();
        user.apply_rule("+@all").unwrap();
        user.apply_rule("-psync").unwrap();
        acl_users()
            .lock()
            .unwrap()
            .insert(username.to_string(), user);

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .username = username.to_string();
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"?"), Bytes::from_static(b"-1")],
        };
        let mut replies = Vec::new();
        let mut pending_unblock = Vec::new();
        backend.ctx.set_script_busy(false, false);

        let handled = backend.try_handle_transport_cmd(
            connection_token,
            token.0,
            &args,
            "psync",
            false,
            &mut replies,
            &mut pending_unblock,
        );
        acl_users().lock().unwrap().remove(username);

        assert!(handled.is_some());
        assert!(matches!(replies.as_slice(), [reply] if reply.starts_with(b"-NOPERM ")));
        assert_eq!(
            backend.replication_runtime.role(ConnectionId(token.0)),
            None
        );
    }

    #[test]
    fn replconf_version_is_retained_on_primary_link() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;

        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"VERSION"),
                Bytes::from_static(b"8.0.0"),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(connection_token, token.0, &args, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(ConnectionId(token.0))
                .unwrap()
                .replica_version(),
            0x080000
        );
    }

    #[test]
    fn replconf_cluster_node_id_promotes_the_matching_handshake_replica() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
        };
        use crate::engine::distributed::cluster::state::ClusterState;

        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let temporary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
        let mut temporary = ClusterNode::new(
            temporary_id.clone(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            false,
        );
        temporary.flags &= !CLUSTER_NODE_MYSELF;
        temporary.flags |= CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(temporary_id.clone(), temporary);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"set-cluster-node-id"),
                Bytes::from_static(node_id.as_bytes()),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(connection_token, token.0, &args, false, &mut replies));

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(!state.nodes.contains_key(&temporary_id));
        let replica = state
            .nodes
            .get(node_id)
            .expect("replica identity must be promoted");
        assert!(replica.is_replica());
        assert_eq!(replica.port(), 6380);
        assert_eq!(replica.cport(), 16_380);
    }

    #[test]
    fn replconf_cluster_node_id_rejects_unknown_node_without_matching_handshake() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        *backend.ctx.cluster_state_mut() = Some(Box::new(
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379),
        ));
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"set-cluster-node-id"),
                Bytes::from_static(node_id.as_bytes()),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(connection_token, token.0, &args, false, &mut replies));

        assert_eq!(
            replies,
            vec![format!("-ERR Unknown node {node_id}\r\n").into_bytes()]
        );
        assert_eq!(
            backend
                .replication_runtime
                .replica_node_id(ConnectionId(token.0)),
            None
        );
        assert!(!backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .nodes
            .contains_key(node_id));
    }

    #[test]
    fn replconf_filter_metadata_is_retained_when_primary_registers_afterward() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"rdb-filter-only"),
                Bytes::from_static(b"functions"),
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
            ],
        };
        let mut replies = Vec::new();

        backend.handle_replconf(connection_token, token.0, &args, false, &mut replies);

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(
            backend
                .replication_runtime
                .primary_for_connection(ConnectionId(token.0))
                .expect("listening-port must register the primary")
                .replica()
                .rdb_filter_mode
                == RdbFilterMode::Functions,
            "rdb-filter-only must survive metadata staging before primary registration"
        );
    }

    #[test]
    fn initial_dual_psync_negotiates_before_full_resync_state_transition() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let main = ConnectionId(token.0);
        let connection_token = token.0;
        backend.ctx.config_mut().replication.repl_diskless_sync = true;
        backend
            .ctx
            .config_mut()
            .replication
            .dual_channel_replication = true;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec![
                    "eof".to_string(),
                    "psync2".to_string(),
                    "dual-channel".to_string(),
                ],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        let psync = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"?"), Bytes::from_static(b"-1")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_psync(connection_token, &psync, "psync", &mut replies));

        assert_eq!(
            replies,
            vec![
                crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY.to_vec()
            ]
        );
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("main link must remain registered");
        assert_eq!(
            link.state(),
            crate::framework::distributed::replication::PrimaryLinkState::AwaitingPsync
        );
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
        assert_eq!(
            link.rdb_channel_wait_deadline_ms, None,
            "the RDB-channel wait timeout must start only after +DUALCHANNELSYNC flushes"
        );
    }

    #[test]
    fn partial_psync_fallback_increments_sync_partial_err() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let main = ConnectionId(token.0);
        let connection_token = token.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(b"retained")
            .unwrap();
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from_static(b"stale-replid"),
                Bytes::from_static(b"0"),
            ],
        };
        let mut replies = Vec::new();
        backend.handle_psync(connection_token, &args, "psync", &mut replies);
        assert_eq!(backend.ctx.stats().sync_partial_err, 1);
        assert!(replies
            .iter()
            .any(|reply| reply.starts_with(b"+FULLRESYNC")));
    }

    #[test]
    fn default_diskless_sync_delay_defers_the_first_eof_replica() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_diskless_sync = true;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let main = ConnectionId(token.0);
        let connection_token = token.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["eof".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        let psync = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"?"), Bytes::from_static(b"-1")],
        };
        let mut replies = Vec::new();

        backend.handle_psync(connection_token, &psync, "psync", &mut replies);

        assert!(
            replies.is_empty(),
            "C waits through the configured default diskless-sync delay before sending FULLRESYNC"
        );
        assert!(backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("PSYNC peer remains registered")
            .is_deferred_diskless());
    }

    #[test]
    fn replica_psync_is_deferred_until_master_link_is_up() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().role = ReplicationRole::Replica;
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        backend.ctx.config_mut().replication.master_link_status = "down".to_string();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![Bytes::from_static(b"old"), Bytes::from_static(b"0")],
        };
        let mut replies = Vec::new();

        backend.handle_psync(connection_token, &args, "psync", &mut replies);

        assert_eq!(
            replies,
            vec![b"-NOMASTERLINK Can't SYNC while not connected with my master\r\n".to_vec()]
        );
        assert_eq!(backend.ctx.stats().sync_full, 0);
        assert_eq!(backend.ctx.stats().sync_partial_err, 0);
        assert_eq!(
            backend.replication_runtime.role(ConnectionId(token.0)),
            None
        );
    }

    #[test]
    fn replconf_rdb_channel_zero_removes_the_returned_channel_transport() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend.associate_waiting_rdb_for_test(main, rdb);
        backend.refresh_replication_view();
        let connection_token = main.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"0")],
        };
        let mut replies = Vec::new();

        backend.handle_replconf(connection_token, main.0, &args, false, &mut replies);

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(backend.connected_clients(), 1);
        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(rdb.0)));
        assert!(!backend.transport.is_registered(rdb_listener.token));
        assert_eq!(
            backend.replication_runtime.role(main),
            Some(ReplicationConnection::Primary(replica_id))
        );
        assert_eq!(backend.replication_runtime.role(rdb), None);
    }

    #[test]
    fn replconf_set_rdb_client_id_rejects_reverse_rdb_association_with_main_payload() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 1;
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_max_replicas = 1;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let main_slab = main.0;
        let rdb_slab = rdb.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .expect("RDB sender must register as a candidate");
        let preexisting_main_frame = b"+PREEXISTING\r\n";
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .queue_reply(preexisting_main_frame.to_vec());
        if let Some(frame) = backend
            .replication_runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(
                rdb,
                backend
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(rdb.0)),
                &mut backend.ctx.replication_mut().backlog,
            )
        {
            backend.connection_registry[crate::framework::capabilities::io::Token(rdb.0)]
                .queue_reply(frame);
        }
        let expected_endoff = crate::engine::distributed::replication::protocol::encode_end_offset(
            offset(backend.ctx.replication().backlog.reported_offset().get()),
            backend.ctx.replication().backlog.replid(),
            0,
            rdb.0 as u64,
        );

        let reverse_args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(main.0.to_string()),
            ],
        };
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry
                        [crate::framework::capabilities::io::Token(rdb_slab)]
                    .client_id()
                    .to_string(),
                ),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &reverse_args, false, &mut replies));
        assert!(
            replies
                .first()
                .is_some_and(|reply| reply.starts_with(b"-ERR")),
            "RDB sender with a main payload must not create a reverse association: {replies:?}"
        );
        assert_eq!(backend.replication_runtime.rdb_channel_for_main(main), None);
        assert_eq!(backend.replication_runtime.rdb_channel_for_main(rdb), None);

        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &args, false, &mut replies));

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main),
            Some(rdb)
        );
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(rdb),
            None,
            "RDB candidate must not become a main channel"
        );
        assert_eq!(
            backend.replication_runtime.role(rdb),
            Some(ReplicationConnection::RdbChannel { main })
        );
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("main primary link must remain registered")
                .dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
        let rdb_output: Vec<u8> = backend.connection_registry
            [crate::framework::capabilities::io::Token(rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
        let main_output: Vec<u8> = backend.connection_registry
            [crate::framework::capabilities::io::Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
        assert!(
            main_output
                .windows(preexisting_main_frame.len())
                .any(|window| window == preexisting_main_frame),
            "main channel must retain its preexisting output: {main_output:?}"
        );
        assert_eq!(
            rdb_output, expected_endoff,
            "RDB candidate must receive exactly one encoded $ENDOFF"
        );
        assert!(
            !main_output
                .windows(b"$ENDOFF:".len())
                .any(|window| window == b"$ENDOFF:"),
            "main channel must not receive $ENDOFF: {main_output:?}"
        );
    }

    #[test]
    fn zero_delay_rdb_replconf_schedules_snapshot_without_capturing_endoff() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        let rdb_slab = rdb.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");

        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &args, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        assert!(backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("main primary link must remain registered")
                .dual_endoff_boundary(),
            None
        );
        assert_eq!(
            backend
                .replication_runtime
                .rebuild_view()
                .replicas_waiting_psync,
            0,
            "ENDOFF must not create a PSYNC wait before snapshot start"
        );
        assert_eq!(
            backend
                .ctx
                .client_info(rdb.0)
                .map(|info| info.last_cmd.as_str()),
            Some("sync"),
            "a dual RDB channel must remain addressable through CLIENT LIST cmd=sync"
        );
        let rdb_output: Vec<u8> = backend.connection_registry
            [crate::framework::capabilities::io::Token(rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
        assert!(
            rdb_output.is_empty(),
            "ENDOFF must follow the REPLCONF reply"
        );
        assert_eq!(
            backend.ctx.replication().backlog.min_protected_cursor(),
            None,
            "an empty C replication backlog is pinned only when its first block is created"
        );
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("main primary link must remain registered")
                .dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
        assert_eq!(
            backend.replication_runtime.role(rdb),
            Some(ReplicationConnection::RdbChannelCandidate)
        );
        assert_eq!(backend.replication_runtime.rdb_channel_for_main(main), None);
    }

    #[test]
    fn active_child_delays_zero_delay_endoff_and_snapshot_boundary() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        backend.ctx.replication_mut().backlog.activate();
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, mut rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .ctx
            .persistence_mut()
            .begin_diskless_child(123)
            .unwrap();
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(rdb.0, rdb.0, &args, false, &mut replies));
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        backend.drive_pending_dual_rdb_snapshots();

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
        assert_eq!(
            backend
                .replication_runtime
                .rebuild_view()
                .replicas_waiting_psync,
            0
        );
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .unwrap()
                .dual_endoff_boundary(),
            None
        );
        rdb_target.set_nonblocking(true).unwrap();
        let mut early_byte = [0_u8; 1];
        assert_eq!(
            rdb_target.read(&mut early_byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "an active child must prevent ENDOFF from reaching the RDB channel"
        );
        rdb_target.set_nonblocking(false).unwrap();

        backend.ctx.persistence_mut().complete_diskless_child();
        let post_child_write = b"*1\r\n$4\r\nPING\r\n";
        backend.propagate_replication_frame(post_child_write);
        let post_child_boundary = backend.ctx.replication().backlog.snapshot_boundary();
        assert_eq!(
            post_child_boundary.reported_offset().get(),
            post_child_write.len() as i64
        );
        assert_eq!(
            post_child_boundary.stream_cursor().get(),
            post_child_write.len() as i64
        );
        let expected_endoff = crate::engine::distributed::replication::protocol::encode_end_offset(
            post_child_boundary.reported_offset(),
            backend.ctx.replication().backlog.replid(),
            0,
            backend.connection_registry[Token(rdb.0)].client_id(),
        );

        backend.drive_pending_dual_rdb_snapshots();

        assert!(!backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
        let child = backend
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .expect("the released child slot must start the direct dual snapshot");
        assert_eq!(child.rdb_connections, vec![rdb]);
        let child_pid = child.pid;
        rdb_target
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut endoff = vec![0; expected_endoff.len()];
        rdb_target.read_exact(&mut endoff).unwrap();
        assert_eq!(endoff, expected_endoff);

        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry[Token(rdb.0)]
                        .client_id()
                        .to_string(),
                ),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main.0, main.0, &associate, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .unwrap()
                .dual_endoff_boundary(),
            Some(post_child_boundary),
            "association must preserve the post-child ENDOFF boundary and stream cursor"
        );
        assert_eq!(
            backend
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .map(|child| child.targets.len()),
            Some(1),
            "the direct snapshot must attach the associated primary target"
        );

        backend.drive_pending_dual_rdb_snapshots();

        assert_eq!(
            backend
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .map(|child| child.pid),
            Some(child_pid),
            "a repeated scheduler drive must not start a second snapshot"
        );
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .unwrap()
                .dual_endoff_boundary(),
            Some(post_child_boundary),
            "a repeated scheduler drive must not replace the ENDOFF boundary"
        );

        backend.abort_direct_dual_rdb_child_for_shutdown();
    }

    #[test]
    fn dual_rdb_delay_defers_endoff_until_snapshot_start() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 5;
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_max_replicas = 0;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec![
                    "eof".to_string(),
                    "psync2".to_string(),
                    "dual-channel".to_string(),
                ],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        let rdb_slab = rdb.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();
        backend.handle_replconf(rdb_slab, rdb.0, &args, false, &mut replies);
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        assert!(backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
    }

    #[test]
    fn dual_endoff_empty_backlog_resumes_psync_at_one() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let (main_peer, mut main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        let main_slab = main.0;
        let rdb_slab = rdb.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");

        let rdb_channel = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &rdb_channel, false, &mut replies,));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        backend.drive_pending_dual_rdb_snapshots();

        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry
                        [crate::framework::capabilities::io::Token(rdb_slab)]
                    .client_id()
                    .to_string(),
                ),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &associate, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("associated main link must remain registered")
                .dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );

        let psync = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(backend.ctx.replication().backlog.replid().to_string()),
                Bytes::from_static(b"1"),
            ],
        };
        replies.clear();
        assert!(!backend.handle_psync(main_slab, &psync, "psync", &mut replies));

        assert_eq!(replies, vec![b"+CONTINUE\r\n".to_vec()]);
        assert_eq!(
            backend
                .ctx
                .client_info(main.0)
                .map(|info| info.last_cmd.as_str()),
            Some("psync"),
            "a dual main channel must remain addressable through CLIENT LIST cmd=psync"
        );
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("dual main link must remain registered after resumed PSYNC");
        assert_eq!(
            link.state(),
            crate::framework::distributed::replication::PrimaryLinkState::Streaming
        );
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .unwrap()
            .mark_dual_rdb_complete();
        let queued_after_psync = b"*1\r\n$4\r\nPING\r\n";
        backend.propagate_replication_frame(queued_after_psync);
        backend.handle_writable(Token(main_slab));
        main_target
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set dual main read timeout");
        let mut received = vec![0_u8; queued_after_psync.len()];
        main_target
            .read_exact(&mut received)
            .expect("read backlog tail sent by propagation before first ACK");
        assert_eq!(received, queued_after_psync);
        assert!(backend.connection_registry[Token(main_slab)]
            .output
            .queue
            .is_empty());

        let ack = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"ACK"), Bytes::from_static(b"0")],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &ack, false, &mut replies));
        assert!(replies.is_empty());
        assert!(
            backend.connection_registry[Token(main_slab)]
                .output
                .queue
                .is_empty(),
            "terminal ACK must not requeue the tail already sent with +CONTINUE"
        );
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("ACK must retain the dual main link")
                .dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::Online
        );
        assert!(
            backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
                .output
                .queue
                .is_empty()
        );
    }

    #[test]
    fn classic_terminal_ack_accounts_for_released_backlog_tail() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (peer, mut target) = connect_runtime_peer(&mut backend);
        let token = peer.token;
        backend.add_connection(peer);
        let main = ConnectionId(token.0);
        let connection_token = main.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["eof".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        {
            let backlog = &backend.ctx.replication().backlog;
            let link = backend
                .replication_runtime
                .primary_for_connection_mut(main)
                .expect("classic primary link must exist");
            assert!(matches!(
                link.plan_full_resync(backlog),
                PrimarySyncPlan::FullResync { .. }
            ));
            link.finish_full_resync();
        }
        let tail = b"*1\r\n$4\r\nPING\r\n";
        backend.ctx.replication_mut().backlog.append(tail).unwrap();

        let ack = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"ACK"), Bytes::from_static(b"0")],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(connection_token, main.0, &ack, false, &mut replies));

        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("terminal ACK must retain the classic primary link");
        assert!(!link.has_pending_replication());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue
        .is_empty());
        target
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set classic replica read timeout");
        let mut received = vec![0_u8; tail.len()];
        target
            .read_exact(&mut received)
            .expect("read classic backlog tail");
        assert_eq!(received, tail);
    }

    #[test]
    fn dual_endoff_resume_uses_snapshot_cursor_after_backlog_advances() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        let main_slab = main.0;
        let rdb_slab = rdb.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");

        let rdb_channel = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &rdb_channel, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        backend.drive_pending_dual_rdb_snapshots();
        let snapshot_offset = backend.ctx.replication().backlog.reported_offset().get();
        assert_eq!(snapshot_offset, 0);

        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry
                        [crate::framework::capabilities::io::Token(rdb_slab)]
                    .client_id()
                    .to_string(),
                ),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &associate, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);

        let pending = b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n";
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(pending)
            .unwrap();
        assert_eq!(
            backend.ctx.replication().backlog.reported_offset().get(),
            snapshot_offset + pending.len() as i64
        );

        let psync = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(backend.ctx.replication().backlog.replid().to_string()),
                Bytes::from((snapshot_offset + 1).to_string()),
            ],
        };
        replies.clear();
        assert!(!backend.handle_psync(main_slab, &psync, "psync", &mut replies));

        assert!(replies.is_empty());
        assert_eq!(
            backend.connection_registry[Token(main_slab)].output.queue[0].as_slice(),
            b"+CONTINUE\r\n"
        );
        assert_eq!(
            backend.connection_registry[Token(main_slab)].output.queue[1].as_slice(),
            pending,
            "the dual resume must stream every post-ENDOFF backlog byte"
        );
        assert_eq!(
            backend.connection_registry[Token(main_slab)]
                .output
                .replication_bytes,
            pending.len()
        );
        let link = backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("dual main link must remain registered after resumed PSYNC");
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
        );
        assert_eq!(
            link.next_send_cursor().get(),
            snapshot_offset + pending.len() as i64
        );
        link.mark_dual_rdb_complete();

        let ack = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"ACK"),
                Bytes::from((snapshot_offset + pending.len() as i64).to_string()),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &ack, false, &mut replies));
        assert!(replies.is_empty());
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(main)
                .expect("ACK must retain the dual main link")
                .dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::Online
        );
    }

    #[test]
    fn dual_terminal_ack_delayed_drain_keeps_main_registered_across_backlog_trims() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");

        let mut replies = Vec::new();
        let rdb_channel = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        assert!(!backend.handle_replconf(rdb.0, rdb.0, &rdb_channel, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        backend.drive_pending_dual_rdb_snapshots();
        let snapshot_offset = backend.ctx.replication().backlog.reported_offset().get();
        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry[crate::framework::capabilities::io::Token(rdb.0)]
                        .client_id()
                        .to_string(),
                ),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main.0, main.0, &associate, false, &mut replies));

        let pending = b"*1\r\n$4\r\nPING\r\n";
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(pending)
            .unwrap();
        let psync = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(backend.ctx.replication().backlog.replid().to_string()),
                Bytes::from((snapshot_offset + 1).to_string()),
            ],
        };
        replies.clear();
        assert!(!backend.handle_psync(main.0, &psync, "psync", &mut replies));
        assert!(replies.is_empty());
        assert_eq!(
            backend.connection_registry[Token(main.0)].output.queue[0].as_slice(),
            b"+CONTINUE\r\n"
        );
        assert_eq!(
            backend.connection_registry[Token(main.0)].output.queue[1].as_slice(),
            pending
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("dual main link must remain registered")
            .mark_dual_rdb_complete();

        let ack = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"ACK"),
                Bytes::from((snapshot_offset + pending.len() as i64).to_string()),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main.0, main.0, &ack, false, &mut replies));

        assert!(backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("dual ACK must retain the main link")
            .has_pending_replication());
        backend.handle_writable(Token(main.0));
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("delayed writable completion must retain the dual main link");
        assert!(!link.has_pending_replication());
        assert_eq!(link.sent_cursor().get(), link.next_send_cursor().get());

        backend.ctx.config_mut().server.repl_backlog_size = 1;
        let first = vec![b'x'; pending.len() + 16];
        backend.propagate_replication_frame(&first);

        assert!(
            backend.connection_registry.contains(Token(main.0)),
            "the first backlog trim must retain the drained dual main"
        );
        let second = vec![b'y'; pending.len() + 16];
        backend.propagate_replication_frame(&second);
        assert!(
            backend.connection_registry.contains(Token(main.0)),
            "the next propagation must retain the dual main while prior output is pending"
        );
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .expect("both backlog trims must retain the dual main link");
        assert_eq!(
            link.state(),
            crate::framework::distributed::replication::PrimaryLinkState::Streaming
        );
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::Online
        );
        assert_eq!(
            link.next_send_cursor().get(),
            backend.ctx.replication().backlog.next_stream_cursor().get()
        );
    }

    #[test]
    fn paired_endoff_rejects_wrong_psync_before_generic_backlog_continue() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        let main_slab = main.0;
        let rdb_slab = rdb.0;
        backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main)
            .expect("main primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main primary link must await an RDB channel");

        let rdb_channel = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &rdb_channel, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        backend.drive_pending_dual_rdb_snapshots();

        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry
                        [crate::framework::capabilities::io::Token(rdb_slab)]
                    .client_id()
                    .to_string(),
                ),
            ],
        };
        replies.clear();
        assert!(!backend.handle_replconf(main_slab, main.0, &associate, false, &mut replies));
        let snapshot_offset = backend.ctx.replication().backlog.reported_offset().get();
        let retained_tail = b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n";
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(retained_tail)
            .unwrap();

        let wrong_resume = CommandArgs {
            name: Bytes::from_static(b"PSYNC"),
            args: vec![
                Bytes::from(backend.ctx.replication().backlog.replid().to_string()),
                Bytes::from((snapshot_offset + 2).to_string()),
            ],
        };
        replies.clear();
        assert!(!backend.handle_psync(main_slab, &wrong_resume, "psync", &mut replies));

        assert_eq!(
            replies,
            vec![b"-ERR dual-channel PSYNC does not match ENDOFF snapshot\r\n".to_vec()]
        );
        assert_eq!(backend.replication_runtime.rdb_channel_for_main(main), None);
        assert_eq!(backend.replication_runtime.role(rdb), None);
        assert_eq!(
            backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)].state,
            crate::framework::capabilities::connection::ConnectionState::CloseAfterReply
        );
    }

    #[test]
    fn retained_dual_resume_rejection_cleans_semantic_state_before_transport_teardown() {
        for (case, replid, raw_offset, expected_reply) in [
            (
                "wrong replid",
                "ffffffffffffffffffffffffffffffffffffffff".to_string(),
                "1".to_string(),
                b"-ERR dual-channel PSYNC does not match ENDOFF snapshot\r\n".as_slice(),
            ),
            (
                "wrong offset",
                String::new(),
                "2".to_string(),
                b"-ERR dual-channel PSYNC does not match ENDOFF snapshot\r\n".as_slice(),
            ),
            (
                "malformed offset",
                String::new(),
                "not-an-offset".to_string(),
                b"-ERR value is not an integer or out of range\r\n".as_slice(),
            ),
        ] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
            let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
            let main_token = main_peer.token;
            let rdb_token = rdb_peer.token;
            let main = ConnectionId(main_token.0);
            let rdb = ConnectionId(rdb_token.0);
            backend.add_connection(main_peer);
            backend.add_connection(rdb_peer);
            let public_client_id = backend.connection_registry[rdb_token].client_id();
            let replica_id = backend.replication_runtime.register_primary(
                main,
                ReplicaConnectionInfo {
                    ip: "127.0.0.1".to_string(),
                    port: 6380,
                    capability: vec!["dual-channel".to_string()],
                    rdb_filter_mode: RdbFilterMode::All,
                },
            );
            backend
                .replication_runtime
                .primary_link_mut(replica_id)
                .unwrap()
                .transition_to_waiting_for_rdb_channel()
                .unwrap();
            backend
                .replication_runtime
                .register_rdb_candidate(rdb)
                .unwrap();
            let boundary = backend.ctx.replication().backlog.snapshot_boundary();
            backend
                .replication_runtime
                .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
            backend
                .replication_runtime
                .backfill_waiting_psync_pins(boundary.stream_cursor());
            assert!(backend
                .replication_runtime
                .retain_completed_unpaired_rdb_wait(rdb, public_client_id, i64::MAX));
            backend.remove_connection(rdb.0);
            backend
                .replication_runtime
                .try_associate_retained_rdb_wait(main, public_client_id, 0)
                .unwrap();
            assert_eq!(backend.replication_runtime.channel_pair_for(main), None);
            assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);
            assert!(backend
                .replication_runtime
                .min_protected_backlog_cursor()
                .is_some());

            let requested_replid = if replid.is_empty() {
                backend.ctx.replication().backlog.replid().to_string()
            } else {
                replid
            };
            let resume = CommandArgs {
                name: Bytes::from_static(b"PSYNC"),
                args: vec![Bytes::from(requested_replid), Bytes::from(raw_offset)],
            };
            let mut replies = Vec::new();
            assert!(!backend.handle_psync(main_token.0, &resume, "psync", &mut replies));

            assert_eq!(
                replies,
                vec![expected_reply.to_vec()],
                "{case} must reject the retained dual resume"
            );
            assert_eq!(
                backend.connection_registry[main_token].state,
                crate::framework::capabilities::connection::ConnectionState::CloseAfterReply,
                "{case} must close after the error reply"
            );
            assert!(
                backend.replication_runtime.primary_links().is_empty(),
                "{case}"
            );
            assert_eq!(backend.replication_runtime.role(main), None, "{case}");
            assert_eq!(
                backend.replication_runtime.psync_wait_owner_count(),
                0,
                "{case}"
            );
            assert_eq!(
                backend.replication_runtime.min_protected_backlog_cursor(),
                None,
                "{case}"
            );
            assert!(backend
                .replication_runtime
                .take_expired_retained_rdb_psync_waits(i64::MAX)
                .is_empty());
            assert_eq!(backend.connected_clients(), 1, "{case}");
            assert!(backend.transport.is_registered(main_token), "{case}");

            backend.remove_connection(main_token.0);

            assert_eq!(backend.connected_clients(), 0, "{case}");
            assert!(!backend.transport.is_registered(main_token), "{case}");
            assert!(
                backend.replication_runtime.primary_links().is_empty(),
                "{case}"
            );
        }
    }

    #[test]
    fn endoff_candidate_uses_explicit_main_identity_after_another_main_was_waiting() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let main_a_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let main_a = ConnectionId(main_a_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        backend.add_connection(RuntimeConnectedPeer {
            token: main_a_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        backend.replication_runtime.register_primary(
            main_a,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main_a)
            .expect("main A primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main A must await an RDB channel");
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        assert!(backend
            .replication_runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(
                rdb,
                true,
                &mut backend.ctx.replication_mut().backlog
            )
            .is_some());

        let main_b_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_b_listener.token,
            peer_addr,
        });
        let main_b = ConnectionId(main_b_listener.token.0);
        backend.replication_runtime.register_primary(
            main_b,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(main_b)
            .expect("main B primary link must exist")
            .transition_to_waiting_for_rdb_channel()
            .expect("main B must await an RDB channel");

        let main_a_slab = main_a.0;
        let main_b_slab = main_b.0;
        let rdb_slab = rdb.0;
        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(
                    backend.connection_registry
                        [crate::framework::capabilities::io::Token(rdb_slab)]
                    .client_id()
                    .to_string(),
                ),
            ],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(main_b_slab, main_b.0, &associate, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main_a),
            None
        );
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main_b),
            Some(rdb)
        );

        replies.clear();
        assert!(!backend.handle_replconf(main_a_slab, main_a.0, &associate, false, &mut replies));
        assert!(replies
            .first()
            .is_some_and(|reply| reply.starts_with(b"-ERR")));
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main_a),
            None
        );
    }

    #[test]
    fn replconf_associates_retained_public_id_after_rdb_transport_removal() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (main_peer, _main_target) = connect_runtime_peer(&mut backend);
        let (rdb_peer, _rdb_target) = connect_runtime_peer(&mut backend);
        let main = ConnectionId(main_peer.token.0);
        let rdb = ConnectionId(rdb_peer.token.0);
        backend.add_connection(main_peer);
        backend.add_connection(rdb_peer);
        let public_client_id = backend.connection_registry[Token(rdb.0)].client_id();
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        let boundary = backend.ctx.replication().backlog.snapshot_boundary();
        backend
            .replication_runtime
            .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
        assert!(backend
            .replication_runtime
            .retain_completed_unpaired_rdb_wait(rdb, public_client_id, i64::MAX,));
        backend.remove_connection(rdb.0);
        assert_eq!(
            backend
                .connection_registry
                .token_for_client_id(public_client_id),
            None
        );
        let replacement_client_id = public_client_id + 1;
        backend
            .connection_registry
            .register(
                crate::framework::capabilities::connection::Connection::new_with_client_id(
                    Token(rdb.0),
                    replacement_client_id,
                    SocketAddr::from(([127, 0, 0, 1], 6380)),
                    false,
                ),
            )
            .unwrap();
        assert_ne!(
            backend.connection_registry[Token(rdb.0)].client_id(),
            public_client_id,
            "a reused runtime token must not inherit the retained public client id"
        );

        let associate = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(public_client_id.to_string()),
            ],
        };
        let mut replies = Vec::new();
        assert!(!backend.handle_replconf(main.0, main.0, &associate, false, &mut replies));

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
        assert_eq!(link.dual_endoff_boundary(), Some(boundary));
        assert!(link.dual_rdb_complete());
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        assert!(backend.full_sync.direct_dual_rdb_child.is_none());

        replies.clear();
        assert!(!backend.handle_replconf(main.0, main.0, &associate, false, &mut replies));
        assert!(replies
            .first()
            .is_some_and(|reply| reply.starts_with(b"-ERR")));
    }

    #[test]
    fn replconf_rdb_channel_assigns_the_next_waiting_main() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 0;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let main_one_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let main_two_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_one_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: main_two_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });

        let main_one = ConnectionId(main_one_listener.token.0);
        let main_two = ConnectionId(main_two_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let rdb_slab = rdb.0;
        for (main, port) in [(main_one, 6380), (main_two, 6381)] {
            backend.replication_runtime.register_primary(
                main,
                ReplicaConnectionInfo {
                    ip: "127.0.0.1".to_string(),
                    port,
                    capability: Vec::new(),
                    rdb_filter_mode: RdbFilterMode::All,
                },
            );
            backend
                .replication_runtime
                .primary_for_connection_mut(main)
                .expect("main primary link must exist")
                .transition_to_waiting_for_rdb_channel()
                .expect("main primary link must await an RDB channel");
        }

        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(rdb_slab, rdb.0, &args, false, &mut replies));

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
        backend.admit_rdb_sync_for_test(rdb);
        assert!(backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main_one),
            None
        );
        assert_eq!(
            backend.replication_runtime.rdb_channel_for_main(main_two),
            None
        );
        for main in [main_one, main_two] {
            assert_eq!(
                backend
                    .replication_runtime
                    .primary_for_connection(main)
                    .expect("main primary link must remain registered")
                    .dual_state(),
                crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
            );
        }
        assert_eq!(
            backend.replication_runtime.role(rdb),
            Some(ReplicationConnection::RdbChannelCandidate)
        );
    }

    #[test]
    fn replconf_cluster_node_id_is_retained_on_primary_ownership() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        let cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = cluster.myself.name.clone();
        *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));

        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;

        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"set-cluster-node-id"),
                Bytes::from(node_id.clone()),
            ],
        };
        let mut replies = Vec::new();

        assert!(!backend.handle_replconf(connection_token, token.0, &args, false, &mut replies));
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend
                .replication_runtime
                .replica_node_id(ConnectionId(token.0)),
            Some(node_id.as_str())
        );
    }

    #[test]
    fn replconf_error_refreshes_view_after_an_earlier_option_mutates_metadata() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;

        let initial = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
            ],
        };
        let mut replies = Vec::new();
        backend.handle_replconf(connection_token, token.0, &initial, false, &mut replies);

        let malformed = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"ip-address"),
                Bytes::from_static(b"198.51.100.7"),
                Bytes::from_static(b"rdb-channel"),
                Bytes::from_static(b"invalid"),
            ],
        };
        replies.clear();
        backend.handle_replconf(connection_token, token.0, &malformed, false, &mut replies);

        assert_eq!(
            replies,
            vec![b"-ERR REPLCONF rdb-channel expects 0 or 1\r\n".to_vec()]
        );
        backend
            .replication_runtime
            .primary_for_connection_mut(ConnectionId(token.0))
            .expect("replica link registered by REPLCONF")
            .finish_full_resync();
        backend.refresh_replication_view();
        assert_eq!(
            backend
                .ctx
                .replication()
                .view
                .replica_info
                .get(&token.0)
                .map(|info| info.ip.as_str()),
            Some("198.51.100.7")
        );
    }

    fn assert_dual_channel_capability_is_ignored_without_primary_prerequisites(
        dual_channel_enabled: bool,
        diskless_sync_enabled: bool,
    ) {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .dual_channel_replication = dual_channel_enabled;
        backend.ctx.config_mut().replication.repl_diskless_sync = diskless_sync_enabled;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;

        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"dual-channel"),
            ],
        };
        let mut replies = Vec::new();

        backend.handle_replconf(connection_token, token.0, &args, false, &mut replies);

        let link = backend
            .replication_runtime
            .primary_for_connection(ConnectionId(token.0))
            .expect("listening-port must register the main channel");
        assert!(
            !link
                .replica()
                .capability
                .iter()
                .any(|capability| capability.eq_ignore_ascii_case("dual-channel")),
            "dual-channel capability must be ignored unless both primary prerequisites hold"
        );
    }

    #[test]
    fn replconf_ignores_dual_channel_capability_when_primary_config_is_disabled() {
        assert_dual_channel_capability_is_ignored_without_primary_prerequisites(false, true);
    }

    #[test]
    fn replconf_ignores_dual_channel_capability_when_diskless_sync_is_disabled() {
        assert_dual_channel_capability_is_ignored_without_primary_prerequisites(true, false);
    }

    #[test]
    fn replconf_ignores_unknown_capability_tokens() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let args = CommandArgs {
            name: Bytes::from_static(b"REPLCONF"),
            args: vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"unknown-capability"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"skip-rdb-checksum"),
            ],
        };
        let mut replies = Vec::new();

        backend.handle_replconf(connection_token, token.0, &args, false, &mut replies);

        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(ConnectionId(token.0))
                .expect("listening-port must register the primary")
                .replica()
                .capability,
            vec![
                "eof".to_string(),
                "psync2".to_string(),
                "skip-rdb-checksum".to_string()
            ]
        );
    }
    #[test]
    fn replconf_capabilities_accumulate_across_frames_and_ignore_unknowns() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .dual_channel_replication = true;
        backend.ctx.config_mut().replication.repl_diskless_sync = true;

        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let connection_token = token.0;
        let mut replies = Vec::new();

        for args in [
            vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6380"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
            ],
            vec![Bytes::from_static(b"capa"), Bytes::from_static(b"psync2")],
            vec![
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"dual-channel"),
            ],
            vec![
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"unknown-capability"),
            ],
        ] {
            replies.clear();
            backend.handle_replconf(
                connection_token,
                token.0,
                &CommandArgs {
                    name: Bytes::from_static(b"REPLCONF"),
                    args,
                },
                false,
                &mut replies,
            );
            assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        }

        assert_eq!(
            backend
                .replication_runtime
                .primary_for_connection(ConnectionId(token.0))
                .expect("listening-port must register the primary")
                .replica()
                .capability,
            vec![
                "eof".to_string(),
                "psync2".to_string(),
                "dual-channel".to_string()
            ]
        );
    }
}
