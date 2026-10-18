use super::*;
use crate::foundation::io::protocol::parser::parse_inline_args;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;

enum ReplconfOption<'a> {
    ListeningPort(u16),
    IpAddress(&'a [u8]),
    Capability(&'a [u8]),
    RdbFilterOnly(RdbFilterMode),
    Version(u32),
    RdbOnly(bool),
    RdbChannel(bool),
    SetRdbClientId(u64),
    SetClusterNodeId(&'a [u8]),
}

#[derive(Clone, Copy)]
struct ReplconfRequest<'a> {
    connection_token: usize,
    connection_id: ConnectionId,
    has_enabled_rdb_channel: bool,
    set_rdb_client_id: Option<&'a [u8]>,
}

struct ReplconfPromotion {
    connection_id: ConnectionId,
    replica_id: crate::framework::distributed::replication::ReplicaId,
    offset: crate::engine::distributed::replication::ReplicationOffset,
    sync_mode: &'static str,
    replica: String,
}

impl<'a> ReplconfRequest<'a> {
    fn new(connection_token: usize, connection_id: ConnectionId, args: &'a CommandArgs) -> Self {
        let set_rdb_client_id = (args.args.len() == 2
            && args.args[0].eq_ignore_ascii_case(b"set-rdb-client-id"))
        .then(|| args.args[1].as_ref());
        Self {
            connection_token,
            connection_id,
            has_enabled_rdb_channel: replconf_option_enabled(args, b"rdb-channel"),
            set_rdb_client_id,
        }
    }
}

impl<'a> ReplconfOption<'a> {
    fn parse(option: &[u8], value: &'a [u8]) -> Result<Self, String> {
        if option.eq_ignore_ascii_case(b"listening-port") {
            return String::from_utf8_lossy(value)
                .parse::<u16>()
                .map(Self::ListeningPort)
                .map_err(|_| "ERR invalid listening port".to_string());
        }
        if option.eq_ignore_ascii_case(b"ip-address") {
            return Ok(Self::IpAddress(value));
        }
        if option.eq_ignore_ascii_case(b"capa") {
            return Ok(Self::Capability(value));
        }
        if option.eq_ignore_ascii_case(b"rdb-filter-only") {
            return parse_rdb_filter_only(value).map(Self::RdbFilterOnly);
        }
        Self::parse_secondary(option, value)
    }

    fn parse_secondary(option: &[u8], value: &'a [u8]) -> Result<Self, String> {
        if option.eq_ignore_ascii_case(b"version") {
            return parse_replconf_version(value)
                .map(Self::Version)
                .ok_or_else(|| {
                    format!(
                        "ERR Unrecognized version format: {}",
                        String::from_utf8_lossy(value)
                    )
                });
        }
        if option.eq_ignore_ascii_case(b"rdb-only") {
            return parse_replconf_bool(value)
                .map(Self::RdbOnly)
                .ok_or_else(|| "ERR REPLCONF rdb-only expects 0 or 1".to_string());
        }
        if option.eq_ignore_ascii_case(b"rdb-channel") {
            return parse_replconf_bool(value)
                .map(Self::RdbChannel)
                .ok_or_else(|| "ERR REPLCONF rdb-channel expects 0 or 1".to_string());
        }
        if option.eq_ignore_ascii_case(b"set-rdb-client-id") {
            return std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Self::SetRdbClientId)
                .ok_or_else(|| "ERR invalid RDB client id".to_string());
        }
        if option.eq_ignore_ascii_case(b"set-cluster-node-id") {
            return Ok(Self::SetClusterNodeId(value));
        }
        Err(format!(
            "ERR Unrecognized REPLCONF option: {}",
            String::from_utf8_lossy(option)
        ))
    }
}

impl ServerWorkerState {
    fn promote_matching_handshake_replica(
        &mut self,
        connection_token: usize,
        node_id: &str,
    ) -> bool {
        let peer_ip = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .peer_addr
        .ip()
        .to_string();
        let connection_id = ConnectionId(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token()
                .0,
        );
        let listening_port = self
            .replication_runtime
            .primary_for_connection(connection_id)
            .map(|link| link.replica().port)
            .unwrap_or(0);
        if listening_port == 0 {
            return false;
        }
        self.ctx
            .cluster_state_mut()
            .as_deref_mut()
            .is_some_and(|state| {
                state.promote_endpoint_handshake_replica(&peer_ip, listening_port, node_id)
            })
    }

    fn handle_replconf_ack(&mut self, connection_token: usize, args: &CommandArgs) -> bool {
        let Some(offset) = args.args.get(1).and_then(|arg| parse_live_ack_offset(arg)) else {
            self.ctx.end_cmd("replconf", false);
            return false;
        };
        let connection_id = ConnectionId(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token()
                .0,
        );
        let Ok(aof_offset) = parse_replconf_fack(args) else {
            self.ctx.end_cmd("replconf", false);
            return false;
        };
        let promotion = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
            .and_then(|link| {
                link.acknowledge(offset, mstime());
                if let Some(aof_offset) = aof_offset {
                    link.acknowledge_aof(aof_offset);
                }
                if link.promote_dual_replica_on_ack() {
                    Some(("dual", link.replica().display_name()))
                } else if link.promote_full_resync_on_ack() {
                    Some(("classic", link.replica().display_name()))
                } else {
                    None
                }
            });
        if let Some((sync_mode, replica)) = promotion {
            if let Some(replica_id) = self
                .replication_runtime
                .replica_id_for_connection(connection_id)
            {
                self.finish_replconf_ack_promotion(
                    connection_token,
                    ReplconfPromotion {
                        connection_id,
                        replica_id,
                        offset,
                        sync_mode,
                        replica,
                    },
                );
            }
        }
        self.refresh_replication_view();
        self.check_wait_replica_wakeups();
        self.ctx.end_cmd("replconf", false);
        false
    }

    fn finish_replconf_ack_promotion(
        &mut self,
        connection_token: usize,
        promotion: ReplconfPromotion,
    ) {
        crate::server_log!(
            notice,
            "replica terminal ACK promoted {} replica={} connection={} offset={}",
            promotion.sync_mode,
            promotion.replica,
            promotion.connection_id.0,
            promotion.offset.get()
        );
        let _ = self.on_diskless_terminal_ack(promotion.replica_id);
        if promotion.sync_mode == "dual" {
            return;
        }
        if promotion.sync_mode == "classic" {
            // EOF-framed diskless sync reaches the command stream at this first
            // post-RDB ACK, matching C's replicaStartCommandStream() call from
            // REPLCONF ACK processing.
            self.start_replica_command_stream();
        }
        let pumped = {
            let backlog = &self.ctx.replication().backlog;
            let link = self
                .replication_runtime
                .primary_for_connection_mut(promotion.connection_id)
                .expect("promoted dual replica link must remain registered");
            link.pump(backlog)
        };
        let tail = match pumped {
            Ok(tail) => tail,
            Err(_) => {
                self.remove_connection(connection_token);
                self.refresh_replication_view();
                return;
            }
        };
        self.replication_runtime
            .primary_for_connection_mut(promotion.connection_id)
            .expect("promoted replica link remains registered after backlog pump")
            .mark_replication_queued(tail.len());
        if !tail.is_empty() {
            self.queue_promoted_replica_tail(connection_token, tail);
        }
    }

    fn queue_promoted_replica_tail(&mut self, connection_token: usize, tail: Vec<u8>) {
        self.ctx
            .stats_mut()
            .add_net_repl_output_bytes(tail.len() as u64);
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_replication_reply(tail);
        self.transport
            .set_interest(
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token(),
                crate::foundation::io::transport::Interest::ReadWritable,
            )
            .ok();
        // The terminal ACK makes the first post-snapshot commands eligible;
        // flush them without waiting for an unrelated readiness batch.
        if let crate::foundation::io::transport::WriteOutcome::Failed(error) =
            self.flush_connection_output(connection_token)
        {
            crate::server_log!(
                warning,
                "promoted replica tail flush failed on token {}: {}",
                connection_token,
                error
            );
            self.remove_connection(connection_token);
        }
    }

    fn apply_replconf_option(
        &mut self,
        request: ReplconfRequest<'_>,
        option: ReplconfOption<'_>,
    ) -> Result<(), String> {
        match option {
            ReplconfOption::ListeningPort(port) => self.set_replconf_listening_port(
                request.connection_token,
                request.connection_id,
                port,
                request.has_enabled_rdb_channel,
            ),
            ReplconfOption::IpAddress(ip) => self.set_replconf_ip(request.connection_id, ip),
            ReplconfOption::Capability(capability) => {
                self.add_replconf_capability(request.connection_id, capability)
            }
            ReplconfOption::RdbFilterOnly(mode) => {
                self.set_replconf_filter(request.connection_id, mode)
            }
            ReplconfOption::Version(version) => {
                self.set_replconf_version(request.connection_id, version)
            }
            control => return self.apply_replconf_control_option(request, control),
        }
        Ok(())
    }

    fn apply_replconf_control_option(
        &mut self,
        request: ReplconfRequest<'_>,
        option: ReplconfOption<'_>,
    ) -> Result<(), String> {
        match option {
            ReplconfOption::RdbOnly(enabled) => {
                self.replication_runtime
                    .set_rdb_only(request.connection_id, enabled);
                Ok(())
            }
            ReplconfOption::RdbChannel(enabled) => self.set_replconf_rdb_channel(
                request.connection_token,
                request.connection_id,
                enabled,
            ),
            ReplconfOption::SetRdbClientId(client_id) => {
                self.set_replconf_rdb_client_id(request.connection_id, client_id)
            }
            ReplconfOption::SetClusterNodeId(node_id) => self.set_replconf_cluster_node_id(
                request.connection_token,
                request.connection_id,
                node_id,
            ),
            _ => unreachable!("metadata REPLCONF options are handled before control options"),
        }
    }

    fn set_replconf_listening_port(
        &mut self,
        connection_token: usize,
        connection_id: ConnectionId,
        port: u16,
        has_enabled_rdb_channel: bool,
    ) {
        if self.replication_runtime.role(connection_id).is_none() {
            self.replication_runtime
                .stage_replconf_port(connection_id, port);
        }
        if !has_enabled_rdb_channel || self.replication_runtime.role(connection_id).is_some() {
            self.ensure_primary_link(connection_token, port);
        }
    }

    fn set_replconf_ip(&mut self, connection_id: ConnectionId, value: &[u8]) {
        let ip = String::from_utf8_lossy(value).to_string();
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.set_replica_ip(ip);
        } else if self.is_rdb_channel(connection_id) {
            let port = self
                .replication_runtime
                .channel_endpoint(connection_id)
                .map_or(0, |(_, port)| port);
            self.replication_runtime
                .set_channel_endpoint(connection_id, ip, port);
        } else {
            self.replication_runtime
                .stage_replconf_ip(connection_id, ip);
        }
    }

    fn add_replconf_capability(&mut self, connection_id: ConnectionId, value: &[u8]) {
        if !self.accepts_replconf_capability(value) {
            return;
        }
        let capability = String::from_utf8_lossy(value).to_string();
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.add_replica_capability(capability);
        } else if self.is_rdb_channel(connection_id) {
            self.replication_runtime
                .add_channel_capability(connection_id, capability);
        } else {
            self.replication_runtime
                .stage_replconf_capability(connection_id, capability);
        }
    }

    fn accepts_replconf_capability(&self, value: &[u8]) -> bool {
        if value.eq_ignore_ascii_case(b"dual-channel") {
            let config = &self.ctx.config().replication;
            return config.dual_channel_replication && config.repl_diskless_sync;
        }
        value.eq_ignore_ascii_case(b"eof")
            || value.eq_ignore_ascii_case(b"psync2")
            || value.eq_ignore_ascii_case(b"skip-rdb-checksum")
    }

    fn set_replconf_filter(&mut self, connection_id: ConnectionId, mode: RdbFilterMode) {
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.set_replica_rdb_filter_mode(mode);
        } else {
            self.replication_runtime
                .stage_replconf_rdb_filter_only(connection_id, mode);
        }
    }

    fn set_replconf_version(&mut self, connection_id: ConnectionId, version: u32) {
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.set_replica_version(version);
        } else {
            self.replication_runtime
                .set_replica_version(connection_id, version);
        }
    }

    fn set_replconf_rdb_channel(
        &mut self,
        connection_token: usize,
        connection_id: ConnectionId,
        enabled: bool,
    ) -> Result<(), String> {
        if enabled {
            if !self.ensure_rdb_candidate(connection_token) {
                return Err("ERR connection is already assigned a replication role".to_string());
            }
            if let Some(info) = self.ctx.client_info_mut(connection_id.0) {
                info.last_cmd = "sync".to_string();
            }
            crate::server_log!(
                debug,
                "dual-rdb handshake: primary accepted rdb-channel rdb={} role={:?}",
                connection_id.0,
                self.replication_runtime.role(connection_id)
            );
            return Ok(());
        }
        let affected = self.replication_runtime.clear_rdb_channel(connection_id);
        for affected_connection in affected {
            if affected_connection == connection_id {
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .state =
                    crate::framework::capabilities::connection::ConnectionState::CloseAfterReply;
            } else {
                self.remove_connection(affected_connection.0);
            }
        }
        Ok(())
    }

    fn set_replconf_rdb_client_id(
        &mut self,
        connection_id: ConnectionId,
        public_client_id: u64,
    ) -> Result<(), String> {
        let Some(rdb_token) = self
            .connection_registry
            .token_for_client_id(public_client_id)
            .map(|token| token.0)
        else {
            self.replication_runtime
                .try_associate_retained_rdb_wait(connection_id, public_client_id, mstime())
                .map_err(|_| "ERR unrecognized RDB client id".to_string())?;
            crate::server_log!(
                debug,
                "dual-rdb boundary: associated retained RDB wait main={} public_client_id={}",
                connection_id.0,
                public_client_id
            );
            return Ok(());
        };
        let rdb_connection_id = ConnectionId(rdb_token);
        self.replication_runtime
            .associate_rdb_channel(connection_id, rdb_connection_id)
            .map_err(|_| "ERR unrecognized RDB client id".to_string())?;
        self.associate_direct_dual_rdb_child_target(rdb_connection_id, connection_id);
        self.finalize_replconf_rdb_association(connection_id, rdb_connection_id);
        self.release_full_dual_snapshot_batch();
        self.log_replconf_rdb_association(connection_id, rdb_connection_id);
        self.drive_zero_delay_dual_snapshot(rdb_connection_id);
        Ok(())
    }

    fn finalize_replconf_rdb_association(
        &mut self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
    ) {
        if let Err(error) = self
            .replication_runtime
            .finalize_dual_channel_rdb_association(main_connection_id, rdb_connection_id)
        {
            log::warn!("finalize_dual_channel_rdb_association failed for main={main_connection_id:?}, rdb={rdb_connection_id:?}: {error:?}");
        }
    }

    fn release_full_dual_snapshot_batch(&mut self) {
        let max_replicas = self
            .ctx
            .config()
            .replication
            .repl_diskless_sync_max_replicas;
        if max_replicas <= 0
            || (self.full_sync.pending_dual_rdb_snapshots.len() as i64) < max_replicas
        {
            return;
        }
        let now = mstime();
        for (ready_at, _) in self.full_sync.pending_dual_rdb_snapshots.values_mut() {
            *ready_at = now;
        }
        self.drive_pending_dual_rdb_snapshots();
    }

    fn log_replconf_rdb_association(
        &self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
    ) {
        crate::server_log!(
            debug,
            "dual-rdb boundary: associated main={} rdb={} paired_rdb={:?} delay_ms={}",
            main_connection_id.0,
            rdb_connection_id.0,
            self.replication_runtime
                .rdb_channel_for_main(main_connection_id)
                .map(|id| id.0),
            self.ctx
                .config()
                .replication
                .repl_diskless_sync_delay
                .saturating_mul(1_000)
        );
    }

    fn drive_zero_delay_dual_snapshot(&mut self, rdb_connection_id: ConnectionId) {
        if self.ctx.config().replication.repl_diskless_sync_delay != 0 {
            return;
        }
        let token = crate::framework::capabilities::io::Token(rdb_connection_id.0);
        if self
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&token)
        {
            self.drive_pending_dual_rdb_snapshots();
        }
    }

    fn set_replconf_cluster_node_id(
        &mut self,
        connection_token: usize,
        connection_id: ConnectionId,
        value: &[u8],
    ) -> Result<(), String> {
        if !self.ctx.config().server.cluster_enabled {
            return Err("ERR This instance has cluster support disabled".to_string());
        }
        let node_id = String::from_utf8_lossy(value).into_owned();
        let known_node = self.ctx.cluster_state().as_ref().is_some_and(|state| {
            state.myself.name == node_id || state.nodes.contains_key(&node_id)
        });
        let promoted_handshake = !known_node
            && is_valid_cluster_node_id(value)
            && self.promote_matching_handshake_replica(connection_token, &node_id);
        if !is_valid_cluster_node_id(value) || (!known_node && !promoted_handshake) {
            return Err(format!("ERR Unknown node {node_id}"));
        }
        self.replication_runtime
            .set_replica_node_id(connection_id, node_id);
        Ok(())
    }

    pub(super) fn handle_replconf(
        &mut self,
        connection_token: usize,
        _token_value: usize,
        args: &CommandArgs,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if !args.args.len().is_multiple_of(2) {
            return self.replconf_error(resp3, replies, "ERR syntax error");
        }
        let sub = args
            .args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .unwrap_or_default();
        if sub == "ack" {
            return self.handle_replconf_ack(connection_token, args);
        }

        let connection_id = ConnectionId(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token()
                .0,
        );
        let request = ReplconfRequest::new(connection_token, connection_id, args);
        for pair in args.args.as_chunks::<2>().0 {
            let option = match ReplconfOption::parse(&pair[0], &pair[1]) {
                Ok(option) => option,
                Err(message) => return self.replconf_error(resp3, replies, &message),
            };
            if let Err(message) = self.apply_replconf_option(request, option) {
                return self.replconf_error(resp3, replies, &message);
            }
        }
        self.finish_replconf(request, resp3, replies)
    }

    fn finish_replconf(
        &mut self,
        request: ReplconfRequest<'_>,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let configured_delay_ms = self.configured_diskless_delay_ms();
        self.refresh_replication_view();
        self.ctx.end_cmd("replconf", false);
        replies.push(if resp3 {
            serialize_to_vec_resp3(&Value::ok())
        } else {
            serialize_to_vec_resp2(&Value::ok())
        });
        if let Some(rdb_client_id) = request.set_rdb_client_id {
            crate::server_log!(
                debug,
                "dual-rdb primary set-rdb-client-id reply queued: main={} rdb={} reply=+OK",
                request.connection_id.0,
                String::from_utf8_lossy(rdb_client_id)
            );
        }
        if request.has_enabled_rdb_channel {
            self.schedule_dual_rdb_snapshot(request.connection_id, configured_delay_ms);
        }
        false
    }

    fn configured_diskless_delay_ms(&self) -> i64 {
        self.ctx
            .config()
            .replication
            .repl_diskless_sync_delay
            .saturating_mul(1_000)
    }

    fn schedule_dual_rdb_snapshot(
        &mut self,
        connection_id: ConnectionId,
        configured_delay_ms: i64,
    ) {
        if self.full_sync.direct_dual_rdb_child.is_some() {
            crate::server_log!(
                notice,
                "Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC"
            );
        }
        self.full_sync.pending_dual_rdb_snapshots.insert(
            crate::framework::capabilities::io::Token(connection_id.0),
            (mstime().saturating_add(configured_delay_ms), Vec::new()),
        );
        let max_replicas = self
            .ctx
            .config()
            .replication
            .repl_diskless_sync_max_replicas;
        if max_replicas <= 0
            || (self.full_sync.pending_dual_rdb_snapshots.len() as i64) < max_replicas
        {
            return;
        }
        let now = mstime();
        for (ready_at, _) in self.full_sync.pending_dual_rdb_snapshots.values_mut() {
            *ready_at = now;
        }
    }

    fn replconf_error(&mut self, resp3: bool, replies: &mut Vec<Vec<u8>>, message: &str) -> bool {
        self.refresh_replication_view();
        self.ctx.end_cmd("replconf", false);
        replies.push(if resp3 {
            serialize_to_vec_resp3(&Value::error(message.as_bytes().to_vec()))
        } else {
            serialize_to_vec_resp2(&Value::error(message.as_bytes().to_vec()))
        });
        false
    }
}

fn replconf_option_enabled(args: &CommandArgs, option_name: &[u8]) -> bool {
    args.args.as_chunks::<2>().0.iter().any(|pair| {
        pair[0].eq_ignore_ascii_case(option_name) && parse_replconf_bool(&pair[1]) == Some(true)
    })
}

fn parse_replconf_fack(
    args: &CommandArgs,
) -> Result<Option<crate::engine::distributed::replication::ReplicationOffset>, ()> {
    let Some(raw) = args
        .args
        .windows(2)
        .find(|window| window[0].eq_ignore_ascii_case(b"fack"))
        .map(|window| window[1].as_ref())
    else {
        return Ok(None);
    };
    parse_live_ack_offset(raw).map(Some).ok_or(())
}

fn parse_live_ack_offset(
    raw: &[u8],
) -> Option<crate::engine::distributed::replication::ReplicationOffset> {
    let raw = std::str::from_utf8(raw).ok()?.parse::<i64>().ok()?;
    let offset = crate::engine::distributed::replication::ReplicationOffset::try_from(raw).ok()?;
    offset.checked_next_request()?;
    Some(offset)
}

fn parse_rdb_filter_only(value: &[u8]) -> Result<RdbFilterMode, String> {
    let filters =
        parse_inline_args(value).map_err(|_| "ERR Missing rdb-filter-only values".to_string())?;
    let saw_filter = !filters.is_empty();
    for filter in filters {
        let filter = String::from_utf8_lossy(&filter);
        if !filter.eq_ignore_ascii_case("functions") {
            return Err(format!("ERR Unsupported rdb-filter-only option: {filter}"));
        }
    }
    Ok(if saw_filter {
        RdbFilterMode::Functions
    } else {
        RdbFilterMode::Empty
    })
}

fn parse_replconf_bool(value: &[u8]) -> Option<bool> {
    match value {
        b"0" => Some(false),
        b"1" => Some(true),
        _ => None,
    }
}

fn parse_replconf_version(value: &[u8]) -> Option<u32> {
    let mut version = 0;
    let mut parts = value.split(|byte| *byte == b'.');
    for _ in 0..3 {
        let part = parts.next()?;
        if part.is_empty() {
            return None;
        }
        let mut component = 0u32;
        for byte in part {
            if !byte.is_ascii_digit() {
                return None;
            }
            component = component
                .checked_mul(10)?
                .checked_add(u32::from(byte - b'0'))?;
            if component > u32::from(u8::MAX) {
                return None;
            }
        }
        version = (version << 8) | component;
    }
    parts.next().is_none().then_some(version)
}

fn is_valid_cluster_node_id(value: &[u8]) -> bool {
    value.len() == 40
        && value
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

pub(crate) fn rewrite_endoff_client_id(frame: Vec<u8>, client_id: u64) -> Vec<u8> {
    let Ok(text) = std::str::from_utf8(&frame) else {
        return frame;
    };
    let Some(last_space) = text.rfind(' ') else {
        return frame;
    };
    format!("{} {}\r\n", &text[..last_space], client_id).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use std::net::TcpListener;

    #[test]
    fn failed_promoted_replica_tail_flush_tears_down_replica_owner() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
        let peer_addr = listener.local_addr().expect("read replica peer address");
        let peer = runtime
            .transport
            .connect_tcp(peer_addr)
            .expect("connect replica transport");
        let (_replica_stream, _) = listener.accept().expect("accept replica transport");
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        });
        let connection_id = ConnectionId(peer.token.0);
        runtime.replication_runtime.register_primary(
            connection_id,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        runtime
            .transport
            .remove_registered(peer.token)
            .expect("remove transport before failed flush");

        runtime.queue_promoted_replica_tail(peer.token.0, b"tail".to_vec());

        assert!(!runtime.connection_registry.contains(Token(peer.token.0)));
        assert!(
            runtime
                .replication_runtime
                .primary_for_connection(connection_id)
                .is_none(),
            "Failed must tear down the downstream replica owner"
        );
    }

    #[test]
    fn classic_ack_pump_error_tears_down_stale_replica_owner() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
        let peer_addr = listener.local_addr().expect("read replica peer address");
        let peer = runtime
            .transport
            .connect_tcp(peer_addr)
            .expect("connect replica transport");
        let (_replica_stream, _) = listener.accept().expect("accept replica transport");
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        });
        let connection_id = ConnectionId(peer.token.0);
        runtime.replication_runtime.register_primary(
            connection_id,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        {
            let backlog = &runtime.ctx.replication().backlog;
            let link = runtime
                .replication_runtime
                .primary_for_connection_mut(connection_id)
                .expect("classic replica link exists");
            link.plan_full_resync(backlog);
            link.finish_full_resync();
        }
        runtime.ctx.replication_mut().backlog.set_capacity(0);
        runtime
            .ctx
            .replication_mut()
            .backlog
            .append(b"evicted-tail")
            .unwrap();
        let args = CommandArgs {
            name: bytes::Bytes::from_static(b"REPLCONF"),
            args: vec![
                bytes::Bytes::from_static(b"ACK"),
                bytes::Bytes::from_static(b"0"),
            ],
        };
        let mut replies = Vec::new();

        runtime.handle_replconf(peer.token.0, peer.token.0, &args, false, &mut replies);

        assert!(!runtime.connection_registry.contains(Token(peer.token.0)));
        assert!(runtime
            .replication_runtime
            .primary_for_connection(connection_id)
            .is_none());
    }
}
