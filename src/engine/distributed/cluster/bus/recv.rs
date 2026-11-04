#[cfg(test)]
use std::io::Read;

use crate::engine::distributed::cluster::action::ClusterAction;
use crate::engine::distributed::cluster::msg::{
    self, bytes_to_slots, ClusterBusBody, ClusterMessageFlags, ClusterMsg, ClusterMsgType,
};
use crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
use crate::engine::distributed::cluster::state::ClusterState;

use super::types::ClusterBusManager;

impl ClusterBusManager {
    fn maximum_claimed_slot_epoch(state: &ClusterState, claimed_slots: &[u8]) -> u64 {
        if claimed_slots.len() != msg::SLOT_BYTES {
            return u64::MAX;
        }
        let claimed_slots = bytes_to_slots(claimed_slots);
        claimed_slots
            .iter()
            .enumerate()
            .filter(|(_, claimed)| **claimed)
            .filter_map(|(slot, _)| state.bound_node_by_slot(slot))
            .map(|owner| owner.config_epoch)
            .max()
            .unwrap_or_default()
    }

    /// Handle incoming data on a cluster bus connection.
    #[cfg(test)]
    pub fn readable(
        &mut self,
        state: &mut ClusterState,
        stream: &mut dyn Read,
        token_value: usize,
        now: i64,
    ) -> bool {
        let conn = match self.connections.get_mut(&token_value) {
            Some(c) => c,
            None => return false,
        };

        let mut buf = [0u8; 16384];
        let mut reached_eof = false;
        loop {
            match stream.read(&mut buf) {
                Ok(0) => {
                    reached_eof = true;
                    break;
                }
                Ok(n) => {
                    conn.read_buf.extend_from_slice(&buf[..n]);
                    conn.last_recv = now;
                    conn.bytes_received = conn.bytes_received.saturating_add(n as u64);
                    conn.bytes_sent_since_read = 0;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    crate::server_log!(warning, "Cluster bus read error on {}: {}", conn.addr, e);
                    return true;
                }
            }
        }

        self.process_read_buffer(state, token_value, now) || reached_eof
    }

    /// Process the read buffer and handle complete messages.
    pub(crate) fn process_read_bytes(
        &mut self,
        state: &mut ClusterState,
        token_value: usize,
        bytes: &[u8],
        now: i64,
    ) -> bool {
        if let Some(connection) = self.connections.get_mut(&token_value) {
            if !bytes.is_empty() {
                connection.read_buf.extend_from_slice(bytes);
                connection.last_recv = now;
                connection.bytes_received =
                    connection.bytes_received.saturating_add(bytes.len() as u64);
                connection.bytes_sent_since_read = 0;
            }
        }
        self.process_read_buffer(state, token_value, now)
    }

    fn process_read_buffer(
        &mut self,
        state: &mut ClusterState,
        token_value: usize,
        now: i64,
    ) -> bool {
        let buf = match self.connections.get(&token_value) {
            Some(c) => c.read_buf.clone(),
            None => return false,
        };

        let mut processed = 0;
        let mut should_close = false;
        loop {
            if buf.len() < processed + msg::CLUSTER_BUS_HEADER_LEN {
                break;
            }
            let remaining = &buf[processed..];
            if remaining.len() < msg::CLUSTER_BUS_HEADER_LEN {
                break;
            }

            match msg::parse_msg(remaining) {
                Ok((cluster_msg, total_len)) => {
                    processed += total_len;
                    state.record_bus_message_received(cluster_msg.msg_type);
                    state.record_bus_frame_received(cluster_msg.msg_type, total_len);

                    if state.should_drop_packet(cluster_msg.msg_type) {
                        crate::server_log!(
                            warning,
                            "Dropping cluster packet of type {:?} that matches debug filter",
                            cluster_msg.msg_type
                        );
                        should_close = state.should_close_link_on_packet_drop();
                        if should_close {
                            self.queue_meet_retry(state, token_value);
                            break;
                        }
                        continue;
                    }
                    if self.mark_noaddr_on_mismatching_pong(state, &cluster_msg, token_value) {
                        should_close = true;
                        break;
                    }
                    if !self.accept_noaddr_sender(state, &cluster_msg, token_value) {
                        continue;
                    }

                    if let Some(conn) = self.connections.get_mut(&token_value) {
                        let bind_known = conn.node_name.is_empty()
                            && conn.handshake_node_name.is_empty()
                            && state.nodes.contains_key(&cluster_msg.sender_id);
                        if bind_known {
                            // Match C getNodeFromLinkAndMsg: a known sender
                            // claims the inbound link before packet-specific
                            // MEET/PING/PONG handling. Without this binding a
                            // reciprocal MEET leaves both peers believing the
                            // other has no inbound connection.
                            conn.node_name = cluster_msg.sender_id.clone();
                        }
                    }

                    self.handle_message(state, &cluster_msg, token_value, now);
                    self.propagate_gossip_failure_quorum(state, &cluster_msg, now);
                    if let Some(sender) = state.nodes.get_mut(&cluster_msg.sender_id) {
                        sender.data_received = now;
                    }
                }
                Err(error) if error.is_incomplete() => break,
                Err(msg::ClusterProtocolError::UnsupportedVersion(version)) => {
                    state.record_bus_message_received_from_header(remaining);
                    crate::server_log!(
                        debug,
                        "Dropping cluster packet with unsupported protocol version {}",
                        version
                    );
                    let frame_len = u32::from_be_bytes([
                        remaining[4],
                        remaining[5],
                        remaining[6],
                        remaining[7],
                    ]) as usize;
                    processed += frame_len;
                }
                Err(e) => {
                    state.record_bus_message_received_from_header(remaining);
                    crate::server_log!(debug, "Cluster msg parse error: {}", e);
                    should_close = true;
                    break;
                }
            }
        }

        if processed > 0 {
            if let Some(conn) = self.connections.get_mut(&token_value) {
                conn.read_buf.drain(..processed);
            }
        }
        should_close
    }

    fn propagate_gossip_failure_quorum(
        &mut self,
        state: &mut ClusterState,
        message: &ClusterMsg,
        now_ms: i64,
    ) {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};

        if !state.nodes.contains_key(&message.sender_id) {
            return;
        }
        for entry in message.gossip() {
            let locally_suspect = state.nodes.get(&entry.node_id).is_some_and(|node| {
                node.has_flag(CLUSTER_NODE_PFAIL) && !node.has_flag(CLUSTER_NODE_FAIL)
            });
            if !locally_suspect || entry.flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL) == 0 {
                continue;
            }
            let (votes, quorum) = state.count_fail_reports(
                &entry.node_id,
                now_ms,
                state.failure_report_node_timeout_ms(),
            );
            if votes < quorum || !state.mark_fail(&entry.node_id) {
                continue;
            }
            // C checks failure quorum inside clusterProcessGossipSection.
            // Publish the decision now so replicas can confirm their failed
            // primary before a due election, without waiting for another cron.
            let failing = &state.nodes[&entry.node_id];
            let fail = self.build_fail(state, failing);
            for connection in self.connections.values_mut() {
                if connection.is_outgoing
                    && connection.connected
                    && connection.handshake_sent
                    && !connection.node_name.is_empty()
                {
                    connection.write_buf.extend_from_slice(&fail);
                    state.record_bus_message_sent(ClusterMsgType::Fail);
                }
            }
        }
    }

    fn accept_noaddr_sender(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
    ) -> bool {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR;

        let Some(node) = state.nodes.get(&msg.sender_id) else {
            return true;
        };
        if !node.has_flag(CLUSTER_NODE_NOADDR) {
            return true;
        }
        let valid_outbound = self
            .connections
            .get(&token_value)
            .is_some_and(|connection| {
                connection.is_outgoing && connection.node_name == msg.sender_id
            });
        if !valid_outbound {
            return false;
        }
        state.clear_node_failure_flags(&msg.sender_id);
        state.mark_topology_changed();
        true
    }

    fn mark_noaddr_on_mismatching_pong(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
    ) -> bool {
        if msg.msg_type != ClusterMsgType::Pong {
            return false;
        }
        let Some(connection) = self
            .connections
            .get(&token_value)
            .filter(|connection| connection.is_outgoing)
        else {
            return false;
        };
        let expected_id = connection.node_name.clone();
        let Some(expected) = state.nodes.get(&expected_id) else {
            return false;
        };
        if expected.has_flag(CLUSTER_NODE_HANDSHAKE) || expected_id == msg.sender_id {
            return false;
        }
        let old_ip = expected.ip().to_string();
        let old_bus_port = expected.cport();
        crate::server_log!(
            notice,
            "PONG contains mismatching sender ID. About node {}",
            expected_id
        );
        state.mark_node_noaddr_failed(&expected_id);
        state.mark_topology_changed();
        state.retain_pending_connects(|(ip, bus_port, _)| {
            ip != &old_ip || *bus_port != old_bus_port
        });
        state.recompute_state();
        state.request_config_save();
        let fail = state
            .nodes
            .get(&expected_id)
            .map(|node| self.build_fail(state, node));
        if let Some(fail) = fail {
            for connection in self.connections.values_mut() {
                if connection.is_outgoing
                    && connection.connected
                    && connection.handshake_sent
                    && connection.node_name != expected_id
                {
                    connection.write_buf.extend_from_slice(&fail);
                    state.record_bus_message_sent(ClusterMsgType::Fail);
                }
            }
        }
        true
    }

    fn queue_meet_retry(&self, state: &mut ClusterState, token_value: usize) {
        let Some(conn) = self.connections.get(&token_value) else {
            return;
        };
        if !conn.is_outgoing || !conn.send_meet {
            return;
        }
        state.mark_handshake_retry(&conn.handshake_node_name);
        let retry = (conn.addr.ip().to_string(), conn.addr.port(), true);
        state.queue_connect_once(retry);
    }

    /// Handle a parsed cluster bus message.
    fn handle_message(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
        now: i64,
    ) {
        Self::observe_known_sender_header(state, msg, now);
        match msg.msg_type {
            ClusterMsgType::Meet => {
                self.handle_meet_received(state, msg, token_value, now);
            }
            ClusterMsgType::Ping => {
                let completes_temporary_handshake =
                    self.connections
                        .get(&token_value)
                        .is_some_and(|connection| {
                            let temporary_outbound = connection.is_outgoing
                                && state
                                    .nodes
                                    .get(&connection.handshake_node_name)
                                    .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE));
                            let separate_inbound = !connection.is_outgoing
                                && connection.handshake_node_name.is_empty()
                                && state.nodes.values().any(|node| {
                                    node.has_flag(CLUSTER_NODE_HANDSHAKE)
                                        && node.ip == msg.sender_ip
                                        && node.port == msg.port
                                        && node.cport == msg.bus_port
                                });
                            temporary_outbound || separate_inbound
                        });
                let same_inbound_temporary_handshake = self
                    .connections
                    .get(&token_value)
                    .and_then(|connection| state.nodes.get(&connection.handshake_node_name))
                    .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE));
                if (state.nodes.contains_key(&msg.sender_id) && !same_inbound_temporary_handshake)
                    || completes_temporary_handshake
                {
                    self.handle_ping_received(state, msg, token_value, now);
                    Self::handle_paused_primary_message(state, msg);
                } else {
                    let pong = self.build_pong(state);
                    if let Some(connection) = self.connections.get_mut(&token_value) {
                        connection.write_buf.extend_from_slice(&pong);
                    }
                }
            }
            ClusterMsgType::Pong => {
                self.handle_pong_received(state, msg, token_value, now);
                Self::handle_paused_primary_message(state, msg);
            }
            ClusterMsgType::Fail => {
                use crate::engine::distributed::cluster::node::{
                    CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF,
                };
                if !state.nodes.contains_key(&msg.sender_id) {
                    return;
                }
                let ClusterBusBody::Fail { node_id } = &msg.body else {
                    return;
                };
                let victim = node_id.to_string();
                // Match C: the sender has already confirmed failure, so a
                // recent local PONG must not delay a replica's election.
                let should_mark_fail = state.nodes.get(&victim).is_some_and(|node| {
                    node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF) == 0
                });
                if should_mark_fail {
                    crate::server_log!(
                        notice,
                        "Cluster FAIL received about {} from {}",
                        victim,
                        msg.sender_id
                    );
                    state.mark_fail(&victim);
                }
            }
            ClusterMsgType::Update => {
                if !state.nodes.contains_key(&msg.sender_id) {
                    return;
                }
                Self::add_or_update_node(state, msg, true);
                if Self::prepare_update_owner(state, msg) {
                    Self::process_update_slot_claims(state, msg, true);
                }
            }
            ClusterMsgType::MfStart => {
                Self::add_or_update_node(state, msg, true);
                if state.myself.is_primary() && msg.replicaof == state.myself.name {
                    if let Ok(requester) =
                        crate::engine::distributed::cluster::NodeId::parse(msg.sender_id.as_bytes())
                    {
                        let timeout_ms = state.manual_failover_timeout_ms();
                        let actions =
                            state.accept_manual_failover_on_primary(requester, now, timeout_ms);
                        state.queue_cluster_actions(actions);
                    }
                }
            }
            ClusterMsgType::FailoverAuthRequest => {
                if !state.nodes.contains_key(&msg.sender_id) {
                    return;
                }
                let safe_to_vote = state.safe_to_vote(now);
                let current_epoch_before_request = state.current_epoch;
                Self::add_or_update_node(state, msg, true);
                use crate::engine::distributed::cluster::state::failover::VoteRequestFacts;
                use crate::engine::distributed::cluster::NodeId;
                let Ok(requester_id) = NodeId::parse(msg.sender_id.as_bytes()) else {
                    return;
                };
                if state.defer_for_better_replica_election(&requester_id, msg.current_epoch, now) {
                    crate::server_log!(
                        notice,
                        "Deferring local failover election after better replica {} requested epoch {}",
                        msg.sender_id,
                        msg.current_epoch,
                    );
                }
                let Some(requester) = state.nodes.get(&msg.sender_id) else {
                    return;
                };
                let requester_primary = if requester.replicaof == state.myself.name {
                    Some(&state.myself)
                } else {
                    state.nodes.get(&requester.replicaof)
                };
                let facts = VoteRequestFacts {
                    requester: requester_id,
                    local_is_voting_primary: ClusterState::is_voting_primary(&state.myself),
                    safe_to_vote,
                    requester_is_replica: requester.is_replica(),
                    requester_primary_known: requester_primary.is_some(),
                    requester_primary_failed: requester_primary.is_some_and(|primary| {
                        primary
                            .has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
                    }),
                    force_ack: msg.has_message_flag(ClusterMessageFlags::FORCE_ACK),
                    request_epoch: msg.current_epoch,
                    current_epoch: current_epoch_before_request.max(msg.current_epoch),
                    request_config_epoch: msg.config_epoch,
                    maximum_claimed_slot_epoch: Self::maximum_claimed_slot_epoch(
                        state,
                        &msg.myslots,
                    ),
                    persisted_last_vote_epoch: state.last_vote_epoch,
                };
                let stale_slot_claim =
                    facts.maximum_claimed_slot_epoch > facts.request_config_epoch;
                let rejection = state.automatic_failover.vote_rejection_reason(&facts);
                if rejection == Some(7) {
                    crate::server_log!(
                        warning,
                        "Failover auth denied to {}: slot epoch {} > reqConfigEpoch {}",
                        msg.sender_id,
                        facts.maximum_claimed_slot_epoch,
                        facts.request_config_epoch,
                    );
                }
                let actions = state.automatic_failover_on_vote_request(&facts);
                if actions
                    .iter()
                    .any(|action| matches!(action, ClusterAction::SendFailoverAuthAck { .. }))
                {
                    crate::server_log!(
                        notice,
                        "Failover auth granted to {} for epoch {}",
                        msg.sender_id,
                        msg.current_epoch
                    );
                } else if !stale_slot_claim {
                    crate::server_log!(
                        debug,
                        "Failover auth denied to {}: voting_primary={} safe_to_vote={} requester_replica={} primary_known={} primary_failed={} force_ack={} request_epoch={} current_epoch={} last_vote_epoch={}",
                        msg.sender_id,
                        facts.local_is_voting_primary,
                        facts.safe_to_vote,
                        facts.requester_is_replica,
                        facts.requester_primary_known,
                        facts.requester_primary_failed,
                        facts.force_ack,
                        facts.request_epoch,
                        facts.current_epoch,
                        facts.persisted_last_vote_epoch,
                    );
                }
                let reply_token = self.connections.iter().find_map(|(token, connection)| {
                    (connection.is_outgoing
                        && connection.connected
                        && connection.node_name == msg.sender_id)
                        .then_some(*token)
                });
                if rejection == Some(7) {
                    let claimed_slots = bytes_to_slots(&msg.myslots);
                    let owner = (0..claimed_slots.len())
                        .filter(|slot| claimed_slots[*slot])
                        .filter_map(|slot| state.declared_node_by_slot(slot))
                        .max_by_key(|node| node.config_epoch)
                        .cloned();
                    if let Some(owner) = owner {
                        crate::server_log!(
                            verbose,
                            "Node {} has old slots configuration, sending an UPDATE message about {}",
                            msg.sender_id,
                            owner.name,
                        );
                        let update = self.build_update_for_node(state, &owner);
                        if let Some(connection) = self
                            .connections
                            .get_mut(&reply_token.unwrap_or(token_value))
                        {
                            connection.write_buf.extend_from_slice(&update);
                            state.record_bus_message_sent(ClusterMsgType::Update);
                        }
                    }
                }
                // Use the same outgoing stream for UPDATE followed by NACK.
                // TCP ordering lets the candidate retry with refreshed slots.
                if let (Some(reason), Some(reply_token)) = (rejection, reply_token) {
                    if state.nodes.get(&msg.sender_id).is_some_and(|node| node.has_flag(
                        crate::engine::distributed::cluster::node::CLUSTER_NODE_FAILOVER_AUTH_NACK_SUPPORTED
                    )) {
                        let nack = crate::engine::distributed::cluster::msg::ClusterBusMessage::from_node(
                            ClusterMsgType::FailoverAuthNack, &state.myself, state.current_epoch,
                            ClusterBusBody::FailoverAuthNack { request_epoch: facts.request_epoch, reason },
                        ).encode().expect("local cluster wire metadata must be valid");
                        if let Some(connection) = self.connections.get_mut(&reply_token) {
                            connection.write_buf.extend_from_slice(&nack);
                            state.record_bus_message_sent(ClusterMsgType::FailoverAuthNack);
                        }
                    }
                }
                self.execute_failover_actions(state, actions, now, Some(token_value));
            }
            ClusterMsgType::FailoverAuthNack => {
                use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
                let Some(voter) = state.nodes.get(&msg.sender_id) else {
                    return;
                };
                if !ClusterState::is_voting_primary(voter) || voter.has_flag(CLUSTER_NODE_FAIL) {
                    return;
                }
                let voter_display_name = voter.display_name();
                let ClusterBusBody::FailoverAuthNack {
                    request_epoch,
                    reason,
                } = &msg.body
                else {
                    return;
                };
                let election_epoch = state.automatic_failover.election_epoch();
                if state.automatic_failover.phase()
                    != crate::engine::distributed::cluster::state::failover::AutomaticFailoverPhase::RequestingVotes
                    || *request_epoch != election_epoch
                {
                    return;
                }
                let reason_name = match reason {
                    1 => "not-safe",
                    2 => "req-epoch-old",
                    3 => "already-voted",
                    4 => "req-is-primary",
                    5 => "no-primary",
                    6 => "primary-up",
                    7 => "stale-config",
                    _ => "unknown",
                };
                if !matches!(reason, 2 | 3 | 7) {
                    crate::server_log!(
                        notice,
                        "Ignoring failover auth NACK [{}] from {} ({}) for epoch {}: a new election would draw the same answer.",
                        reason_name,
                        msg.sender_id,
                        voter_display_name,
                        election_epoch,
                    );
                    return;
                }
                let Ok(voter_id) =
                    crate::engine::distributed::cluster::NodeId::parse(msg.sender_id.as_bytes())
                else {
                    return;
                };
                let voting_primaries = state.primary_count();
                let dead_voters = std::iter::once(&state.myself)
                    .chain(state.nodes.values())
                    .filter(|node| {
                        if !ClusterState::is_voting_primary(node)
                            || !node.has_flag(CLUSTER_NODE_FAIL)
                        {
                            return false;
                        }
                        crate::engine::distributed::cluster::NodeId::parse(node.name.as_bytes())
                            .is_ok_and(|failed_id| {
                                failed_id != voter_id
                                    && !state.automatic_failover.has_voter_response(&failed_id)
                            })
                    })
                    .count();
                let Some(cannot_win) = state.automatic_failover.on_vote_nack(
                    voter_id,
                    *request_epoch,
                    voting_primaries,
                    dead_voters,
                ) else {
                    return;
                };
                let nack_count = state.automatic_failover.rejected_voter_count();
                let quorum = voting_primaries / 2 + 1;
                crate::server_log!(
                    notice,
                    "Failover auth NACK [{}] from {} ({}) for epoch {} (NACKs {}, quorum {})",
                    reason_name,
                    msg.sender_id,
                    voter_display_name,
                    election_epoch,
                    nack_count,
                    quorum,
                );
                if cannot_win {
                    let ack_count = state.automatic_failover.voter_count();
                    crate::server_log!(
                        notice,
                        "Failover election for epoch {} cannot reach quorum {} (ACKs {}, NACKs {}, dead voters {}). Resetting the election since we cannot win an election without quorum.",
                        election_epoch, quorum, ack_count, nack_count, dead_voters,
                    );
                }
            }
            ClusterMsgType::FailoverAuthAck => {
                use crate::engine::distributed::cluster::NodeId;
                let Some(voter) = state.nodes.get(&msg.sender_id) else {
                    return;
                };
                let Ok(voter_id) = NodeId::parse(msg.sender_id.as_bytes()) else {
                    return;
                };
                let voter_is_voting_primary = ClusterState::is_voting_primary(voter);
                state.advance_current_epoch(msg.current_epoch);
                let phase = state.automatic_failover.phase();
                let voters_before = state.automatic_failover.voter_count();
                let actions = state.automatic_failover_on_vote_ack(
                    voter_id,
                    msg.current_epoch,
                    voter_is_voting_primary,
                );
                crate::server_log!(
                    debug,
                    "Failover auth ACK from {} epoch={} voting_primary={} phase={:?} voters={}=>{} promotion={}",
                    msg.sender_id,
                    msg.current_epoch,
                    voter_is_voting_primary,
                    phase,
                    voters_before,
                    state.automatic_failover.voter_count(),
                    actions
                        .iter()
                        .any(|action| matches!(action, ClusterAction::PromoteSelf(_))),
                );
                self.pending_failover_actions.extend(actions);
            }
            ClusterMsgType::Publish | ClusterMsgType::PublishShard => {
                if !state.nodes.contains_key(&msg.sender_id) {
                    return;
                }
                let ClusterBusBody::Publish(publish) = &msg.body else {
                    return;
                };
                self.pending_publishes
                    .push(super::types::ReceivedClusterPublish {
                        channel: publish.channel().clone(),
                        payload: publish.payload().clone(),
                        shard: publish.is_shard(),
                    });
            }
            ClusterMsgType::Module => {
                crate::server_log!(debug, "Unhandled cluster message type: {:?}", msg.msg_type);
            }
        }
    }

    fn handle_paused_primary_message(state: &mut ClusterState, msg: &ClusterMsg) {
        if !state.myself.is_replica()
            || msg.sender_id != state.myself.replicaof
            || !msg.has_message_flag(ClusterMessageFlags::PAUSED)
        {
            return;
        }
        let Ok(primary) =
            crate::engine::distributed::cluster::NodeId::parse(msg.sender_id.as_bytes())
        else {
            return;
        };
        let Ok(offset) =
            crate::engine::distributed::replication::ReplicationOffset::try_from(msg.repl_offset)
        else {
            return;
        };
        let actions = state.observe_manual_primary_paused(&primary, offset);
        state.queue_cluster_actions(actions);
    }

    fn prepare_update_owner(state: &mut ClusterState, msg: &ClusterMsg) -> bool {
        let ClusterBusBody::Update(update) = &msg.body else {
            return false;
        };
        if !state.nodes.contains_key(&msg.sender_id) {
            return false;
        }
        let owner_id = update.node_id.as_str();
        if owner_id == state.myself.name {
            if state.myself.config_epoch > update.config_epoch {
                return false;
            }
            if state.myself.config_epoch < update.config_epoch {
                state.promote_local_update_owner(update.config_epoch);
                state.mark_topology_changed();
            }
            return true;
        }
        let Some(owner_epoch) = state.nodes.get(owner_id).map(|owner| owner.config_epoch) else {
            return false;
        };
        if owner_epoch > update.config_epoch {
            return false;
        }
        if owner_epoch < update.config_epoch {
            state.promote_remote_update_owner(owner_id, update.config_epoch);
            state.mark_topology_changed();
        }
        true
    }

    fn sender_role_changed(state: &ClusterState, msg: &ClusterMsg) -> bool {
        let Some(sender) = state.nodes.get(&msg.sender_id) else {
            return true;
        };
        let incoming_replica = !msg.replicaof.is_empty()
            || (msg.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA) != 0;
        let incoming_primary = !incoming_replica
            && (msg.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY) != 0;

        sender.is_replica() != incoming_replica || (incoming_primary && !sender.is_primary())
    }

    fn close_endpoint_handshake_links(&self, state: &mut ClusterState, msg: &ClusterMsg) {
        let placeholders: Vec<String> = self
            .connections
            .values()
            .filter(|connection| {
                !connection.handshake_node_name.is_empty()
                    && state
                        .nodes
                        .get(&connection.handshake_node_name)
                        .is_some_and(|node| {
                            node.has_flag(CLUSTER_NODE_HANDSHAKE)
                                && node.ip == msg.sender_ip
                                && node.port == msg.port
                                && node.cport == msg.bus_port
                        })
            })
            .map(|connection| connection.handshake_node_name.clone())
            .collect();
        for placeholder in placeholders {
            let close = (
                placeholder.clone(),
                crate::engine::distributed::cluster::ClusterLinkDirection::All,
            );
            state.queue_link_close_once(close);
            state.remove_handshake_node(&placeholder);
        }
    }

    /// Handle an incoming MEET message.
    fn handle_meet_received(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
        now: i64,
    ) {
        let sender_was_known = state.nodes.contains_key(&msg.sender_id);
        if !sender_was_known {
            let link_is_temporary_handshake = self
                .connections
                .get(&token_value)
                .and_then(|connection| state.nodes.get(&connection.handshake_node_name))
                .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE));
            if link_is_temporary_handshake {
                // A peer that has not received our PONG can send another
                // MEET before the handshake expires. This is still the same
                // anonymous inbound handshake, not identity confirmation.
                // C leaves it as a no-op and waits for the outbound-side
                // PING/PONG exchange; resolving it here makes a one-sided
                // timeout retain a member that should be forgotten.
                let pong = self.build_pong(state);
                if let Some(conn) = self.connections.get_mut(&token_value) {
                    conn.write_buf.extend_from_slice(&pong);
                }
                return;
            }
            let existing_handshake_id = state
                .nodes
                .values()
                .find(|node| {
                    node.has_flag(CLUSTER_NODE_HANDSHAKE)
                        && node.ip == msg.sender_ip
                        && node.port == msg.port
                        && node.cport == msg.bus_port
                })
                .map(|node| node.name.clone());
            if let Some(handshake_id) = existing_handshake_id.as_deref() {
                // Rust shares the temporary node record across inbound sockets.
                // Once a second link sends the same endpoint's MEET, converge
                // that record to its advertised identity so the recovery-side
                // reconnect can reuse the real node. The same-link branch
                // above stays anonymous, matching the one-sided timeout case.
                Self::add_or_update_node(state, msg, true);
                let close = (
                    handshake_id.to_string(),
                    crate::engine::distributed::cluster::ClusterLinkDirection::All,
                );
                state.queue_link_close_once(close);
                state.remove_handshake_node(handshake_id);
                if let Some(conn) = self.connections.get_mut(&token_value) {
                    conn.node_name = msg.sender_id.clone();
                    conn.handshake_node_name.clear();
                    conn.handshake_sent = true;
                }
                let pong = self.build_pong(state);
                if let Some(conn) = self.connections.get_mut(&token_value) {
                    conn.write_buf.extend_from_slice(&pong);
                }
                return;
            }
            let handshake_id =
                state.ensure_handshake_node(&msg.sender_ip, msg.port, msg.bus_port, now);
            if let Some(conn) = self.connections.get_mut(&token_value) {
                conn.handshake_node_name = handshake_id;
                conn.handshake_sent = true;
            }
            // Match C clusterProcessPacket: the receiver of an inbound MEET
            // creates the handshake placeholder and processes gossip but does not
            // dial inline. The outbound link is opened on the next cron tick via
            // schedule_missing_outgoing_connections (C's
            // clusterNodeCronHandleReconnect dials every linkless node). The
            // inbound-learned placeholder owns no MEET flag, so that link sends a
            // PING; only the CLUSTER MEET initiator sends a MEET.
            Self::process_gossip_entries(state, msg, now);
            let pong = self.build_pong(state);
            if let Some(conn) = self.connections.get_mut(&token_value) {
                conn.write_buf.extend_from_slice(&pong);
            }
            return;
        }
        let existing_handshake_id = state
            .nodes
            .values()
            .find(|node| {
                node.has_flag(CLUSTER_NODE_HANDSHAKE)
                    && node.ip == msg.sender_ip
                    && node.port == msg.port
                    && node.cport == msg.bus_port
            })
            .map(|node| node.name.clone());
        if let Some(handshake_id) = existing_handshake_id.as_deref() {
            // Gossip may identify the real node before an earlier outbound
            // handshake has been resolved. A direct MEET from that known node
            // proves both records describe the same peer, so retain the real
            // ID and discard the random handshake identity. Keep the current
            // inbound link for its PONG before retiring other placeholder
            // links, matching C's known-sender MEET response.
            if let Some(connection) = self.connections.get_mut(&token_value) {
                if !connection.is_outgoing && connection.handshake_node_name == handshake_id {
                    connection.node_name = msg.sender_id.clone();
                    connection.handshake_node_name.clear();
                }
            }
            let close = (
                handshake_id.to_string(),
                crate::engine::distributed::cluster::ClusterLinkDirection::All,
            );
            state.queue_link_close_once(close);
            state.remove_handshake_node(handshake_id);
        }
        // A peer may first be learned from trusted gossip, then send the
        // reciprocal MEET that identifies its complete slot bitmap. Treat
        // that direct topology payload like PING/PONG: refresh metadata first
        // and let the epoch-aware claim path install the live ownership.
        // Skipping this left cluster-port setups with known peers but missing
        // ranges after ownership updates stopped pre-merging bitmaps.
        let slot_claim_deltas = Self::sender_slot_claim_deltas(state, msg);
        let slot_claims_changed = !slot_claim_deltas.is_empty();
        let sender_role_changed = Self::sender_role_changed(state, msg);
        Self::add_or_update_node_with_slot_bitmap(
            state,
            msg,
            true,
            slot_claims_changed || sender_role_changed,
        );
        if slot_claims_changed {
            Self::process_changed_slot_claims(state, msg, false, &slot_claim_deltas);
        }
        let handshake_timeout = state
            .failure_report_node_timeout_ms()
            .max(1_000)
            .min(i64::MAX as u64) as i64;
        let should_replace_outbound = state.nodes.get(&msg.sender_id).is_some_and(|node| {
            node.asymmetric_meet_started_at > 0
                || (node.created_at > 0 && now.saturating_sub(node.created_at) > handshake_timeout)
        });
        let received_on_inbound = self
            .connections
            .get(&token_value)
            .is_some_and(|connection| !connection.is_outgoing);
        let has_outbound = self
            .connections
            .values()
            .any(|connection| connection.is_outgoing && connection.node_name == msg.sender_id);
        // A known peer sending MEET on its inbound link proves that it still
        // does not know us. Mirror C clusterProcessPacket by replacing our
        // outbound link immediately; the repair scheduler has already gated
        // this path by the handshake timeout and may have refreshed its retry
        // timestamp before this reciprocal MEET arrives.
        if received_on_inbound && should_replace_outbound && has_outbound {
            let close = (
                msg.sender_id.clone(),
                crate::engine::distributed::cluster::ClusterLinkDirection::To,
            );
            state.queue_link_close_once(close);
        }
        let pong = self.build_pong(state);
        if let Some(conn) = self.connections.get_mut(&token_value) {
            conn.write_buf.extend_from_slice(&pong);
        }

        crate::server_log!(
            notice,
            "Cluster MEET received from {} ({})",
            msg.sender_id,
            msg.port
        );
    }

    /// Handle an incoming PING message.
    fn handle_ping_received(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
        _now: i64,
    ) {
        let slot_claim_deltas = Self::sender_slot_claim_deltas(state, msg);
        let slot_claims_changed = !slot_claim_deltas.is_empty();
        let handshake_id = self
            .connections
            .get(&token_value)
            .map(|connection| connection.handshake_node_name.clone())
            .filter(|node_id| {
                node_id != &msg.sender_id
                    && state
                        .nodes
                        .get(node_id)
                        .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE))
            });
        let endpoint_handshake_id = (!state.nodes.contains_key(&msg.sender_id))
            .then(|| {
                state
                    .nodes
                    .values()
                    .find(|node| {
                        node.has_flag(CLUSTER_NODE_HANDSHAKE)
                            && node.ip == msg.sender_ip
                            && node.port == msg.port
                            && node.cport == msg.bus_port
                    })
                    .map(|node| node.name.clone())
            })
            .flatten();
        let sender_role_changed = Self::sender_role_changed(state, msg);
        // Match C: a PING on an INBOUND link must NOT rename/resolve our
        // handshake placeholder. The placeholder (random name) is only promoted
        // to the peer's real identity when the peer's PONG comes back on the
        // outbound link we opened (see handle_pong). Resolving it here on an
        // inbound ping let us look the peer up and prematurely clear our MEET,
        // which made node1 stop re-sending MEETs and flaked cluster-multiple-meets.
        let on_inbound_link = self
            .connections
            .get(&token_value)
            .is_some_and(|connection| !connection.is_outgoing);
        let resolve_handshake =
            handshake_id.is_some() || (endpoint_handshake_id.is_some() && !on_inbound_link);
        Self::add_or_update_node_with_slot_bitmap(
            state,
            msg,
            resolve_handshake,
            slot_claims_changed || sender_role_changed,
        );
        state.complete_node_meet(&msg.sender_id);
        self.close_endpoint_handshake_links(state, msg);

        if let Some(connection) = self.connections.get_mut(&token_value) {
            connection.node_name = msg.sender_id.clone();
            connection.handshake_node_name.clear();
        }
        // Mirror UPDATE's epoch-gated slot reassignment + self-demote on
        // PING/PONG too: a freshly-restarted stale primary that reloads its
        // own slots from disk only learns the true claimant through gossip
        // and otherwise stays master forever (ROLE=master check fails).
        if slot_claims_changed {
            Self::process_changed_slot_claims(state, msg, false, &slot_claim_deltas);
        }
        Self::resolve_config_epoch_collisions(state, msg);

        let pong = self.build_pong(state);
        if let Some(conn) = self.connections.get_mut(&token_value) {
            conn.write_buf.extend_from_slice(&pong);
        }
    }

    /// Handle an incoming PONG message.
    fn handle_pong_received(
        &mut self,
        state: &mut ClusterState,
        msg: &ClusterMsg,
        token_value: usize,
        now: i64,
    ) {
        let same_inbound_temporary_handshake = self
            .connections
            .get(&token_value)
            .filter(|connection| !connection.is_outgoing)
            .and_then(|connection| state.nodes.get(&connection.handshake_node_name))
            .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE));
        if same_inbound_temporary_handshake {
            let pong = self.build_pong(state);
            if let Some(connection) = self.connections.get_mut(&token_value) {
                connection.write_buf.extend_from_slice(&pong);
            }
            return;
        }
        let completes_outbound_probe = self
            .connections
            .get(&token_value)
            .is_some_and(|connection| connection.is_outgoing);
        let slot_claim_deltas = Self::sender_slot_claim_deltas(state, msg);
        let slot_claims_changed = !slot_claim_deltas.is_empty();
        let pong_completes_explicit_meet = self
            .connections
            .get(&token_value)
            .is_some_and(|connection| connection.is_outgoing && connection.send_meet);
        let pong_completes_node_meet =
            self.connections
                .get(&token_value)
                .is_some_and(|connection| {
                    connection.is_outgoing
                        && connection.meet_sent_at > 0
                        && connection.node_name == msg.sender_id
                });
        let completed_asymmetric_repair =
            self.connections
                .get(&token_value)
                .is_some_and(|connection| {
                    connection.is_outgoing
                        && connection.send_meet
                        && connection.node_name == msg.sender_id
                        && state
                            .nodes
                            .get(&msg.sender_id)
                            .is_some_and(|node| node.asymmetric_meet_started_at > 0)
                        && self.connections.values().any(|peer| {
                            !peer.is_outgoing && peer.connected && peer.node_name == msg.sender_id
                        })
                });
        let handshake_id = self
            .connections
            .get(&token_value)
            .map(|conn| conn.handshake_node_name.clone())
            .filter(|node_id| {
                node_id != &msg.sender_id
                    && state
                        .nodes
                        .get(node_id)
                        .is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE))
            });
        let handshake_requires_repair = handshake_id.as_deref().is_some_and(|node_id| {
            state
                .nodes
                .get(node_id)
                .is_some_and(|node| node.asymmetric_meet_started_at > 0)
        });
        let sender_role_changed = Self::sender_role_changed(state, msg);
        Self::add_or_update_node_with_slot_bitmap(
            state,
            msg,
            true,
            slot_claims_changed || sender_role_changed,
        );
        if pong_completes_explicit_meet || pong_completes_node_meet {
            state.complete_node_meet(&msg.sender_id);
        }
        if pong_completes_explicit_meet
            && state.myself.is_primary()
            && state.myself.numslots == 0
            && !state.has_ever_owned_slots()
        {
            for entry in msg.gossip() {
                state.confirm_gossip_handshake(&entry.node_id, now);
            }
        }
        if let Some(handshake_id) = handshake_id.as_deref() {
            // The stored handshake endpoint can differ from the peer's final
            // advertised endpoint when CLUSTER MEET uses a custom bus port.
            // C deletes every link owned by the placeholder with the node.
            // Capture its identity before removal; the confirmed current link
            // is rebound below before the runtime retires the remaining owners.
            state.queue_link_close_once((
                handshake_id.to_string(),
                crate::engine::distributed::cluster::ClusterLinkDirection::All,
            ));
            state.remove_handshake_node(handshake_id);
        }
        self.close_endpoint_handshake_links(state, msg);
        let stale_endpoint_handshakes: Vec<String> = state
            .nodes
            .values()
            .filter(|node| {
                node.has_flag(CLUSTER_NODE_HANDSHAKE)
                    && node.ip == msg.sender_ip
                    && node.port == msg.port
                    && node.cport == msg.bus_port
            })
            .map(|node| node.name.clone())
            .collect();
        for handshake_id in stale_endpoint_handshakes {
            state.remove_handshake_node(&handshake_id);
        }

        if let Some(conn) = self.connections.get_mut(&token_value) {
            conn.node_name = msg.sender_id.clone();
            conn.handshake_node_name.clear();
            if pong_completes_explicit_meet {
                conn.send_meet = false;
            }
        }

        let mut topology_changed = false;
        if let Some(mut node) = state.nodes.get(&msg.sender_id).cloned() {
            let previous_flags = node.flags;
            if completes_outbound_probe {
                node.pong_received = now;
                node.ping_sent = 0;
            }
            if (pong_completes_explicit_meet || pong_completes_node_meet) && node.meet_pending {
                // A PONG for the ordinary liveness probe sent to a
                // gossip-discovered peer does not prove that peer knows us.
                // Keep the node-owned reciprocal-MEET intent until the MEET
                // itself completes; otherwise every successful PING cancels
                // the repair before the next cron can send it.
                node.meet_pending = false;
            }
            if completed_asymmetric_repair {
                node.asymmetric_meet_started_at = 0;
            } else if handshake_requires_repair {
                node.asymmetric_meet_started_at = now;
            }
            node.flags &= !CLUSTER_NODE_HANDSHAKE;
            use crate::engine::distributed::cluster::node::{
                CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL,
            };
            if completes_outbound_probe
                && (node.has_flag(CLUSTER_NODE_PFAIL) || node.has_flag(CLUSTER_NODE_FAIL))
            {
                let was_fail = node.has_flag(CLUSTER_NODE_FAIL);
                let was_pfail = node.has_flag(CLUSTER_NODE_PFAIL);
                node.flags &= !CLUSTER_NODE_PFAIL;
                let undo_timeout_ms = state
                    .failure_report_node_timeout_ms()
                    .saturating_mul(2)
                    .min(i64::MAX as u64) as i64;
                let fail_is_old_enough = now.saturating_sub(node.fail_time) > undo_timeout_ms;
                if !ClusterState::is_voting_primary(&node) || fail_is_old_enough {
                    node.flags &= !CLUSTER_NODE_FAIL;
                    node.fail_time = 0;
                }
                if was_pfail || (was_fail && !node.has_flag(CLUSTER_NODE_FAIL)) {
                    crate::server_log!(
                        notice,
                        "Cluster cleared {} for {} after fresh PONG",
                        if was_fail && !node.has_flag(CLUSTER_NODE_FAIL) {
                            "FAIL/PFAIL"
                        } else {
                            "PFAIL"
                        },
                        msg.sender_id
                    );
                }
            }
            if !node.is_primary() && !node.is_replica() {
                node.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY;
            }
            topology_changed = node.flags != previous_flags;
            state.commit_direct_sender_observation(node);
        }
        if topology_changed {
            state.mark_topology_changed();
            state.recompute_state();
        }

        if slot_claims_changed {
            Self::process_changed_slot_claims(state, msg, false, &slot_claim_deltas);
        }
        Self::resolve_config_epoch_collisions(state, msg);
    }

    fn process_gossip_entries(state: &mut ClusterState, msg: &ClusterMsg, now: i64) {
        // Only a confirmed sender may mutate known peers, introduce nodes, or
        // contribute failure reports.
        if Self::apply_gossip_observations(state, msg, now) {
            state.mark_topology_changed();
            state.request_config_save();
        }
    }
}
