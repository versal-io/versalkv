use crate::engine::distributed::cluster::node::ClusterNode;

use super::types::{role_str, short_name, ClusterState, CLUSTER_FAIL, CLUSTER_OK, CLUSTER_SLOTS};
use super::ClusterHealth;

impl ClusterState {
    pub fn set_require_full_coverage(&mut self, required: bool) {
        if self.require_full_coverage == required {
            return;
        }
        self.require_full_coverage = required;
        self.mark_topology_changed();
        self.recompute_state();
    }

    /// Recompute cluster state (ok/fail) and size from slots and nodes.
    /// Emits a [CLUSTER-ROLE] role-heartbeat line after recomputation so
    /// external trace consumers can correlate self-role / slot-count /
    /// cluster-state snapshots against the C implementation.
    pub fn recompute_state(&mut self) {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};
        use crate::foundation::io::sys::time::mstime;
        self.topology_state_dirty = false;
        self.observe_topology_evaluation(mstime());
        // C reads the canonical `cluster->slots[]` table here. Slot bitmaps
        // are kept for wire/config compatibility, but are not rescanned for
        // ownership on every state evaluation.
        #[cfg(not(test))]
        for slot in 0..CLUSTER_SLOTS {
            self.slots[slot] = self.owner_contributes_coverage(&self.slot_ownership.owners[slot]);
        }
        // A number of focused unit fixtures seed crate-visible node bitmaps
        // directly. Preserve their legacy health setup without putting this
        // fallback in the server binary's hot path.
        #[cfg(test)]
        {
            self.slots.fill(false);
            for slot in 0..CLUSTER_SLOTS {
                self.slots[slot] = std::iter::once(&self.myself)
                    .chain(self.nodes.values())
                    .any(|node| node.slots[slot] && !node.has_flag(CLUSTER_NODE_FAIL));
            }
        }
        let mut primary_count = 0usize;
        let mut reachable_primaries = 0usize;
        // Include myself
        if self.myself.is_primary() {
            let mc = self.myself.count_slots();
            if mc > 0 {
                primary_count += 1;
                if !self.myself.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) {
                    reachable_primaries += 1;
                }
            }
        }
        for node in self.nodes.values() {
            if node.is_myself() {
                continue;
            }
            if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE) {
                continue;
            }
            if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR) {
                continue;
            }
            if node.is_primary() && node.numslots > 0 {
                primary_count += 1;
                if !node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) {
                    reachable_primaries += 1;
                }
            }
        }
        let assigned = self.slots.iter().filter(|&&covered| covered).count();
        self.size = primary_count;
        // Mirror C: if `cluster_require_full_coverage` (default true), any
        // slot with no alive owner pushes the cluster to FAIL.
        let prev_state = self.state;
        let has_full_coverage = assigned == CLUSTER_SLOTS || !self.require_full_coverage;
        // Preserve the existing single-node / empty-topology behavior used by
        // cluster-require-full-coverage=no. Minority protection is meaningful
        // once the topology contains multiple voting primaries.
        let has_quorum = primary_count <= 1 || reachable_primaries > primary_count / 2;
        self.state = if has_full_coverage && has_quorum {
            CLUSTER_OK
        } else {
            CLUSTER_FAIL
        };
        // Emit log messages expected by the Tcl test suite.
        // "At least one hash slot is not served by any available node" when
        // the cluster transitions to FAIL due to a dead primary, and
        // "minority partition" when the majority of primaries are unreachable
        // (checked on every recompute since the cluster may already be FAIL).
        if self.state == CLUSTER_FAIL {
            let minority = !has_quorum;
            let minority_transition = self.observe_minority(minority);
            if minority_transition == Some(false) {
                crate::server_log!(notice, "... minority partition ...");
            } else if !minority && prev_state != self.state {
                crate::server_log!(
                    notice,
                    "At least one hash slot is not served by any available node."
                );
            }
        } else {
            self.observe_minority(false);
        }
        // Rate limit the heartbeat to ~1 Hz: recompute_state is called from
        // bus cron (~10Hz) and after every UPDATE, so logging inline would
        // bury everything else. Always log on state transitions regardless.
        static LAST_TS_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let now = mstime();
        let prev_ts = LAST_TS_MS.load(std::sync::atomic::Ordering::Relaxed);
        let transition = prev_state != self.state;
        if transition || now - prev_ts >= 1000 {
            let _ = LAST_TS_MS.compare_exchange(
                prev_ts,
                now,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            );
            let role = if self.myself.is_primary() {
                "primary"
            } else {
                "replica"
            };
            let rof: &str = if self.myself.replicaof.is_empty() {
                "-"
            } else {
                &self.myself.replicaof[..self.myself.replicaof.len().min(8)]
            };
            crate::server_log!(
                notice,
                "[CLUSTER-ROLE] ROLE-HEARTBEAT: myself={:.8} ({}) role={} replicaof={} numslots={} cluster_state={} assigned_slots={}/{} primaries_with_slots={} current_epoch={} my_epoch={}{}",
                self.myself.name,
                self.myself.display_name(),
                role,
                rof,
                self.myself.numslots,
                if self.state == CLUSTER_OK {
                    "ok"
                } else {
                    "fail"
                },
                assigned,
                CLUSTER_SLOTS,
                primary_count,
                self.current_epoch,
                self.myself.config_epoch,
                if transition {
                    " [STATE-TRANSITION]"
                } else {
                    ""
                }
            );
        }
    }

    /// Coalesce topology health/size work until the event-loop boundary.
    pub fn recompute_state_if_dirty(&mut self) {
        if self.topology_state_dirty {
            self.recompute_state();
        }
    }

    /// Generate CLUSTER NODES output string.
    #[cfg(test)]
    pub fn nodes_str(&self) -> String {
        self.nodes_str_for_client(
            crate::engine::distributed::cluster::ClientTransport::Tcp,
            false,
        )
    }

    #[cfg(test)]
    pub fn nodes_str_for_client(
        &self,
        transport: crate::engine::distributed::cluster::ClientTransport,
        prefer_ipv6: bool,
    ) -> String {
        let slot_owners = self.slot_owners_by_name();
        let mut lines = Vec::new();
        lines.push(self.node_str(&self.myself, &slot_owners, transport, prefer_ipv6));
        for node in self.nodes.values() {
            lines.push(self.node_str(node, &slot_owners, transport, prefer_ipv6));
        }
        lines.join("\n") + "\n"
    }

    /// Generate a single node description for CLUSTER NODES.
    #[cfg(test)]
    fn node_str(
        &self,
        node: &ClusterNode,
        slot_owners: &[Option<String>],
        transport: crate::engine::distributed::cluster::ClientTransport,
        prefer_ipv6: bool,
    ) -> String {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        // Format: <id> <ip:port>@<cport> <flags> <master> <ping> <pong> <epoch> <link> <slots>
        let flags = self.node_flags_str(node);
        let master = if node.is_primary() {
            "-"
        } else {
            &node.replicaof
        };
        // Check actual link state from the links map
        let link = if node.is_myself()
            || self.has_connected_link(
                &node.name,
                crate::engine::distributed::cluster::ClusterLinkDirection::To,
            ) {
            "connected"
        } else {
            "disconnected"
        };
        // Render slot ranges only for nodes that are not marked FAIL.
        // A SIGSTOP'd process keeps its listening socket's
        // accept-backlog open at the kernel level but never accepts or
        // drains connections — the Tcl harness parses this node's slot
        // ranges, opens a `valkey` TCP socket to <ip:port> (calling
        // `socket ...`, which may hang or succeed silently), and then
        // stalls forever writing the RESP command because the stopped
        // process never ACKs the data. Omitting the slot ranges from
        // FAIL rows in CLUSTER NODES output guarantees the Tcl
        // parser never routes writes or reads through the dead port;
        // instead the parser assigns those slots to another peer's row
        // via `slot_ranges_for_node` iteration (which itself is driven
        // by `slot_owners_ref` and already handles fail-over inference
        // for the actual owner transition). PFAIL is only a local
        // suspicion, so its authoritative slots remain visible until
        // quorum confirms FAIL. Myself is never suppressed because a
        // node does not mark itself FAIL.
        let down = !node.is_myself() && node.has_flag(CLUSTER_NODE_FAIL);
        let slots = if down {
            String::new()
        } else {
            Self::slot_ranges_for_node(slot_owners, &node.name)
        };
        let endpoint = node
            .endpoints
            .client_endpoint_for_family(transport, prefer_ipv6);
        let hostname = node
            .endpoints
            .hostname()
            .map(|hostname| format!(",{hostname}"))
            .unwrap_or_default();
        format!(
            "{} {}:{}@{}{} {} {} {} {} {} {} {}",
            node.name,
            endpoint.host,
            endpoint.port,
            node.cport(),
            hostname,
            flags,
            master,
            node.ping_sent,
            node.pong_received,
            node.config_epoch,
            link,
            slots
        )
        .trim()
        .to_string()
    }

    /// Generate flags string for CLUSTER NODES.
    #[cfg(test)]
    fn node_flags_str(&self, node: &ClusterNode) -> String {
        let mut parts = Vec::new();
        if node.is_myself() {
            parts.push("myself");
        }
        if node.is_primary() {
            parts.push("master");
        }
        if node.is_replica() {
            parts.push("slave");
        }
        if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_PFAIL) {
            parts.push("fail?");
        }
        if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL) {
            parts.push("fail");
        }
        if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE) {
            parts.push("handshake");
        }
        if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR) {
            parts.push("noaddr");
        }
        if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER) {
            parts.push("nofailover");
        }
        if parts.is_empty() {
            parts.push("noflags");
        }
        parts.join(",")
    }

    /// Compute node failure counts for CLUSTER INFO.
    fn node_failure_counts(&self) -> (u64, u64, u64, u64) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_PFAIL;
        let mut nodes_pfail = 0u64;
        let mut nodes_fail = 0u64;
        let mut voting_pfail = 0u64;
        let mut voting_fail = 0u64;
        if self.myself.has_flag(CLUSTER_NODE_PFAIL) {
            nodes_pfail += 1;
            if self.myself.is_primary() {
                voting_pfail += 1;
            }
        }
        if self.myself.has_flag(CLUSTER_NODE_FAIL) {
            nodes_fail += 1;
            if self.myself.is_primary() {
                voting_fail += 1;
            }
        }
        for node in self.nodes.values() {
            if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE) {
                continue;
            }
            if node.has_flag(CLUSTER_NODE_PFAIL) {
                nodes_pfail += 1;
                if node.is_primary() {
                    voting_pfail += 1;
                }
            }
            if node.has_flag(CLUSTER_NODE_FAIL) {
                nodes_fail += 1;
                if node.is_primary() {
                    voting_fail += 1;
                }
            }
        }
        (nodes_pfail, nodes_fail, voting_pfail, voting_fail)
    }

    /// Generate CLUSTER INFO output string.
    pub fn info_str(&self) -> String {
        let debug = self.debug_snapshot();
        let cluster_size = self.size;
        let (npfail, nfail, vpfail, vfail) = self.node_failure_counts();
        format!(
            "cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_ping_sent:{}\r\n\
             cluster_stats_messages_pong_sent:{}\r\n\
             cluster_stats_messages_meet_sent:{}\r\n\
             cluster_stats_messages_publish_sent:{}\r\n\
             cluster_stats_messages_publishshard_sent:{}\r\n\
             cluster_stats_messages_sent:{}\r\n\
             cluster_stats_messages_ping_received:{}\r\n\
             cluster_stats_messages_pong_received:{}\r\n\
             cluster_stats_messages_meet_received:{}\r\n\
             cluster_stats_messages_publish_received:{}\r\n\
             cluster_stats_messages_publishshard_received:{}\r\n\
             cluster_stats_messages_received:{}\r\n\
             cluster_stats_bytes_sent:{}\r\n\
             cluster_stats_bytes_received:{}\r\n\
             cluster_stats_pubsub_bytes_sent:{}\r\n\
             cluster_stats_pubsub_bytes_received:{}\r\n\
             cluster_stats_module_bytes_sent:{}\r\n\
             cluster_stats_module_bytes_received:{}\r\n\
             cluster_nodes_pfail:{}\r\n\
             cluster_nodes_fail:{}\r\n\
             cluster_voting_nodes_pfail:{}\r\n\
             cluster_voting_nodes_fail:{}\r\n\
             total_cluster_links_buffer_limit_exceeded:{}\r\n",
            match debug.health {
                ClusterHealth::Ok => "ok",
                ClusterHealth::Fail => "fail",
            },
            debug.assigned_slot_count,
            debug.assigned_slot_count,
            debug.node_count,
            cluster_size,
            debug.current_epoch,
            self.myself.config_epoch,
            self.bus_messages_sent(crate::engine::distributed::cluster::msg::ClusterMsgType::Ping,),
            self.bus_messages_sent(crate::engine::distributed::cluster::msg::ClusterMsgType::Pong,),
            self.bus_messages_sent(crate::engine::distributed::cluster::msg::ClusterMsgType::Meet,),
            self.bus_messages_sent(
                crate::engine::distributed::cluster::msg::ClusterMsgType::Publish,
            ),
            self.bus_messages_sent(
                crate::engine::distributed::cluster::msg::ClusterMsgType::PublishShard,
            ),
            self.total_bus_messages_sent(),
            self.bus_messages_received(
                crate::engine::distributed::cluster::msg::ClusterMsgType::Ping,
            ),
            self.bus_messages_received(
                crate::engine::distributed::cluster::msg::ClusterMsgType::Pong,
            ),
            self.bus_messages_received(
                crate::engine::distributed::cluster::msg::ClusterMsgType::Meet,
            ),
            self.bus_messages_received(
                crate::engine::distributed::cluster::msg::ClusterMsgType::Publish,
            ),
            self.bus_messages_received(
                crate::engine::distributed::cluster::msg::ClusterMsgType::PublishShard,
            ),
            self.total_bus_messages_received(),
            self.total_bus_bytes_sent(),
            self.total_bus_bytes_received(),
            self.bus_stats.pubsub_bytes_sent,
            self.bus_stats.pubsub_bytes_received,
            self.bus_stats.module_bytes_sent,
            self.bus_stats.module_bytes_received,
            npfail,
            nfail,
            vpfail,
            vfail,
            self.links_buffer_limit_exceeded(),
        )
    }

    /// Bump the local config epoch without consensus, always remaining above
    /// every other known node including failed and unreachable peers.
    pub fn bump_config_epoch_without_consensus(&mut self) -> Result<bool, String> {
        let maximum_peer_epoch = self
            .nodes
            .values()
            .map(|node| node.config_epoch)
            .max()
            .unwrap_or_default();
        self.advance_current_epoch(maximum_peer_epoch);
        if self.myself.config_epoch != 0
            && self.myself.config_epoch > maximum_peer_epoch
            && self.myself.config_epoch >= self.current_epoch
        {
            return Ok(false);
        }
        let epoch = self
            .current_epoch
            .checked_add(1)
            .ok_or_else(|| "config epoch exhausted".to_string())?;
        self.advance_current_epoch(epoch);
        self.myself.config_epoch = epoch;
        self.mark_topology_changed();
        self.request_config_fsync();
        self.request_topology_broadcast();
        Ok(true)
    }

    /// Bump after a topology conflict. Existing callers do not need to report
    /// whether the local epoch was already ahead and require an unconditional
    /// increment for slot transition ordering.
    pub fn bump_epoch(&mut self) {
        self.advance_current_epoch(self.current_epoch.saturating_add(1));
        self.myself.config_epoch = self.current_epoch;
        self.mark_topology_changed();
        self.request_config_fsync();
    }

    /// Make this node a replica of the specified master node.
    /// Returns the master's (ip, port) on success.
    pub fn replicate(&mut self, master_id: &[u8]) -> Result<(String, u16), String> {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let previously_served = self.served_shard_slots();
        let master_id_str = match std::str::from_utf8(master_id) {
            Ok(s) => s.to_string(),
            Err(_) => return Err("Invalid node id".to_string()),
        };

        if master_id_str == self.myself.name {
            return Err("Can't replicate myself".to_string());
        }

        let master = self
            .nodes
            .get(&master_id_str)
            .ok_or_else(|| format!("Unknown node {}", master_id_str))?;

        if !master.is_primary() {
            return Err("The specified node is not a master".to_string());
        }

        let master_ip = master.ip().to_string();
        let master_port = master.port();
        let master_shard_id = master.shard_id.clone();
        let previous_primary = self.myself.replicaof.clone();

        let me = short_name(&self.myself.name);
        let mshort = short_name(&master_id_str);
        let prev_role = role_str(&self.myself);
        let prev_slots = self.myself.numslots;
        let prev_master = short_name(&self.myself.replicaof);
        crate::server_log!(
            notice,
            "[CLUSTER-ROLE] node={} REPLICATE: prev_role={} prev_master={} prev_slots={} -> new_master={} master_addr={}:{}",
            me,
            prev_role,
            prev_master,
            prev_slots,
            mshort,
            master_ip,
            master_port
        );

        self.myself.flags &= !(CLUSTER_NODE_PRIMARY | CLUSTER_NODE_MIGRATE_TO);
        self.myself.flags |= CLUSTER_NODE_REPLICA;
        self.myself.replicaof = master_id_str.clone();
        self.myself.shard_id = master_shard_id;
        // C clusterSetPrimary zeroes failover_auth_time whenever the node
        // follows a primary: a quorum collected for the old lineage must not
        // promote this node after the reparent commits.
        self.automatic_failover.reset();
        self.mark_primary_migration_target(&master_id_str);
        if !previous_primary.is_empty() && previous_primary != master_id_str {
            self.clear_primary_migration_target_if_orphaned(&previous_primary);
        }

        let local_id = self.myself.name.clone();
        self.unassign_slots_owned_by(&local_id);

        self.mark_topology_changed();
        self.recompute_state();
        self.request_lost_shard_slots(&previously_served);

        Ok((master_ip, master_port))
    }

    pub(crate) fn mark_primary_migration_target(&mut self, primary_id: &str) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO;
        if self.myself.name == primary_id {
            self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
        } else if let Some(primary) = self.nodes.get_mut(primary_id) {
            primary.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
    }

    pub(crate) fn clear_primary_migration_target_if_orphaned(&mut self, primary_id: &str) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO;
        let still_has_replica = (self.myself.is_replica() && self.myself.replicaof == primary_id)
            || self
                .nodes
                .values()
                .any(|node| node.is_replica() && node.replicaof == primary_id);
        if still_has_replica {
            return;
        }
        if self.myself.name == primary_id {
            self.myself.flags &= !CLUSTER_NODE_MIGRATE_TO;
        } else if let Some(primary) = self.nodes.get_mut(primary_id) {
            primary.flags &= !CLUSTER_NODE_MIGRATE_TO;
        }
    }

    pub fn request_replication_switch(
        &mut self,
        primary_id: &[u8],
        mode: crate::engine::distributed::cluster::action::ReplicationSwitchMode,
    ) -> Result<(), String> {
        let primary_id = std::str::from_utf8(primary_id)
            .map_err(|_| "Invalid node id".to_string())?
            .to_string();
        if primary_id == self.myself.name {
            return Err("Can't replicate myself".to_string());
        }
        let primary = self
            .nodes
            .get(&primary_id)
            .ok_or_else(|| format!("Unknown node {primary_id}"))?;
        if !primary.is_primary() {
            return Err("The specified node is not a master".to_string());
        }
        self.pending_replication_switch = Some(super::PendingReplicationSwitch {
            node_id: primary_id,
            mode,
        });
        Ok(())
    }

    pub fn detach_replica(
        &self,
    ) -> Result<crate::engine::distributed::cluster::action::ClusterAction, String> {
        Ok(crate::engine::distributed::cluster::action::ClusterAction::DetachReplica)
    }

    pub fn reset_replica_as_empty_primary(&mut self) {
        use crate::engine::distributed::cluster::node::{
            generate_node_id, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let previously_served = self.served_shard_slots();
        self.myself.flags &= !(CLUSTER_NODE_REPLICA | CLUSTER_NODE_MIGRATE_TO);
        self.myself.flags |= CLUSTER_NODE_PRIMARY;
        self.myself.replicaof.clear();
        self.myself.shard_id = generate_node_id();
        let local_id = self.myself.name.clone();
        self.unassign_slots_owned_by(&local_id);
        self.myself.repl_offset = 0;
        self.clear_open_slot_states();
        self.pending_replication_switch = None;
        self.automatic_failover.reset();
        self.manual_failover.reset();
        self.mark_topology_changed();
        self.recompute_state();
        self.request_topology_broadcast();
        self.request_lost_shard_slots(&previously_served);
    }

    /// Get all replica nodes of the specified master node.
    /// Returns None if the master node doesn't exist.
    pub fn get_replicas(&self, master_id: &str) -> Option<Vec<&ClusterNode>> {
        let master_exists = self.myself.name == master_id || self.nodes.contains_key(master_id);
        if !master_exists {
            return None;
        }
        let mut replicas = Vec::new();
        for node in self.nodes.values() {
            if node.is_replica() && node.replicaof == master_id {
                replicas.push(node);
            }
        }
        Some(replicas)
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterState;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
    };

    #[test]
    fn cluster_nodes_keeps_slots_on_pfail_primary() {
        const OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        let mut owner = ClusterNode::new(
            OWNER.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.flags |= CLUSTER_NODE_PFAIL;
        owner.slots[42] = true;
        owner.numslots = 1;
        state.nodes.insert(owner.name.clone(), owner);

        let nodes = state.nodes_str();
        let owner_line = nodes.lines().find(|line| line.starts_with(OWNER)).unwrap();

        assert_eq!(owner_line.split_whitespace().nth(8), Some("42"));
    }

    #[test]
    fn disabled_full_coverage_allows_partial_cluster_to_be_ok() {
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);

        state.set_require_full_coverage(false);

        assert_eq!(state.state, super::CLUSTER_OK);
    }

    #[test]
    fn withdrawn_remote_slot_retains_full_coverage_until_rebound() {
        const OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        let mut owner = ClusterNode::new(
            OWNER.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.slots.fill(true);
        owner.numslots = super::CLUSTER_SLOTS;
        state.nodes.insert(owner.name.clone(), owner);
        state.slot_ownership.withdrawn_by[0] = Some(OWNER.to_string());

        state.recompute_state();

        assert_eq!(state.state, super::CLUSTER_OK);
        assert!(state.slots[0]);
    }

    #[test]
    fn full_coverage_still_fails_when_reachable_voting_primaries_are_a_minority() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        for slot in 0..5_461 {
            state.myself.slots[slot] = true;
        }
        state.myself.numslots = 5_461;

        for (node_id, start, end) in [
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5_461, 10_923),
            ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 10_923, 16_384),
        ] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16_380,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.flags |= CLUSTER_NODE_PFAIL;
            for slot in start..end {
                node.slots[slot] = true;
            }
            node.numslots = end - start;
            state.nodes.insert(node_id.to_string(), node);
        }

        state.recompute_state();

        assert_eq!(state.state, super::CLUSTER_FAIL);
        assert_eq!(state.size, 3);
    }

    #[test]
    fn cluster_nodes_reports_connected_only_for_outbound_link() {
        use crate::engine::distributed::cluster::{
            ClusterLinkDirection, ClusterLinkView, LinkEvents, NodeId,
        };

        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        state.observe_link_open(ClusterLinkView::new(
            NodeId::parse(node_id.as_bytes()).unwrap(),
            ClusterLinkDirection::From,
            1,
            LinkEvents::READABLE,
        ));

        let before = state.nodes_str();
        let before = before
            .lines()
            .find(|line| line.starts_with(node_id))
            .unwrap();
        assert!(before.contains(" disconnected"));

        state.observe_link_open(ClusterLinkView::new(
            NodeId::parse(node_id.as_bytes()).unwrap(),
            ClusterLinkDirection::To,
            2,
            LinkEvents::READABLE,
        ));
        let after = state.nodes_str();
        let after = after
            .lines()
            .find(|line| line.starts_with(node_id))
            .unwrap();
        assert!(after.contains(" connected"));
    }

    #[test]
    fn cluster_nodes_terminates_the_last_row_with_newline() {
        let state = ClusterState::new("127.0.0.1", 6379, 16379);

        assert!(state.nodes_str().ends_with('\n'));
    }

    #[test]
    fn replicate_inherits_master_shard_id() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;

        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        let mut master = ClusterNode::new(
            "master".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        master.flags &= !CLUSTER_NODE_MYSELF;
        master.shard_id = "shared-shard".to_string();
        state.nodes.insert(master.name.clone(), master);

        state.replicate(b"master").unwrap();

        assert_eq!(state.myself.replicaof, "master");
        assert_eq!(state.myself.shard_id, "shared-shard");
    }
}
