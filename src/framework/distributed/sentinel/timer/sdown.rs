use crate::engine::distributed::sentinel::{
    DownState, FailoverState, InstanceRole, SentinelInstanceId, SentinelState, SRI_PRIMARY_REBOOT,
};
use crate::framework::context::ServerContext;

pub fn check_sdown(ctx: &mut ServerContext, id: SentinelInstanceId, now: i64) {
    let state = ctx.sentinel_state_mut();
    let (
        down_after_ms,
        act_ping_time,
        current_state,
        configured_role,
        reported_role,
        role_reported_time,
        link_down_since_time,
        name,
        addr,
    ) = {
        let inst = match state.registry.instances().get(&id) {
            Some(i) => i,
            None => return,
        };
        (
            inst.down_after_ms,
            inst.act_ping_time,
            inst.down_state,
            inst.role,
            inst.role_reported,
            inst.role_reported_time,
            inst.link_down_since_time,
            inst.name.clone(),
            inst.addr.clone(),
        )
    };

    let elapsed = if act_ping_time > 0 {
        now - act_ping_time
    } else if link_down_since_time > 0 {
        now - link_down_since_time
    } else {
        0
    };
    let role_mismatch_elapsed = now - role_reported_time;
    let master_reports_replica = configured_role == InstanceRole::Master
        && reported_role == InstanceRole::Replica
        && role_reported_time > 0
        && role_mismatch_elapsed > down_after_ms + state.tunables.info_period_ms * 2;
    let should_be_down = elapsed > down_after_ms || master_reports_replica;
    let min_link_reconnect_period = state.tunables.min_link_reconnect_period_ms;
    if let Some(inst) = state.registry.instances().get(&id) {
        if inst.act_ping_time > 0
            && now - inst.act_ping_time > down_after_ms / 2
            && now - inst.last_ping_ok > min_link_reconnect_period
        {
            crate::server_log!(debug, "closing stale command link for {} {}", name, addr);
            if !state
                .pending_link_close
                .iter()
                .any(|pending| pending == &addr)
            {
                state.pending_link_close.push(addr.clone());
            }
        }
    }
    if should_be_down {
        if current_state == DownState::Up {
            if let Some(inst) = state.registry.instance_mut(id) {
                inst.down_state = DownState::SDown;
                inst.sdown_since_time = now;
                inst.sdown_time = now;
            }
            let event_msg = format!("sentinel instance {} {}", name, addr);
            let event_inst = state.registry.instances().get(&id).cloned();
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut state.script_engine,
                "warning",
                "+sdown",
                event_inst.as_ref(),
                &event_msg,
            );
        }
        // Always sync the master's subjective down state when the
        // instance is considered subjectively down, even if it was
        // already SDown from a previous tick. This ensures check_odown
        // reads the correct subjective state.
        sync_master_down_state(state, id, DownState::SDown, now);
    } else if current_state == DownState::SDown {
        if let Some(inst) = state.registry.instance_mut(id) {
            inst.down_state = DownState::Up;
            inst.sdown_since_time = 0;
            inst.sdown_time = 0;
        }
        let event_msg = format!("sentinel instance {} {}", name, addr);
        let event_inst = state.registry.instances().get(&id).cloned();
        crate::engine::distributed::sentinel::event::sentinel_event(
            &mut state.script_engine,
            "warning",
            "-sdown",
            event_inst.as_ref(),
            &event_msg,
        );
        sync_master_down_state(state, id, DownState::Up, now);
    }
    // If current_state is Up or ODown, sync_master_down_state handles cleanup
    if let Some(inst_ref) = state.registry.instances().get(&id) {
        let flags = inst_ref.flags;
        let reboot_time = inst_ref.reboot_time;
        let primary_reboot_down_after_period = inst_ref.primary_reboot_down_after_period;
        let name = inst_ref.name.clone();
        let addr = inst_ref.addr.clone();
        if flags & SRI_PRIMARY_REBOOT != 0
            && primary_reboot_down_after_period > 0
            && now - reboot_time > primary_reboot_down_after_period
        {
            if let Some(inst) = state.registry.instance_mut(id) {
                inst.down_state = DownState::SDown;
                inst.sdown_since_time = now;
                inst.sdown_time = now;
            }
            let event_msg = format!("{} {} (reboot)", name, addr);
            let event_inst = state.registry.instances().get(&id).cloned();
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut state.script_engine,
                "warning",
                "+sdown",
                event_inst.as_ref(),
                &event_msg,
            );
            sync_master_down_state(state, id, DownState::SDown, now);
        }
    }
}

pub(super) fn sync_master_down_state(
    state: &mut SentinelState,
    instance_id: SentinelInstanceId,
    down_state: DownState,
    now: i64,
) {
    let master = match state.registry.master_by_id_mut(instance_id) {
        Some(m) => m,
        None => return,
    };
    // Don't override ODown — check_odown manages the ODown↔SDown transition.
    if down_state == DownState::SDown && master.down_state == DownState::ODown {
        return;
    }
    master.down_state = down_state;
    master.sdown_since_time = if down_state == DownState::SDown {
        now
    } else {
        0
    };
}

pub(super) fn check_odown(ctx: &mut ServerContext, master_id: SentinelInstanceId, now: i64) {
    let state = ctx.sentinel_state_mut();
    let master = match state
        .registry
        .masters()
        .map(|(_, master)| master)
        .find(|m| m.id == master_id)
    {
        Some(m) => m.clone(),
        None => return,
    };
    if master.down_state == DownState::Up {
        return;
    }
    let ask_period = state.tunables.ask_period_ms;
    let mut votes: i64 = 1;
    for sid in &master.sentinels {
        if let Some(peer_state) = master.peer_states.get(sid) {
            if peer_state.reported_master_down
                && now - peer_state.last_master_down_reply <= ask_period * 5
            {
                votes += 1;
            }
        } else {
            crate::server_log!(debug, "ODOWN PEER {}: no peer_state entry", sid);
        }
    }
    crate::server_log!(
        debug,
        "ODOWN: votes={} >= quorum={} for master {} (current state={:?})",
        votes,
        master.quorum,
        master.name,
        master.down_state
    );
    if votes >= master.quorum {
        if master.down_state != DownState::ODown {
            if let Some(m) = state.registry.master_by_id_mut(master_id) {
                m.down_state = DownState::ODown;
                m.odown_since_time = now;
            }
            let event_msg = format!(
                "master {} {}; quorum {}",
                master.name, master.addr, master.quorum
            );
            let event_inst = state.registry.instances().get(&master_id).cloned();
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut state.script_engine,
                "warning",
                "+odown",
                event_inst.as_ref(),
                &event_msg,
            );
        }
    } else {
        if master.down_state == DownState::ODown && master.failover_state == FailoverState::None {
            if let Some(m) = state.registry.master_by_id_mut(master_id) {
                m.down_state = DownState::SDown;
                m.odown_since_time = 0;
            }
            let event_msg = format!("master {} {}", master.name, master.addr);
            let event_inst = state.registry.instances().get(&master_id).cloned();
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut state.script_engine,
                "warning",
                "-odown",
                event_inst.as_ref(),
                &event_msg,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{check_odown, check_sdown};
    use crate::engine::distributed::sentinel::{
        DownState, InstanceRole, SentinelInstance, SentinelPeerMasterState,
    };
    use crate::framework::context::ServerContext;

    #[test]
    fn odown_uses_down_reports_for_the_current_master_only() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let peer_id = 3;
        let mut peer = SentinelInstance::new_master(peer_id, "peer", "127.0.0.1:26380", 1);
        peer.role = InstanceRole::Sentinel;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(peer)
            .unwrap();

        let mut master_without_report =
            SentinelInstance::new_master(1, "primary-a", "127.0.0.1:6379", 2);
        master_without_report.down_state = DownState::SDown;
        master_without_report.sentinels = vec![peer_id];
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master_without_report.name.clone(), master_without_report)
            .unwrap();

        let mut master_with_report =
            SentinelInstance::new_master(2, "primary-b", "127.0.0.1:6380", 2);
        master_with_report.down_state = DownState::SDown;
        master_with_report.sentinels = vec![peer_id];
        master_with_report.peer_states.insert(
            peer_id,
            SentinelPeerMasterState {
                reported_master_down: true,
                last_master_down_reply: 10_000,
                leader: String::new(),
                leader_epoch: 0,
            },
        );
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master_with_report.name.clone(), master_with_report)
            .unwrap();

        check_odown(&mut ctx, 1, 10_000);
        check_odown(&mut ctx, 2, 10_000);

        assert_eq!(
            ctx.sentinel_state_mut()
                .registry
                .master("primary-a")
                .unwrap()
                .down_state,
            DownState::SDown
        );
        assert_eq!(
            ctx.sentinel_state_mut()
                .registry
                .master("primary-b")
                .unwrap()
                .down_state,
            DownState::ODown
        );
    }

    #[test]
    fn acknowledged_ping_does_not_become_sdown_from_old_send_time() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.down_after_ms = 1_000;
        replica.last_ping_sent = 1_000;
        replica.last_ping_ok = 1_001;
        replica.act_ping_time = 0;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        check_sdown(&mut ctx, 2, 2_100);

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::Up
        );
    }

    #[test]
    fn first_unacknowledged_ping_can_mark_instance_sdown() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.down_after_ms = 1_000;
        replica.last_ping_ok = 0;
        replica.act_ping_time = 1_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        check_sdown(&mut ctx, 2, 2_001);

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn master_reporting_replica_role_becomes_sdown_after_grace_period() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;
        ctx.sentinel_state_mut().tunables.info_period_ms = 100;

        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
        master.down_after_ms = 1_000;
        master.role_reported = InstanceRole::Replica;
        master.role_reported_time = 1_000;
        master.last_ping_ok = 2_500;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        check_sdown(&mut ctx, 1, 2_201);

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn rebooting_primary_stays_sdown_while_loading() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
        master.flags |= crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT;
        master.reboot_time = 1_000;
        master.primary_reboot_down_after_period = 5_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        check_sdown(&mut ctx, 1, 6_001);
        crate::framework::distributed::sentinel::timer::apply_ping_reply(
            &mut ctx,
            1,
            b"LOADING Redis is loading the dataset in memory",
            6_100,
        );

        assert_eq!(
            ctx.sentinel_state()
                .registry
                .master("primary")
                .unwrap()
                .down_state,
            DownState::SDown
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn rebooting_primary_pong_does_not_clear_reboot_down_window() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
        master.flags |= crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT;
        master.reboot_time = 1_000;
        master.primary_reboot_down_after_period = 5_000;
        master.down_state = DownState::SDown;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        crate::framework::distributed::sentinel::timer::apply_ping_reply(
            &mut ctx, 1, b"PONG", 1_500,
        );

        assert_ne!(
            ctx.sentinel_state().registry.instances()[&1].flags
                & crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT,
            0
        );
        assert_eq!(
            ctx.sentinel_state()
                .registry
                .master("primary")
                .unwrap()
                .down_state,
            DownState::SDown
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn disconnected_instance_becomes_sdown_without_pending_ping() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut peer = SentinelInstance::new_master(2, "peer", "127.0.0.1:26380", 1);
        peer.role = InstanceRole::Sentinel;
        peer.down_after_ms = 1_000;
        peer.last_ping_ok = 1_000;
        peer.act_ping_time = 0;
        peer.link_state = 1;
        peer.link_down_since_time = 1_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(peer)
            .unwrap();

        check_sdown(&mut ctx, 2, 2_001);

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn connected_transport_without_valid_pong_remains_sdown() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut peer = SentinelInstance::new_master(2, "peer", "127.0.0.1:26380", 1);
        peer.role = InstanceRole::Sentinel;
        peer.down_after_ms = 1_000;
        peer.act_ping_time = 0;
        peer.link_state = 0;
        peer.link_down_since_time = 1_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(peer)
            .unwrap();

        check_sdown(&mut ctx, 2, 2_001);

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn indirect_info_does_not_hide_a_stale_command_link() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;
        ctx.sentinel_state_mut()
            .tunables
            .min_link_reconnect_period_ms = 1_000;

        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.down_after_ms = 2_000;
        replica.last_ping_ok = 1_000;
        replica.last_info_time = 2_900;
        replica.act_ping_time = 1_500;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        check_sdown(&mut ctx, 2, 3_001);

        assert_eq!(
            ctx.sentinel_state().pending_link_close,
            vec!["127.0.0.1:6380".to_string()]
        );
    }

    #[test]
    fn sdown_is_strictly_greater_than_threshold_at_boundary() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.down_after_ms = 1_000;
        replica.last_ping_ok = 0;
        replica.act_ping_time = 1_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        check_sdown(&mut ctx, 2, 2_000);
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::Up
        );

        check_sdown(&mut ctx, 2, 2_001);
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].down_state,
            DownState::SDown
        );
    }

    #[test]
    fn odown_requires_at_least_quorum_votes_boundary() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;

        let peer_a = 3;
        let mut peer = SentinelInstance::new_master(peer_a, "peer-a", "127.0.0.1:26380", 1);
        peer.role = InstanceRole::Sentinel;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(peer)
            .unwrap();

        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
        master.down_state = DownState::SDown;
        master.sentinels = vec![peer_a];
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        check_odown(&mut ctx, 1, 10_000);
        assert_eq!(
            ctx.sentinel_state()
                .registry
                .master("primary")
                .unwrap()
                .down_state,
            DownState::SDown,
            " self 1 quorum=2 ODOWN"
        );

        let mut m = ctx
            .sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .clone();
        m.peer_states.insert(
            peer_a,
            SentinelPeerMasterState {
                reported_master_down: true,
                last_master_down_reply: 10_000,
                leader: String::new(),
                leader_epoch: 0,
            },
        );
        ctx.sentinel_state_mut()
            .registry
            .replace_master(m.name.clone(), m)
            .unwrap();
        check_odown(&mut ctx, 1, 10_000);
        assert_eq!(
            ctx.sentinel_state()
                .registry
                .master("primary")
                .unwrap()
                .down_state,
            DownState::ODown,
            "self + 1 peer  quorum  ODOWN"
        );
    }

    #[test]
    fn misconf_error_replies_are_recorded_so_sdown_accumulates() {
        let mut ctx = ServerContext::new(1);
        ctx.sentinel_state_mut().sentinel_mode = true;
        ctx.sentinel_state_mut().initialized = true;
        ctx.sentinel_state_mut()
            .tunables
            .min_link_reconnect_period_ms = 1_000;

        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
        master.down_after_ms = 3_000;
        master.last_ping_ok = 0;
        master.act_ping_time = 0;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        ctx.sentinel_state_mut()
            .registry
            .instance_mut(1)
            .unwrap()
            .act_ping_time = 1_000;
        super::super::apply_ping_reply(
            &mut ctx,
            1,
            b"MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled.",
            1_002,
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].last_ping_ok,
            1_002
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].act_ping_time,
            1_000
        );

        for round in 1i64..=50 {
            let now = 1_002 + round * 100;
            super::super::apply_ping_reply(
                &mut ctx,
                1,
                b"MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled.",
                now,
            );
            check_sdown(&mut ctx, 1, now);
            if now > 4_000 {
                // now(4001+) - act_ping_time(1000) = 3001 > down_after_ms(3000) → SDOWN
                assert_eq!(
                    ctx.sentinel_state().registry.instances()[&1].down_state,
                    DownState::SDown,
                    "round={round} now={now} elapsed={} should exceed down_after_ms=3000",
                    now - 1_000
                );
            }
        }
        assert!(
            ctx.sentinel_state().pending_link_close.is_empty(),
            "last_ping_ok "
        );
    }
}
