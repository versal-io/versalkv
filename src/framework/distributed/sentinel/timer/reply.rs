use crate::engine::distributed::sentinel::{DownState, SentinelInstanceId};
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::link::RespFrame;

use super::super::transport::send_script_kill;
use super::info::parse_info_response;
use super::sdown::sync_master_down_state;

pub fn apply_ping_reply(ctx: &mut ServerContext, id: SentinelInstanceId, reply: &[u8], now: i64) {
    let is_reachability_ok =
        reply == b"PONG" || reply.starts_with(b"LOADING") || reply.starts_with(b"MASTERDOWN");

    if reply.starts_with(b"BUSY") {
        if let Some(inst) = ctx.sentinel_state_mut().registry.instances().get(&id) {
            let addr = inst.addr.clone();
            if let Err(e) = send_script_kill(&addr) {
                crate::server_log!(
                    warning,
                    "sentinel failed to send SCRIPT KILL to {} (BUSY): {}",
                    addr,
                    e
                );
            }
        }
    }

    let Some(inst) = ctx.sentinel_state_mut().registry.instance_mut(id) else {
        return;
    };

    inst.last_ping_ok = now;
    inst.last_ping_time = now;

    if !is_reachability_ok {
        return;
    }

    inst.act_ping_time = 0;
    inst.link_down_since_time = 0;
    let is_rebooting_primary = inst.role
        == crate::engine::distributed::sentinel::InstanceRole::Master
        && inst.flags & crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT != 0
        && inst.primary_reboot_down_after_period > 0;
    let is_loading = reply.starts_with(b"LOADING");
    let is_reboot_loading =
        is_loading && inst.flags & crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT != 0;
    if reply.starts_with(b"PONG")
        && inst.flags & crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT != 0
        && !is_rebooting_primary
    {
        inst.flags &= !crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT;
        crate::server_log!(notice, "-reboot instance {} {}", inst.name, inst.addr);
    }
    if reply.starts_with(b"PONG") && is_rebooting_primary {
        return;
    }
    if inst.down_state == DownState::SDown && !is_reboot_loading {
        inst.down_state = DownState::Up;
        inst.sdown_since_time = 0;
        inst.sdown_time = 0;
    }
    if is_reboot_loading {
        return;
    }
    sync_master_down_state(ctx.sentinel_state_mut(), id, DownState::Up, now);
}

pub fn apply_info_reply(ctx: &mut ServerContext, id: SentinelInstanceId, info: &str, now: i64) {
    if let Some(inst) = ctx.sentinel_state_mut().registry.instance_mut(id) {
        inst.last_info_time = now;
        inst.info_refresh = now;
        inst.info_cache = info.to_string();
    }
    parse_info_response(ctx, id, info, now);
}

pub fn apply_vote_reply(
    ctx: &mut ServerContext,
    master_id: SentinelInstanceId,
    peer_id: SentinelInstanceId,
    parts: &[RespFrame],
    now: i64,
) {
    let reported_down = match parts.first() {
        Some(RespFrame::Integer(value)) => *value == 1,
        _ => {
            crate::server_log!(
                notice,
                "VOTE_REPLY: no integer first part parts={:?}",
                parts
            );
            return;
        }
    };
    let leader = match parts.get(1) {
        Some(RespFrame::Bulk(value)) => String::from_utf8_lossy(value).to_string(),
        Some(RespFrame::Simple(value)) => String::from_utf8_lossy(value).to_string(),
        _ => {
            crate::server_log!(notice, "VOTE_REPLY: no leader in parts={:?}", parts);
            return;
        }
    };
    let leader_epoch = match parts.get(2) {
        Some(RespFrame::Integer(value)) => *value,
        _ => {
            crate::server_log!(notice, "VOTE_REPLY: no leader_epoch in parts={:?}", parts);
            return;
        }
    };
    crate::server_log!(
        debug,
        "VOTE_REPLY: master_id={} peer_id={} reported_down={} leader={} leader_epoch={}",
        master_id,
        peer_id,
        reported_down,
        leader,
        leader_epoch
    );
    if let Some(master) = ctx
        .sentinel_state_mut()
        .registry
        .master_by_id_mut(master_id)
    {
        let peer_state = master.peer_states.entry(peer_id).or_insert_with(|| {
            crate::engine::distributed::sentinel::SentinelPeerMasterState {
                reported_master_down: false,
                last_master_down_reply: 0,
                leader: String::new(),
                leader_epoch: 0,
            }
        });
        peer_state.reported_master_down = reported_down;
        peer_state.last_master_down_reply = now;
        if leader != "*" {
            peer_state.leader = leader;
            peer_state.leader_epoch = leader_epoch;
        }
    }
}
