mod failover;
mod info;
mod reply;
mod sdown;

pub use reply::{apply_info_reply, apply_ping_reply, apply_vote_reply};
pub use sdown::check_sdown;

use crate::engine::distributed::sentinel::SentinelInstanceId;
use crate::engine::extension::config::server::CONFIG_DEFAULT_HZ;
use crate::foundation::io::sys::rand::random_usize;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;

pub fn sentinel_cron(ctx: &mut ServerContext) {
    if !ctx.sentinel_state_mut().sentinel_mode || !ctx.sentinel_state_mut().initialized {
        return;
    }
    let now = mstime();

    ctx.sentinel_state_mut().check_tilt_condition(now);

    if ctx.sentinel_state_mut().tilt {
        return;
    }

    let mut to_check_sdown: Vec<SentinelInstanceId> = Vec::new();

    let master_ids: Vec<SentinelInstanceId> = ctx
        .sentinel_state_mut()
        .registry
        .masters()
        .map(|(_, master)| master)
        .map(|m| m.id)
        .collect();

    for master_id in &master_ids {
        failover::collect_pending_actions(ctx, *master_id, now, &mut to_check_sdown);

        let replica_ids: Vec<SentinelInstanceId> = {
            let master = match ctx
                .sentinel_state_mut()
                .registry
                .masters()
                .map(|(_, master)| master)
                .find(|m| m.id == *master_id)
            {
                Some(m) => m,
                None => continue,
            };
            master.replicas.clone()
        };
        for rid in &replica_ids {
            failover::collect_pending_actions(ctx, *rid, now, &mut to_check_sdown);
        }

        let sentinel_ids: Vec<SentinelInstanceId> = {
            let master = match ctx
                .sentinel_state_mut()
                .registry
                .masters()
                .map(|(_, master)| master)
                .find(|m| m.id == *master_id)
            {
                Some(m) => m,
                None => continue,
            };
            master.sentinels.clone()
        };
        for sid in &sentinel_ids {
            failover::collect_pending_actions(ctx, *sid, now, &mut to_check_sdown);
        }
    }

    for id in &to_check_sdown {
        sdown::check_sdown(ctx, *id, now);
    }
    for id in &master_ids {
        sdown::check_odown(ctx, *id, now);
    }
    failover::drive_failovers(ctx, now);

    // server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ
    ctx.config_mut().server.hz =
        CONFIG_DEFAULT_HZ + (random_usize(CONFIG_DEFAULT_HZ as usize) as i32);

    ctx.sentinel_state_mut().script_engine.run_pending();
    ctx.sentinel_state_mut().script_engine.collect_terminated();
    ctx.sentinel_state_mut().script_engine.kill_timedout();
}
