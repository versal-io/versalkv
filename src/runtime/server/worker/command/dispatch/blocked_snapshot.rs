//! Native waiter registration and command-local snapshot targets.

use bytes::Bytes;

use crate::engine::command::context::{BlockedCmd, BlockedState};
use crate::engine::command::SessionId;
use crate::engine::state::blocking_registry::BlockingClientRegistry;

pub(crate) fn register_key_blocked_command(
    registry: &mut BlockingClientRegistry,
    db_id: u32,
    blocked: &BlockedState,
    session: SessionId,
) {
    match &blocked.cmd {
        BlockedCmd::Blpop
        | BlockedCmd::Brpop
        | BlockedCmd::Brpoplpush { .. }
        | BlockedCmd::Blmove { .. }
        | BlockedCmd::Blmpop { .. }
        | BlockedCmd::Bzpopmin
        | BlockedCmd::Bzpopmax
        | BlockedCmd::Bzmpop { .. }
        | BlockedCmd::Xread
        | BlockedCmd::Xreadgroup => {
            registry.register_snapshot_guarded_client(db_id, &blocked.keys, session);
        }
        _ => registry.register_client(db_id, &blocked.keys, session),
    }
}

pub(crate) fn forkless_blocked_key_targets(
    db_id: u32,
    blocked: &BlockedState,
) -> Vec<(u32, Bytes)> {
    // BlockedState already stores all source keys of multikey list, zset and
    // stream reads. Move destinations are not wait keys.
    let mut targets: Vec<_> = blocked
        .keys
        .iter()
        .map(|key| (db_id, key.clone()))
        .collect();
    let destination = match &blocked.cmd {
        BlockedCmd::Brpoplpush { dst_key } | BlockedCmd::Blmove { dst_key, .. } => dst_key,
        _ => return targets,
    };
    let target = (db_id, destination.clone());
    if !targets.contains(&target) {
        targets.push(target);
    }
    targets
}
