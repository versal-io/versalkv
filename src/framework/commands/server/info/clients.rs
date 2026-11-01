use crate::foundation::io::sys::time::mstime;
use crate::framework::context::{PauseAction, ServerContext};

use super::util::emit_line;

pub(super) fn gen_clients_section(
    connected_clients: u64,
    blocked_clients: usize,
    ctx: &ServerContext,
) -> String {
    let mut s = String::new();
    let (watching_clients, total_watched_keys) = ctx.watched_client_summary();
    s.push_str("# Clients\r\n");
    emit_line(&mut s, "connected_clients", connected_clients);
    emit_line(
        &mut s,
        "connected_priority_clients",
        ctx.stats().connected_priority_clients,
    );
    emit_line(
        &mut s,
        "cluster_connections",
        if ctx.config().server.cluster_enabled {
            ctx.cluster_state().as_ref().map_or(0, |state| state.connection_budget())
        } else {
            0
        },
    );
    emit_line(&mut s, "maxclients", ctx.config().server.maxclients);
    emit_line(&mut s, "client_recent_max_input_buffer", 0);
    emit_line(&mut s, "client_recent_max_output_buffer", 0);
    emit_line(&mut s, "blocked_clients", blocked_clients);
    emit_line(&mut s, "tracking_clients", ctx.tracking().clients.len());
    emit_line(&mut s, "pubsub_clients", ctx.pubsub().subscribed_client_count());
    emit_line(&mut s, "watching_clients", watching_clients);
    emit_line(&mut s, "clients_in_timeout_table", 0);
    emit_line(&mut s, "total_watched_keys", total_watched_keys);
    emit_line(
        &mut s,
        "total_blocking_keys",
        ctx.blocking_registry().key_count(),
    );
    emit_line(
        &mut s,
        "total_blocking_keys_on_nokey",
        ctx.blocking_registry().keys_with_multiple_waiters(),
    );
    let now = mstime();
    let pause = ctx.client_pause().is_paused(now);
    let (paused_reason, paused_actions) = match pause {
        PauseAction::None => ("none", "none"),
        PauseAction::Write => (ctx.client_pause().reason, "write"),
        PauseAction::All => (ctx.client_pause().reason, "all"),
    };
    let remaining = if pause == PauseAction::None {
        0
    } else {
        (ctx.client_pause().end_time_ms - now).max(0)
    };
    emit_line(&mut s, "paused_reason", paused_reason);
    emit_line(&mut s, "paused_actions", paused_actions);
    emit_line(&mut s, "paused_timeout_milliseconds", remaining);
    s
}
