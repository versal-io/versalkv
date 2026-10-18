use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;

pub fn config_resetstat(ctx: &mut ServerContext) -> Value {
    ctx.stats_mut().reset();
    ctx.reset_lazyfree_stats();
    ctx.reset_io_thread_counters();
    ctx.slot_metrics_mut().reset_counters();
    ctx.script_mut().script_cache.reset_stats();
    ctx.clear_expired_deletions();
    if let Some(state) = ctx.cluster_state_mut().as_deref_mut() {
        state.reset_cluster_stats();
    }
    ctx.set_cmd_fullname("config|resetstat");
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::config_resetstat;
    use crate::framework::context::ServerContext;

    #[test]
    fn resetstat_clears_slot_counters() {
        let mut ctx = ServerContext::new(1);
        ctx.slot_metrics_mut().record(&[7], 3, 5, 8);

        let _ = config_resetstat(&mut ctx);

        assert_eq!(ctx.slot_metrics().counters(7).unwrap().cpu_usec, 0);
        assert_eq!(
            ctx.slot_metrics().counters(7).unwrap().network_bytes_in,
            0
        );
        assert_eq!(
            ctx.slot_metrics().counters(7).unwrap().network_bytes_out,
            0
        );
    }
}
