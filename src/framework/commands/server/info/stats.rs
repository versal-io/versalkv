use crate::engine::extension::acl::{
    ACL_STATS_AUTH, ACL_STATS_CHANNEL, ACL_STATS_CMD, ACL_STATS_DB, ACL_STATS_KEY,
};
use crate::foundation::memory::metrics::stats::ServerStats;
use crate::framework::context::ServerContext;

use super::util::emit_line;

fn safe_info_name(name: &str) -> String {
    name.chars()
        .map(|character| match character {
            '#' | ':' | '\n' | '\r' => '_',
            _ => character,
        })
        .collect()
}

pub(super) fn gen_commandstats_section(stats: &ServerStats) -> String {
    let mut s = String::new();
    s.push_str("# Commandstats\r\n");
    if stats.cmd_stats.is_empty() {
        s.push_str(
            "cmdstat_ping:calls=0,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0\r\n",
        );
    } else {
        let mut keys: Vec<&String> = stats.cmd_stats.keys().collect();
        keys.sort();
        for k in keys {
            let c = &stats.cmd_stats[k];
            let safe_name = safe_info_name(k);
            let per = if c.calls > 0 {
                (c.usec as f64) / (c.calls as f64)
            } else {
                0.0
            };
            s.push_str(&format!("cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={}\r\n", safe_name, c.calls, c.usec, per, c.rejected_calls, c.failed_calls));
        }
    }
    s
}

pub(super) fn gen_errorstats_section(stats: &ServerStats) -> String {
    let mut s = String::new();
    s.push_str("# Errorstats\r\n");
    let mut keys: Vec<&String> = stats.error_stats.keys().collect();
    keys.sort();
    for k in keys {
        let count = stats.error_stats[k];
        let _ =
            std::fmt::Write::write_fmt(&mut s, format_args!("errorstat_{}:count={}\r\n", k, count));
    }
    s
}

pub(super) fn gen_latencystats_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Latencystats\r\n");
    if !ctx.config().server.latency_tracking {
        return s;
    }
    let percentiles = &ctx.config().server.latency_tracking_info_percentiles;
    let emit_pct_line =
        |s: &mut String, k: &str, c: &crate::foundation::memory::metrics::stats::CmdStat| {
            if c.calls == 0 {
                return;
            }
            let mut parts: Vec<String> = Vec::new();
            for p in percentiles.iter() {
                let Some(val) = c.percentile_usec(*p) else {
                    return;
                };
                let label = if (p.fract()).abs() < 0.01 {
                    format!("p{}", *p as i64)
                } else {
                    format!("p{}", p)
                };
                parts.push(format!("{}={:.3}", label, val as f64));
            }
            s.push_str(&format!(
                "latency_percentiles_usec_{}:{}\r\n",
                k,
                parts.join(",")
            ));
        };
    let mut keys: Vec<&String> = ctx.stats().cmd_stats.keys().collect();
    keys.sort();
    for k in keys {
        let c = &ctx.stats().cmd_stats[k];
        emit_pct_line(&mut s, k, c);
        match k.as_str() {
            "client" => emit_pct_line(&mut s, "client|id", c),
            "config" => {
                emit_pct_line(&mut s, "config|set", c);
                emit_pct_line(&mut s, "config|get", c);
                emit_pct_line(&mut s, "config|resetstat", c);
            }
            _ => {}
        }
    }
    s
}

pub(super) fn gen_stats_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Stats\r\n");
    emit_connection_and_network_stats(&mut s, ctx);
    emit_expiration_and_keyspace_stats(&mut s, ctx);
    emit_observability_and_io_stats(&mut s, ctx);
    emit_eventloop_and_acl_stats(&mut s, ctx);
    s
}

fn emit_connection_and_network_stats(s: &mut String, ctx: &ServerContext) {
    let stats = ctx.stats();
    emit_line(s, "total_connections_received", stats.connections_received);
    emit_line(
        s,
        "total_commands_processed",
        stats.total_commands_processed,
    );
    emit_line(
        s,
        "instantaneous_ops_per_sec",
        stats.instantaneous_ops_per_sec,
    );
    emit_line(s, "total_net_input_bytes", stats.total_net_input_bytes);
    emit_line(s, "total_net_output_bytes", stats.total_net_output_bytes);
    emit_line(
        s,
        "total_net_repl_input_bytes",
        stats.total_net_repl_input_bytes,
    );
    emit_line(
        s,
        "total_net_repl_output_bytes",
        stats.total_net_repl_output_bytes,
    );
    emit_line(s, "total_net_cluster_slot_import_bytes", 0);
    emit_line(s, "total_net_cluster_slot_export_bytes", 0);
    emit_line(
        s,
        "instantaneous_input_kbps",
        format!("{:.2}", stats.instantaneous_input_kbps),
    );
    emit_line(
        s,
        "instantaneous_output_kbps",
        format!("{:.2}", stats.instantaneous_output_kbps),
    );
    emit_line(s, "instantaneous_input_repl_kbps", "0.00");
    emit_line(s, "instantaneous_output_repl_kbps", "0.00");
}

fn emit_expiration_and_keyspace_stats(s: &mut String, ctx: &ServerContext) {
    let stats = ctx.stats();
    emit_line(s, "rejected_connections", stats.rejected_connections);
    emit_line(s, "sync_full", stats.sync_full);
    emit_line(s, "sync_partial_ok", stats.sync_partial_ok);
    emit_line(s, "sync_partial_err", stats.sync_partial_err);
    emit_line(s, "expired_keys", stats.expired_keys);
    emit_line(s, "expired_fields", stats.expired_fields);
    emit_line(s, "expired_stale_perc", "0.00");
    emit_line(s, "expired_time_cap_reached_count", 0);
    emit_line(s, "expire_cycle_cpu_milliseconds", 0);
    emit_line(s, "evicted_keys", stats.evicted_keys);
    emit_line(s, "evicted_clients", stats.evicted_clients);
    emit_line(
        s,
        "evicted_scripts",
        ctx.script().script_cache.evicted_scripts(),
    );
    emit_line(s, "total_eviction_exceeded_time", 0);
    emit_line(s, "current_eviction_exceeded_time", 0);
    emit_line(s, "keyspace_hits", stats.keyspace_hits);
    emit_line(s, "keyspace_misses", stats.keyspace_misses);
    emit_line(s, "pubsub_channels", ctx.pubsub().channel_count());
    emit_line(s, "pubsub_patterns", ctx.pubsub().pattern_count());
    emit_line(
        s,
        "pubsubshard_channels",
        ctx.pubsub().shard_channel_count(),
    );
    emit_line(s, "latest_fork_usec", stats.latest_fork_usec);
    emit_line(s, "total_forks", stats.total_forks);
    emit_line(s, "migrate_cached_sockets", ctx.migrate_sockets().len());
    let tracked_expires: usize = (0..ctx.num_dbs())
        .filter_map(|idx| ctx.db(idx as u32))
        .map(|db| db.expires_count())
        .sum();
    emit_line(s, "slave_expires_tracked_keys", tracked_expires);
}

fn emit_observability_and_io_stats(s: &mut String, ctx: &ServerContext) {
    let stats = ctx.stats();
    emit_line(s, "active_defrag_hits", stats.active_defrag_hits);
    emit_line(s, "active_defrag_misses", stats.active_defrag_misses);
    emit_line(s, "active_defrag_key_hits", stats.active_defrag_key_hits);
    emit_line(
        s,
        "active_defrag_key_misses",
        stats.active_defrag_key_misses,
    );
    emit_line(
        s,
        "total_active_defrag_time",
        stats.total_active_defrag_time,
    );
    emit_line(
        s,
        "current_active_defrag_time",
        stats.current_active_defrag_time,
    );
    emit_line(s, "tracking_total_keys", ctx.tracking().total_keys());
    emit_line(s, "tracking_total_items", ctx.tracking().total_items());
    emit_line(
        s,
        "tracking_total_prefixes",
        ctx.tracking().total_prefixes(),
    );
    emit_line(s, "unexpected_error_replies", 0);
    emit_line(s, "total_error_replies", stats.total_error_replies);
    emit_line(s, "dump_payload_sanitizations", 0);
    emit_line(s, "total_reads_processed", 0);
    emit_line(s, "total_writes_processed", 0);
    emit_line(
        s,
        "io_threaded_reads_processed",
        ctx.io_thread_info().reads_processed,
    );
    emit_line(
        s,
        "io_threaded_writes_processed",
        ctx.io_thread_info().writes_processed,
    );
    emit_line(s, "io_threaded_freed_objects", 0);
    emit_line(s, "io_threaded_accept_processed", 0);
    emit_line(
        s,
        "io_threaded_poll_processed",
        ctx.io_thread_info().polls_processed,
    );
    emit_line(
        s,
        "io_threaded_total_prefetch_batches",
        stats.io_threaded_total_prefetch_batches,
    );
    emit_line(
        s,
        "io_threaded_total_prefetch_entries",
        stats.io_threaded_total_prefetch_entries,
    );
    emit_line(
        s,
        "client_query_buffer_limit_disconnections",
        stats.client_query_buffer_limit_disconnections,
    );
    emit_line(
        s,
        "client_output_buffer_limit_disconnections",
        stats.client_output_buffer_limit_disconnections,
    );
    emit_line(s, "reply_buffer_shrinks", 0);
    emit_line(s, "reply_buffer_expands", 0);
}

fn emit_eventloop_and_acl_stats(s: &mut String, ctx: &ServerContext) {
    let stats = ctx.stats();
    emit_line(s, "eventloop_cycles", stats.eventloop_cycles);
    emit_line(s, "eventloop_duration_sum", stats.eventloop_duration_sum);
    emit_line(
        s,
        "eventloop_duration_cmd_sum",
        stats.eventloop_duration_cmd_sum,
    );
    emit_line(
        s,
        "eventloop_events_collected",
        stats.eventloop_events_collected,
    );
    emit_line(
        s,
        "eventloop_events_handled",
        stats.eventloop_events_handled,
    );
    emit_line(
        s,
        "eventloop_events_deferred",
        stats.eventloop_events_deferred,
    );
    emit_line(
        s,
        "eventloop_events_requeued",
        stats.eventloop_events_requeued,
    );
    emit_line(s, "eventloop_events_stale", stats.eventloop_events_stale);
    emit_line(
        s,
        "eventloop_events_owner_closed",
        stats.eventloop_events_owner_closed,
    );
    emit_line(
        s,
        "eventloop_events_dispatch_latency_sum",
        stats.eventloop_events_dispatch_latency_sum,
    );
    emit_line(
        s,
        "instantaneous_eventloop_cycles_per_sec",
        stats.instantaneous_eventloop_cycles_per_sec,
    );
    emit_line(
        s,
        "instantaneous_eventloop_duration_usec",
        stats.instantaneous_eventloop_duration_usec,
    );
    emit_line(
        s,
        "acl_access_denied_auth",
        ACL_STATS_AUTH.load(std::sync::atomic::Ordering::Relaxed),
    );
    emit_line(
        s,
        "acl_access_denied_cmd",
        ACL_STATS_CMD.load(std::sync::atomic::Ordering::Relaxed),
    );
    emit_line(
        s,
        "acl_access_denied_key",
        ACL_STATS_KEY.load(std::sync::atomic::Ordering::Relaxed),
    );
    emit_line(
        s,
        "acl_access_denied_channel",
        ACL_STATS_CHANNEL.load(std::sync::atomic::Ordering::Relaxed),
    );
    emit_line(s, "acl_access_denied_tls_cert", 0);
    emit_line(
        s,
        "acl_access_denied_db",
        ACL_STATS_DB.load(std::sync::atomic::Ordering::Relaxed),
    );
}

pub(super) fn gen_debug_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Debug\r\n");
    emit_line(
        &mut s,
        "eventloop_duration_aof_sum",
        ctx.stats().eventloop_duration_aof_sum,
    );
    emit_line(
        &mut s,
        "eventloop_duration_cron_sum",
        ctx.stats().eventloop_duration_cron_sum,
    );
    emit_line(
        &mut s,
        "eventloop_duration_max",
        ctx.stats().eventloop_duration_max,
    );
    emit_line(
        &mut s,
        "eventloop_cmd_per_cycle_max",
        ctx.stats().eventloop_cmd_per_cycle_max,
    );
    emit_line(
        &mut s,
        "io_threaded_reads_pending",
        ctx.io_thread_info().reads_pending,
    );
    emit_line(
        &mut s,
        "io_threaded_writes_pending",
        ctx.io_thread_info().writes_pending,
    );
    emit_line(
        &mut s,
        "io_threaded_polls_pending",
        ctx.io_thread_info().polls_pending,
    );
    s
}

#[cfg(test)]
mod tests {
    use super::gen_commandstats_section;
    use crate::foundation::memory::metrics::stats::ServerStats;

    #[test]
    fn stats_section_preserves_all_field_names_and_order() {
        let ctx = crate::framework::context::ServerContext::new(1);
        let section = super::gen_stats_section(&ctx);
        let fields: Vec<_> = section
            .lines()
            .skip(1)
            .map(|line| line.split_once(':').expect("stats field separator").0)
            .collect();
        assert_eq!(
            fields,
            vec![
                "total_connections_received",
                "total_commands_processed",
                "instantaneous_ops_per_sec",
                "total_net_input_bytes",
                "total_net_output_bytes",
                "total_net_repl_input_bytes",
                "total_net_repl_output_bytes",
                "total_net_cluster_slot_import_bytes",
                "total_net_cluster_slot_export_bytes",
                "instantaneous_input_kbps",
                "instantaneous_output_kbps",
                "instantaneous_input_repl_kbps",
                "instantaneous_output_repl_kbps",
                "rejected_connections",
                "sync_full",
                "sync_partial_ok",
                "sync_partial_err",
                "expired_keys",
                "expired_fields",
                "expired_stale_perc",
                "expired_time_cap_reached_count",
                "expire_cycle_cpu_milliseconds",
                "evicted_keys",
                "evicted_clients",
                "evicted_scripts",
                "total_eviction_exceeded_time",
                "current_eviction_exceeded_time",
                "keyspace_hits",
                "keyspace_misses",
                "pubsub_channels",
                "pubsub_patterns",
                "pubsubshard_channels",
                "latest_fork_usec",
                "total_forks",
                "migrate_cached_sockets",
                "slave_expires_tracked_keys",
                "active_defrag_hits",
                "active_defrag_misses",
                "active_defrag_key_hits",
                "active_defrag_key_misses",
                "total_active_defrag_time",
                "current_active_defrag_time",
                "tracking_total_keys",
                "tracking_total_items",
                "tracking_total_prefixes",
                "unexpected_error_replies",
                "total_error_replies",
                "dump_payload_sanitizations",
                "total_reads_processed",
                "total_writes_processed",
                "io_threaded_reads_processed",
                "io_threaded_writes_processed",
                "io_threaded_freed_objects",
                "io_threaded_accept_processed",
                "io_threaded_poll_processed",
                "io_threaded_total_prefetch_batches",
                "io_threaded_total_prefetch_entries",
                "client_query_buffer_limit_disconnections",
                "client_output_buffer_limit_disconnections",
                "reply_buffer_shrinks",
                "reply_buffer_expands",
                "eventloop_cycles",
                "eventloop_duration_sum",
                "eventloop_duration_cmd_sum",
                "eventloop_events_collected",
                "eventloop_events_handled",
                "eventloop_events_deferred",
                "eventloop_events_requeued",
                "eventloop_events_stale",
                "eventloop_events_owner_closed",
                "eventloop_events_dispatch_latency_sum",
                "instantaneous_eventloop_cycles_per_sec",
                "instantaneous_eventloop_duration_usec",
                "acl_access_denied_auth",
                "acl_access_denied_cmd",
                "acl_access_denied_key",
                "acl_access_denied_channel",
                "acl_access_denied_tls_cert",
                "acl_access_denied_db",
            ]
        );
    }

    #[test]
    fn commandstats_sanitizes_unsafe_command_name_characters() {
        let mut stats = ServerStats::new();
        stats.incr_cmd_call("test.weird:cmd");

        let section = gen_commandstats_section(&stats);

        assert!(section.contains("cmdstat_test.weird_cmd:calls=1"));
        assert!(!section.contains("cmdstat_test.weird:cmd"));
    }

    #[test]
    fn stats_section_reports_live_threaded_io_counters() {
        let mut ctx = crate::framework::context::ServerContext::new(1);
        ctx.set_io_thread_info(crate::framework::context::IoThreadInfo {
            reads_processed: 7,
            writes_processed: 8,
            polls_processed: 9,
            reads_pending: 2,
            writes_pending: 3,
            polls_pending: 4,
            ..crate::framework::context::IoThreadInfo::default()
        });

        let stats = super::gen_stats_section(&ctx);
        let debug = super::gen_debug_section(&ctx);

        assert!(stats.contains("io_threaded_reads_processed:7\r\n"));
        assert!(stats.contains("io_threaded_writes_processed:8\r\n"));
        assert!(stats.contains("io_threaded_poll_processed:9\r\n"));
        assert!(debug.contains("io_threaded_reads_pending:2\r\n"));
        assert!(debug.contains("io_threaded_writes_pending:3\r\n"));
        assert!(debug.contains("io_threaded_polls_pending:4\r\n"));
    }

    #[test]
    fn stats_section_reports_live_prefetch_counters() {
        let mut ctx = crate::framework::context::ServerContext::new(1);
        ctx.stats_mut().io_threaded_total_prefetch_batches = 3;
        ctx.stats_mut().io_threaded_total_prefetch_entries = 7;

        let stats = super::gen_stats_section(&ctx);
        assert!(stats.contains("io_threaded_total_prefetch_batches:3\r\n"));
        assert!(stats.contains("io_threaded_total_prefetch_entries:7\r\n"));
    }

    #[test]
    fn stats_section_reports_live_active_defrag_counters() {
        let mut ctx = crate::framework::context::ServerContext::new(1);
        ctx.stats_mut().active_defrag_hits = 3;
        ctx.stats_mut().active_defrag_key_hits = 2;
        ctx.stats_mut().total_active_defrag_time = 11;

        let stats = super::gen_stats_section(&ctx);
        assert!(stats.contains("active_defrag_hits:3\r\n"));
        assert!(stats.contains("active_defrag_key_hits:2\r\n"));
        assert!(stats.contains("total_active_defrag_time:11\r\n"));
    }

    #[test]
    fn stats_section_reports_eventloop_event_counters() {
        let mut ctx = crate::framework::context::ServerContext::new(1);
        ctx.stats_mut().eventloop_events_collected = 5;
        ctx.stats_mut().eventloop_events_handled = 2;
        ctx.stats_mut().eventloop_events_deferred = 1;
        ctx.stats_mut().eventloop_events_requeued = 3;
        ctx.stats_mut().eventloop_events_stale = 4;
        ctx.stats_mut().eventloop_events_owner_closed = 6;
        ctx.stats_mut().eventloop_events_dispatch_latency_sum = 42;

        let stats = super::gen_stats_section(&ctx);

        assert!(stats.contains("eventloop_events_collected:5\r\n"));
        assert!(stats.contains("eventloop_events_handled:2\r\n"));
        assert!(stats.contains("eventloop_events_deferred:1\r\n"));
        assert!(stats.contains("eventloop_events_requeued:3\r\n"));
        assert!(stats.contains("eventloop_events_stale:4\r\n"));
        assert!(stats.contains("eventloop_events_owner_closed:6\r\n"));
        assert!(stats.contains("eventloop_events_dispatch_latency_sum:42\r\n"));
    }
}
