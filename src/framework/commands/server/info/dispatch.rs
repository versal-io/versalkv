use crate::framework::context::ServerContext;

use super::{clients, cluster, memory, persistence, replication, server, stats};

const DEFAULT_SECTIONS: &[&str] = &[
    "server", "clients", "memory", "persistence", "stats", "replication", "cpu",
    "module_list", "errorstats", "cluster", "keyspace",
];

const SENTINEL_DEFAULT_SECTIONS: &[&str] =
    &["server", "clients", "cpu", "stats", "errorstats", "sentinel", "keyspace"];

const ALL_SECTIONS: &[&str] = &[
    "server", "tls", "clients", "memory", "persistence", "stats", "replication", "cpu",
    "module_list", "commandstats", "errorstats", "latencystats", "cluster", "cluster_info",
    "sentinel", "scriptingengines", "keyspace",
];

const SENTINEL_ALL_SECTIONS: &[&str] = SENTINEL_DEFAULT_SECTIONS;

pub(super) fn default_sections(sentinel: bool) -> &'static [&'static str] {
    if sentinel { SENTINEL_DEFAULT_SECTIONS } else { DEFAULT_SECTIONS }
}

pub(super) fn all_sections(sentinel: bool) -> &'static [&'static str] {
    if sentinel { SENTINEL_ALL_SECTIONS } else { ALL_SECTIONS }
}

pub(super) fn gen_one(ctx: &mut ServerContext, sec: &str) -> String {
    match sec {
        "server" => server::gen_server_section(ctx),
        "clients" => {
            let replication_connections = ctx.replication().view.replica_info.len()
                + usize::from(ctx.replication().is_replica());
            clients::gen_clients_section(
                ctx.stats()
                    .connected_clients
                    .saturating_sub(replication_connections as u64),
                ctx.blocking_registry().count()
                    + ctx.paused_clients().len()
                    + ctx.shutdown_state().blocked_clients,
                ctx,
            )
        }
        "memory" => memory::gen_memory_section(ctx),
        "persistence" => persistence::gen_persistence_section(
            ctx.config(),
            ctx.persistence(),
            ctx.shutdown_state(),
            ctx.is_loading(),
            ctx.replication().async_loading,
        ),
        "cpu" => persistence::gen_cpu_section(ctx),
        "replication" => {
            replication::gen_replication_section(
                ctx.config(),
                ctx.dbs(),
                ctx.replication(),
                crate::foundation::io::sys::time::mstime(),
            )
        }
        "keyspace" => replication::gen_keyspace_section(ctx.dbs()),
        "commandstats" => stats::gen_commandstats_section(ctx.stats()),
        "latencystats" => stats::gen_latencystats_section(ctx),
        "errorstats" => stats::gen_errorstats_section(ctx.stats()),
        "stats" => stats::gen_stats_section(ctx),
        "sentinel" if ctx.sentinel_state().sentinel_mode => cluster::gen_sentinel_section(ctx),
        "tls" => cluster::gen_tls_section(),
        "module_list" | "modules" => cluster::gen_modules_section(ctx),
        "cluster" => cluster::gen_cluster_section(ctx),
        "cluster_info" => cluster::gen_cluster_info_section(ctx),
        "scriptingengines" => cluster::gen_scriptingengines_section(ctx),
        "debug" => stats::gen_debug_section(ctx),
        _ => String::new(),
    }
}
