use super::util::{emit_line, format_bytes, total_system_memory};
use crate::foundation::memory::metrics::memory_stats::{get_tracker, MemoryCategory};
use crate::framework::context::ServerContext;

pub(super) fn gen_memory_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Memory\r\n");

    let alloc_stats = crate::foundation::memory::util_alloc::allocator_stats();
    let rss_val = alloc_stats.resident;
    let maxmemory_state = ctx.maxmemory_state();

    let mem = get_tracker();
    let replica_repl_buffer = if ctx.replication().is_replica() {
        ctx.replication().replica_output_buffer_bytes
    } else {
        0
    };
    let overhead = mem.overhead().saturating_add(replica_repl_buffer);
    let startup = mem.startup_allocated();
    // Dataset mutations maintain this tracker incrementally. INFO must not
    // walk every key: cluster-manager calls INFO while migrating each slot,
    // and a scan here turns a rebalance into O(slots * dataset).
    let dataset = mem.get(MemoryCategory::Dataset);
    // Keep the maxmemory control plane and INFO's primary used-memory value
    // on the same C-equivalent accounting source (`zmalloc_used_memory`).
    // Dataset/overhead remain separately reported diagnostic categories.
    let used = maxmemory_state.reported;
    let peak = crate::foundation::memory::util_alloc::peak_allocated().max(used);
    let peak_perc = if peak > 0 {
        (used as f64) / (peak as f64) * 100.0
    } else {
        100.0
    };
    let dataset_perc = if used > 0 {
        (dataset as f64) / (used as f64) * 100.0
    } else {
        0.0
    };

    let total_system = total_system_memory();

    emit_line(&mut s, "used_memory", used);
    emit_line(&mut s, "used_memory_human", format_bytes(used));
    emit_line(&mut s, "used_memory_rss", rss_val);
    emit_line(&mut s, "used_memory_rss_human", format_bytes(rss_val));
    emit_line(&mut s, "used_memory_peak", peak);
    emit_line(&mut s, "used_memory_peak_human", format_bytes(peak));
    emit_line(
        &mut s,
        "used_memory_peak_perc",
        format!("{:.2}%", peak_perc),
    );
    emit_line(&mut s, "used_memory_overhead", overhead);
    emit_line(&mut s, "used_memory_startup", startup);
    emit_line(&mut s, "used_memory_dataset", dataset);
    emit_line(
        &mut s,
        "used_memory_dataset_perc",
        format!("{:.2}%", dataset_perc),
    );
    emit_line(
        &mut s,
        "mem_not_counted_for_evict",
        maxmemory_state.not_counted,
    );

    let repl_backlog = mem.get(MemoryCategory::ReplBacklog);
    let clients_normal = ctx.normal_client_memory();
    let aof_buffer = if ctx.config().persistence.aof_enabled {
        ctx.aof().buffer_allocation_bytes()
    } else {
        mem.get(MemoryCategory::AofBuffer)
    };
    let lua_vm = mem.get(MemoryCategory::LuaVm);
    let functions_vm = mem.get(MemoryCategory::FunctionsVm);
    let scripts_eval = mem.get(MemoryCategory::ScriptsCaches);
    let cluster_links = ctx.cluster_state().as_deref().map_or_else(
        || mem.get(MemoryCategory::ClusterLinks),
        |state| state.cluster_link_memory(),
    );

    // Replication buffer accounting mirrors C's object.c getMemoryOverheadData:
    // the backlog and replica output buffers share one pool (`repl_buffer_mem`).
    // Only the excess over `repl-backlog-size` counts as replica-client memory;
    // the rest is attributed to the backlog. With no replicas the whole pool is
    // the backlog.
    let raw_repl_buffer_mem = ctx.replication().repl_buffer_mem();
    let repl_backlog_size = ctx.config().server.repl_backlog_size.max(0) as usize;
    let has_replicas = ctx.replication().view.live_replica_count > 0;
    let repl_buffer_mem = raw_repl_buffer_mem;
    let (mem_replication_backlog, mem_clients_slaves) =
        if has_replicas && repl_buffer_mem > repl_backlog_size {
            (repl_backlog_size, repl_buffer_mem - repl_backlog_size)
        } else {
            (repl_buffer_mem, 0)
        };

    let vm_total = lua_vm + functions_vm;
    let scripts_total = scripts_eval + functions_vm;
    let mem_overhead_db_hashtable_rehashing: usize = (0..ctx.dbs().num_dbs() as u32)
        .map(|idx| ctx.dbs().rehashing_bucket_count(idx) * 64)
        .sum();

    emit_line(&mut s, "used_memory_repl_backlog", repl_backlog);
    emit_line(&mut s, "used_memory_clients_normal", clients_normal);
    emit_line(&mut s, "used_memory_aof_buffer", aof_buffer);
    emit_line(&mut s, "used_memory_lua", lua_vm);
    emit_line(&mut s, "used_memory_lua_human", format_bytes(lua_vm));
    emit_line(&mut s, "used_memory_vm_eval", lua_vm);
    emit_line(&mut s, "used_memory_vm_functions", functions_vm);
    emit_line(&mut s, "used_memory_scripts_eval", scripts_eval);
    emit_line(&mut s, "used_memory_functions", functions_vm);

    emit_line(
        &mut s,
        "number_of_functions",
        ctx.script().function_registry.function_count() as i64,
    );
    emit_line(
        &mut s,
        "number_of_libraries",
        ctx.script().function_registry.library_count() as i64,
    );
    emit_line(&mut s, "used_memory_vm_total", vm_total);
    emit_line(&mut s, "used_memory_vm_total_human", format_bytes(vm_total));
    emit_line(&mut s, "used_memory_scripts", scripts_total);
    emit_line(
        &mut s,
        "used_memory_scripts_human",
        format_bytes(scripts_total),
    );

    emit_line(&mut s, "used_memory_cluster_links", cluster_links);
    emit_line(
        &mut s,
        "lazyfree_pending_objects",
        ctx.stats().lazyfree_pending_objects,
    );
    emit_line(&mut s, "lazyfreed_objects", ctx.stats().lazyfreed_objects);

    let frag_ratio = if alloc_stats.allocated > 0 {
        alloc_stats.active as f64 / alloc_stats.allocated as f64
    } else {
        1.0
    };
    let frag_bytes = (alloc_stats.active as isize) - (alloc_stats.allocated as isize);
    let rss_ratio = if alloc_stats.active > 0 {
        alloc_stats.resident as f64 / alloc_stats.active as f64
    } else {
        1.0
    };
    let rss_extra = (alloc_stats.resident as isize) - (alloc_stats.active as isize);

    emit_line(&mut s, "allocator_frag_ratio", format!("{:.2}", frag_ratio));
    emit_line(&mut s, "allocator_frag_bytes", frag_bytes.max(0));
    emit_line(&mut s, "allocator_rss_ratio", format!("{:.2}", rss_ratio));
    emit_line(&mut s, "allocator_rss_bytes", rss_extra.max(0));

    emit_line(&mut s, "total_system_memory", total_system);
    emit_line(
        &mut s,
        "total_system_memory_human",
        format_bytes(total_system),
    );

    let maxmemory = ctx.config().server.maxmemory;
    emit_line(&mut s, "maxmemory", maxmemory);
    emit_line(&mut s, "maxmemory_human", format_bytes(maxmemory as usize));
    emit_line(
        &mut s,
        "maxmemory_policy",
        ctx.config().server.maxmemory_policy.clone(),
    );

    emit_line(&mut s, "rss_overhead_ratio", format!("{:.2}", rss_ratio));
    emit_line(&mut s, "rss_overhead_bytes", rss_extra.max(0));
    emit_line(
        &mut s,
        "mem_fragmentation_ratio",
        format!("{:.2}", frag_ratio),
    );
    emit_line(&mut s, "mem_fragmentation_bytes", frag_bytes.max(0));

    emit_line(&mut s, "mem_replication_backlog", mem_replication_backlog);
    emit_line(&mut s, "mem_total_replication_buffers", repl_buffer_mem);
    emit_line(&mut s, "mem_replicas_repl_buffer", replica_repl_buffer);
    emit_line(&mut s, "mem_clients_slaves", mem_clients_slaves);
    emit_line(&mut s, "mem_clients_normal", clients_normal);
    emit_line(&mut s, "mem_cluster_links", cluster_links);
    emit_line(&mut s, "mem_aof_buffer", aof_buffer);
    emit_line(
        &mut s,
        "mem_overhead_db_hashtable_rehashing",
        mem_overhead_db_hashtable_rehashing,
    );
    emit_line(&mut s, "mem_allocator", "libc");
    emit_line(
        &mut s,
        "active_defrag_running",
        i64::from(ctx.stats().active_defrag_running),
    );

    emit_line(&mut s, "allocator_allocated", alloc_stats.allocated);
    emit_line(&mut s, "allocator_active", alloc_stats.active);
    emit_line(&mut s, "allocator_resident", alloc_stats.resident);
    emit_line(&mut s, "allocator_muzzy", alloc_stats.muzzy);
    emit_line(
        &mut s,
        "number_of_cached_scripts",
        ctx.script().script_cache.len(),
    );

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info_value(section: &str, name: &str) -> Option<usize> {
        section.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key == name).then(|| value.parse::<usize>().ok()).flatten()
        })
    }

    #[test]
    fn memory_section_reports_rehashing_hashtable_bytes() {
        let mut ctx = ServerContext::new(1);
        let db = ctx.dbs_mut().get_mut(0).unwrap();
        for index in 0..8u64 {
            db.set(
                format!("k{index}").into_bytes(),
                crate::foundation::io::store::Robj::string("v"),
            );
        }

        let section = gen_memory_section(&ctx);
        assert_eq!(
            info_value(&section, "mem_overhead_db_hashtable_rehashing"),
            Some(64)
        );
    }

    #[test]
    fn memory_section_uses_canonical_not_counted_projection() {
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.repl_backlog_size = 1_024;
        ctx.replication_mut().replica_output_buffer_bytes = 4_096;
        ctx.set_slot_export_output_memory(512);

        let section = gen_memory_section(&ctx);

        assert_eq!(
            info_value(&section, "mem_not_counted_for_evict"),
            Some(ctx.maxmemory_state().not_counted)
        );
        assert!(info_value(&section, "used_memory").is_some_and(|used| used > 0));
    }

    #[test]
    fn memory_section_reports_runtime_normal_client_projection() {
        let mut ctx = ServerContext::new(1);
        ctx.adjust_normal_client_memory(0, 12_345);

        let section = gen_memory_section(&ctx);

        assert_eq!(info_value(&section, "mem_clients_normal"), Some(12_345));
        assert_eq!(
            info_value(&section, "used_memory_clients_normal"),
            Some(12_345)
        );
    }
}
