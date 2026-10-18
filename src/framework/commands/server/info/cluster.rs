use crate::framework::context::ServerContext;

use super::util::emit_line;

pub(super) fn gen_tls_section() -> String {
    let mut s = String::new();
    s.push_str("# TLS\r\n");
    emit_line(&mut s, "tls_server_cert_serial", "none");
    emit_line(&mut s, "tls_client_cert_serial", "none");
    emit_line(&mut s, "tls_ca_cert_serial", "none");
    emit_line(&mut s, "tls_server_cert_expires_in_seconds", 0);
    emit_line(&mut s, "tls_client_cert_expires_in_seconds", 0);
    emit_line(&mut s, "tls_ca_cert_expires_in_seconds", 0);
    s
}

pub(super) fn gen_modules_section(_ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Modules\r\n");
    s
}

pub(super) fn gen_cluster_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Cluster\r\n");
    let enabled = if ctx.cluster_state().is_some() { 1 } else { 0 };
    emit_line(&mut s, "cluster_enabled", enabled);
    s
}

pub(super) fn gen_cluster_info_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Cluster Info\r\n");
    if ctx.cluster_state().is_some() {
        // TODO: cluster enabled - output detailed info
    }
    s
}

pub(super) fn gen_scriptingengines_section(_ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Scripting Engines\r\n");
    emit_line(&mut s, "engines_count", 1);
    emit_line(&mut s, "engines_total_used_memory", 0);
    emit_line(&mut s, "engines_total_memory_overhead", 0);
    emit_line(
        &mut s,
        "engine_0",
        "name=lua,module=built-in,abi_version=5.1,used_memory=0,memory_overhead=0",
    );
    s
}

pub(super) fn gen_sentinel_section(ctx: &ServerContext) -> String {
    use crate::foundation::io::sys::time::mstime;
    let mut s = String::new();
    s.push_str("# Sentinel\r\n");
    let state = ctx.sentinel_state();
    emit_line(
        &mut s,
        "sentinel_masters",
        state.registry.master_count(),
    );
    emit_line(&mut s, "sentinel_tilt", if state.tilt { 1 } else { 0 });
    let tilt_since = if state.tilt && state.tilt_start_time > 0 {
        (mstime() - state.tilt_start_time) / 1000
    } else {
        -1
    };
    emit_line(&mut s, "sentinel_tilt_since_seconds", tilt_since);
    emit_line(&mut s, "sentinel_total_tilt", state.total_tilt);
    emit_line(
        &mut s,
        "sentinel_running_scripts",
        state.script_engine.running.len(),
    );
    emit_line(
        &mut s,
        "sentinel_scripts_queue_length",
        state.script_engine.pending.len(),
    );
    emit_line(
        &mut s,
        "sentinel_simulate_failure_flags",
        state.simfailure_flags,
    );
    s
}

