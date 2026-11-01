use crate::framework::context::ServerContext;

use super::util::emit_line;

pub(super) fn gen_tls_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# TLS\r\n");
    let info = ctx.tls_info();
    let now_sec = crate::foundation::io::sys::time::mstime() / 1000;
    for (prefix, cert) in [
        ("tls_server", &info.server), ("tls_server_alt", &info.alternate),
        ("tls_client", &info.client), ("tls_ca", &info.ca),
    ] {
        emit_line(&mut s, &format!("{prefix}_cert_serial"), cert.serial.as_deref().unwrap_or("none"));
        emit_line(&mut s, &format!("{prefix}_cert_expires_in_seconds"), cert.expires_in_seconds(now_sec));
    }
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

#[cfg(test)]
mod tls_tests {
    #[test]
    fn tls_info_has_independent_alternate_certificate_fields() {
        let info = super::gen_tls_section(&crate::framework::context::ServerContext::new(1));
        assert!(info.contains("tls_server_alt_cert_serial:none\r\n"));
        assert!(info.contains("tls_server_alt_cert_expires_in_seconds:0\r\n"));
    }
}
