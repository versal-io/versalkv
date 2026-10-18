use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;

fn compute_rbs(reply_bytes_curr_cmd: usize, idle_ms: u64) -> usize {
    if reply_bytes_curr_cmd > 0 {
        let tail = ((reply_bytes_curr_cmd - 1) % 16384) + 1;
        if tail < 1024 { 1024 } else { tail }
    } else if idle_ms >= 500 {
        1638
    } else {
        16384
    }
}

pub fn client_info(conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    let now = std::time::Instant::now();
    let age_secs = now.duration_since(conn.create_time()).as_secs();
    let idle_ms = now.duration_since(conn.last_interaction()).as_millis() as u64;
    let idle_secs = idle_ms / 1000;
    let mut flags = String::new();
    if let Some(role) = conn.slot_migration_role() {
        flags.push(match role {
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(_) => 'E',
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(_) => 'i',
        });
    }
    if conn.import_source() {
        flags.push('I');
    }
    if conn.readonly() {
        flags.push('r');
    }
    if conn.blocked().is_some() {
        flags.push('b');
    }
    if conn.no_touch() {
        flags.push('N');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let name_str = match &conn.name() {
        Some(n) => String::from_utf8_lossy(n).to_string(),
        None => String::new(),
    };
    let lib_name = conn.lib_name();
    let lib_ver = conn.lib_ver();
    let qbuf = conn.query_buffer_used();
    let qbuf_free = conn.query_buffer_free();
    let rbs = compute_rbs(conn.reply_bytes_curr_cmd(), idle_ms);
    let argv_mem = conn.req_bytes_curr_cmd();
    let multi_mem = conn.transaction_memory_usage();
    let omem = conn.output_buffer_memory_usage();
    let tot_mem = conn.total_memory_usage();
    // C networking.c exposes CLIENT_CAPA_REDIRECT as the compact `r` flag.
    let capa = if conn
        .capa_flags()
        .iter()
        .any(|capability| capability.eq_ignore_ascii_case("redirect"))
    {
        "r"
    } else {
        ""
    };
    let info = format!(
        "id={} addr={} laddr=127.0.0.1:0 fd={} name={} age={} idle={} flags={} capa={} db={} sub={} psub={} ssub={} multi=-1 watch={} qbuf={} qbuf-free={} argv-mem={} multi-mem={} rbs={} rbp=0 obl=0 oll={} omem={} tot-mem={} events=r cmd=client|info user=default redir=-1 resp={} lib-name={} lib-ver={} tot-net-in={} tot-net-out={} tot-cmds={}",
        conn.client_id(),
        conn.peer_addr(),
        conn.file_descriptor(),
        name_str,
        age_secs,
        idle_secs,
        flags,
        capa,
        conn.selected_db(),
        conn.sub_channel_count(),
        conn.psub_channel_count(),
        conn.ssub_channel_count(),
        conn.watched_keys().len(),
        qbuf,
        qbuf_free,
        argv_mem,
        multi_mem,
        rbs,
        conn.output_list_len(),
        omem,
        tot_mem,
        conn.resp_version(),
        lib_name,
        lib_ver,
        conn.tot_net_in(),
        conn.tot_net_out(),
        conn.cmd_count(),
    );
    let _ = ctx;
    Value::bulk(Bytes::from(info))
}

#[cfg(test)]
mod tests {
    use super::client_info;
    use crate::engine::command::context::CommandConn;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use crate::framework::capabilities::connection::Connection;

    #[test]
    fn client_info_reports_shard_subscriptions_separately() {
        let mut conn = Connection::new_fake();
        conn.set_sub_channel_count(2);
        conn.set_psub_channel_count(3);
        conn.set_ssub_channel_count(4);

        let Value::Bulk(info) = client_info(&mut conn, &mut ServerContext::new(1)) else {
            panic!("CLIENT INFO must return a bulk string");
        };
        let info = String::from_utf8_lossy(&info);
        assert!(info.contains(" sub=2 psub=3 ssub=4 "));
    }

    #[test]
    fn client_info_reports_redirect_capability_with_c_compatible_flag() {
        let mut conn = Connection::new_fake();
        conn.capa_flags.push("redirect".to_string());

        let Value::Bulk(info) = client_info(&mut conn, &mut ServerContext::new(1)) else {
            panic!("CLIENT INFO must return a bulk string");
        };

        assert!(String::from_utf8_lossy(&info).contains(" capa=r "));
    }
}
