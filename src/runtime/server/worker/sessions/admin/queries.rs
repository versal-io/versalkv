use super::super::super::ServerWorkerState;
use super::filters::ClientFilters;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use helpers::flag_char_for_conn;

mod helpers {
    pub(super) use super::super::helpers::flag_char_for_conn;
}

/// Compute the read buffer size (rbs) for a connection given queued bytes and idle time.
fn compute_rbs(queued: usize, idle_ms: u64) -> usize {
    if queued > 0 {
        let tail = ((queued - 1) % 16384) + 1;
        if tail < 1024 {
            1024
        } else {
            tail
        }
    } else if idle_ms >= 500 {
        1638
    } else {
        16384
    }
}

/// Format a single CLIENT LIST line for one connection.
/// Also updates `client_infos` for the connection as a side effect (mirrors
/// the original inlined logic).
fn format_client_list_line(
    conn: &Connection,
    now: std::time::Instant,
    token_value: usize,
    client_infos: &mut std::collections::HashMap<usize, crate::engine::state::db::ClientInfo>,
    is_replica: bool,
    is_master_link: bool,
) -> String {
    let age_secs = now.duration_since(conn.create_time).as_secs();
    let idle_ms = now.duration_since(conn.last_interaction).as_millis() as u64;
    let idle_secs = idle_ms / 1000;
    let queued: usize = conn.output.queue.iter().map(|v| v.len()).sum();
    let rbs = compute_rbs(queued, idle_ms);
    let name = match &conn.name {
        Some(n) => String::from_utf8_lossy(n).to_string(),
        None => String::new(),
    };
    let flags = flag_char_for_conn(conn, is_replica, is_master_link);
    let cmd_str = if conn.token().0 == token_value {
        "client|list".to_string()
    } else if let Some(ci) = client_infos.get(&conn.token().0) {
        ci.last_cmd.clone()
    } else {
        String::new()
    };
    if let Some(ci) = client_infos.get_mut(&conn.token().0) {
        ci.name = name.clone();
        ci.rbs = rbs as i64;
        ci.watched_keys = conn.transaction.watched_key_count();
        ci.last_interaction = conn.last_interaction;
    }
    let qbuf = conn.query_buffer_used();
    let qbuf_free = conn.query_buffer_free();
    let memory = conn.client_memory_usage();
    let capa_str = if conn.capa_flags.is_empty() {
        String::new()
    } else {
        conn.capa_flags.join(",")
    };
    format!(
        "id={} addr={} laddr=127.0.0.1:0 fd={} name={} age={} idle={} flags={} capa={} db={} sub={} psub={} ssub={} multi=-1 watch={} qbuf={} qbuf-free={} argv-mem={} multi-mem={} rbs={} rbp=0 obl=0 oll={} omem={} tot-mem={} events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={} tot-net-in={} tot-net-out={} tot-cmds={}\n",
        conn.client_id(),
        conn.peer_addr,
        conn.token().0,
        name,
        age_secs,
        idle_secs,
        flags,
        capa_str,
        conn.selected_db,
        conn.sub_channel_count,
        conn.psub_channel_count,
        conn.ssub_channel_count,
        conn.transaction.watched_key_count(),
        qbuf,
        qbuf_free,
        memory.argv,
        memory.transaction,
        rbs,
        conn.pending_output_list_len(),
        memory.output,
        memory.total,
        cmd_str,
        conn.resp_version,
        conn.lib_name,
        conn.lib_ver,
        conn.tot_net_in,
        conn.tot_net_out,
        conn.cmd_count,
    )
}

impl ServerWorkerState {
    /// CLIENT ID — return the stable, externally visible connection ID.
    pub(super) fn handle_client_id(
        &mut self,
        token_value: usize,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        self.ctx.set_cmd_fullname("client|id");
        self.ctx.end_cmd(name_lower, false);
        let client_id = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(token_value))
            .map(|conn| conn.client_id())
            .unwrap_or(0);
        replies.push(if resp3 {
            serialize_to_vec_resp3(&Value::integer(client_id as i64))
        } else {
            serialize_to_vec_resp2(&Value::integer(client_id as i64))
        });
        Some(false)
    }

    /// CLIENT LIST — list all connections matching optional filters.
    pub(super) fn handle_client_list(
        &mut self,
        token_value: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        let (value, is_error) = self.client_list_value(token_value, args);
        self.ctx.set_cmd_fullname("client|list");
        self.ctx.end_cmd(name_lower, is_error);
        if !is_error {
            if let Some(ci) = self.ctx.client_info_mut(token_value) {
                ci.last_cmd = "client|list".to_string();
            }
        }
        replies.push(if resp3 {
            serialize_to_vec_resp3(&value)
        } else {
            serialize_to_vec_resp2(&value)
        });
        Some(is_error)
    }

    /// Pure core for `CLIENT LIST`: parse filters, render matching connections and
    /// return `(reply, is_error)` without touching stats or the socket, so a
    /// queued `CLIENT LIST` also works inside EXEC replay.
    pub(crate) fn client_list_value(
        &mut self,
        token_value: usize,
        args: &CommandArgs,
    ) -> (Value, bool) {
        let filters = match ClientFilters::parse(args) {
            Ok(f) => f,
            Err(err) => return (err, true),
        };
        let now = std::time::Instant::now();
        let mut out = String::new();
        for (_, conn) in self.connection_registry.iter() {
            if !self.conn_matches_filters(conn, &filters, token_value, false) {
                continue;
            }
            let tok = conn.token().0;
            let is_replica = self.is_replica_connection(tok);
            let is_master_link = self.is_master_link(tok);
            let line = format_client_list_line(
                conn,
                now,
                token_value,
                self.ctx.client_infos_mut(),
                is_replica,
                is_master_link,
            );
            out.push_str(&line);
        }
        (Value::bulk(out.into_bytes()), false)
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use bytes::Bytes;

    #[test]
    fn client_list_uses_lf_line_endings_like_c_valkey() {
        let mut runtime = ServerWorkerState::new(1).expect("create test event loop");
        for token_value in [41, 42] {
            let mut connection = Connection::new_fake();
            connection.set_token_for_test(Token(token_value));
            connection.set_client_id_for_test(token_value as u64);
            let _connection_token = runtime.connection_registry.insert(connection);
        }
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLIENT"),
            Bytes::from_static(b"LIST"),
        ]);

        let (reply, is_error) = runtime.client_list_value(41, &args);

        assert!(!is_error);
        let Value::Bulk(output) = reply else {
            panic!("CLIENT LIST must return a bulk string");
        };
        assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 2);
        assert!(output.ends_with(b"\n"));
        assert!(
            !output.contains(&b'\r'),
            "C Valkey separates CLIENT LIST records with LF only"
        );
    }
}
