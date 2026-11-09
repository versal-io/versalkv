use super::super::super::ServerWorkerState;
use super::filters::ClientFilters;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::distributed::replication::MasterReconnectCause;

/// Build a CLIENT KILL filter for the legacy `ip:port` format.
fn old_format_kill_filter(addr: String) -> ClientFilters {
    ClientFilters::for_legacy_address(addr)
}

impl ServerWorkerState {
    /// CLIENT KILL — close connections matching filters.
    /// Supports both the old `ip:port` format and the new filter-based format.
    pub(super) fn handle_client_kill(
        &mut self,
        token_value: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        let (value, is_error) = self.client_kill_value(token_value, args, name_lower);
        self.ctx.end_cmd(name_lower, is_error);
        replies.push(if resp3 {
            serialize_to_vec_resp3(&value)
        } else {
            serialize_to_vec_resp2(&value)
        });
        Some(is_error)
    }

    /// Pure core for `CLIENT KILL`: parse filters, close matching connections and
    /// return `(reply, is_error)` without touching stats or the socket. This lets
    /// the same logic run both on the live path and during EXEC replay, where a
    /// queued `CLIENT KILL <filter>` must produce a real reply instead of falling
    /// back to the generic `client` handler (which knows no `kill` subcommand).
    pub(crate) fn client_kill_value(
        &mut self,
        token_value: usize,
        args: &CommandArgs,
        name_lower: &str,
    ) -> (Value, bool) {
        if args.args.len() < 2 {
            return (
                Value::error(format!(
                    "ERR wrong number of arguments for '{}|kill' command",
                    name_lower
                )),
                true,
            );
        }

        let first_arg = String::from_utf8_lossy(&args.args[1]).to_string();
        let single_arg = args.args.len() == 2;
        let is_old_format = single_arg && first_arg.contains(':');

        let filters = if is_old_format {
            old_format_kill_filter(first_arg)
        } else {
            match ClientFilters::parse(args) {
                Ok(f) => f,
                Err(err) => return (err, true),
            }
        };

        //
        let skipme_default = !is_old_format;
        let current_token = token_value;
        let mut to_kill: Vec<usize> = Vec::new();
        let mut close_this_client = false;

        for (_, conn) in self.connection_registry.iter() {
            if !self.conn_matches_filters(conn, &filters, current_token, skipme_default) {
                continue;
            }
            if conn.token().0 == current_token {
                close_this_client = true;
            } else {
                to_kill.push(conn.token().0);
            }
        }

        let mut killed = 0usize;
        for tok in &to_kill {
            if self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(*tok))
            {
                // C queues a nonblocking reconnect after CLIENT KILL, while this
                // runtime can advance a cached reconnect in the same command
                // finalization turn. Keep the administrative close observable
                // without delaying ordinary established-link recovery.
                self.remove_connection_with_reconnect_cause(
                    *tok,
                    Some(MasterReconnectCause::AdministrativeClientKill),
                );
                killed += 1;
            }
        }
        if close_this_client {
            killed += 1;
        }

        let (reply, is_error) = if is_old_format && killed == 0 {
            (Value::error("ERR No such client"), false)
        } else if is_old_format {
            (Value::ok(), false)
        } else {
            (Value::integer(killed as i64), false)
        };

        if close_this_client {
            if let Some(conn) = self
                .connection_registry
                .get_mut(crate::framework::capabilities::io::Token(current_token))
            {
                conn.state =
                    crate::framework::capabilities::connection::ConnectionState::CloseAfterReply;
            }
        }
        (reply, is_error)
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::{Connection, ConnectionState};
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};
    use crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer;
    use bytes::Bytes;
    use std::net::SocketAddr;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn old_format_client_kill_returns_ok_after_a_match() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(42));
        connection.set_client_id_for_test(4242);
        connection.peer_addr = "127.0.0.1:6379".parse().unwrap();
        let connection_token = runtime.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLIENT"),
            Bytes::from_static(b"KILL"),
            Bytes::from_static(b"127.0.0.1:6379"),
        ]);

        let (reply, is_error) = runtime.client_kill_value(42, &args, "client");

        assert_eq!(reply, Value::ok());
        assert!(!is_error);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .state,
            ConnectionState::CloseAfterReply
        );
    }

    #[test]
    fn client_kill_master_link_uses_administrative_reconnect_backoff() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = runtime.transport.bind_tcp(peer_addr).unwrap();
        let master_token = listener.token;
        runtime.add_connection(RuntimeConnectedPeer {
            token: master_token,
            peer_addr,
        });
        runtime
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(master_token.0),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
                "0123456789abcdef0123456789abcdef01234567".to_string(),
                offset(42),
            ));
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLIENT"),
            Bytes::from_static(b"KILL"),
            Bytes::from(peer_addr.to_string()),
        ]);
        let before_ms = crate::foundation::io::sys::time::mstime();

        let (reply, is_error) = runtime.client_kill_value(usize::MAX, &args, "client");

        assert_eq!(reply, Value::ok());
        assert!(!is_error);
        assert!(
            !runtime
                .replication_runtime
                .master_connect_allowed(before_ms.saturating_add(999)),
            "an administrative CLIENT KILL must not reconnect the master link immediately"
        );
        assert!(
            runtime.replication_runtime.master_connect_allowed(
                crate::foundation::io::sys::time::mstime().saturating_add(1_000)
            ),
            "the administratively killed master link must become eligible after the backoff"
        );
    }
}
