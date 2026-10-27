use crate::framework::capabilities::connection::Connection;

/// Valid client flag characters for CLIENT LIST/KILL flag filters.
const VALID_FLAG_CHARS: &[u8] = b"NOSMbrmIdutTxekKLXpBPcRUWqmnvCDAHsijJFVfgGYwyE";

pub(crate) fn is_valid_flag(flag: &str) -> bool {
    flag.as_bytes()
        .iter()
        .all(|c| VALID_FLAG_CHARS.contains(c) || *c == b':')
}

pub(crate) fn flag_char_for_conn(
    conn: &Connection,
    is_replica: bool,
    is_master_link: bool,
) -> String {
    let mut flags = String::new();
    if conn.monitoring {
        flags.push('O');
    }
    if let Some(role) = conn.slot_migration_role.as_ref() {
        flags.push(match role {
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                _,
            ) => 'E',
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(
                _,
            ) => 'i',
        });
    }
    if is_master_link {
        flags.push('M');
    }
    if is_replica {
        flags.push('S');
    }
    if conn.import_source {
        flags.push('I');
    }
    if conn.readonly {
        flags.push('r');
    }
    if conn.blocked.is_some() {
        flags.push('b');
    }
    if conn.is_async_close_pending {
        flags.push('A');
    } else if matches!(
        conn.state,
        crate::framework::capabilities::connection::ConnectionState::Closing
            | crate::framework::capabilities::connection::ConnectionState::CloseAfterReply
    ) {
        flags.push('c');
    }
    if conn.no_touch {
        flags.push('N');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    flags
}

pub(crate) fn type_for_conn(
    sub_count: usize,
    psub_count: usize,
    ssub_count: usize,
    is_master_link: bool,
    has_repl_handshake: bool,
) -> &'static str {
    if sub_count > 0 || psub_count > 0 || ssub_count > 0 {
        "pubsub"
    } else if is_master_link {
        "master"
    } else if has_repl_handshake {
        "replica"
    } else {
        "normal"
    }
}

#[cfg(test)]
mod tests {
    use super::{flag_char_for_conn, type_for_conn};
    use crate::framework::capabilities::connection::Connection;

    #[test]
    fn shard_only_subscriber_is_a_pubsub_client() {
        assert_eq!(type_for_conn(0, 0, 1, false, false), "pubsub");
        assert_eq!(type_for_conn(0, 0, 0, false, false), "normal");
    }

    #[test]
    fn monitor_client_uses_o_flag_but_remains_normal_type() {
        let mut connection = Connection::new_fake();
        connection.monitoring = true;

        assert_eq!(flag_char_for_conn(&connection, false, false), "O");
        assert_eq!(type_for_conn(0, 0, 0, false, false), "normal");
    }
}
