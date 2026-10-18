use crate::foundation::io::store::kv_engine::DbSet;

use super::util::emit_line;

pub(super) fn gen_replication_section(
    rcfg: &crate::framework::context::RuntimeConfig,
    dbs: &DbSet,
    replication: &crate::engine::distributed::replication::ReplicationState,
    now_ms: i64,
) -> String {
    let mut s = String::new();
    let tracked_expires: usize = (0..dbs.num_dbs())
        .filter_map(|idx| dbs.get(idx as u32))
        .map(|db| db.expires_count())
        .sum();
    s.push_str("# Replication\r\n");
    if replication.is_replica() {
        emit_line(&mut s, "role", "slave");
        emit_line(
            &mut s,
            "master_host",
            rcfg.replication.master_host.as_deref().unwrap_or(""),
        );
        emit_line(&mut s, "master_port", rcfg.replication.master_port);
        emit_line(
            &mut s,
            "master_link_status",
            if rcfg.replication.master_link_status == "up" {
                "up"
            } else {
                "down"
            },
        );
        emit_line(
            &mut s,
            "master_last_io_seconds_ago",
            replication.primary_data_age_ms(now_ms) / 1_000,
        );
        let syncing = if rcfg.replication.master_link_status == "sync" {
            1
        } else {
            0
        };
        emit_line(&mut s, "master_sync_in_progress", syncing);
        emit_line(
            &mut s,
            "slave_repl_offset",
            replication.slave_repl_offset.get(),
        );
        emit_line(
            &mut s,
            "slave_read_repl_offset",
            replication.slave_read_repl_offset.get(),
        );
        emit_line(
            &mut s,
            "slave_priority",
            rcfg.replication.replica_priority,
        );
        emit_line(&mut s, "slave_read_only", 1);
        emit_line(
            &mut s,
            "replica_announced",
            if rcfg.replication.replica_announced { 1 } else { 0 },
        );
        if rcfg.replication.master_link_status == "down" {
            emit_line(
                &mut s,
                "master_link_down_since_seconds",
                replication.primary_data_age_ms(now_ms) / 1_000,
            );
        }
    } else {
        emit_line(&mut s, "role", "master");
    }
    let replica_info = &replication.view.replica_info;
    emit_line(
        &mut s,
        "connected_slaves",
        replication.view.connected_replica_count,
    );
    let mut slave_index = 0;
    for key in &replication.view.info_order {
        if let Some(info) = replica_info.get(key) {
            s.push_str(&format!(
                "slave{}:ip={},port={},state={},offset={},lag={},type=replica\r\n",
                slave_index,
                info.ip,
                info.port,
                info.state,
                info.offset.map_or(-1, |offset| offset.get()),
                info.lag_ms
            ));
            slave_index += 1;
        } else if let Some(channel) = replication.view.channel_info.get(key) {
            let (offset, lag_ms) = replica_info
                .get(key)
                .map(|info| (info.offset.map_or(-1, |offset| offset.get()), info.lag_ms))
                .unwrap_or((0, 0));
            s.push_str(&format!(
                "slave{}:ip={},port={},state={},offset={},lag={},type={}\r\n",
                slave_index,
                channel.ip,
                channel.port,
                channel.state,
                offset,
                lag_ms,
                channel.channel_type
            ));
            slave_index += 1;
        }
    }
    emit_line(&mut s, "master_replid", replication.backlog.replid());
    let replid2_display = if replication.backlog.second_replid().is_empty() {
        "0000000000000000000000000000000000000000".to_string()
    } else {
        replication.backlog.second_replid().to_string()
    };
    emit_line(&mut s, "master_replid2", replid2_display);
    let master_repl_offset = if replication.is_replica() {
        replication.slave_repl_offset.get()
    } else {
        replication.backlog.reported_offset().get()
    };
    emit_line(&mut s, "master_repl_offset", master_repl_offset);
    emit_line(
        &mut s,
        "second_repl_offset",
        replication
            .backlog
            .second_replid_request_boundary()
            .map_or(-1, |request| request.get()),
    );
    emit_line(
        &mut s,
        "repl_backlog_active",
        if replication.backlog.is_active() { 1 } else { 0 },
    );
    emit_line(&mut s, "repl_backlog_size", rcfg.server.repl_backlog_size);
    emit_line(
        &mut s,
        "repl_backlog_first_byte_offset",
        replication.backlog.first_byte_reported_offset().get(),
    );
    emit_line(
        &mut s,
        "repl_backlog_histlen",
        replication.backlog.history_len(),
    );
    emit_line(
        &mut s,
        "replicas_repl_buffer_size",
        replication.replica_output_buffer_bytes,
    );
    emit_line(
        &mut s,
        "replicas_repl_buffer_peak",
        replication.replica_output_buffer_peak,
    );
    emit_line(&mut s, "master_failover_state", &replication.failover_state);
    emit_line(&mut s, "slave_expires_tracked_keys", tracked_expires);
    emit_line(
        &mut s,
        "replicas_waiting_psync",
        replication.view.replicas_waiting_psync,
    );
    s
}

pub(super) fn gen_keyspace_section(dbs: &DbSet) -> String {
    let mut s = String::new();
    s.push_str("# Keyspace\r\n");
    for i in 0..dbs.num_dbs() {
        let keys = dbs.db_size(i as u32);
        let exp = dbs.get(i as u32).map(|d| d.expires_count()).unwrap_or(0);
        let keysvitems = dbs.keys_with_volatile_items_count(i as u32);
        if keys > 0 || exp > 0 || keysvitems > 0 {
            s.push_str(&format!(
                "db{}:keys={},expires={},avg_ttl=0,keys_with_volatile_items={}\r\n",
                i, keys, exp, keysvitems
            ));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::gen_replication_section;
    use crate::engine::distributed::replication::{
        ReplicationOffset, ReplicationRole, ReplicationState,
    };
    use crate::engine::extension::config::RuntimeConfig;
    use crate::foundation::io::store::kv_engine::DbSet;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn replica_info_exposes_applied_offset_as_master_repl_offset() {
        let mut config = RuntimeConfig::new();
        config.replication.master_host = Some("127.0.0.1".to_string());
        config.replication.master_port = 6379;
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.role = ReplicationRole::Replica;
        replication.slave_repl_offset = offset(123);
        replication.slave_read_repl_offset = offset(120);

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("slave_repl_offset:123\r\n"), "{info}");
        assert!(info.contains("slave_read_repl_offset:120\r\n"), "{info}");
        assert!(info.contains("master_repl_offset:123\r\n"), "{info}");
    }

    #[test]
    fn replica_info_exposes_configured_priority() {
        let mut config = RuntimeConfig::new();
        config.replication.replica_priority = 42;
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.role = ReplicationRole::Replica;

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("slave_priority:42\r\n"), "{info}");
    }

    #[test]
    fn primary_info_reports_fresh_backlog_as_inactive_with_zero_first_byte() {
        let config = RuntimeConfig::new();
        let replication = ReplicationState::new("replid".to_string(), 1024);

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("repl_backlog_active:0\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:0\r\n"),
            "{info}"
        );
        assert!(info.contains("repl_backlog_histlen:0\r\n"), "{info}");
    }

    #[test]
    fn primary_info_reports_cleared_history_as_inactive_with_zero_first_byte() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.backlog.append(b"abc").unwrap();
        replication.backlog.clear_history();

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("master_repl_offset:3\r\n"), "{info}");
        assert!(info.contains("repl_backlog_active:0\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:0\r\n"),
            "{info}"
        );
        assert!(info.contains("repl_backlog_histlen:0\r\n"), "{info}");
    }

    #[test]
    fn primary_info_reports_active_empty_backlog_at_next_inclusive_byte() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.backlog.activate();

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("repl_backlog_active:1\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:1\r\n"),
            "{info}"
        );
        assert!(info.contains("repl_backlog_histlen:0\r\n"), "{info}");
    }

    #[test]
    fn primary_info_projects_normal_history_first_byte_into_reported_domain() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.backlog.append(b"abc").unwrap();

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("repl_backlog_active:1\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:1\r\n"),
            "{info}"
        );
        assert!(info.contains("repl_backlog_histlen:3\r\n"), "{info}");
    }

    #[test]
    fn primary_info_reports_second_replid_wire_request_boundary() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("old-primary".to_string(), 1024);
        replication.backlog.append(b"abcdef").unwrap();
        replication.backlog.shift_replication_id();

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("master_repl_offset:6\r\n"), "{info}");
        assert!(info.contains("second_repl_offset:7\r\n"), "{info}");
    }

    #[test]
    fn primary_info_reports_restored_empty_backlog_at_next_inclusive_byte() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("local".to_string(), 1024);
        let boundary = crate::engine::distributed::replication::backlog::ReplicationBacklog::rdb_snapshot_boundary(offset(41))
            .unwrap();
        replication
            .backlog
            .restore_primary_from_rdb_boundary("persisted", boundary);

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("master_repl_offset:41\r\n"), "{info}");
        assert!(info.contains("repl_backlog_active:1\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:42\r\n"),
            "{info}"
        );
        assert!(info.contains("repl_backlog_histlen:0\r\n"), "{info}");
    }

    #[test]
    fn primary_info_projects_trimmed_history_from_reported_offset_and_length() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("replid".to_string(), 4);
        replication.backlog.append(b"abcdef").unwrap();

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("master_repl_offset:6\r\n"), "{info}");
        assert!(info.contains("repl_backlog_active:1\r\n"), "{info}");
        assert!(info.contains("repl_backlog_histlen:4\r\n"), "{info}");
        assert!(
            info.contains("repl_backlog_first_byte_offset:3\r\n"),
            "{info}"
        );
    }

    #[test]
    fn replica_info_exposes_primary_data_age() {
        let config = RuntimeConfig::new();
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.role = ReplicationRole::Replica;
        replication.observe_primary_disconnected(1_000);

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 3_500);

        assert!(
            info.contains("master_last_io_seconds_ago:2\r\n"),
            "{info}"
        );
        assert!(
            info.contains("master_link_down_since_seconds:2\r\n"),
            "{info}"
        );
    }

    #[test]
    fn replica_transfer_reports_link_down_and_sync_in_progress_before_loading() {
        let mut config = RuntimeConfig::new();
        config.replication.master_link_status = "sync".to_string();
        let mut replication = ReplicationState::new("replid".to_string(), 1024);
        replication.role = ReplicationRole::Replica;

        let info = gen_replication_section(&config, &DbSet::new(1), &replication, 0);

        assert!(info.contains("master_link_status:down\r\n"), "{info}");
        assert!(info.contains("master_sync_in_progress:1\r\n"), "{info}");
    }
}
