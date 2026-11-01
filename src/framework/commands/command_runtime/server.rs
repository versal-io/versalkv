use bytes::Bytes;

use super::shared::{RuntimeLimits, RuntimePolicies, RuntimeSession};
use super::{CommandRuntime, FlushMode};
use crate::engine::command::context::CommandConn;
use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::engine::extension::acl::{
    acl_users, add_acl_log_entry, ACL_DENIED_DB, ACL_LOG_CTX_TOPLEVEL,
};
use crate::foundation::io::store::kv_engine::{now_ms, Db};
use crate::framework::context::ServerContext;
use crate::framework::observability::keyspace::{
    self, NOTIFY_EXPIRED, NOTIFY_GENERIC, NOTIFY_HASH, NOTIFY_KEY_MISS, NOTIFY_LIST, NOTIFY_NEW,
    NOTIFY_PATH_HASH, NOTIFY_SET, NOTIFY_STREAM, NOTIFY_STRING, NOTIFY_ZSET,
};

pub(crate) struct ServerCommandRuntime<'a> {
    conn: &'a mut dyn CommandConn,
    ctx: &'a mut ServerContext,
}

impl<'a> ServerCommandRuntime<'a> {
    pub(crate) fn new(conn: &'a mut dyn CommandConn, ctx: &'a mut ServerContext) -> Self {
        Self { conn, ctx }
    }
}

impl CommandRuntime for ServerCommandRuntime<'_> {
    fn must_obey_command(&self) -> bool {
        self.conn.import_source() || self.ctx.is_loading()
    }

    fn command_catalog(&self) -> &dyn crate::framework::commands::introspection::CommandCatalog {
        self.ctx
    }

    fn selected_db(&self) -> u32 {
        self.conn.selected_db()
    }

    fn db_mut_at(&mut self, index: u32) -> Option<&mut Db> {
        self.ctx.db_mut(index)
    }

    fn num_dbs(&self) -> usize {
        self.ctx.num_dbs()
    }

    fn now_ms(&self) -> u64 {
        now_ms()
    }

    fn now_us(&self) -> u64 {
        crate::foundation::io::sys::time::ustime().max(0) as u64
    }

    fn ensure_db_access(&mut self, index: u32, command: &str) -> Result<(), Value> {
        if index as usize >= self.ctx.num_dbs() {
            return Err(crate::engine::command::CommandError::db_index_out_of_range());
        }
        let users = acl_users().lock().unwrap();
        if let Some(user) = users.get(self.conn.username()) {
            if !user.can_access_db(index as i64) {
                let client_info = format!("addr={} cmd={command}", self.conn.peer_addr());
                add_acl_log_entry(
                    ACL_DENIED_DB,
                    ACL_LOG_CTX_TOPLEVEL,
                    index.to_string(),
                    self.conn.username().to_string(),
                    client_info,
                );
                return Err(Value::error(format!(
                    "NOPERM User {} has no permissions to access a database",
                    self.conn.username()
                )));
            }
        }
        Ok(())
    }

    fn validate_sort_access(
        &self,
        command_name: &str,
        args: &crate::engine::command::CommandArgs,
    ) -> Result<bool, Value> {
        if self.ctx.config().server.cluster_enabled {
            let key_slot = crate::engine::distributed::cluster::node::key_hash_slot(&args.args[0]);
            let mut index = 1;
            while index < args.args.len() {
                let option = String::from_utf8_lossy(&args.args[index]).to_ascii_lowercase();
                match option.as_str() {
                    "by" if index + 1 < args.args.len() => {
                        let pattern = &args.args[index + 1];
                        if pattern.contains(&b'*')
                            && crate::engine::distributed::cluster::node::pattern_hash_slot(pattern)
                                != Some(key_slot)
                        {
                            return Err(Value::error(
                                "ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.",
                            ));
                        }
                        index += 1;
                    }
                    "get" if index + 1 < args.args.len() => {
                        let pattern = &args.args[index + 1];
                        if pattern.as_ref() != b"#"
                            && crate::engine::distributed::cluster::node::pattern_hash_slot(pattern)
                                != Some(key_slot)
                        {
                            return Err(Value::error(
                                "ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.",
                            ));
                        }
                        index += 1;
                    }
                    "limit" | "store" => index += 1,
                    _ => {}
                }
                index += 1;
            }
        }
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        let Some(user) = users.get(self.conn.username()) else {
            return Ok(true);
        };
        let acl_categories = crate::framework::generated::commands::top_level_meta(command_name)
            .map(|meta| meta.acl_categories)
            .unwrap_or(&[]);
        let command = crate::engine::extension::acl::AclCommand::new(
            command_name,
            &args.args,
            acl_categories,
        );
        let extracted = crate::framework::commands::server::command::extract_command_keys(
            command_name,
            &args.args,
            self.ctx,
        );
        let keys: Vec<_> = extracted
            .iter()
            .map(|(key, flags)| {
                let mut required = 0;
                if flags.contains(&"access") {
                    required |= crate::engine::extension::acl::ACL_READ_PERMISSION;
                }
                if flags
                    .iter()
                    .any(|flag| matches!(*flag, "insert" | "update" | "delete"))
                {
                    required |= crate::engine::extension::acl::ACL_WRITE_PERMISSION;
                }
                (*key, required)
            })
            .collect();
        Ok(user.has_cmd_with_unrestricted_key_access(
            command,
            i64::from(self.conn.selected_db()),
            crate::engine::extension::acl::ACL_READ_PERMISSION,
            &keys,
        ))
    }

    fn limits(&self) -> RuntimeLimits {
        RuntimeLimits {
            list_max_listpack_size: self.ctx.config().encoding.list_max_listpack_size,
            set_encoding: (
                self.ctx.config().encoding.set_max_intset_entries as usize,
                self.ctx.config().encoding.set_max_listpack_entries as usize,
                self.ctx.config().encoding.set_max_listpack_value as usize,
            ),
            hash_encoding: (
                self.ctx.config().encoding.hash_max_listpack_entries as usize,
                self.ctx.config().encoding.hash_max_listpack_value as usize,
            ),
            zset_encoding: (
                self.ctx.config().encoding.zset_max_ziplist_entries as usize,
                self.ctx.config().encoding.zset_max_ziplist_value as usize,
            ),
            stream_node: (
                self.ctx.config().encoding.stream_node_max_entries.max(0) as usize,
                self.ctx.config().encoding.stream_node_max_bytes.max(0) as usize,
            ),
            proto_max_bulk_len: self.ctx.config().server.proto_max_bulk_len.max(0) as usize,
            normal_output_buffer_hard_limit: self
                .ctx
                .config()
                .replication
                .normal_output_buffer_hard_limit,
        }
    }

    fn policies(&self) -> RuntimePolicies {
        let is_read_only_replica =
            self.ctx.replication().is_replica() && self.ctx.config().replication.replica_read_only;
        RuntimePolicies {
            read_expired_values: self.conn.import_source(),
            suppress_lazy_expire: self.ctx.config().server.import_mode || self.conn.import_source(),
            touch_read_keys: !self.conn.no_touch(),
            mark_replica_local_expire: self.ctx.replication().is_replica()
                && !self.ctx.config().replication.replica_read_only
                && !self.conn.import_source(),
            extended_redis_compatibility: self.ctx.config().server.extended_redis_compatibility,
            hll_sparse_max_bytes: self.ctx.config().server.hll_sparse_max_bytes,
            preserve_expired_keys: self.conn.import_source() || is_read_only_replica,
            rdb_compression: self.ctx.config().persistence.rdbcompression.uses_lzf(),
            skip_checksum_validation: self.ctx.config().server.skip_checksum_validation,
            rdb_version_check_relaxed: self.ctx.config().rdb_version_check_relaxed(),
            lazyfree_server_del: self.ctx.config().server.lazyfree_lazy_server_del,
            active_expire: self.ctx.config().server.active_expire_enabled && !is_read_only_replica,
            lazyfree_user_del: self.ctx.config().server.lazyfree_lazy_user_del,
        }
    }

    fn preserves_expired_deadline(&self) -> bool {
        self.ctx.is_loading() || self.ctx.replication().is_replica() || self.suppress_lazy_expire()
    }

    fn session(&self) -> RuntimeSession {
        RuntimeSession {
            resp3: self.conn.resp_version() >= 3,
            output_buffer_memory_usage: self.conn.output_buffer_memory_usage(),
        }
    }

    fn ping_response(&self, message: Option<&Bytes>) -> Value {
        if (self.conn.sub_channel_count() > 0
            || self.conn.psub_channel_count() > 0
            || self.conn.ssub_channel_count() > 0)
            && self.conn.resp_version() < 3
        {
            return Value::Array(vec![
                Value::bulk(Bytes::from_static(b"pong")),
                Value::bulk(message.cloned().unwrap_or_default()),
            ]);
        }
        match message {
            Some(message) => Value::bulk(message.clone()),
            None => Value::pong(),
        }
    }

    fn reset_replication_selected_db(&mut self) {
        self.ctx.reset_repl_seldb();
    }

    fn flushdb(&mut self, mode: FlushMode) -> CommandOutcome {
        let db_idx = self.conn.selected_db();
        let users = acl_users().lock().unwrap();
        if let Some(user) = users.get(self.conn.username()) {
            if !user.can_access_db(db_idx as i64) {
                let client_info = format!("addr={} cmd=flushdb", self.conn.peer_addr());
                add_acl_log_entry(
                    ACL_DENIED_DB,
                    ACL_LOG_CTX_TOPLEVEL,
                    db_idx.to_string(),
                    self.conn.username().to_string(),
                    client_info,
                );
                return CommandOutcome::reply(Value::error(format!(
                    "NOPERM User {} has no permissions to access a database",
                    self.conn.username()
                )));
            }
        }
        drop(users);
        fail_slot_migrations_for_flush(self.ctx);
        self.ctx.hotkeys().purge(|id, _| id == db_idx);
        if self.ctx.db(db_idx).is_some_and(|db| db.dbsize() > 0) {
            self.ctx.persistence().cancel_forkless_save();
        }
        let lazyfree = match mode {
            FlushMode::Default => self.ctx.config().server.lazyfree_lazy_user_flush,
            FlushMode::Sync => false,
            FlushMode::Async => true,
        };
        if let Some(db) = self.ctx.db(db_idx) {
            for key in db.keys() {
                if !db.is_expired(&key) {
                    self.conn.record_dirty_key((db_idx, Bytes::from(key)));
                }
            }
        }
        self.conn.record_dirty_key((db_idx, Bytes::new()));
        let deferred_objects = if let Some(db) = self.ctx.db_mut(db_idx) {
            if lazyfree {
                db.drain_values_for_lazyfree()
            } else {
                db.flush();
                Vec::new()
            }
        } else {
            Vec::new()
        };
        for object in deferred_objects {
            self.conn.defer_lazyfree_object(object);
        }
        CommandOutcome::reply(Value::ok())
    }

    fn flushall(&mut self, mode: FlushMode) -> CommandOutcome {
        let users = acl_users().lock().unwrap();
        if let Some(user) = users.get(self.conn.username()) {
            if !user.can_access_db(-1) {
                let client_info = format!("addr={} cmd=flushall", self.conn.peer_addr());
                add_acl_log_entry(
                    ACL_DENIED_DB,
                    ACL_LOG_CTX_TOPLEVEL,
                    "flushall".to_string(),
                    self.conn.username().to_string(),
                    client_info,
                );
                return CommandOutcome::reply(Value::error(format!(
                    "NOPERM User {} has no permissions to access a database",
                    self.conn.username()
                )));
            }
        }
        drop(users);
        fail_slot_migrations_for_flush(self.ctx);
        self.ctx.hotkeys().reset();
        self.ctx.persistence().cancel_forkless_save();
        let lazyfree = match mode {
            FlushMode::Default => self.ctx.config().server.lazyfree_lazy_user_flush,
            FlushMode::Sync => false,
            FlushMode::Async => true,
        };
        if let Some(pid) = self.ctx.persistence().rdb_child_pid() {
            crate::server_log!(notice, "Aborting background saving child {}", pid);
            let _ = crate::foundation::io::sys::process::send_signal(
                pid,
                crate::foundation::io::sys::process::ProcessSignal::User1,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(pid);
            let temp_path = std::path::Path::new(&self.ctx.config().persistence.dir)
                .join(format!("temp-{}.rdb", pid));
            let _ = std::fs::remove_file(temp_path);
            self.ctx.persistence_mut().abort_child();
        }
        let mut effects = CommandEffects::default();
        for db_idx in 0..self.ctx.num_dbs() {
            if let Some(db) = self.ctx.db(db_idx as u32) {
                for key in db.keys() {
                    if !db.is_expired(&key) {
                        effects.changed_keys.push((db_idx as u32, Bytes::from(key)));
                    }
                }
            }
        }
        effects.changed_keys.push((0, Bytes::new()));
        if lazyfree {
            effects.deferred_objects = self.ctx.dbs_mut().drain_all_values_for_lazyfree();
        } else {
            self.ctx.dbs_mut().flush_all();
        }
        if !self.ctx.config_mut().persistence.saveparams.is_empty() {
            self.ctx.persistence_mut().set_dirty(-1);
        }
        CommandOutcome {
            reply: Value::ok(),
            effects,
        }
    }

    fn select_db(&mut self, index: u32) -> CommandOutcome {
        if index as usize >= self.ctx.num_dbs() {
            return CommandOutcome::reply(
                crate::engine::command::CommandError::db_index_out_of_range(),
            );
        }
        let users = acl_users().lock().unwrap();
        if let Some(user) = users.get(self.conn.username()) {
            if !user.can_access_db(index as i64) {
                let client_info = format!("addr={} cmd=select", self.conn.peer_addr());
                add_acl_log_entry(
                    ACL_DENIED_DB,
                    ACL_LOG_CTX_TOPLEVEL,
                    index.to_string(),
                    self.conn.username().to_string(),
                    client_info,
                );
                return CommandOutcome::reply(Value::error(format!(
                    "NOPERM User {} has no permissions to access a database",
                    self.conn.username()
                )));
            }
        }
        drop(users);
        self.conn.set_selected_db(index);
        CommandOutcome::reply(Value::ok())
    }

    fn swapdb(&mut self, first: u32, second: u32) -> CommandOutcome {
        if self.ctx.config().server.cluster_enabled {
            return CommandOutcome::reply(Value::error(
                "ERR SWAPDB is not allowed in cluster mode",
            ));
        }
        if first as usize >= self.ctx.num_dbs() || second as usize >= self.ctx.num_dbs() {
            return CommandOutcome::reply(Value::error("ERR DB index is out of range"));
        }
        let users = acl_users().lock().unwrap();
        if let Some(user) = users.get(self.conn.username()) {
            for index in [first, second] {
                if !user.can_access_db(index as i64) {
                    let client_info = format!("addr={} cmd=swapdb", self.conn.peer_addr());
                    add_acl_log_entry(
                        ACL_DENIED_DB,
                        ACL_LOG_CTX_TOPLEVEL,
                        index.to_string(),
                        self.conn.username().to_string(),
                        client_info,
                    );
                    return CommandOutcome::reply(Value::error(format!(
                        "NOPERM User {} has no permissions to access a database",
                        self.conn.username()
                    )));
                }
            }
        }
        drop(users);
        if first == second {
            return CommandOutcome::reply(Value::ok());
        }
        self.ctx.swap_dbs(first, second);
        let dirty_keys = self
            .ctx
            .blocking_registry_mut()
            .iter_keys()
            .filter(|(db_id, _)| *db_id == first || *db_id == second)
            .map(|(db_id, key)| (*db_id, key.clone()))
            .collect();
        self.conn.replace_dirty_keys(dirty_keys);
        self.conn.set_swapdb_dbs(Some((first, second)));
        CommandOutcome::reply(Value::ok())
    }
}

fn fail_slot_migrations_for_flush(ctx: &mut ServerContext) {
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return;
    };
    let actions = state.fail_all_migrations(
        "Data was flushed",
        crate::foundation::io::sys::time::unixtime(),
    );
    state.queue_migration_actions(actions);
}

pub(crate) fn execute_server_command(
    conn: &mut dyn CommandConn,
    args: &crate::engine::command::CommandArgs,
    ctx: &mut ServerContext,
    handler: impl FnOnce(
        &mut dyn CommandRuntime,
        &crate::engine::command::CommandArgs,
    ) -> CommandOutcome,
) -> Value {
    let (db_idx, mut outcome) = prepare_server_command(conn, args, ctx, handler);
    apply_server_command_effects(conn, ctx, db_idx, &mut outcome.effects);
    outcome.reply
}

pub(crate) fn prepare_server_command(
    conn: &mut dyn CommandConn,
    args: &crate::engine::command::CommandArgs,
    ctx: &mut ServerContext,
    handler: impl FnOnce(
        &mut dyn CommandRuntime,
        &crate::engine::command::CommandArgs,
    ) -> CommandOutcome,
) -> (u32, CommandOutcome) {
    let db_idx = conn.selected_db();
    let outcome = {
        let _scope = ctx.hotkeys_command_scope(args);
        let mut runtime = ServerCommandRuntime::new(conn, ctx);
        handler(&mut runtime, args)
    };
    (db_idx, outcome)
}

pub(crate) fn apply_server_command_effects(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
    db_idx: u32,
    effects: &mut CommandEffects,
) {
    if effects.suppress_reply {
        conn.suppress_reply();
    }
    if effects.close_connection {
        conn.mark_closing();
    }
    let mut changed_keys_seen: std::collections::HashSet<_> =
        conn.dirty_keys().iter().cloned().collect();
    for changed_key in effects.changed_keys.drain(..) {
        if changed_keys_seen.insert(changed_key.clone()) {
            conn.record_dirty_key(changed_key);
        }
    }
    if effects.expired_hash_fields > 0 {
        ctx.incr_expired_fields(effects.expired_hash_fields);
    }
    if effects.skip_propagate {
        conn.set_skip_propagate(true);
    }
    if let Some(delta) = effects.dirty_delta.take() {
        conn.record_dirty_delta(delta);
    }
    for object in effects.deferred_objects.drain(..) {
        conn.defer_lazyfree_object(object);
    }
    for propagation in &effects.custom_propagations {
        let parts: Vec<&[u8]> = propagation.iter().map(Bytes::as_ref).collect();
        conn.record_custom_propagation(encode_command(&parts));
    }
    for event in &effects.events {
        apply_server_event(conn, ctx, db_idx, event);
    }
    for (event_db_idx, event) in &effects.db_events {
        apply_server_event(conn, ctx, *event_db_idx, event);
    }
}

fn apply_server_event(
    conn: &mut dyn CommandConn,
    ctx: &ServerContext,
    db_idx: u32,
    event: &crate::engine::core::CoreEvent,
) {
    let notification_type = match event.kind {
        "new" => NOTIFY_NEW,
        "keymiss" => NOTIFY_KEY_MISS,
        "set" | "setbit" | "incrby" | "incrbyfloat" => NOTIFY_STRING,
        "expired" => NOTIFY_EXPIRED,
        "phset" | "phmset" | "phdel" | "phdelprefix" => NOTIFY_PATH_HASH,
        "linsert" | "lmove_from" | "lmove_to" | "lpop" | "lpush" | "lrem" | "lset" | "ltrim"
        | "rpop" | "rpush" => NOTIFY_LIST,
        "sadd" | "sdiffstore" | "sinterstore" | "srem" | "sunionstore" => NOTIFY_SET,
        "hdel" | "hexpire" | "hexpired" | "hincrby" | "hincrbyfloat" | "hpersist" | "hset" => {
            NOTIFY_HASH
        }
        "zadd" | "zdiffstore" | "zincr" | "zinterstore" | "zpopmax" | "zpopmin" | "zrangestore"
        | "zrem" | "zunionstore" => NOTIFY_ZSET,
        "xack"
        | "xadd"
        | "xdel"
        | "xgroup"
        | "xgroup-create"
        | "xgroup-createconsumer"
        | "xgroup-delconsumer"
        | "xgroup-destroy"
        | "xgroup-setid"
        | "xsetid"
        | "xtrim" => NOTIFY_STREAM,
        "copy_to" | "del" | "expire" | "move_from" | "move_to" | "rename_from" | "rename_to"
        | "restore" | "sortstore" => NOTIFY_GENERIC,
        _ => return,
    };
    keyspace::notify_keyspace_event(conn, ctx, notification_type, event.kind, db_idx, &event.key);
}

#[cfg(test)]
mod tests {
    use super::{apply_server_command_effects, execute_server_command};
    use crate::engine::command::context::CommandConn;
    use crate::engine::core::{CommandEffects, CoreEvent};
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandArgs;
    use bytes::Bytes;
    use std::collections::VecDeque;

    fn large_list() -> Robj {
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(
            (0..65)
                .map(|_| Bytes::from_static(b"value"))
                .collect::<Vec<_>>(),
        )))
    }

    #[test]
    fn server_effects_move_generic_dirty_delta_once() {
        use crate::framework::capabilities::connection::CommandEffectDrain;
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        let mut effects = CommandEffects {
            dirty_delta: Some(7),
            ..CommandEffects::default()
        };
        apply_server_command_effects(&mut conn, &mut ctx, 0, &mut effects);
        assert_eq!(effects.dirty_delta, None);
        apply_server_command_effects(&mut conn, &mut ctx, 0, &mut effects);
        let mut drained = conn
            .take_command_effects(CommandEffectDrain::Completed)
            .unwrap();
        assert_eq!(drained.take_dirty_delta(), Some(7));
        assert_eq!(drained.take_dirty_delta(), None);
    }

    fn run_flushdb(conn: &mut Connection, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
        execute_server_command(
            conn,
            args,
            ctx,
            crate::framework::commands::server::flushdb::flushdb,
        )
    }

    #[test]
    fn replication_import_source_keeps_keyspace_notifications() {
        let mut conn = Connection::new_fake();
        conn.set_import_source(true);
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.notify_keyspace_events = "Eh".to_owned();
        let mut effects = CommandEffects {
            events: vec![CoreEvent {
                kind: "hset",
                key: Bytes::from_static(b"hash"),
            }],
            ..CommandEffects::default()
        };

        apply_server_command_effects(&mut conn, &mut ctx, 0, &mut effects);

        assert_eq!(
            conn.pending_notifications(),
            &[(
                Bytes::from_static(b"__keyevent@0__:hset"),
                Bytes::from_static(b"hash"),
            )]
        );
    }

    #[test]
    fn changed_keys_are_forwarded_to_the_connection_effects() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(2);
        let mut effects = CommandEffects {
            changed_keys: vec![(1, Bytes::from_static(b"destination"))],
            ..CommandEffects::default()
        };

        apply_server_command_effects(&mut conn, &mut ctx, 0, &mut effects);

        assert_eq!(
            conn.dirty_keys(),
            &[(1, Bytes::from_static(b"destination"))]
        );
        assert!(effects.changed_keys.is_empty());
    }

    #[test]
    fn event_categories_and_database_overrides_are_preserved() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(2);
        ctx.config_mut().server.notify_keyspace_events = "KEA".to_owned();
        let mut effects = CommandEffects {
            events: vec![
                CoreEvent {
                    kind: "hexpire",
                    key: Bytes::from_static(b"hash"),
                },
                CoreEvent {
                    kind: "zrem",
                    key: Bytes::from_static(b"zset"),
                },
            ],
            db_events: vec![(
                1,
                CoreEvent {
                    kind: "move_to",
                    key: Bytes::from_static(b"moved"),
                },
            )],
            ..CommandEffects::default()
        };

        apply_server_command_effects(&mut conn, &mut ctx, 0, &mut effects);

        assert!(conn.pending_notifications().iter().any(|(channel, value)| {
            channel.as_ref() == b"__keyspace@0__:hash" && value.as_ref() == b"hexpire"
        }));
        assert!(conn.pending_notifications().iter().any(|(channel, value)| {
            channel.as_ref() == b"__keyspace@0__:zset" && value.as_ref() == b"zrem"
        }));
        assert!(conn.pending_notifications().iter().any(|(channel, value)| {
            channel.as_ref() == b"__keyspace@1__:moved" && value.as_ref() == b"move_to"
        }));
    }

    #[test]
    fn expired_hsetnx_emits_one_expiry_event_before_hset() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.notify_keyspace_events = "KEA".to_owned();
        ctx.config_mut().server.active_expire_enabled = false;
        let db = ctx.db_mut(0).unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"old"));
        db.set_hash_field_expire(b"hash", b"field", 0);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HSETNX"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"new"),
        ]);

        let reply = execute_server_command(
            &mut conn,
            &args,
            &mut ctx,
            crate::framework::commands::hash::hsetnx::hsetnx,
        );

        assert_eq!(reply, Value::integer(1));
        let payloads = conn
            .pending_notifications()
            .iter()
            .map(|(_, payload)| payload.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            payloads,
            vec![b"hexpired".as_slice(), b"hash", b"hset", b"hash"]
        );
    }

    #[test]
    fn replication_hset_over_expired_field_emits_only_hset() {
        let mut conn = Connection::new_fake();
        conn.set_import_source(true);
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.notify_keyspace_events = "KEA".to_owned();
        let db = ctx.db_mut(0).unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"old"));
        db.set_hash_field_expire(b"hash", b"field", 0);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HSET"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"new"),
        ]);

        let reply = execute_server_command(
            &mut conn,
            &args,
            &mut ctx,
            crate::framework::commands::hash::hset::hset,
        );

        assert_eq!(reply, Value::integer(0));
        let payloads = conn
            .pending_notifications()
            .iter()
            .map(|(_, payload)| payload.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec![b"hset".as_slice(), b"hash"]);
    }

    #[test]
    fn flushdb_without_mode_defers_values_when_user_lazyfree_is_enabled() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        ctx.db_mut(0).unwrap().set(b"key".to_vec(), large_list());
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"FLUSHDB")]);

        assert_eq!(run_flushdb(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(ctx.db_size(0), 0);
        assert_eq!(conn.pending_lazyfree_objects().len(), 1);
    }

    #[test]
    fn flushdb_sync_overrides_user_lazyfree_setting() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        ctx.db_mut(0).unwrap().set(b"key".to_vec(), large_list());
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FLUSHDB"),
            Bytes::from_static(b"SYNC"),
        ]);

        assert_eq!(run_flushdb(&mut conn, &args, &mut ctx), Value::ok());
        assert!(conn.pending_lazyfree_objects().is_empty());
    }

    #[test]
    fn flushdb_async_overrides_disabled_user_lazyfree_setting() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        ctx.config_mut().server.lazyfree_lazy_user_flush = false;
        ctx.db_mut(0).unwrap().set(b"key".to_vec(), large_list());
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FLUSHDB"),
            Bytes::from_static(b"ASYNC"),
        ]);

        assert_eq!(run_flushdb(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(conn.pending_lazyfree_objects().len(), 1);
    }
}
