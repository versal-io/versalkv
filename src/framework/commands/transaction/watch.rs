use crate::engine::command::context::{CommandConn, WatchedKey};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::now_ms;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("WATCH")]
pub fn watch(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if conn.in_transaction() {
        conn.mark_transaction_queue_error();
        return Value::error("ERR Command 'watch' not allowed inside a transaction");
    }
    let db_id = conn.selected_db();
    let now = now_ms();
    let watched = args
        .args
        .iter()
        .map(|key| match ctx.db_mut(db_id) {
            Some(db) => {
                let expired = db.is_expired_at(key, now);
                WatchedKey::with_epoch(db_id, key.clone(), expired, db.watch_key(key))
            }
            None => WatchedKey::new(db_id, key.clone(), false),
        })
        .collect();
    conn.watch_keys(watched);
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::connection::{Connection, TransactionEnd};
    use bytes::Bytes;

    fn watch_x(conn: &mut Connection, ctx: &mut ServerContext) {
        let args =
            CommandArgs::from_parts(vec![Bytes::from_static(b"WATCH"), Bytes::from_static(b"x")]);
        assert_eq!(watch(conn, &args, ctx), Value::ok());
    }

    fn expired_replica_key(ctx: &mut ServerContext) {
        let db = ctx.db_mut(0).unwrap();
        db.set_slice(b"x", Robj::string("v1"));
        db.set_expire(b"x", 0);
        db.set_no_lazy_expire(true);
        assert!(!db.exists(b"x"));
        assert!(db.get_allow_expired(b"x").is_some());
    }

    #[test]
    fn replica_overwrite_invalidates_every_expired_watch_without_runtime_broadcast() {
        let mut ctx = ServerContext::new(1);
        expired_replica_key(&mut ctx);
        let mut first = Connection::new_fake();
        let mut second = Connection::new_fake();
        watch_x(&mut first, &mut ctx);
        watch_x(&mut second, &mut ctx);
        assert!(first.watched_keys()[0].expired);
        assert!(second.watched_keys()[0].expired);
        assert!(first.begin_transaction());
        assert!(second.begin_transaction());

        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v2"));

        for conn in [&mut first, &mut second] {
            assert!(conn.transaction.is_watch_dirty());
            conn.transaction.end(TransactionEnd::WatchInvalidated);
            assert!(!conn.transaction.is_watch_dirty());
            assert!(conn.watched_keys().is_empty());
        }
        assert_eq!(
            ctx.db(0).unwrap().get_no_expire(b"x").unwrap().as_bytes(),
            b"v2"
        );
    }

    #[test]
    fn stale_deletion_keeps_every_watch_clean_until_recreation() {
        let mut ctx = ServerContext::new(1);
        expired_replica_key(&mut ctx);
        let mut first = Connection::new_fake();
        let mut second = Connection::new_fake();
        watch_x(&mut first, &mut ctx);
        watch_x(&mut second, &mut ctx);

        assert!(ctx.db_mut(0).unwrap().del(b"x"));

        for conn in [&mut first, &mut second] {
            assert!(!conn.transaction.is_watch_dirty());
            assert!(!conn.transaction.touch_watched_key(0, b"x", false));
            assert!(!conn.watched_keys()[0].expired);
            assert!(!conn.transaction.is_watch_dirty());
        }
        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v2"));
        assert!(first.transaction.is_watch_dirty());
        assert!(second.transaction.is_watch_dirty());
    }

    #[test]
    fn deletion_of_a_live_watch_is_not_ignored() {
        let mut ctx = ServerContext::new(1);
        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v1"));
        let mut conn = Connection::new_fake();
        watch_x(&mut conn, &mut ctx);
        assert!(!conn.watched_keys()[0].expired);

        assert!(ctx.db_mut(0).unwrap().del(b"x"));

        assert!(conn.transaction.is_watch_dirty());
    }

    #[test]
    fn watch_epochs_are_isolated_by_key_database_and_unwatch() {
        let mut ctx = ServerContext::new(2);
        expired_replica_key(&mut ctx);
        let mut first = Connection::new_fake();
        let mut other_db = Connection::new_fake();
        other_db.set_selected_db(1);
        watch_x(&mut first, &mut ctx);
        watch_x(&mut other_db, &mut ctx);

        ctx.db_mut(0)
            .unwrap()
            .set_slice(b"unrelated", Robj::string("v2"));
        assert!(!first.transaction.is_watch_dirty());
        first.clear_watched_keys();
        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v2"));
        assert!(!first.transaction.is_watch_dirty());
        assert!(!other_db.transaction.is_watch_dirty());
        watch_x(&mut first, &mut ctx);
        assert!(!first.transaction.is_watch_dirty());
    }

    #[test]
    fn repeated_watch_does_not_rebase_an_invalidated_epoch() {
        let mut ctx = ServerContext::new(1);
        expired_replica_key(&mut ctx);
        let mut conn = Connection::new_fake();
        watch_x(&mut conn, &mut ctx);
        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v2"));

        watch_x(&mut conn, &mut ctx);

        assert_eq!(conn.watched_keys().len(), 1);
        assert!(conn.transaction.is_watch_dirty());
    }

    #[test]
    fn swap_preserves_watch_epoch_database_ownership() {
        for (first_db, second_db) in [(0, 1), (1, 0)] {
            let mut ctx = ServerContext::new(2);
            let mut first = Connection::new_fake();
            let mut second = Connection::new_fake();
            first.set_selected_db(first_db);
            second.set_selected_db(second_db);
            watch_x(&mut first, &mut ctx);
            watch_x(&mut second, &mut ctx);
            ctx.dbs_mut().swap(first_db, second_db);

            ctx.db_mut(first_db)
                .unwrap()
                .set_slice(b"x", Robj::string("v2"));

            assert!(first.transaction.is_watch_dirty());
            assert!(!second.transaction.is_watch_dirty());
        }
    }

    #[test]
    fn flush_does_not_invalidate_stale_or_missing_watches() {
        let mut ctx = ServerContext::new(1);
        expired_replica_key(&mut ctx);
        let mut stale = Connection::new_fake();
        watch_x(&mut stale, &mut ctx);
        let mut missing = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"WATCH"),
            Bytes::from_static(b"missing"),
        ]);
        assert_eq!(watch(&mut missing, &args, &mut ctx), Value::ok());

        ctx.db_mut(0).unwrap().flush();

        assert!(!stale.transaction.is_watch_dirty());
        assert!(!missing.transaction.is_watch_dirty());
        ctx.db_mut(0).unwrap().set_slice(b"x", Robj::string("v2"));
        assert!(stale.transaction.is_watch_dirty());
        assert!(!missing.transaction.is_watch_dirty());
    }

    #[test]
    fn keep_ttl_overwrite_invalidates_every_stale_watcher() {
        let mut ctx = ServerContext::new(1);
        expired_replica_key(&mut ctx);
        let mut first = Connection::new_fake();
        let mut second = Connection::new_fake();
        watch_x(&mut first, &mut ctx);
        watch_x(&mut second, &mut ctx);

        ctx.db_mut(0)
            .unwrap()
            .set_keep_ttl_slice(b"x", Robj::string("v2"));

        assert_eq!(ctx.db(0).unwrap().get_expire(b"x"), Some(0));
        assert!(first.transaction.is_watch_dirty());
        assert!(second.transaction.is_watch_dirty());
    }

    #[test]
    fn ttl_removal_and_extension_invalidate_stale_watches() {
        for should_remove_ttl in [false, true] {
            let mut ctx = ServerContext::new(1);
            expired_replica_key(&mut ctx);
            let mut conn = Connection::new_fake();
            watch_x(&mut conn, &mut ctx);
            let db = ctx.db_mut(0).unwrap();
            if should_remove_ttl {
                db.remove_expire(b"x");
            } else {
                db.set_expire(b"x", u64::MAX);
            }

            assert!(conn.transaction.is_watch_dirty());
        }
    }
}
