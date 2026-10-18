use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::store::kv_engine::now_ms;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::{CommandConn, WatchedKey};
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
        .map(|key| {
            let expired = ctx
                .db(db_id)
                .is_some_and(|db| db.is_expired_at(key, now));
            WatchedKey::new(db_id, key.clone(), expired)
        })
        .collect();
    conn.watch_keys(watched);
    Value::ok()
}
