use crate::engine::command::context::CommandConn;
use crate::framework::context::ServerContext;

pub const NOTIFY_GENERIC: char = 'g';
pub const NOTIFY_STRING: char = '$';
pub const NOTIFY_LIST: char = 'l';
pub const NOTIFY_SET: char = 's';
pub const NOTIFY_HASH: char = 'h';
pub const NOTIFY_ZSET: char = 'z';
pub const NOTIFY_STREAM: char = 't';
pub const NOTIFY_PATH_HASH: char = 'p';
pub const NOTIFY_EXPIRED: char = 'x';
pub const NOTIFY_EVICTED: char = 'e';
pub const NOTIFY_NEW: char = 'n';
pub const NOTIFY_KEY_MISS: char = 'm';

pub fn notify_keyspace_event(
    conn: &mut dyn CommandConn,
    ctx: &ServerContext,
    event_type: char,
    event: &str,
    db_idx: u32,
    key: &[u8],
) {
    let event_char = match event_type {
        NOTIFY_GENERIC => 'g',
        NOTIFY_STRING => '$',
        NOTIFY_LIST => 'l',
        NOTIFY_SET => 's',
        NOTIFY_HASH => 'h',
        NOTIFY_ZSET => 'z',
        NOTIFY_STREAM => 't',
        NOTIFY_PATH_HASH => 'p',
        NOTIFY_EXPIRED => 'x',
        NOTIFY_EVICTED => 'e',
        NOTIFY_NEW => 'n',
        NOTIFY_KEY_MISS => 'm',
        _ => return,
    };
    let mut notifications = Vec::new();
    crate::engine::distributed::pubsub::PubSubState::publish_keyspace_event(
        ctx.config(),
        &mut notifications,
        db_idx,
        key,
        event_char,
        event,
    );
    conn.extend_notifications(notifications);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    fn conn() -> Connection {
        Connection::new(Token(1), ([127, 0, 0, 1], 6379).into(), false)
    }

    #[test]
    fn notification_is_skipped_when_disabled_or_type_not_enabled() {
        let mut conn = conn();
        let mut ctx = ServerContext::new(1);

        notify_keyspace_event(&mut conn, &ctx, NOTIFY_STRING, "set", 0, b"k");
        assert!(conn.pending_notifications().is_empty());

        ctx.config_mut().server.notify_keyspace_events = "Kz".to_string();
        notify_keyspace_event(&mut conn, &ctx, NOTIFY_STRING, "set", 0, b"k");
        assert!(conn.pending_notifications().is_empty());

        notify_keyspace_event(&mut conn, &ctx, '?', "unknown", 0, b"k");
        assert!(conn.pending_notifications().is_empty());
    }

    #[test]
    fn keyspace_and_keyevent_channels_are_queued_when_enabled() {
        let mut conn = conn();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.notify_keyspace_events = "KEx".to_string();

        notify_keyspace_event(&mut conn, &ctx, NOTIFY_EXPIRED, "expired", 2, b"key");

        assert_eq!(
            conn.pending_notifications(),
            vec![
                (
                    Bytes::from("__keyspace@2__:key".as_bytes().to_vec()),
                    Bytes::from("expired".as_bytes().to_vec())
                ),
                (
                    Bytes::from("__keyevent@2__:expired".as_bytes().to_vec()),
                    Bytes::from_static(b"key")
                ),
            ]
        );
    }

    #[test]
    fn all_flag_enables_event_types_and_preserves_utf8_lossy_key_names() {
        let mut conn = conn();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.notify_keyspace_events = "KA".to_string();

        notify_keyspace_event(&mut conn, &ctx, NOTIFY_SET, "sadd", 0, b"set-key");
        notify_keyspace_event(&mut conn, &ctx, NOTIFY_HASH, "hset", 1, b"h\xff");

        assert_eq!(conn.pending_notifications().len(), 2);
        assert_eq!(
            conn.pending_notifications()[0],
            (
                Bytes::from("__keyspace@0__:set-key".as_bytes().to_vec()),
                Bytes::from("sadd".as_bytes().to_vec())
            )
        );
        assert_eq!(
            conn.pending_notifications()[1].0,
            Bytes::from("__keyspace@1__:h\u{fffd}".as_bytes().to_vec())
        );
        assert_eq!(
            conn.pending_notifications()[1].1,
            Bytes::from("hset".as_bytes().to_vec())
        );
    }
}
