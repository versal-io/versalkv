//! Keyspace notification helpers.
//! Aligned with Valkey `notifyKeyspaceEvent`: given a key-affecting event, build
//! the `__keyspace@<db>__:<key>` / `__keyevent@<db>__:<event>` channel-payload
//! pairs gated by `notify-keyspace-events` config flags. The actual delivery to
//! subscribed clients is performed by the event loop just like a regular PUBLISH.

use bytes::Bytes;

use crate::engine::extension::config::RuntimeConfig;

/// Build keyspace notification (channel, payload) pairs for a given event and
/// append them to `pending_publish`.
/// Event flag conventions (subset used in this codebase):
///   g  generic (DEL, EXPIRE, RENAME, ...)
///   $  string commands
///   h  hash commands
///   l  list commands
///   s  set commands
///   z  sorted set commands
///   x  expired events
///   e  evicted events
///   t  stream commands
///   d  module key-type events
///   K  keyspace events (channel __keyspace@<db>__:<key>)
///   E  keyevent events (channel __keyevent@<db>__:<event>)
///   A  alias for g$lshzxet
pub fn keyspace_notification_pairs(
    rcfg: &RuntimeConfig,
    pending_publish: &mut Vec<(Bytes, Bytes)>,
    db_idx: u32,
    key: &[u8],
    event_type: char,
    event_name: &str,
) {
    if !rcfg.is_any_keyspace_event_enabled() || !rcfg.is_event_type_enabled(event_type) {
        return;
    }
    let db_str = db_idx.to_string();
    if rcfg.is_event_type_enabled('K') {
        let key_str = String::from_utf8_lossy(key);
        let ch = format!("__keyspace@{}__:{}", db_str, key_str);
        pending_publish.push((
            Bytes::from(ch.into_bytes()),
            Bytes::copy_from_slice(event_name.as_bytes()),
        ));
    }
    if rcfg.is_event_type_enabled('E') {
        let ch = format!("__keyevent@{}__:{}", db_str, event_name);
        pending_publish.push((Bytes::from(ch.into_bytes()), Bytes::copy_from_slice(key)));
    }
}

impl super::PubSubState {
    /// Build keyspace/keyevent notification pairs for a key-affecting event and
    /// append them to `pending_publish` (delivered by the event loop like PUBLISH).
    /// - 'K' flag → `__keyspace@<db>__:<key>`  payload = `<event>`
    /// - 'E' flag → `__keyevent@<db>__:<event>`  payload = `<key>`
    ///
    /// `event_type` is the command-class flag char, e.g. '$' (string), 'g' (generic),
    /// 'x' (expired), 'e' (evicted); `event_name` is the event op (e.g. "set",
    /// "del", "expired", "evicted").
    pub fn publish_keyspace_event(
        rcfg: &RuntimeConfig,
        pending_publish: &mut Vec<(Bytes, Bytes)>,
        db_idx: u32,
        key: &[u8],
        event_type: char,
        event_name: &str,
    ) {
        keyspace_notification_pairs(rcfg, pending_publish, db_idx, key, event_type, event_name);
    }
}
