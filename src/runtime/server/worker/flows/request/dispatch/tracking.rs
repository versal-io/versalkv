use super::super::super::ServerWorkerState;
use super::side_effects::CommandEffects;
use crate::engine::command::context::CommandConn;
use crate::engine::extension::script::{PropagationOp, PropagationTargets};
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::context::TrackingState;
use crate::runtime::server::worker::distributed::propagation::sequence_propagation;
use crate::runtime::server::worker::sessions::ConnectionRegistry;
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrackingOwner(Token);

impl TrackingOwner {
    fn new(value: usize) -> Self {
        Self(Token(value))
    }

    fn from_token(token: Token) -> Self {
        Self(token)
    }

    fn token(self) -> Token {
        self.0
    }
}

struct TrackingDelivery {
    owner: TrackingOwner,
    keys: Vec<Bytes>,
}

impl TrackingDelivery {
    fn new(owner: TrackingOwner, keys: Vec<Bytes>) -> Self {
        Self { owner, keys }
    }
}

enum TrackingTarget {
    Deliver {
        token: Token,
        resp3: bool,
        pubsub_message: bool,
    },
    BrokenRedirect {
        owner: Token,
        redirect_client_id: usize,
        resp3: bool,
    },
    MissingOwner,
}

fn encode_tracking_invalidation(resp3: bool, pubsub_message: bool, keys: Vec<Bytes>) -> Vec<u8> {
    let key_values = Value::Array(keys.into_iter().map(Value::bulk).collect());
    if pubsub_message {
        return serialize_to_vec_resp2(&Value::Array(vec![
            Value::bulk(Bytes::from_static(b"message")),
            Value::bulk(Bytes::from_static(b"__redis__:invalidate")),
            key_values,
        ]));
    }
    let message = Value::push(vec![
        Value::bulk(Bytes::from_static(b"invalidate")),
        key_values,
    ]);
    if resp3 {
        serialize_to_vec_resp3(&message)
    } else {
        serialize_to_vec_resp2(&message)
    }
}

fn encode_tracking_broken_redirect(resp3: bool, client_id: usize) -> Vec<u8> {
    let value = if resp3 {
        Value::push(vec![
            Value::bulk(Bytes::from_static(b"tracking-redir-broken")),
            Value::integer(client_id as i64),
        ])
    } else {
        Value::Array(vec![
            Value::bulk(Bytes::from_static(b"tracking-redir-broken")),
            Value::integer(client_id as i64),
        ])
    };
    if resp3 {
        serialize_to_vec_resp3(&value)
    } else {
        serialize_to_vec_resp2(&value)
    }
}

fn resolve_tracking_target(
    owner: TrackingOwner,
    connections: &mut ConnectionRegistry,
) -> TrackingTarget {
    let owner_token = owner.token();
    let Some(owner_connection) = connections.get(owner_token) else {
        return TrackingTarget::MissingOwner;
    };
    let Some(redirect_client_id) = owner_connection.tracking_redirect() else {
        return direct_tracking_target(owner_token, owner_connection);
    };
    let Some(target) = connections.token_for_client_id(redirect_client_id as u64) else {
        return broken_tracking_redirect(owner_token, redirect_client_id, connections);
    };
    let Some(target_connection) = connections.get(target) else {
        return broken_tracking_redirect(owner_token, redirect_client_id, connections);
    };
    let pubsub_message = target_connection.resp_version < 3
        && (target_connection.sub_channel_count > 0
            || target_connection.psub_channel_count > 0
            || target_connection.ssub_channel_count > 0);
    if target_connection.resp_version < 3 && !pubsub_message {
        return TrackingTarget::MissingOwner;
    }
    TrackingTarget::Deliver {
        token: target,
        resp3: target_connection.resp_version >= 3,
        pubsub_message,
    }
}

fn direct_tracking_target(owner: Token, connection: &Connection) -> TrackingTarget {
    if connection.resp_version < 3 {
        TrackingTarget::MissingOwner
    } else {
        TrackingTarget::Deliver {
            token: owner,
            resp3: true,
            pubsub_message: false,
        }
    }
}

fn broken_tracking_redirect(
    owner: Token,
    redirect_client_id: usize,
    connections: &mut ConnectionRegistry,
) -> TrackingTarget {
    let Some(owner_connection) = connections.get_mut(owner) else {
        return TrackingTarget::MissingOwner;
    };
    owner_connection.set_tracking_broken_redirect(true);
    TrackingTarget::BrokenRedirect {
        owner,
        redirect_client_id,
        resp3: owner_connection.resp_version >= 3,
    }
}

fn collect_tracking_deliveries(
    tracking: &mut TrackingState,
    connections: &ConnectionRegistry,
    dirty_keys: &[(u32, Bytes)],
    source: Option<Token>,
) -> Vec<TrackingDelivery> {
    if dirty_keys.iter().any(|(_, key)| key.is_empty()) {
        return tracking
            .invalidate_all_keys()
            .into_iter()
            .map(|owner| TrackingDelivery::new(TrackingOwner::new(owner), Vec::new()))
            .collect();
    }

    let dirty_counts = dirty_key_counts(dirty_keys);
    let mut deliveries =
        direct_tracking_invalidations(tracking, connections, dirty_keys, &dirty_counts, source);
    deliveries.extend(broadcast_tracking_invalidations(
        connections,
        dirty_keys,
        &dirty_counts,
        source,
    ));
    deliveries
}

fn dirty_key_counts(dirty_keys: &[(u32, Bytes)]) -> HashMap<Bytes, usize> {
    let mut counts = HashMap::new();
    for (_, key) in dirty_keys {
        *counts.entry(key.clone()).or_insert(0) += 1;
    }
    counts
}

fn direct_tracking_invalidations(
    tracking: &mut TrackingState,
    connections: &ConnectionRegistry,
    dirty_keys: &[(u32, Bytes)],
    dirty_counts: &HashMap<Bytes, usize>,
    source: Option<Token>,
) -> Vec<TrackingDelivery> {
    let mut deliveries = Vec::new();
    for (_, key) in dirty_keys {
        let server_side_dirty = dirty_counts.get(key).copied().unwrap_or(0) > 1;
        for owner in tracking.take_clients_for_key(key) {
            let owner = TrackingOwner::new(owner);
            let Some(connection) = connections.get(owner.token()) else {
                continue;
            };
            if !connection.tracking() || connection.tracking_bcast() {
                continue;
            }
            if connection.tracking_noloop() && source == Some(owner.token()) && !server_side_dirty {
                continue;
            }
            deliveries.push(TrackingDelivery::new(owner, vec![key.clone()]));
        }
    }
    deliveries
}

fn broadcast_tracking_invalidations(
    connections: &ConnectionRegistry,
    dirty_keys: &[(u32, Bytes)],
    dirty_counts: &HashMap<Bytes, usize>,
    source: Option<Token>,
) -> Vec<TrackingDelivery> {
    let mut groups: HashMap<(Token, usize), Vec<Bytes>> = HashMap::new();
    for (_, key) in dirty_keys {
        let server_side_dirty = dirty_counts.get(key).copied().unwrap_or(0) > 1;
        for (_, connection) in connections.iter() {
            if !connection.tracking() || !connection.tracking_bcast() {
                continue;
            }
            if connection.tracking_noloop()
                && source == Some(connection.token())
                && !server_side_dirty
            {
                continue;
            }
            let Some(group) = broadcast_tracking_group(connection, key) else {
                continue;
            };
            let keys = groups.entry((connection.token(), group)).or_default();
            if !keys.iter().any(|existing| existing == key) {
                keys.push(key.clone());
            }
        }
    }
    groups
        .into_iter()
        .map(|((token, _), keys)| TrackingDelivery::new(TrackingOwner::from_token(token), keys))
        .collect()
}

fn broadcast_tracking_group(connection: &Connection, key: &Bytes) -> Option<usize> {
    if connection.tracking_prefixes().is_empty() {
        return Some(0);
    }
    connection
        .tracking_prefixes()
        .iter()
        .position(|prefix| key.as_ref().starts_with(prefix.as_ref()))
}

impl ServerWorkerState {
    fn queue_tracking_frame(
        &mut self,
        owner: TrackingOwner,
        keys: Vec<Bytes>,
        pending_tracking: &mut Vec<(usize, Vec<u8>)>,
    ) {
        match resolve_tracking_target(owner, &mut self.connection_registry) {
            TrackingTarget::Deliver {
                token,
                resp3,
                pubsub_message,
            } => pending_tracking.push((
                token.0,
                encode_tracking_invalidation(resp3, pubsub_message, keys),
            )),
            TrackingTarget::BrokenRedirect {
                owner,
                redirect_client_id,
                resp3,
            } => pending_tracking.push((
                owner.0,
                encode_tracking_broken_redirect(resp3, redirect_client_id),
            )),
            TrackingTarget::MissingOwner => {}
        }
    }

    pub(crate) fn collect_tracking_invalidations(
        &mut self,
        dirty_keys: &[(u32, Bytes)],
        source: Option<Token>,
        pending_tracking: &mut Vec<(usize, Vec<u8>)>,
    ) {
        let deliveries = collect_tracking_deliveries(
            self.ctx.tracking_mut(),
            &self.connection_registry,
            dirty_keys,
            source,
        );
        for delivery in deliveries {
            self.queue_tracking_frame(delivery.owner, delivery.keys, pending_tracking);
        }
    }

    pub(super) fn apply_command_evictions(
        &mut self,
        current_token: usize,
        evicted: Vec<(u32, Bytes)>,
        fx: &mut CommandEffects,
    ) {
        let mut queued_remote_invalidation = false;
        let mut pending_publish = Vec::new();
        for (db_idx, key) in evicted {
            self._queue_eviction_unlink(db_idx, &key, fx);
            crate::runtime::server::worker::flows::maintenance::enqueue_evicted_key_notifications(
                self.ctx.config(),
                &mut pending_publish,
                db_idx,
                &key,
            );
            let mut frames = Vec::new();
            self.collect_tracking_invalidations(&[(db_idx, key)], None, &mut frames);
            for (target, frame) in frames {
                if target == current_token {
                    fx.replies.push(frame);
                } else {
                    queued_remote_invalidation |= self.queue_client_reply(
                        crate::framework::capabilities::io::Token(target),
                        frame,
                    );
                }
            }
        }
        if !pending_publish.is_empty() {
            self.deliver_published(pending_publish, Vec::new());
        }
        if queued_remote_invalidation {
            self.drain_current_ordinary_writes();
        }
    }

    pub(super) fn _queue_eviction_unlink(
        &mut self,
        db_idx: u32,
        key: &[u8],
        fx: &mut CommandEffects,
    ) {
        let propagate_aof = self.ctx.config().persistence.aof_enabled;
        let propagate_replication =
            !self.ctx.replication().is_replica() && self.ctx.replication().backlog.is_active();
        if !propagate_aof && !propagate_replication {
            return;
        }

        if propagate_aof && self.ctx.persistence_mut().take_rewrite_force_select() {
            self.ctx.persistence_mut().reset_aof_selected_db();
        }
        let frame =
            crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", key]);
        let targets = match (propagate_replication, propagate_aof) {
            (true, true) => PropagationTargets::Both,
            (true, false) => PropagationTargets::Replication,
            (false, true) => PropagationTargets::Aof,
            (false, false) => return,
        };
        let op = PropagationOp::new(db_idx, targets, frame);
        let sequenced = sequence_propagation(
            &[op],
            self.ctx.repl_seldb(),
            self.ctx.persistence().aof_selected_db(),
        );
        self.ctx.set_repl_seldb(sequenced.replication_selected_db);
        self.ctx
            .persistence_mut()
            .set_aof_selected_db(sequenced.aof_selected_db);
        fx.replication_only.extend(sequenced.replication_frames);
        fx.aof_only.extend(sequenced.aof_frames);
    }

    pub(crate) fn evict_tracking_table_if_needed(
        &mut self,
        pending_tracking: &mut Vec<(usize, Vec<u8>)>,
    ) {
        for (key, owners) in self.ctx.tracking_mut().evict_over_limit() {
            for owner in owners {
                self.queue_tracking_frame(
                    TrackingOwner::new(owner),
                    vec![key.clone()],
                    pending_tracking,
                );
            }
        }
    }

    pub(crate) fn resume_tracking_eviction(&mut self) {
        if self.ctx.tracking().total_keys() <= self.ctx.tracking().max_keys {
            return;
        }
        let mut pending_tracking = Vec::new();
        self.evict_tracking_table_if_needed(&mut pending_tracking);
        for (target_id, frame) in pending_tracking {
            self.queue_client_reply(Token(target_id), frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        collect_tracking_deliveries, encode_tracking_broken_redirect, encode_tracking_invalidation,
        resolve_tracking_target, TrackingOwner, TrackingTarget,
    };
    use crate::engine::command::context::CommandConn;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::context::TrackingState;
    use crate::runtime::server::worker::sessions::ConnectionRegistry;
    use bytes::Bytes;

    #[test]
    fn invalidation_encoding_is_selected_from_delivery_shape() {
        assert_eq!(
            encode_tracking_invalidation(true, false, vec![Bytes::from_static(b"key")]),
            b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nkey\r\n"
        );
        assert_eq!(
            encode_tracking_invalidation(false, true, vec![Bytes::from_static(b"key")]),
            b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\nkey\r\n"
        );
    }

    #[test]
    fn broken_redirect_uses_the_recipient_protocol() {
        assert!(encode_tracking_broken_redirect(true, 42).starts_with(b">2\r\n"));
        assert!(encode_tracking_broken_redirect(false, 42).starts_with(b"*2\r\n"));
    }

    #[test]
    fn missing_redirect_marks_the_owner_and_returns_one_typed_failure() {
        let mut connections = ConnectionRegistry::default();
        let mut owner = Connection::new_fake();
        owner.set_token_for_test(Token(7));
        owner.resp_version = 3;
        owner.set_tracking_redirect(Some(99));
        connections.register_for_test(owner);

        let target = resolve_tracking_target(TrackingOwner::from_token(Token(7)), &mut connections);

        assert!(matches!(
            target,
            TrackingTarget::BrokenRedirect {
                owner: Token(7),
                redirect_client_id: 99,
                resp3: true,
            }
        ));
        assert!(connections[Token(7)].tracking_broken_redirect());
    }

    #[test]
    fn no_loop_suppresses_only_the_single_client_write() {
        let key = Bytes::from_static(b"key");
        let mut tracking = TrackingState::new();
        tracking.remember_key(7, &key);
        let mut connections = ConnectionRegistry::default();
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        connection.set_tracking(true);
        connection.set_tracking_noloop(true);
        connections.register_for_test(connection);

        assert!(collect_tracking_deliveries(
            &mut tracking,
            &connections,
            &[(0, key.clone())],
            Some(Token(7)),
        )
        .is_empty());

        tracking.remember_key(7, &key);
        assert_eq!(
            collect_tracking_deliveries(
                &mut tracking,
                &connections,
                &[(0, key.clone()), (0, key)],
                Some(Token(7)),
            )
            .len(),
            1,
            "a repeated server-side dirty key must still invalidate NOLOOP clients"
        );
    }
}
