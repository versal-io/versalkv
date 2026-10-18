use super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::engine::command::CommandError;
use crate::engine::distributed::pubsub;
use crate::engine::distributed::sentinel::hello::{HelloMessage, SENTINEL_HELLO_CHANNEL};
use crate::engine::extension::acl::{add_acl_log_entry, ACL_DENIED_CHANNEL, ACL_LOG_CTX_TOPLEVEL};
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::distributed::sentinel::apply_hello_and_flush_config;
use bytes::Bytes;

#[path = "pubsub/cluster_bus.rs"]
mod cluster_bus;
mod shard;

impl ServerWorkerState {
    pub(crate) fn is_subscribe_mode_allowed(name_lower: &str) -> bool {
        matches!(
            name_lower,
            "subscribe"
                | "unsubscribe"
                | "psubscribe"
                | "punsubscribe"
                | "ssubscribe"
                | "sunsubscribe"
                | "ping"
                | "quit"
                | "reset"
        )
    }

    pub(crate) fn deliver_published(
        &mut self,
        pending: Vec<(Bytes, Bytes)>,
        pending_shard: Vec<(Bytes, Bytes)>,
    ) {
        self.deliver_published_locally(pending, pending_shard);
    }

    pub(super) fn deliver_published_locally(
        &mut self,
        pending: Vec<(Bytes, Bytes)>,
        pending_shard: Vec<(Bytes, Bytes)>,
    ) {
        for (channel, payload) in pending {
            for token_value in self.ctx.pubsub_mut().subscribers(&channel) {
                let connection_token = match self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token_value))
                    .then_some(token_value)
                {
                    Some(k) => k,
                    None => continue,
                };
                if !self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    continue;
                }
                let resp3 = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .resp_version
                    >= 3;
                let frame = pubsub::encode_message(&channel, &payload, resp3);
                let token = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token();
                self.queue_client_reply(token, frame);
            }
            for (token_value, pattern) in self.ctx.pubsub_mut().pattern_subscribers(&channel) {
                let connection_token = match self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token_value))
                    .then_some(token_value)
                {
                    Some(k) => k,
                    None => continue,
                };
                if !self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    continue;
                }
                let resp3 = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .resp_version
                    >= 3;
                let frame = pubsub::encode_pmessage(&pattern, &channel, &payload, resp3);
                let token = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token();
                self.queue_client_reply(token, frame);
            }
        }
        for (channel, payload) in pending_shard {
            for token_value in self.ctx.pubsub_mut().shard_subscribers(&channel) {
                let connection_token = match self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token_value))
                    .then_some(token_value)
                {
                    Some(k) => k,
                    None => continue,
                };
                if !self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    continue;
                }
                let resp3 = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .resp_version
                    >= 3;
                let frame = pubsub::encode_smessage(&channel, &payload, resp3);
                let frame_len = frame.len() as u64;
                let token = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token();
                if self.queue_client_reply(token, frame)
                    && self.ctx.config().server.cluster_enabled
                    && self.ctx.config().server.cluster_slot_stats_enabled
                {
                    let slot = crate::engine::distributed::cluster::node::key_hash_slot(&channel);
                    self.ctx.slot_metrics_mut().record(&[slot], 0, 0, frame_len);
                }
            }
        }
    }

    pub(crate) fn try_handle_pubsub_cmd(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
        pending_publish: &mut Vec<(Bytes, Bytes)>,
    ) -> Option<bool> {
        if !self.check_pubsub_acl(connection_token, args, name_lower, resp3, replies) {
            return Some(true);
        }
        if name_lower == "ssubscribe"
            && self.reject_sharded_pubsub_route(connection_token, args, resp3, replies)
        {
            self.ctx.end_cmd(name_lower, true);
            return Some(true);
        }
        match name_lower {
            "subscribe" => {
                self.handle_subscribe(connection_token, args, name_lower, resp3, replies)
            }
            "unsubscribe" => {
                self.handle_unsubscribe(connection_token, args, name_lower, resp3, replies)
            }
            "publish" => self.handle_publish(
                connection_token,
                args,
                name_lower,
                resp3,
                replies,
                pending_publish,
            ),
            "psubscribe" => {
                self.handle_psubscribe(connection_token, args, name_lower, resp3, replies)
            }
            "punsubscribe" => {
                self.handle_punsubscribe(connection_token, args, name_lower, resp3, replies)
            }
            "ssubscribe" => {
                self.handle_ssubscribe(connection_token, args, name_lower, resp3, replies)
            }
            "sunsubscribe" => {
                self.handle_sunsubscribe(connection_token, args, name_lower, resp3, replies)
            }
            "spublish" => None,
            _ => None,
        }
    }

    fn check_pubsub_acl(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let username = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .username
        .clone();
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        if let Some(user) = users.get(&username) {
            if !user.can_access_channel_for_command(name_lower, &args.args) {
                let channel_name = args
                    .args
                    .first()
                    .and_then(|c| std::str::from_utf8(c).ok())
                    .unwrap_or("<binary>");
                let client_info = format!(
                    "addr={} cmd={}",
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .peer_addr,
                    name_lower
                );
                add_acl_log_entry(
                    ACL_DENIED_CHANNEL,
                    ACL_LOG_CTX_TOPLEVEL,
                    channel_name.to_string(),
                    username.clone(),
                    client_info,
                );
                drop(users);
                self.ctx.end_cmd(name_lower, true);
                replies.push(if resp3 {
                    serialize_to_vec_resp3(&Value::error(format!(
                        "NOPERM User {} has no permissions to access a channel",
                        username
                    )))
                } else {
                    serialize_to_vec_resp2(&Value::error(format!(
                        "NOPERM User {} has no permissions to access a channel",
                        username
                    )))
                });
                return false;
            }
        }
        true
    }

    fn handle_subscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        if args.args.is_empty() {
            self.ctx.end_cmd(name_lower, true);
            replies.push(if resp3 {
                serialize_to_vec_resp3(&CommandError::wrong_arity("subscribe"))
            } else {
                serialize_to_vec_resp2(&CommandError::wrong_arity("subscribe"))
            });
            return Some(true);
        }
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            for ch in &args.args {
                let count = self.ctx.pubsub_mut().subscribe(conn.token().0, ch.clone());
                conn.sub_channel_count = count;
                let total = conn.sub_channel_count + conn.psub_channel_count;
                conn.queue_command_push_reply(pubsub::encode_confirm(
                    b"subscribe",
                    Some(ch),
                    total as i64,
                    resp3,
                ));
            }
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }

    fn handle_unsubscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        _replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            if args.args.is_empty() {
                let removed = self.ctx.pubsub_mut().unsubscribe_all(conn.token().0);
                conn.sub_channel_count = 0;
                let total = conn.psub_channel_count;
                if removed.is_empty() {
                    conn.queue_command_push_reply(pubsub::encode_confirm(
                        b"unsubscribe",
                        None,
                        total as i64,
                        resp3,
                    ));
                } else {
                    for ch in removed {
                        conn.queue_command_push_reply(pubsub::encode_confirm(
                            b"unsubscribe",
                            Some(&ch),
                            total as i64,
                            resp3,
                        ));
                    }
                }
            } else {
                for ch in &args.args {
                    let count = self.ctx.pubsub_mut().unsubscribe(conn.token().0, ch);
                    conn.sub_channel_count = count;
                    let total = conn.sub_channel_count + conn.psub_channel_count;
                    conn.queue_command_push_reply(pubsub::encode_confirm(
                        b"unsubscribe",
                        Some(ch),
                        total as i64,
                        resp3,
                    ));
                }
            }
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }

    fn handle_publish(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
        pending_publish: &mut Vec<(Bytes, Bytes)>,
    ) -> Option<bool> {
        if args.args.len() != 2 {
            self.ctx.end_cmd(name_lower, true);
            replies.push(if resp3 {
                serialize_to_vec_resp3(&CommandError::wrong_arity("publish"))
            } else {
                serialize_to_vec_resp2(&CommandError::wrong_arity("publish"))
            });
            return Some(true);
        }
        {
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            let now = mstime();
            let pause = self.ctx.client_pause().is_paused(now);
            let should_pause = match pause {
                crate::framework::context::PauseAction::None => false,
                crate::framework::context::PauseAction::Write => true,
                crate::framework::context::PauseAction::All => true,
            };
            if should_pause && !conn.transaction.is_active() {
                return None;
            }
        }
        let channel = args.args[0].clone();
        let payload = args.args[1].clone();
        if self.ctx.sentinel_state_mut().sentinel_mode
            && channel.as_ref() == SENTINEL_HELLO_CHANNEL.as_bytes()
        {
            if let Ok(message) = std::str::from_utf8(&payload) {
                if let Some(hello) = HelloMessage::parse(message) {
                    apply_hello_and_flush_config(&mut self.ctx, &hello, mstime());
                }
            }
        }
        let receivers = self.ctx.pubsub_mut().subscribers(&channel).len()
            + self.ctx.pubsub_mut().pattern_subscribers(&channel).len();
        pending_publish.push((channel.clone(), payload.clone()));
        if self.ctx.config().server.cluster_enabled {
            self.queue_cluster_publish(channel, payload, false);
        }
        self.ctx.end_cmd(name_lower, false);
        replies.push(if resp3 {
            serialize_to_vec_resp3(&Value::integer(receivers as i64))
        } else {
            serialize_to_vec_resp2(&Value::integer(receivers as i64))
        });
        Some(false)
    }

    fn handle_psubscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        if args.args.is_empty() {
            self.ctx.end_cmd(name_lower, true);
            replies.push(if resp3 {
                serialize_to_vec_resp3(&CommandError::wrong_arity("psubscribe"))
            } else {
                serialize_to_vec_resp2(&CommandError::wrong_arity("psubscribe"))
            });
            return Some(true);
        }
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            for pat in &args.args {
                let count = self
                    .ctx
                    .pubsub_mut()
                    .psubscribe(conn.token().0, pat.clone());
                conn.psub_channel_count = count;
                let total = conn.sub_channel_count + conn.psub_channel_count;
                conn.queue_command_push_reply(pubsub::encode_confirm(
                    b"psubscribe",
                    Some(pat),
                    total as i64,
                    resp3,
                ));
            }
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }

    fn handle_punsubscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        _replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            if args.args.is_empty() {
                let removed = self.ctx.pubsub_mut().punsubscribe_all(conn.token().0);
                conn.psub_channel_count = 0;
                let total = conn.sub_channel_count;
                if removed.is_empty() {
                    conn.queue_command_push_reply(pubsub::encode_confirm(
                        b"punsubscribe",
                        None,
                        total as i64,
                        resp3,
                    ));
                } else {
                    for pat in removed {
                        conn.queue_command_push_reply(pubsub::encode_confirm(
                            b"punsubscribe",
                            Some(&pat),
                            total as i64,
                            resp3,
                        ));
                    }
                }
            } else {
                for pat in &args.args {
                    let count = self.ctx.pubsub_mut().punsubscribe(conn.token().0, pat);
                    conn.psub_channel_count = count;
                    let total = conn.sub_channel_count + conn.psub_channel_count;
                    conn.queue_command_push_reply(pubsub::encode_confirm(
                        b"punsubscribe",
                        Some(pat),
                        total as i64,
                        resp3,
                    ));
                }
            }
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::CommandArgs;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(parts.iter().map(|p| Bytes::copy_from_slice(p)).collect())
    }

    fn add_conn(backend: &mut ServerWorkerState, token_value: usize, resp_version: u8) -> usize {
        let token = Token(token_value);
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(token);
        conn.resp_version = resp_version;

        backend.connection_registry.insert(conn)
    }

    fn queued(conn: &Connection) -> Vec<u8> {
        conn.output
            .queue
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect()
    }

    #[test]
    fn subscribe_mode_allows_only_pubsub_control_commands() {
        for name in [
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
            "ssubscribe",
            "sunsubscribe",
            "ping",
            "quit",
            "reset",
        ] {
            assert!(ServerWorkerState::is_subscribe_mode_allowed(name));
        }

        assert!(!ServerWorkerState::is_subscribe_mode_allowed("get"));
        assert!(!ServerWorkerState::is_subscribe_mode_allowed("publish"));
    }

    #[test]
    fn subscribe_and_unsubscribe_queue_confirmations_and_update_counts() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let connection_token = add_conn(&mut backend, 11, 2);
        let mut replies = Vec::new();
        let mut pending = Vec::new();

        let handled = backend.try_handle_pubsub_cmd(
            connection_token,
            &args(&[b"subscribe", b"alpha", b"beta"]),
            "subscribe",
            false,
            &mut replies,
            &mut pending,
        );

        assert_eq!(handled, Some(false));
        assert!(replies.is_empty());
        assert!(pending.is_empty());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .sub_channel_count,
            2
        );
        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            [
                pubsub::encode_confirm(b"subscribe", Some(b"alpha".as_slice()), 1, false),
                pubsub::encode_confirm(b"subscribe", Some(b"beta".as_slice()), 2, false),
            ]
            .concat()
        );

        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .clear_output_queue();
        let handled = backend.try_handle_pubsub_cmd(
            connection_token,
            &args(&[b"unsubscribe", b"alpha"]),
            "unsubscribe",
            false,
            &mut replies,
            &mut pending,
        );

        assert_eq!(handled, Some(false));
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .sub_channel_count,
            1
        );
        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            pubsub::encode_confirm(b"unsubscribe", Some(b"alpha".as_slice()), 1, false)
        );

        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .clear_output_queue();
        let handled = backend.try_handle_pubsub_cmd(
            connection_token,
            &args(&[b"unsubscribe"]),
            "unsubscribe",
            false,
            &mut replies,
            &mut pending,
        );

        assert_eq!(handled, Some(false));
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .sub_channel_count,
            0
        );
        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            pubsub::encode_confirm(b"unsubscribe", Some(b"beta".as_slice()), 0, false)
        );
    }

    #[test]
    fn pattern_and_shard_subscriptions_use_separate_indexes() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let connection_token = add_conn(&mut backend, 12, 3);
        let mut replies = Vec::new();
        let mut pending = Vec::new();

        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"psubscribe", b"news.*"]),
                "psubscribe",
                true,
                &mut replies,
                &mut pending,
            ),
            Some(false)
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .psub_channel_count,
            1
        );

        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"ssubscribe", b"shard"]),
                "ssubscribe",
                true,
                &mut replies,
                &mut pending,
            ),
            Some(false)
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ssub_channel_count,
            1
        );

        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .clear_output_queue();
        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"punsubscribe"]),
                "punsubscribe",
                true,
                &mut replies,
                &mut pending,
            ),
            Some(false)
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .psub_channel_count,
            0
        );
        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            pubsub::encode_confirm(b"punsubscribe", Some(b"news.*".as_slice()), 0, true)
        );

        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .clear_output_queue();
        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"sunsubscribe"]),
                "sunsubscribe",
                true,
                &mut replies,
                &mut pending,
            ),
            Some(false)
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ssub_channel_count,
            0
        );
        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            pubsub::encode_confirm(b"sunsubscribe", Some(b"shard".as_slice()), 0, true)
        );
    }

    #[test]
    fn publish_reports_receivers_and_delivery_fans_out_to_channel_and_pattern_subscribers() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let publisher = add_conn(&mut backend, 20, 2);
        let normal = add_conn(&mut backend, 21, 2);
        let pattern = add_conn(&mut backend, 22, 3);

        backend.ctx.pubsub_mut().subscribe(
            backend.connection_registry[crate::framework::capabilities::io::Token(normal)]
                .token()
                .0,
            Bytes::from_static(b"news.1"),
        );
        backend.ctx.pubsub_mut().psubscribe(
            backend.connection_registry[crate::framework::capabilities::io::Token(pattern)]
                .token()
                .0,
            Bytes::from_static(b"news.*"),
        );
        backend
            .ctx
            .pubsub_mut()
            .subscribe(999, Bytes::from_static(b"news.1"));

        let mut replies = Vec::new();
        let mut pending = Vec::new();
        let handled = backend.try_handle_pubsub_cmd(
            publisher,
            &args(&[b"publish", b"news.1", b"payload"]),
            "publish",
            false,
            &mut replies,
            &mut pending,
        );

        assert_eq!(handled, Some(false));
        assert_eq!(replies, vec![serialize_to_vec_resp2(&Value::integer(3))]);
        assert_eq!(
            pending,
            vec![(
                Bytes::from_static(b"news.1"),
                Bytes::from_static(b"payload")
            )]
        );

        backend.deliver_published(pending, Vec::new());

        assert_eq!(
            queued(&backend.connection_registry[crate::framework::capabilities::io::Token(normal)]),
            pubsub::encode_message(b"news.1".as_slice(), b"payload".as_slice(), false)
        );
        assert_eq!(
            queued(
                &backend.connection_registry[crate::framework::capabilities::io::Token(pattern)]
            ),
            pubsub::encode_pmessage(
                b"news.*".as_slice(),
                b"news.1".as_slice(),
                b"payload".as_slice(),
                true,
            )
        );
    }

    #[test]
    fn publish_enforces_subscriber_output_limit_at_delivery_time() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        backend
            .ctx
            .config_mut()
            .replication
            .pubsub_output_buffer_hard_limit = 10;
        let subscriber = add_conn(&mut backend, 23, 2);
        let token = backend.connection_registry
            [crate::framework::capabilities::io::Token(subscriber)]
        .token();
        backend.connection_registry[crate::framework::capabilities::io::Token(subscriber)]
            .sub_channel_count = 1;
        backend
            .ctx
            .pubsub_mut()
            .subscribe(token.0, Bytes::from_static(b"news"));

        backend.deliver_published(
            vec![(Bytes::from_static(b"news"), Bytes::from(vec![b'x'; 20_000]))],
            Vec::new(),
        );

        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            backend
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }

    #[test]
    fn shard_delivery_records_serialized_output_for_the_channel_slot() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
        let subscriber = add_conn(&mut backend, 31, 2);
        backend.ctx.pubsub_mut().ssubscribe(
            backend.connection_registry[crate::framework::capabilities::io::Token(subscriber)]
                .token()
                .0,
            Bytes::from_static(b"channel"),
        );

        backend.deliver_published_locally(
            Vec::new(),
            vec![(Bytes::from_static(b"channel"), Bytes::from_static(b"hello"))],
        );

        let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"channel") as usize;
        assert_eq!(
            backend
                .ctx
                .slot_metrics()
                .counters(slot)
                .unwrap()
                .network_bytes_out,
            42
        );
    }

    #[test]
    fn teardown_isolates_pubsub_subscriber_and_retries_l1_only() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = "127.0.0.1:0".parse().unwrap();
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(
            crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
                token,
                peer_addr,
            },
        );
        backend
            .ctx
            .pubsub_mut()
            .subscribe(token.0, Bytes::from_static(b"teardown-channel"));
        backend
            .transport
            .deregister_for_test(token)
            .expect("deregister client transport");
        backend.remove_connection(token.0);

        // Isolate-first teardown removes the subscriber from the pubsub index and
        // the connections slab at once; only the retry entry survives the failed
        // L1 deregister.
        assert!(backend.is_pending_transport_removal(token));
        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));

        backend.transport.clear_interest_requests_for_test();
        backend.deliver_published(
            vec![(
                Bytes::from_static(b"teardown-channel"),
                Bytes::from_static(b"payload"),
            )],
            Vec::new(),
        );

        // The isolated owner is no longer a subscriber, so publish never touches it.
        assert!(backend.transport.interest_requests_for_test().is_empty());

        backend
            .transport
            .reregister_for_test(token)
            .expect("reregister client transport");
        backend.run_background_tasks();

        assert!(!backend.is_pending_transport_removal(token));
        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
    }

    #[test]
    fn arity_errors_return_rejected_and_spublish_falls_through() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let connection_token = add_conn(&mut backend, 30, 3);
        let mut replies = Vec::new();
        let mut pending = Vec::new();

        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"subscribe"]),
                "subscribe",
                true,
                &mut replies,
                &mut pending,
            ),
            Some(true)
        );
        assert_eq!(replies.len(), 1);
        assert!(String::from_utf8_lossy(&replies[0]).contains("wrong number of arguments"));

        replies.clear();
        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"publish", b"only-channel"]),
                "publish",
                false,
                &mut replies,
                &mut pending,
            ),
            Some(true)
        );
        assert_eq!(replies.len(), 1);
        assert!(String::from_utf8_lossy(&replies[0]).contains("wrong number of arguments"));

        assert_eq!(
            backend.try_handle_pubsub_cmd(
                connection_token,
                &args(&[b"spublish", b"ch", b"msg"]),
                "spublish",
                false,
                &mut replies,
                &mut pending,
            ),
            None
        );
    }
}
