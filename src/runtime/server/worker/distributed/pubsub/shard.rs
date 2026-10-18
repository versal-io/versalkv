use super::super::super::ServerWorkerState;
use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::distributed::pubsub;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};

impl ServerWorkerState {
    pub(super) fn reject_sharded_pubsub_route(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        if !self.ctx.config().server.cluster_enabled {
            return false;
        }
        let Some(spec) = self.registry.lookup(&args.name) else {
            return false;
        };
        let meta = crate::runtime::server::worker::flows::request::dispatch::CmdPreconditions {
            name: spec.name,
            arity: spec.arity,
            flags: spec.flags,
            first_key: spec.first_key,
            last_key: spec.last_key,
            key_step: spec.key_step,
            key_flags: spec.key_flags,
            acl_categories: spec.acl_categories,
            get_keys: spec.get_keys,
        };
        let facts =
            crate::runtime::server::worker::flows::request::dispatch::CommandClientFacts::capture(
                &self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)],
            );
        let Some(outcome) = self.stage_cluster_redirect(
            &facts,
            &meta,
            args,
            crate::runtime::server::worker::flows::request::dispatch::CmdCallFlags::default(),
        ) else {
            return false;
        };
        let Some(reply) = outcome.reject_reply else {
            return false;
        };
        replies.push(if resp3 {
            serialize_to_vec_resp3(&reply)
        } else {
            serialize_to_vec_resp2(&reply)
        });
        true
    }

    pub(super) fn handle_ssubscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        if args.args.is_empty() {
            self.ctx.end_cmd(name_lower, true);
            let error = CommandError::wrong_arity("ssubscribe");
            replies.push(if resp3 {
                serialize_to_vec_resp3(&error)
            } else {
                serialize_to_vec_resp2(&error)
            });
            return Some(true);
        }
        let mut confirmation_bytes = 0usize;
        {
            let conn = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            for channel in &args.args {
                let count = self
                    .ctx
                    .pubsub_mut()
                    .ssubscribe(conn.token().0, channel.clone());
                conn.ssub_channel_count = count;
                let confirmation =
                    pubsub::encode_confirm(b"ssubscribe", Some(channel), count as i64, resp3);
                confirmation_bytes = confirmation_bytes.saturating_add(confirmation.len());
                conn.queue_command_push_reply(confirmation);
            }
        }
        let duration_us = self.ctx.end_cmd(name_lower, false);
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        self.record_command_slot_metrics(
            args,
            &[],
            crate::runtime::server::worker::distributed::migration::accounting::SlotMetricSample {
                cpu_usec: duration_us,
                network_bytes_in: conn.request_bytes_current_command() as u64,
                network_bytes_out: confirmation_bytes as u64,
            },
        );
        Some(false)
    }

    pub(super) fn handle_sunsubscribe(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        _replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        let conn = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        if args.args.is_empty() {
            let removed = self.ctx.pubsub_mut().sunsubscribe_all(conn.token().0);
            conn.ssub_channel_count = 0;
            if removed.is_empty() {
                conn.queue_command_push_reply(pubsub::encode_confirm(
                    b"sunsubscribe",
                    None,
                    0,
                    resp3,
                ));
            } else {
                for channel in removed {
                    conn.queue_command_push_reply(pubsub::encode_confirm(
                        b"sunsubscribe",
                        Some(&channel),
                        0,
                        resp3,
                    ));
                }
            }
        } else {
            for channel in &args.args {
                let count = self.ctx.pubsub_mut().sunsubscribe(conn.token().0, channel);
                conn.ssub_channel_count = count;
                conn.queue_command_push_reply(pubsub::encode_confirm(
                    b"sunsubscribe",
                    Some(channel),
                    count as i64,
                    resp3,
                ));
            }
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }
}

impl ServerWorkerState {
    pub(crate) fn prune_unserved_shard_subscriptions(&mut self, changed_slots: &[u16]) {
        if changed_slots.is_empty() {
            return;
        }
        let Some(cluster) = self.ctx.cluster_state() else {
            return;
        };
        let mut slots: Vec<usize> = changed_slots
            .iter()
            .map(|slot| *slot as usize)
            .filter(|slot| !cluster.serves_shard_slot(*slot))
            .collect();
        slots.sort_unstable();
        slots.dedup();
        let slots = slots
            .into_iter()
            .map(|slot| slot as u16)
            .collect::<Vec<_>>();
        for (token_value, channel, remaining) in self.ctx.pubsub_mut().sunsubscribe_slots(&slots) {
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(token_value))
                .then_some(token_value)
            else {
                continue;
            };
            if !self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_token))
            {
                continue;
            }
            let (token, frame) = {
                let conn = &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)];
                conn.ssub_channel_count = remaining;
                let frame = pubsub::encode_confirm(
                    b"sunsubscribe",
                    Some(&channel),
                    remaining as i64,
                    conn.resp_version >= 3,
                );
                (conn.token(), frame)
            };
            self.queue_client_reply(token, frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::state::CLUSTER_OK;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::flows::request::dispatch::{
        CmdPreconditions, DispatchAction,
    };
    use bytes::Bytes;

    const REMOTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn add_conn(backend: &mut ServerWorkerState, token_value: usize) -> usize {
        let token = Token(token_value);
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(token);

        backend.connection_registry.insert(conn)
    }

    fn queued(conn: &Connection) -> Vec<u8> {
        conn.output
            .queue
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect()
    }

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    fn cluster_with_remote_owner(
        channel: &[u8],
    ) -> crate::engine::distributed::cluster::ClusterState {
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(channel) as usize;
        let mut cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16379);
        let mut remote = ClusterNode::new(
            REMOTE.to_string(),
            "192.0.2.1".to_string(),
            6380,
            16380,
            true,
        );
        remote.flags &= !CLUSTER_NODE_MYSELF;
        remote.slots[slot] = true;
        remote.numslots = 1;
        cluster.nodes.insert(REMOTE.to_string(), remote);
        cluster.state = CLUSTER_OK;
        cluster
    }

    #[test]
    fn ssubscribe_cross_slot_rejects_before_registering_channels() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        let connection_token = add_conn(&mut backend, 11);
        let token = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token()
        .0;
        let mut cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16379);
        cluster.state = CLUSTER_OK;
        *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
        let mut replies = Vec::new();
        let mut pending = Vec::new();

        let handled = backend.try_handle_pubsub_cmd(
            connection_token,
            &args(&[b"SSUBSCRIBE", b"{one}channel", b"{two}channel"]),
            "ssubscribe",
            false,
            &mut replies,
            &mut pending,
        );

        assert_eq!(handled, Some(true));
        assert!(String::from_utf8_lossy(&replies.concat()).contains("CROSSSLOT"));
        assert!(backend
            .ctx
            .pubsub_mut()
            .client_shard_channels(token)
            .is_empty());
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ssub_channel_count,
            0
        );
    }

    #[test]
    fn remote_ssubscribe_and_spublish_return_the_same_moved_owner() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        let connection_token = add_conn(&mut backend, 12);
        let token = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token()
        .0;
        *backend.ctx.cluster_state_mut() = Some(Box::new(cluster_with_remote_owner(b"channel")));
        let mut replies = Vec::new();
        let mut pending = Vec::new();

        let handled = backend.try_handle_pubsub_cmd(
            connection_token,
            &args(&[b"SSUBSCRIBE", b"channel"]),
            "ssubscribe",
            false,
            &mut replies,
            &mut pending,
        );
        assert_eq!(handled, Some(true));
        let subscribe_error = String::from_utf8_lossy(&replies.concat()).into_owned();
        assert!(subscribe_error.starts_with("-MOVED "));
        assert!(subscribe_error.ends_with(" 192.0.2.1:6380\r\n"));
        assert!(backend
            .ctx
            .pubsub_mut()
            .client_shard_channels(token)
            .is_empty());

        let publish_args = args(&[b"SPUBLISH", b"channel", b"payload"]);
        let spec = backend.registry.lookup(b"SPUBLISH").unwrap();
        let outcome = backend.pre_dispatch_exec(
            connection_token,
            CmdPreconditions {
                name: spec.name,
                arity: spec.arity,
                flags: spec.flags,
                first_key: spec.first_key,
                last_key: spec.last_key,
                key_step: spec.key_step,
                key_flags: spec.key_flags,
                acl_categories: spec.acl_categories,
                get_keys: spec.get_keys,
            },
            &publish_args,
        );
        assert_eq!(outcome.action, DispatchAction::Rejected);
        let publish_error = match outcome.reject_reply.unwrap() {
            crate::foundation::io::protocol::Value::Error(error) => {
                String::from_utf8_lossy(&error).into_owned()
            }
            other => panic!("expected MOVED error, got {other:?}"),
        };
        assert_eq!(
            publish_error,
            subscribe_error.trim_start_matches('-').trim_end()
        );
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .pending_shard_notifications()
        .is_empty());
    }

    #[test]
    fn ssubscribe_records_channel_slot_input_and_confirmation_output() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
        let connection_token = add_conn(&mut backend, 13);
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_request_bytes_for_test(34);
        let args = args(&[b"SSUBSCRIBE", b"channel"]);
        let mut replies = Vec::new();
        backend.ctx.begin_cmd();

        assert_eq!(
            backend.handle_ssubscribe(connection_token, &args, "ssubscribe", false, &mut replies),
            Some(false)
        );

        let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"channel") as usize;
        let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
        assert_eq!(counters.network_bytes_in, 34);
        assert_eq!(counters.network_bytes_out, 38);
    }

    #[test]
    fn queues_sunsubscribe_and_preserves_other_channels() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let connection_token = add_conn(&mut backend, 13);
        let token = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token()
        .0;
        let channel = Bytes::from_static(b"ch3");
        let other = Bytes::from_static(b"ch7");
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(&channel) as usize;
        let other_slot = crate::engine::distributed::cluster::node::key_hash_slot(&other) as usize;
        let mut cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16379);
        cluster.add_slot(slot).unwrap();
        cluster.add_slot(other_slot).unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
        backend.ctx.pubsub_mut().ssubscribe(token, channel.clone());
        backend.ctx.pubsub_mut().ssubscribe(token, other.clone());
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .ssub_channel_count = 2;

        backend
            .ctx
            .cluster_state_mut()
            .as_mut()
            .unwrap()
            .del_slot(slot)
            .unwrap();
        backend.prune_unserved_shard_subscriptions(&[slot as u16]);

        assert_eq!(
            queued(
                &backend.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
            ),
            pubsub::encode_confirm(b"sunsubscribe", Some(&channel), 1, false)
        );
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ssub_channel_count,
            1
        );
        assert_eq!(
            backend.ctx.pubsub_mut().client_shard_channels(token),
            vec![other]
        );
    }
}
