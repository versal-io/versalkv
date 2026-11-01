use crate::framework::context::ServerContext;

pub(crate) fn apply_cluster_config_snapshot(ctx: &mut ServerContext) -> Result<(), String> {
    let config = ctx.config().server.clone();
    if !matches!(
        config.cluster_config_save_behavior.as_str(),
        "sync" | "best-effort"
    ) {
        return Err("cluster-config-save-behavior must be sync or best-effort".to_string());
    }
    if !(1..=100).contains(&config.cluster_message_gossip_perc) {
        return Err("cluster-message-gossip-perc must be between 1 and 100".to_string());
    }
    let Some(state) = ctx.cluster_state_mut() else {
        return Ok(());
    };
    let mut endpoints = state.myself.endpoints.clone();

    endpoints
        .set_ip(config.cluster_announce_ip.as_bytes())
        .map_err(|error| error.to_string())?;
    endpoints
        .set_hostname(config.cluster_announce_hostname.as_bytes())
        .map_err(|error| error.to_string())?;
    endpoints
        .set_human_name(config.cluster_announce_human_nodename.as_bytes())
        .map_err(|error| error.to_string())?;
    endpoints
        .set_client_ipv4(config.cluster_announce_client_ipv4.as_bytes())
        .map_err(|error| error.to_string())?;
    endpoints
        .set_client_ipv6(config.cluster_announce_client_ipv6.as_bytes())
        .map_err(|error| error.to_string())?;
    endpoints
        .set_availability_zone(config.availability_zone.as_bytes())
        .map_err(|error| error.to_string())?;

    let client_port = if config.cluster_announce_port == 0 {
        config.listening_port
    } else {
        config.cluster_announce_port
    };
    let bus_port = if config.cluster_announce_bus_port == 0 {
        config.cluster_port
    } else {
        config.cluster_announce_bus_port
    };
    endpoints.set_port(client_port);
    endpoints.set_tls_port(config.cluster_announce_tls_port);
    endpoints.set_bus_port(bus_port);
    endpoints.set_client_port(config.cluster_announce_client_port);
    endpoints.set_client_tls_port(config.cluster_announce_client_tls_port);

    state.apply_local_cluster_config(
        endpoints,
        config.cluster_replica_no_failover
            == crate::engine::extension::config::server::ClusterReplicaNoFailover::Yes,
    );
    state.myself.cluster_replica_priority = config.cluster_replica_priority;
    state.set_require_full_coverage(config.cluster_require_full_coverage);
    state.set_gossip_percentage(config.cluster_message_gossip_perc as usize);
    state.bump_topology_generation();
    state.request_topology_broadcast();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{config_set, log_level_test_guard};
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    fn args(parts: &[&str]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        )
    }

    fn context() -> ServerContext {
        let mut context = ServerContext::new(1);
        context.config_mut().server.listening_port = 6379;
        context.config_mut().server.cluster_port = 16379;
        context.config_mut().server.bindaddr = "127.0.0.1".to_string();
        *context.cluster_state_mut() = Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));
        context
    }

    #[test]
    fn replica_no_failover_updates_the_local_cluster_flag() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;

        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);

        assert_eq!(
            config_set(
                &mut connection,
                &args(&["CONFIG", "SET", "cluster-replica-no-failover", "yes"]),
                &mut context,
            ),
            Value::ok()
        );
        assert!(context
            .cluster_state()
            .as_ref()
            .unwrap()
            .myself
            .has_flag(CLUSTER_NODE_NOFAILOVER));
        {
            let state = context.cluster_state_mut().as_deref_mut().unwrap();
            assert!(state.take_config_save_request());
            assert!(state.take_topology_broadcast_request());
        }

        assert_eq!(
            config_set(
                &mut connection,
                &args(&["CONFIG", "SET", "cluster-replica-no-failover", "no"]),
                &mut context,
            ),
            Value::ok()
        );
        let state = context.cluster_state_mut().as_deref_mut().unwrap();
        assert!(!state.myself.has_flag(CLUSTER_NODE_NOFAILOVER));
        assert!(state.take_config_save_request());
        assert!(state.take_topology_broadcast_request());
    }

    #[test]
    fn replica_no_failover_if_empty_and_alias_round_trip() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;
        use crate::engine::extension::config::spec_registry::ConfigRegistry;

        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);
        for name in ["cluster-replica-no-failover", "cluster-slave-no-failover"] {
            assert_eq!(
                config_set(
                    &mut connection,
                    &args(&["CONFIG", "SET", name, "if-empty"]),
                    &mut context
                ),
                Value::ok()
            );
            let registry = ConfigRegistry::new();
            assert_eq!(
                (registry.lookup(name).unwrap().get)(context.config()).as_str(),
                "if-empty"
            );
            assert!(!context
                .cluster_state()
                .as_ref()
                .unwrap()
                .myself
                .has_flag(CLUSTER_NODE_NOFAILOVER));
        }
    }

    #[test]
    fn replica_no_failover_case_insensitive_values_round_trip() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;
        use crate::engine::extension::config::server::ClusterReplicaNoFailover;
        use crate::engine::extension::config::spec_registry::ConfigRegistry;

        let registry = ConfigRegistry::new();
        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);
        for name in ["cluster-replica-no-failover", "cluster-slave-no-failover"] {
            for (text, expected) in [
                ("NO", ClusterReplicaNoFailover::No),
                ("YeS", ClusterReplicaNoFailover::Yes),
                ("IF-EMPTY", ClusterReplicaNoFailover::IfEmpty),
                ("If-EmPtY", ClusterReplicaNoFailover::IfEmpty),
            ] {
                assert_eq!(
                    config_set(
                        &mut connection,
                        &args(&["CONFIG", "SET", name, text]),
                        &mut context,
                    ),
                    Value::ok(),
                    "{name} {text}"
                );
                assert_eq!(
                    context.config().server.cluster_replica_no_failover,
                    expected
                );
                assert_eq!(
                    (registry.lookup(name).unwrap().get)(context.config()).as_str(),
                    expected.as_str()
                );
                assert_eq!(
                    context
                        .cluster_state()
                        .as_ref()
                        .unwrap()
                        .myself
                        .has_flag(CLUSTER_NODE_NOFAILOVER),
                    expected == ClusterReplicaNoFailover::Yes
                );
                let mut startup = crate::engine::extension::config::RuntimeConfig::new();
                registry
                    .try_set_startup(&mut startup, name, text)
                    .unwrap()
                    .unwrap();
                assert_eq!(startup.server.cluster_replica_no_failover, expected);
            }
        }
    }

    #[test]
    fn replica_no_failover_file_rewrite_preserves_each_enum_value() {
        use crate::engine::extension::config::server::ClusterReplicaNoFailover;
        use crate::engine::extension::config::spec_registry::ConfigRegistry;
        use crate::framework::commands::server::config::config_rewrite;
        use crate::framework::config::Config;
        use clap::Parser;

        let path = std::env::temp_dir().join(format!(
            "versal-cluster-no-failover-{}-{}.conf",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = ConfigRegistry::new();
        for (text, expected) in [
            ("no", ClusterReplicaNoFailover::No),
            ("yes", ClusterReplicaNoFailover::Yes),
            ("if-empty", ClusterReplicaNoFailover::IfEmpty),
            ("NO", ClusterReplicaNoFailover::No),
            ("YeS", ClusterReplicaNoFailover::Yes),
            ("IF-EMPTY", ClusterReplicaNoFailover::IfEmpty),
            ("If-EmPtY", ClusterReplicaNoFailover::IfEmpty),
        ] {
            std::fs::write(&path, format!("cluster-slave-no-failover {text}\n")).unwrap();
            let mut parsed = Config::parse_from(["versal-server"]);
            parsed.load_from_file(path.to_str().unwrap()).unwrap();
            let mut loaded_context = context();
            for (name, value) in &parsed.extra_cli_configs {
                registry
                    .try_set_startup(loaded_context.config_mut(), name, value)
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(
                loaded_context.config().server.cluster_replica_no_failover,
                expected
            );
            loaded_context.config_mut().server.config_file =
                Some(path.to_str().unwrap().to_string());
            assert_eq!(config_rewrite(&mut loaded_context), Value::ok());
            let rewritten = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                rewritten
                    .lines()
                    .filter(|line| line.starts_with("cluster-replica-no-failover "))
                    .collect::<Vec<_>>(),
                [format!("cluster-replica-no-failover {}", expected.as_str())]
            );
            let mut restored = Config::parse_from(["versal-server"]);
            restored.load_from_file(path.to_str().unwrap()).unwrap();
            let mut restored_context = context();
            for (name, value) in restored.extra_cli_configs {
                if matches!(
                    name.as_str(),
                    "cluster-replica-no-failover" | "cluster-slave-no-failover"
                ) {
                    registry
                        .try_set_startup(restored_context.config_mut(), &name, &value)
                        .unwrap()
                        .unwrap();
                }
            }
            assert_eq!(
                restored_context.config().server.cluster_replica_no_failover,
                expected
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn runtime_port_update_is_deferred_to_the_event_loop() {
        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);

        assert_eq!(
            config_set(
                &mut connection,
                &args(&["CONFIG", "SET", "port", "6380"]),
                &mut context,
            ),
            Value::Null
        );
        assert_eq!(context.config().server.listening_port, 6380);
        assert!(connection.has_custom_reply());
        let request = context.take_tcp_rebind_request().unwrap();
        assert_eq!(request.port, 6380);
        assert_eq!(request.previous_config.server.listening_port, 6379);
        #[cfg(target_os = "linux")]
        assert!(request.previous_oom_score_adj.is_some());
        #[cfg(not(target_os = "linux"))]
        assert_eq!(
            request.previous_oom_score_adj,
            super::super::read_oom_score_adj()
        );
        assert_eq!(
            context.cluster_state().as_ref().unwrap().myself.port(),
            6379
        );
    }

    #[test]
    fn runtime_io_thread_update_is_deferred_to_the_event_loop() {
        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);

        assert_eq!(
            config_set(
                &mut connection,
                &args(&["CONFIG", "SET", "io-threads", "5"]),
                &mut context,
            ),
            Value::ok()
        );

        let request = context
            .take_io_threads_reconfigure_request()
            .expect("CONFIG SET io-threads must stage a runtime apply");
        assert_eq!(request.previous.server.io_threads, 1);
        assert_eq!(request.requested, 5);
        assert_eq!(context.config().server.io_threads, 5);
    }

    #[test]
    fn multi_pair_cluster_side_effects_apply_after_every_pair_validates() {
        let mut context = context();
        let mut connection = Connection::new(Token(1), "127.0.0.1:40000".parse().unwrap(), false);

        let failed = config_set(
            &mut connection,
            &args(&[
                "CONFIG",
                "SET",
                "cluster-announce-hostname",
                "cache.example.com",
                "cluster-announce-client-ipv4",
                "banana",
            ]),
            &mut context,
        );
        assert!(matches!(failed, Value::Error(_)));
        assert_eq!(
            context
                .cluster_state()
                .as_ref()
                .unwrap()
                .topology_generation(),
            0
        );
        assert_eq!(
            context
                .cluster_state()
                .as_ref()
                .unwrap()
                .myself
                .endpoints
                .hostname(),
            None
        );
        assert!(context.config().server.cluster_announce_hostname.is_empty());

        let succeeded = config_set(
            &mut connection,
            &args(&[
                "CONFIG",
                "SET",
                "cluster-announce-ip",
                "198.51.100.10",
                "cluster-announce-hostname",
                "cache.example.com",
                "cluster-announce-client-ipv4",
                "192.0.2.10",
                "cluster-announce-client-ipv6",
                "2001:db8::10",
                "cluster-announce-human-nodename",
                "cache-east-1",
                "availability-zone",
                "zone a",
                "cluster-announce-port",
                "7000",
                "cluster-announce-tls-port",
                "7443",
                "cluster-announce-client-port",
                "7001",
                "cluster-announce-client-tls-port",
                "7444",
                "cluster-announce-bus-port",
                "17000",
            ]),
            &mut context,
        );
        assert_eq!(succeeded, Value::ok());
        let endpoints = &context.cluster_state().as_ref().unwrap().myself.endpoints;
        assert_eq!(endpoints.ip(), "198.51.100.10");
        assert_eq!(endpoints.hostname(), Some("cache.example.com"));
        assert_eq!(endpoints.client_ipv4(), Some("192.0.2.10"));
        assert_eq!(endpoints.client_ipv6(), Some("2001:db8::10"));
        assert_eq!(endpoints.human_name(), Some("cache-east-1"));
        assert_eq!(endpoints.availability_zone(), Some("zone a"));
        assert_eq!(endpoints.port(), 7000);
        assert_eq!(endpoints.tls_port(), 7443);
        assert_eq!(endpoints.client_port(), 7001);
        assert_eq!(endpoints.client_tls_port(), 7444);
        assert_eq!(endpoints.bus_port(), 17000);
        assert_eq!(
            context
                .cluster_state()
                .as_ref()
                .unwrap()
                .topology_generation(),
            1
        );

        assert_eq!(
            config_set(
                &mut connection,
                &args(&[
                    "CONFIG",
                    "SET",
                    "cluster-announce-ip",
                    "",
                    "cluster-announce-hostname",
                    "",
                    "cluster-announce-client-ipv4",
                    "",
                    "cluster-announce-client-ipv6",
                    "",
                    "cluster-announce-human-nodename",
                    "",
                    "availability-zone",
                    "",
                    "cluster-announce-port",
                    "0",
                    "cluster-announce-tls-port",
                    "0",
                    "cluster-announce-client-port",
                    "0",
                    "cluster-announce-client-tls-port",
                    "0",
                    "cluster-announce-bus-port",
                    "0",
                ]),
                &mut context,
            ),
            Value::ok()
        );
        let endpoints = &context.cluster_state().as_ref().unwrap().myself.endpoints;
        assert_eq!(endpoints.ip(), "127.0.0.1");
        assert_eq!(endpoints.hostname(), None);
        assert_eq!(endpoints.client_ipv4(), None);
        assert_eq!(endpoints.client_ipv6(), None);
        assert_eq!(endpoints.human_name(), None);
        assert_eq!(endpoints.availability_zone(), None);
        assert_eq!(endpoints.port(), 6379);
        assert_eq!(endpoints.tls_port(), 0);
        assert_eq!(endpoints.client_port(), 0);
        assert_eq!(endpoints.client_tls_port(), 0);
        assert_eq!(endpoints.bus_port(), 16379);
    }

    #[test]
    fn non_cluster_config_does_not_change_topology_generation() {
        let _log_level_guard = log_level_test_guard();
        let mut context = context();
        let mut connection = Connection::new(Token(2), "127.0.0.1:40001".parse().unwrap(), false);

        assert_eq!(
            config_set(
                &mut connection,
                &args(&["CONFIG", "SET", "loglevel", "debug"]),
                &mut context,
            ),
            Value::ok()
        );
        assert_eq!(
            context
                .cluster_state()
                .as_ref()
                .unwrap()
                .topology_generation(),
            0
        );
    }
}
