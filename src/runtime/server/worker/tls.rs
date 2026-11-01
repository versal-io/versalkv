use super::ServerWorkerState;
use crate::engine::extension::config::server::ServerConfig;
use crate::foundation::io::transport::{
    TlsClientAuth, TlsClientConfig, TlsConfig, TlsContextBatch, TlsContextPolicy,
    TlsMaterialMetadata,
};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::Instant;

#[derive(Default)]
pub(super) struct TlsReloadState {
    pub(super) generation: u64,
    pub(super) metadata: TlsMaterialMetadata,
    last_check: Option<Instant>,
    pending: Option<(u64, Receiver<io::Result<Option<TlsContextBatch>>>)>,
}

fn material_configs(
    config: &ServerConfig,
) -> io::Result<Option<(TlsConfig, Option<TlsClientConfig>)>> {
    if config.tls_port == 0 && !config.tls_replication && !config.tls_cluster {
        return Ok(None);
    }
    if config.tls_cert_file.is_empty() || config.tls_key_file.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TLS certificate and private key must be configured together",
        ));
    }
    let path = |value: &str| (!value.is_empty()).then(|| PathBuf::from(value));
    let policy = TlsContextPolicy {
        protocols: config.tls_protocols.clone(),
        ciphers: config.tls_ciphers.clone(),
        ciphersuites: config.tls_ciphersuites.clone(),
    };
    let server = TlsConfig {
        cert_file: path(&config.tls_cert_file),
        key_file: path(&config.tls_key_file),
        key_password: config.tls_key_file_pass.clone(),
        alt_cert_file: path(&config.tls_alt_cert_file),
        alt_key_file: path(&config.tls_alt_key_file),
        alt_key_password: config.tls_alt_key_file_pass.clone(),
        ca_cert_file: path(&config.tls_ca_cert_file),
        ca_cert_dir: path(&config.tls_ca_cert_dir),
        client_auth: TlsClientAuth::parse(&config.tls_auth_clients)?,
        policy: policy.clone(),
        prefer_server_ciphers: config.tls_prefer_server_ciphers,
        session_caching: config.tls_session_caching,
        session_cache_size: config.tls_session_cache_size,
    };
    let has_client =
        !config.tls_client_cert_file.is_empty() || !config.tls_client_key_file.is_empty();
    let needs_client = config.tls_replication
        || config.tls_cluster
        || has_client
        || !config.tls_ca_cert_file.is_empty()
        || !config.tls_ca_cert_dir.is_empty();
    let client = needs_client.then(|| TlsClientConfig {
        cert_file: path(if has_client {
            &config.tls_client_cert_file
        } else {
            &config.tls_cert_file
        }),
        key_file: path(if has_client {
            &config.tls_client_key_file
        } else {
            &config.tls_key_file
        }),
        key_password: if has_client {
            config.tls_client_key_file_pass.clone()
        } else {
            config.tls_key_file_pass.clone()
        },
        report_certificate: has_client,
        ca_cert_file: path(&config.tls_ca_cert_file),
        ca_cert_dir: path(&config.tls_ca_cert_dir),
        verify_peer: true,
        policy,
    });
    Ok(Some((server, client)))
}

pub(super) fn prepare_contexts(config: &ServerConfig) -> io::Result<TlsContextBatch> {
    #[cfg(not(feature = "tls"))]
    if config.tls_port != 0 || config.tls_replication || config.tls_cluster {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS support is not compiled in",
        ));
    }
    match material_configs(config)? {
        Some((server, client)) => TlsContextBatch::prepare(Some(&server), client.as_ref()),
        None => TlsContextBatch::prepare(None, None),
    }
}

impl ServerWorkerState {
    pub(crate) fn authenticate_tls_peer(
        &mut self,
        token: crate::framework::capabilities::io::Token,
    ) {
        let mode = &self.ctx.config().server.tls_auth_clients_user;
        if self.connection_registry[token].tls_identity_checked {
            return;
        }
        let Some(names) = self.transport.tls_peer_names(token, mode == "uri") else {
            return;
        };
        self.connection_registry[token].tls_identity_checked = true;
        if mode == "off" {
            return;
        }
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        let mut last_name = None;
        let mut matched = None;
        for name in names {
            if mode == "uri" && (name.is_empty() || name.contains(&0)) {
                continue;
            }
            last_name = Some(String::from_utf8_lossy(&name).into_owned());
            if let Ok(name) = std::str::from_utf8(&name) {
                if users.get(name).is_some_and(|user| user.is_on()) {
                    matched = Some(name.to_string());
                    break;
                }
            }
        }
        drop(users);
        if let Some(name) = matched {
            let conn = &mut self.connection_registry[token];
            conn.username = name;
            conn.authenticated = true;
            conn.ever_authenticated = true;
        } else if let Some(name) = last_name {
            self.ctx.stats_mut().acl_access_denied_tls_cert += 1;
            crate::engine::extension::acl::add_acl_log_entry(
                crate::engine::extension::acl::ACL_DENIED_TLS_CERT,
                crate::engine::extension::acl::ACL_LOG_CTX_TOPLEVEL,
                "tls-cert".to_string(),
                name,
                format!("addr={}", self.connection_registry[token].peer_addr),
            );
        }
    }

    pub(crate) fn configure_tls_contexts_from_config(&mut self) -> io::Result<()> {
        self.tls_reload.generation = self.tls_reload.generation.wrapping_add(1);
        let batch = prepare_contexts(&self.ctx.config().server)?;
        self.publish_tls_contexts(batch)?;
        Ok(())
    }

    pub(super) fn publish_tls_contexts(
        &mut self,
        batch: TlsContextBatch,
    ) -> io::Result<TlsContextBatch> {
        let info = batch.info.clone();
        let metadata = batch.metadata.clone();
        let old = self.transport.swap_tls_contexts(batch)?;
        self.ctx.set_tls_info(info);
        self.tls_reload.metadata = metadata;
        Ok(old)
    }

    pub(crate) fn drive_tls_reload(&mut self) {
        if let Some((generation, receiver)) = &self.tls_reload.pending {
            match receiver.try_recv() {
                Ok(result) => {
                    let generation = *generation;
                    self.tls_reload.pending = None;
                    if generation != self.tls_reload.generation {
                        return;
                    }
                    match result {
                        Ok(Some(batch)) => match self.publish_tls_contexts(batch) {
                            Ok(_) => {
                                crate::server_log!(notice, "TLS materials reloaded successfully")
                            }
                            Err(error) => {
                                crate::server_log!(warning, "TLS reload failed: {}", error)
                            }
                        },
                        Ok(None) => crate::server_log!(debug, "TLS materials unchanged"),
                        Err(error) => crate::server_log!(warning, "TLS reload failed: {}", error),
                    }
                }
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.tls_reload.pending = None;
                }
            }
        }
        let interval_sec = self.ctx.config().server.tls_auto_reload_interval;
        if interval_sec <= 0
            || self.tls_reload.pending.is_some()
            || self
                .tls_reload
                .last_check
                .is_some_and(|last| last.elapsed().as_secs() < interval_sec as u64)
        {
            return;
        }
        self.tls_reload.last_check = Some(Instant::now());
        let config = self.ctx.config().server.clone();
        let previous_metadata = self.tls_reload.metadata.clone();
        let generation = self.tls_reload.generation;
        let (sender, receiver) = mpsc::channel();
        let spawn = std::thread::Builder::new()
            .name("tls-reload".to_string())
            .spawn(move || {
                #[cfg(feature = "tls")]
                let result = (|| {
                    let Some((server, client)) = material_configs(&config)? else {
                        return Ok(None);
                    };
                    let metadata =
                        TlsMaterialMetadata::capture_for_reload(&server, client.as_ref())?;
                    if metadata == previous_metadata {
                        return Ok(None);
                    }
                    let batch = TlsContextBatch::prepare(Some(&server), client.as_ref())?;
                    if batch.metadata != metadata {
                        return Err(io::Error::other(
                            "TLS materials changed while preparing reload",
                        ));
                    }
                    Ok(Some(batch))
                })();
                #[cfg(not(feature = "tls"))]
                let result = {
                    let _ = (config, previous_metadata);
                    Ok(None)
                };
                let _ = sender.send(result);
            });
        match spawn {
            Ok(_) => self.tls_reload.pending = Some((generation, receiver)),
            Err(error) => crate::server_log!(warning, "Failed starting TLS reload: {}", error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::material_configs;
    use crate::engine::extension::config::server::ServerConfig;

    fn server_only_config() -> ServerConfig {
        ServerConfig {
            tls_port: 16379,
            tls_cert_file: "server.crt".to_string(),
            tls_key_file: "server.key".to_string(),
            tls_auth_clients: "no".to_string(),
            ..ServerConfig::new()
        }
    }

    #[test]
    fn tls_server_only_without_ca_omits_outgoing_connector() {
        let config = server_only_config();
        let (_, client) = material_configs(&config).unwrap().unwrap();
        assert!(client.is_none());
    }

    #[test]
    fn tls_outgoing_requirements_keep_verified_primary_identity_fallback() {
        for (replication, cluster, ca_file, ca_dir) in [
            (true, false, "", ""),
            (false, true, "", ""),
            (false, false, "ca.crt", ""),
            (false, false, "", "ca"),
        ] {
            let mut config = server_only_config();
            config.tls_replication = replication;
            config.tls_cluster = cluster;
            config.tls_ca_cert_file = ca_file.to_string();
            config.tls_ca_cert_dir = ca_dir.to_string();
            config.tls_key_file_pass = "primary-secret".to_string().into();
            let (server, client) = material_configs(&config).unwrap().unwrap();
            let client = client.unwrap();
            assert!(client.verify_peer);
            assert!(!client.report_certificate);
            assert_eq!(client.cert_file, server.cert_file);
            assert_eq!(client.key_file, server.key_file);
            assert_eq!(client.key_password, server.key_password);
        }
    }

    #[test]
    fn tls_explicit_client_identity_keeps_independent_password_and_validation() {
        let mut config = server_only_config();
        config.tls_client_cert_file = "client.crt".to_string();
        config.tls_client_key_file = "client.key".to_string();
        config.tls_client_key_file_pass = "client-secret".to_string().into();
        let (_, client) = material_configs(&config).unwrap().unwrap();
        let client = client.unwrap();
        assert!(client.verify_peer);
        assert!(client.report_certificate);
        assert_eq!(client.cert_file.unwrap().to_str(), Some("client.crt"));
        assert_eq!(client.key_file.unwrap().to_str(), Some("client.key"));
        assert_eq!(client.key_password, config.tls_client_key_file_pass);
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_not_compiled_precedes_enabled_material_validation() {
        for (port, replication, cluster) in
            [(16379, false, false), (0, true, false), (0, false, true)]
        {
            let config = ServerConfig {
                tls_port: port,
                tls_replication: replication,
                tls_cluster: cluster,
                ..ServerConfig::new()
            };
            let error = super::prepare_contexts(&config)
                .err()
                .expect("TLS is unavailable");
            assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
            assert_eq!(error.to_string(), "TLS support is not compiled in");
        }
        assert!(super::prepare_contexts(&ServerConfig::new()).is_ok());
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_not_compiled_config_reply_is_explicit_and_rolls_back() {
        use crate::engine::command::CommandArgs;
        use crate::foundation::io::protocol::Value;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;
        use crate::framework::commands::server::config::config_set;
        use bytes::Bytes;

        let mut backend = super::ServerWorkerState::new(1).unwrap();
        let old_threshold = backend.ctx.slowlog().threshold_us;
        let mut conn = Connection::new_fake();
        conn.set_token_for_test(Token(42));
        let token = Token(backend.connection_registry.insert(conn));
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"slowlog-log-slower-than"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"tls-cluster"),
            Bytes::from_static(b"yes"),
        ]);
        assert_eq!(
            config_set(
                &mut backend.connection_registry[token],
                &args,
                &mut backend.ctx
            ),
            Value::Null
        );
        let reply = backend.resolve_tls_reconfigure_request(token.0).unwrap();
        let Value::Error(error) = reply else {
            panic!("enabling TLS without support must fail");
        };
        let error = std::str::from_utf8(&error).unwrap();
        assert!(error.contains("TLS support is not compiled in"), "{error}");
        assert!(!backend.ctx.config().server.tls_cluster);
        assert_eq!(
            backend.ctx.config().server.commandlog_slow_threshold,
            old_threshold
        );
        assert_eq!(backend.ctx.slowlog().threshold_us, old_threshold);
        assert_eq!(backend.ctx.commandlog().slow.threshold_us, old_threshold);
    }
}
