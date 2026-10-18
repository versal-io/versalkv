use openssl::ssl::{
    SslAcceptor, SslConnector, SslContextBuilder, SslFiletype, SslMethod, SslOptions,
    SslSessionCacheMode, SslVerifyMode,
};
use std::io;
use std::path::PathBuf;

/// TLS protocol names accepted by Valkey's `tls-protocols` setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsProtocol {
    Tls1,
    Tls1_1,
    Tls1_2,
    Tls1_3,
}

/// Common OpenSSL policy for server and outgoing-client contexts. Empty values
/// retain Valkey's TLS 1.2/1.3 defaults or OpenSSL's cipher defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsContextPolicy {
    pub protocols: String,
    pub ciphers: String,
    pub ciphersuites: String,
}

impl TlsContextPolicy {
    pub fn protocols(&self) -> io::Result<Vec<TlsProtocol>> {
        if self.protocols.trim().is_empty() {
            return Ok(vec![TlsProtocol::Tls1_2, TlsProtocol::Tls1_3]);
        }
        self.protocols
            .split_whitespace()
            .map(|protocol| match protocol.to_ascii_lowercase().as_str() {
                "tlsv1" => Ok(TlsProtocol::Tls1),
                "tlsv1.1" => Ok(TlsProtocol::Tls1_1),
                "tlsv1.2" => Ok(TlsProtocol::Tls1_2),
                "tlsv1.3" => Ok(TlsProtocol::Tls1_3),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tls-protocols must contain only TLSv1, TLSv1.1, TLSv1.2, or TLSv1.3",
                )),
            })
            .collect()
    }

    pub fn disabled_protocols(&self) -> io::Result<SslOptions> {
        let protocols = self.protocols()?;
        let mut disabled = SslOptions::empty();
        if !protocols.contains(&TlsProtocol::Tls1) {
            disabled |= SslOptions::NO_TLSV1;
        }
        if !protocols.contains(&TlsProtocol::Tls1_1) {
            disabled |= SslOptions::NO_TLSV1_1;
        }
        if !protocols.contains(&TlsProtocol::Tls1_2) {
            disabled |= SslOptions::NO_TLSV1_2;
        }
        if !protocols.contains(&TlsProtocol::Tls1_3) {
            disabled |= SslOptions::NO_TLSV1_3;
        }
        Ok(disabled)
    }

    fn apply_to(&self, builder: &mut SslContextBuilder) -> io::Result<()> {
        builder.set_options(self.disabled_protocols()?);
        if !self.ciphers.trim().is_empty() {
            builder
                .set_cipher_list(&self.ciphers)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        if !self.ciphersuites.trim().is_empty() {
            builder
                .set_ciphersuites(&self.ciphersuites)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsClientAuth {
    No,
    Optional,
    #[default]
    Yes,
}

impl TlsClientAuth {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "no" => Ok(Self::No),
            "optional" => Ok(Self::Optional),
            "yes" => Ok(Self::Yes),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tls-auth-clients must be no, yes, or optional",
            )),
        }
    }
}

/// File-backed server-side TLS material. A missing certificate/key pair means
/// TLS is disabled; supplying only one side is always a configuration error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsConfig {
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_cert_file: Option<PathBuf>,
    pub ca_cert_dir: Option<PathBuf>,
    pub client_auth: TlsClientAuth,
    pub policy: TlsContextPolicy,
    pub prefer_server_ciphers: bool,
    pub session_caching: bool,
    pub session_cache_size: i32,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_file: None,
            key_file: None,
            ca_cert_file: None,
            ca_cert_dir: None,
            client_auth: TlsClientAuth::Yes,
            policy: TlsContextPolicy::default(),
            prefer_server_ciphers: false,
            session_caching: true,
            session_cache_size: 20 * 1024,
        }
    }
}

/// File-backed material and peer-verification policy for outgoing TLS links.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientConfig {
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_cert_file: Option<PathBuf>,
    pub ca_cert_dir: Option<PathBuf>,
    pub verify_peer: bool,
    pub policy: TlsContextPolicy,
}

impl Default for TlsClientConfig {
    fn default() -> Self {
        Self {
            cert_file: None,
            key_file: None,
            ca_cert_file: None,
            ca_cert_dir: None,
            verify_peer: true,
            policy: TlsContextPolicy::default(),
        }
    }
}

impl TlsClientConfig {
    pub fn connector(&self) -> io::Result<SslConnector> {
        let (cert_file, key_file) = match (&self.cert_file, &self.key_file) {
            (None, None) => (None, None),
            (Some(cert_file), Some(key_file)) => (Some(cert_file), Some(key_file)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TLS client certificate and private key must be configured together",
                ));
            }
        };
        let mut builder = SslConnector::builder(SslMethod::tls()).map_err(io::Error::other)?;
        self.policy.apply_to(&mut builder)?;
        if self.verify_peer && self.ca_cert_file.is_none() && self.ca_cert_dir.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS peer verification requires tls-ca-cert-file or tls-ca-cert-dir",
            ));
        }
        if self.ca_cert_file.is_some() || self.ca_cert_dir.is_some() {
            builder
                .load_verify_locations(self.ca_cert_file.as_deref(), self.ca_cert_dir.as_deref())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
        if let (Some(cert_file), Some(key_file)) = (cert_file, key_file) {
            builder
                .set_certificate_file(cert_file, SslFiletype::PEM)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            builder
                .set_private_key_file(key_file, SslFiletype::PEM)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            builder
                .check_private_key()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
        builder.set_verify(if self.verify_peer {
            SslVerifyMode::PEER
        } else {
            SslVerifyMode::NONE
        });
        Ok(builder.build())
    }
}

impl TlsConfig {
    pub fn server_acceptor(&self) -> io::Result<Option<SslAcceptor>> {
        let (cert_file, key_file) = match (&self.cert_file, &self.key_file) {
            (None, None) => return Ok(None),
            (Some(cert_file), Some(key_file)) => (cert_file, key_file),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TLS certificate and private key must be configured together",
                ));
            }
        };
        if self.client_auth != TlsClientAuth::No
            && self.ca_cert_file.is_none()
            && self.ca_cert_dir.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS client authentication requires tls-ca-cert-file or tls-ca-cert-dir",
            ));
        }
        let mut builder =
            SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).map_err(io::Error::other)?;
        self.policy.apply_to(&mut builder)?;
        builder
            .set_certificate_file(cert_file, SslFiletype::PEM)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        builder
            .set_private_key_file(key_file, SslFiletype::PEM)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        builder
            .check_private_key()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if self.ca_cert_file.is_some() || self.ca_cert_dir.is_some() {
            builder
                .load_verify_locations(self.ca_cert_file.as_deref(), self.ca_cert_dir.as_deref())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
        let verify = match self.client_auth {
            TlsClientAuth::No => SslVerifyMode::NONE,
            TlsClientAuth::Optional => SslVerifyMode::PEER,
            TlsClientAuth::Yes => SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
        };
        builder.set_verify(verify);
        if self.session_caching {
            builder.set_session_cache_mode(SslSessionCacheMode::SERVER);
            builder.set_session_cache_size(self.session_cache_size);
            builder
                .set_session_id_context(b"valkey")
                .map_err(io::Error::other)?;
        } else {
            builder.set_session_cache_mode(SslSessionCacheMode::OFF);
        }
        if self.prefer_server_ciphers {
            builder.set_options(SslOptions::CIPHER_SERVER_PREFERENCE);
        }
        Ok(Some(builder.build()))
    }
}

#[cfg(test)]
mod tests {
    use super::{TlsClientConfig, TlsContextPolicy, TlsProtocol};
    use openssl::ssl::SslOptions;

    #[test]
    fn default_tls_policy_allows_only_tls_1_2_and_1_3() {
        let disabled = TlsContextPolicy::default().disabled_protocols().unwrap();

        assert!(disabled.contains(SslOptions::NO_TLSV1));
        assert!(disabled.contains(SslOptions::NO_TLSV1_1));
        assert!(!disabled.contains(SslOptions::NO_TLSV1_2));
        assert!(!disabled.contains(SslOptions::NO_TLSV1_3));
    }

    #[test]
    fn tls_policy_rejects_unknown_protocol_tokens_and_accepts_a_subset() {
        let invalid = TlsContextPolicy {
            protocols: "TLSv1.2 TLSv9".to_string(),
            ..TlsContextPolicy::default()
        };
        assert!(invalid.disabled_protocols().is_err());

        let tls_1_2_only = TlsContextPolicy {
            protocols: "TLSv1.2".to_string(),
            ..TlsContextPolicy::default()
        };
        assert_eq!(tls_1_2_only.protocols().unwrap(), vec![TlsProtocol::Tls1_2]);
        assert!(tls_1_2_only
            .disabled_protocols()
            .unwrap()
            .contains(SslOptions::NO_TLSV1_3));
    }

    #[test]
    fn tls_policy_rejects_an_invalid_openssl_cipher_list() {
        let config = TlsClientConfig {
            policy: TlsContextPolicy {
                ciphers: "not-a-real-cipher".to_string(),
                ..TlsContextPolicy::default()
            },
            ..TlsClientConfig::default()
        };

        assert!(config.connector().is_err());
    }

    #[test]
    fn verified_tls_client_requires_an_explicit_ca_source() {
        assert!(TlsClientConfig::default().connector().is_err());
    }
}
