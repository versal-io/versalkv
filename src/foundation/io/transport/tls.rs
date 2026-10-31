#[cfg(feature = "tls")]
use crate::foundation::memory::tls::TlsCertificateInfo;
pub use crate::foundation::memory::tls::{TlsInfo, TlsPassword};
#[cfg(feature = "tls")]
use openssl::ssl::{
    SslAcceptor, SslConnector, SslContextBuilder, SslMethod, SslOptions, SslSessionCacheMode,
    SslVerifyMode,
};
use std::io;
use std::path::PathBuf;

static FORCE_TLS_WRITE_ERROR: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_tls_write_error(should_fail: bool) {
    FORCE_TLS_WRITE_ERROR.store(should_fail, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(feature = "tls")]
pub(super) fn should_fail_tls_write() -> bool {
    FORCE_TLS_WRITE_ERROR.load(std::sync::atomic::Ordering::Relaxed)
}

/// A fully prepared batch. Swapping this value never reads material files.
#[derive(Default)]
pub struct TlsContextBatch {
    #[cfg(feature = "tls")]
    pub(super) server: Option<SslAcceptor>,
    #[cfg(feature = "tls")]
    pub(super) client: Option<SslConnector>,
    pub info: TlsInfo,
    pub metadata: TlsMaterialMetadata,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsMaterialMetadata {
    entries: Vec<(PathBuf, Vec<u8>)>,
    ca_fingerprints: Vec<(PathBuf, Vec<u8>)>,
}

impl TlsMaterialMetadata {
    #[cfg(feature = "tls")]
    pub fn capture(server: &TlsConfig, client: Option<&TlsClientConfig>) -> io::Result<Self> {
        Self::capture_inner(server, client, true)
    }

    #[cfg(feature = "tls")]
    pub fn capture_for_reload(
        server: &TlsConfig,
        client: Option<&TlsClientConfig>,
    ) -> io::Result<Self> {
        // Preserve metadata-driven directory change detection used by Valkey.
        Self::capture_inner(server, client, false)
    }

    #[cfg(feature = "tls")]
    fn capture_inner(
        server: &TlsConfig,
        client: Option<&TlsClientConfig>,
        should_fence_ca_members: bool,
    ) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let mut certificates: Vec<_> = [
            server.cert_file.as_ref(),
            server.alt_cert_file.as_ref(),
            client.and_then(|client| client.cert_file.as_ref()),
            server.ca_cert_file.as_ref(),
            client.and_then(|client| client.ca_cert_file.as_ref()),
        ]
        .into_iter()
        .flatten()
        .collect();
        certificates.sort();
        certificates.dedup();
        let mut entries = Vec::new();
        let mut ca_fingerprints = Vec::new();
        for path in certificates {
            let data = std::fs::read(path)?;
            let entry = (path.clone(), openssl::sha::sha256(&data).to_vec());
            if server.ca_cert_file.as_ref() == Some(path)
                || client.and_then(|client| client.ca_cert_file.as_ref()) == Some(path)
            {
                ca_fingerprints.push(entry.clone());
            }
            entries.push(entry);
        }
        let mut metadata_paths: Vec<_> = [
            server.key_file.as_ref(),
            server.alt_key_file.as_ref(),
            client.and_then(|client| client.key_file.as_ref()),
            server.ca_cert_dir.as_ref(),
            client.and_then(|client| client.ca_cert_dir.as_ref()),
        ]
        .into_iter()
        .flatten()
        .collect();
        metadata_paths.sort();
        metadata_paths.dedup();
        for path in metadata_paths {
            let metadata = std::fs::metadata(path)?;
            let stamp = format!(
                "{}:{}:{}:{}:{}",
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.len(),
                metadata.ctime_nsec()
            );
            entries.push((path.clone(), stamp.into_bytes()));
        }
        if should_fence_ca_members {
            let mut directories: Vec<_> = [
                server.ca_cert_dir.as_ref(),
                client.and_then(|client| client.ca_cert_dir.as_ref()),
            ]
            .into_iter()
            .flatten()
            .collect();
            directories.sort();
            directories.dedup();
            for directory in directories {
                for path in ca_directory_files(directory)? {
                    let data = std::fs::read(&path)?;
                    let entry = (path, openssl::sha::sha256(&data).to_vec());
                    ca_fingerprints.push(entry.clone());
                    entries.push(entry);
                }
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(Self {
            entries,
            ca_fingerprints,
        })
    }
}

impl TlsContextBatch {
    pub fn prepare(
        server: Option<&TlsConfig>,
        client: Option<&TlsClientConfig>,
    ) -> io::Result<Self> {
        #[cfg(feature = "tls")]
        {
            let empty_server = TlsConfig::default();
            let fence_server = server.unwrap_or(&empty_server);
            let initial_metadata = TlsMaterialMetadata::capture(fence_server, client)?;
            let server_ca = server
                .map(|server| {
                    load_ca_snapshot(
                        server.ca_cert_file.as_deref(),
                        server.ca_cert_dir.as_deref(),
                    )
                })
                .transpose()?;
            let client_ca = match (server, client) {
                (Some(server), Some(client))
                    if server.ca_cert_file == client.ca_cert_file
                        && server.ca_cert_dir == client.ca_cert_dir =>
                {
                    None
                }
                (_, Some(client)) => Some(load_ca_snapshot(
                    client.ca_cert_file.as_deref(),
                    client.ca_cert_dir.as_deref(),
                )?),
                _ => None,
            };
            for (snapshot, file, dir) in server_ca
                .iter()
                .map(|snapshot| {
                    (
                        snapshot,
                        fence_server.ca_cert_file.as_deref(),
                        fence_server.ca_cert_dir.as_deref(),
                    )
                })
                .chain(client_ca.iter().map(|snapshot| {
                    (
                        snapshot,
                        client.and_then(|client| client.ca_cert_file.as_deref()),
                        client.and_then(|client| client.ca_cert_dir.as_deref()),
                    )
                }))
            {
                if !snapshot.matches_metadata(&initial_metadata, file, dir) {
                    return Err(io::Error::other(
                        "TLS CA materials changed while loading snapshot",
                    ));
                }
            }
            let mut batch = Self::default();
            if let (Some(server), Some(ca)) = (server, server_ca.as_ref()) {
                batch.server = server.server_acceptor_with_ca(&ca.certificates)?;
                if let Some(path) = &server.cert_file {
                    batch.info.server =
                        certificate_info(load_certificate(path, "Server")?.as_ref())?;
                }
                if let Some(path) = &server.alt_cert_file {
                    batch.info.alternate =
                        certificate_info(load_certificate(path, "Alternate server")?.as_ref())?;
                }
                batch.info.ca = ca.certificates.iter().try_fold(
                    TlsCertificateInfo::default(),
                    |earliest, cert| {
                        let info = certificate_info(cert)?;
                        Ok::<_, io::Error>(
                            if earliest.expires_at_sec.is_none()
                                || info.expires_at_sec < earliest.expires_at_sec
                            {
                                info
                            } else {
                                earliest
                            },
                        )
                    },
                )?;
            }
            if let Some(client) = client {
                let ca = client_ca.as_ref().or(server_ca.as_ref());
                batch.client = Some(
                    client.connector_with_ca(ca.map_or(&[], |ca| ca.certificates.as_slice()))?,
                );
                if client.report_certificate {
                    if let Some(path) = &client.cert_file {
                        batch.info.client =
                            certificate_info(load_certificate(path, "Client")?.as_ref())?;
                    }
                }
            }
            batch.metadata = TlsMaterialMetadata::capture_for_reload(fence_server, client)?;
            if initial_metadata != TlsMaterialMetadata::capture(fence_server, client)? {
                return Err(io::Error::other(
                    "TLS materials changed while preparing contexts",
                ));
            }
            Ok(batch)
        }
        #[cfg(not(feature = "tls"))]
        {
            if server.is_none() && client.is_none() {
                Ok(Self::default())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "TLS support is not compiled in",
                ))
            }
        }
    }
}

#[cfg(feature = "tls")]
fn certificate_info(cert: &openssl::x509::X509Ref) -> io::Result<TlsCertificateInfo> {
    let epoch = openssl::asn1::Asn1Time::from_unix(0).map_err(io::Error::other)?;
    let diff = epoch.diff(cert.not_after()).map_err(io::Error::other)?;
    Ok(TlsCertificateInfo {
        serial: Some(
            cert.serial_number()
                .to_bn()
                .and_then(|bn| bn.to_hex_str())
                .map_err(io::Error::other)?
                .to_string(),
        ),
        expires_at_sec: Some(i64::from(diff.days) * 86400 + i64::from(diff.secs)),
    })
}

#[cfg(feature = "tls")]
fn validate_certificate(cert: &openssl::x509::X509Ref, role: &str) -> io::Result<()> {
    let now = openssl::asn1::Asn1Time::days_from_now(0).map_err(io::Error::other)?;
    if cert
        .not_before()
        .compare(&now)
        .map_err(io::Error::other)?
        .is_gt()
        || !cert
            .not_after()
            .compare(&now)
            .map_err(io::Error::other)?
            .is_gt()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{role} TLS certificate is invalid. Aborting TLS configuration."),
        ));
    }
    Ok(())
}

#[cfg(feature = "tls")]
fn load_certificate(path: &std::path::Path, role: &str) -> io::Result<openssl::x509::X509> {
    let data = std::fs::read(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("Failed to load certificate: {}: {error}", path.display()),
        )
    })?;
    let cert = openssl::x509::X509::from_pem(&data).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to load certificate: {}: {error}", path.display()),
        )
    })?;
    validate_certificate(&cert, role)?;
    Ok(cert)
}

#[cfg(feature = "tls")]
fn load_ca_certificates(
    file: Option<&std::path::Path>,
    dir: Option<&std::path::Path>,
) -> io::Result<Vec<openssl::x509::X509>> {
    Ok(load_ca_snapshot(file, dir)?.certificates)
}

#[cfg(feature = "tls")]
struct CaSnapshot {
    certificates: Vec<openssl::x509::X509>,
    fingerprints: Vec<(PathBuf, Vec<u8>)>,
}

#[cfg(feature = "tls")]
impl CaSnapshot {
    fn matches_metadata(
        &self,
        metadata: &TlsMaterialMetadata,
        file: Option<&std::path::Path>,
        dir: Option<&std::path::Path>,
    ) -> bool {
        let mut expected: Vec<_> = metadata
            .ca_fingerprints
            .iter()
            .filter(|(path, _)| {
                file == Some(path.as_path()) || dir.is_some_and(|dir| path.parent() == Some(dir))
            })
            .collect();
        let mut actual: Vec<_> = self.fingerprints.iter().collect();
        expected.sort();
        expected.dedup();
        actual.sort();
        actual.dedup();
        actual == expected
    }
}

#[cfg(feature = "tls")]
fn ca_directory_files(dir: &std::path::Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

#[cfg(feature = "tls")]
fn load_ca_snapshot(
    file: Option<&std::path::Path>,
    dir: Option<&std::path::Path>,
) -> io::Result<CaSnapshot> {
    let mut certificates = Vec::new();
    let mut fingerprints = Vec::new();
    if let Some(path) = file {
        let data = std::fs::read(path)?;
        fingerprints.push((path.to_path_buf(), openssl::sha::sha256(&data).to_vec()));
        certificates.extend(openssl::x509::X509::stack_from_pem(&data).map_err(io::Error::other)?);
        if certificates.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No CA certificates loaded from file",
            ));
        }
    }
    if let Some(dir) = dir {
        let count = certificates.len();
        for path in ca_directory_files(dir)? {
            let data = std::fs::read(&path)?;
            fingerprints.push((path, openssl::sha::sha256(&data).to_vec()));
            if let Ok(certs) = openssl::x509::X509::stack_from_pem(&data) {
                certificates.extend(certs);
            }
        }
        if certificates.len() == count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No CA certificates loaded from directory",
            ));
        }
    }
    for cert in &certificates {
        validate_certificate(cert, "CA").map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "One or more loaded CA certificates are invalid. Aborting TLS configuration.",
            )
        })?;
    }
    Ok(CaSnapshot {
        certificates,
        fingerprints,
    })
}

#[cfg(feature = "tls")]
fn apply_ca(
    builder: &mut SslContextBuilder,
    certificates: &[openssl::x509::X509],
) -> io::Result<()> {
    for cert in certificates {
        builder
            .cert_store_mut()
            .add_cert(cert.clone())
            .map_err(io::Error::other)?;
    }
    Ok(())
}

#[cfg(feature = "tls")]
fn apply_identity(
    builder: &mut SslContextBuilder,
    cert: &std::path::Path,
    key: &std::path::Path,
    password: &TlsPassword,
) -> io::Result<()> {
    builder.set_certificate_chain_file(cert).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to load certificate: {}: {error}", cert.display()),
        )
    })?;
    let pem = std::fs::read(key)?;
    let private_key = openssl::pkey::PKey::private_key_from_pem_callback(&pem, |buffer| {
        let bytes = password.expose_secret().as_bytes();
        if bytes.is_empty() || bytes.len() > buffer.len() {
            return Err(openssl::error::ErrorStack::get());
        }
        buffer[..bytes.len()].copy_from_slice(bytes);
        Ok(bytes.len())
    })
    .map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to load private key: {}: {error}", key.display()),
        )
    })?;
    builder
        .set_private_key(&private_key)
        .map_err(io::Error::other)?;
    builder.check_private_key().map_err(io::Error::other)
}

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

    #[cfg(feature = "tls")]
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

    #[cfg(feature = "tls")]
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
    pub key_password: TlsPassword,
    pub alt_cert_file: Option<PathBuf>,
    pub alt_key_file: Option<PathBuf>,
    pub alt_key_password: TlsPassword,
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
            key_password: TlsPassword::default(),
            alt_cert_file: None,
            alt_key_file: None,
            alt_key_password: TlsPassword::default(),
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
    pub key_password: TlsPassword,
    pub report_certificate: bool,
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
            key_password: TlsPassword::default(),
            report_certificate: true,
            ca_cert_file: None,
            ca_cert_dir: None,
            verify_peer: true,
            policy: TlsContextPolicy::default(),
        }
    }
}

impl TlsClientConfig {
    #[cfg(feature = "tls")]
    pub fn connector(&self) -> io::Result<SslConnector> {
        let ca = load_ca_certificates(self.ca_cert_file.as_deref(), self.ca_cert_dir.as_deref())?;
        self.connector_with_ca(&ca)
    }

    #[cfg(feature = "tls")]
    fn connector_with_ca(&self, ca: &[openssl::x509::X509]) -> io::Result<SslConnector> {
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
            apply_ca(&mut builder, ca)?;
        }
        if let (Some(cert_file), Some(key_file)) = (cert_file, key_file) {
            load_certificate(cert_file, "Client")?;
            apply_identity(&mut builder, cert_file, key_file, &self.key_password)?;
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
    #[cfg(feature = "tls")]
    pub fn server_acceptor(&self) -> io::Result<Option<SslAcceptor>> {
        if self.cert_file.is_none() && self.key_file.is_none() {
            return Ok(None);
        }
        let ca = load_ca_certificates(self.ca_cert_file.as_deref(), self.ca_cert_dir.as_deref())?;
        self.server_acceptor_with_ca(&ca)
    }

    #[cfg(feature = "tls")]
    fn server_acceptor_with_ca(
        &self,
        ca: &[openssl::x509::X509],
    ) -> io::Result<Option<SslAcceptor>> {
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
        let alternate = match (&self.alt_cert_file, &self.alt_key_file) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some((cert, key)),
            (Some(_), None) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tls-alt-cert-file provided without a key",
                ))
            }
            (None, Some(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tls-alt-key-file provided without a certificate",
                ))
            }
        };
        let primary = load_certificate(cert_file, "Server")?;
        if let Some((cert, _)) = alternate {
            let alternate = load_certificate(cert, "Alternate server")?;
            let primary_key = primary.public_key().map_err(io::Error::other)?;
            let alternate_key = alternate.public_key().map_err(io::Error::other)?;
            let algorithm = |key: &openssl::pkey::PKey<openssl::pkey::Public>| {
                // RSA-PSS and RSA share the same OpenSSL certificate slot.
                if key.id() == openssl::pkey::Id::RSA_PSS {
                    openssl::pkey::Id::RSA
                } else {
                    key.id()
                }
            };
            if algorithm(&primary_key) == algorithm(&alternate_key) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Primary and alternate certificates must use different key algorithms",
                ));
            }
        }
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
        apply_identity(&mut builder, cert_file, key_file, &self.key_password)?;
        if let Some((cert, key)) = alternate {
            apply_identity(&mut builder, cert, key, &self.alt_key_password)?;
        }
        if self.ca_cert_file.is_some() || self.ca_cert_dir.is_some() {
            apply_ca(&mut builder, ca)?;
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
    #[cfg(feature = "tls")]
    use super::TlsClientConfig;
    use super::{TlsContextPolicy, TlsProtocol};
    #[cfg(feature = "tls")]
    use openssl::ssl::SslOptions;

    #[test]
    fn default_tls_policy_allows_only_tls_1_2_and_1_3() {
        assert_eq!(
            TlsContextPolicy::default().protocols().unwrap(),
            vec![TlsProtocol::Tls1_2, TlsProtocol::Tls1_3]
        );

        #[cfg(feature = "tls")]
        {
            let disabled = TlsContextPolicy::default().disabled_protocols().unwrap();
            assert!(disabled.contains(SslOptions::NO_TLSV1));
            assert!(disabled.contains(SslOptions::NO_TLSV1_1));
            assert!(!disabled.contains(SslOptions::NO_TLSV1_2));
            assert!(!disabled.contains(SslOptions::NO_TLSV1_3));
        }
    }

    #[test]
    fn tls_policy_rejects_unknown_protocol_tokens_and_accepts_a_subset() {
        let invalid = TlsContextPolicy {
            protocols: "TLSv1.2 TLSv9".to_string(),
            ..TlsContextPolicy::default()
        };
        assert!(invalid.protocols().is_err());

        let tls_1_2_only = TlsContextPolicy {
            protocols: "TLSv1.2".to_string(),
            ..TlsContextPolicy::default()
        };
        assert_eq!(tls_1_2_only.protocols().unwrap(), vec![TlsProtocol::Tls1_2]);
        #[cfg(feature = "tls")]
        assert!(tls_1_2_only
            .disabled_protocols()
            .unwrap()
            .contains(SslOptions::NO_TLSV1_3));
    }

    #[cfg(feature = "tls")]
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

    #[cfg(feature = "tls")]
    #[test]
    fn verified_tls_client_requires_an_explicit_ca_source() {
        assert!(TlsClientConfig::default().connector().is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_contexts_reject_expired_certificates() {
        use openssl::asn1::Asn1Time;
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509NameBuilder, X509};
        use std::time::{SystemTime, UNIX_EPOCH};

        let directory = std::env::temp_dir().join(format!(
            "versal-tls-validity-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "localhost").unwrap();
        let name = name.build();
        let mut certificate = X509::builder().unwrap();
        certificate.set_version(2).unwrap();
        certificate.set_subject_name(&name).unwrap();
        certificate.set_issuer_name(&name).unwrap();
        certificate.set_pubkey(&key).unwrap();
        certificate
            .set_not_before(&Asn1Time::from_unix(1).unwrap())
            .unwrap();
        certificate
            .set_not_after(&Asn1Time::from_unix(2).unwrap())
            .unwrap();
        certificate.sign(&key, MessageDigest::sha256()).unwrap();
        let cert_file = directory.join("server.crt");
        let key_file = directory.join("server.key");
        std::fs::write(&cert_file, certificate.build().to_pem().unwrap()).unwrap();
        std::fs::write(&key_file, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        let server = super::TlsConfig {
            cert_file: Some(cert_file.clone()),
            key_file: Some(key_file.clone()),
            client_auth: super::TlsClientAuth::No,
            ..super::TlsConfig::default()
        };
        let client = TlsClientConfig {
            cert_file: Some(cert_file),
            key_file: Some(key_file),
            verify_peer: false,
            ..TlsClientConfig::default()
        };
        let server_result = server.server_acceptor();
        let client_result = client.connector();
        std::fs::remove_dir_all(directory).unwrap();
        assert_eq!(
            (server_result.is_err(), client_result.is_err()),
            (true, true),
            "both server and client must reject expired certificates"
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_ca_directory_fence_detects_in_place_content_changes() {
        use openssl::asn1::Asn1Time;
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509NameBuilder, X509};
        use std::os::unix::fs::MetadataExt;
        use std::time::{SystemTime, UNIX_EPOCH};

        let directory = std::env::temp_dir().join(format!(
            "versal-tls-ca-fence-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let ca_directory = directory.join("ca");
        std::fs::create_dir(&ca_directory).unwrap();
        let make_ca = |common_name: &str, serial: u32| {
            let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let mut name = X509NameBuilder::new().unwrap();
            name.append_entry_by_text("CN", common_name).unwrap();
            let name = name.build();
            let mut cert = X509::builder().unwrap();
            cert.set_version(2).unwrap();
            cert.set_serial_number(
                &openssl::bn::BigNum::from_u32(serial)
                    .unwrap()
                    .to_asn1_integer()
                    .unwrap(),
            )
            .unwrap();
            cert.set_subject_name(&name).unwrap();
            cert.set_issuer_name(&name).unwrap();
            cert.set_pubkey(&key).unwrap();
            cert.set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            cert.set_not_after(&Asn1Time::days_from_now(2).unwrap())
                .unwrap();
            cert.append_extension(
                openssl::x509::extension::BasicConstraints::new()
                    .critical()
                    .ca()
                    .build()
                    .unwrap(),
            )
            .unwrap();
            cert.sign(&key, MessageDigest::sha256()).unwrap();
            (
                cert.build().to_pem().unwrap(),
                key.private_key_to_pem_pkcs8().unwrap(),
            )
        };
        let (first, first_key) = make_ca("TLS-Fence-CA-A", 1);
        let (second, _) = make_ca("TLS-Fence-CA-B", 2);
        let member = ca_directory.join("current.pem");
        std::fs::write(&member, &first).unwrap();
        let cert_file = directory.join("server.crt");
        let key_file = directory.join("server.key");
        std::fs::write(&cert_file, &first).unwrap();
        std::fs::write(&key_file, first_key).unwrap();
        let server = super::TlsConfig {
            cert_file: Some(cert_file.clone()),
            key_file: Some(key_file.clone()),
            ca_cert_dir: Some(ca_directory.clone()),
            ..super::TlsConfig::default()
        };
        let client = TlsClientConfig {
            cert_file: Some(cert_file),
            key_file: Some(key_file),
            ca_cert_dir: Some(ca_directory.clone()),
            ..TlsClientConfig::default()
        };
        let initial = super::TlsMaterialMetadata::capture(&server, Some(&client)).unwrap();
        let reload_before =
            super::TlsMaterialMetadata::capture_for_reload(&server, Some(&client)).unwrap();
        let loaded = super::load_ca_snapshot(None, Some(&ca_directory)).unwrap();
        assert!(loaded.matches_metadata(&initial, None, Some(&ca_directory)));
        let batch = super::TlsContextBatch::prepare(Some(&server), Some(&client)).unwrap();
        assert_eq!(
            batch.info.ca,
            super::certificate_info(&loaded.certificates[0]).unwrap()
        );
        for context in [
            batch.server.as_ref().unwrap().context(),
            batch.client.as_ref().unwrap().context(),
        ] {
            assert!(context
                .cert_store()
                .all_certificates()
                .iter()
                .any(|cert| { cert.to_pem().unwrap() == first }));
        }
        let combined_server = super::TlsConfig {
            ca_cert_dir: Some(directory.clone()),
            ..server.clone()
        };
        let combined_client = TlsClientConfig {
            ca_cert_dir: Some(directory.clone()),
            ..client.clone()
        };
        assert!(
            super::TlsContextBatch::prepare(Some(&combined_server), Some(&combined_client)).is_ok(),
            "CA directories may also contain identity files and private keys"
        );
        let missing_members = super::CaSnapshot {
            certificates: Vec::new(),
            fingerprints: Vec::new(),
        };
        assert!(!missing_members.matches_metadata(&initial, None, Some(&ca_directory)));
        let acceptor = server
            .server_acceptor_with_ca(&loaded.certificates)
            .unwrap()
            .unwrap();
        let before = std::fs::metadata(&ca_directory).unwrap();
        std::fs::write(&member, &second).unwrap();
        let after = std::fs::metadata(&ca_directory).unwrap();
        let connector = client.connector_with_ca(&loaded.certificates).unwrap();
        let loaded_info = super::certificate_info(&loaded.certificates[0]).unwrap();
        let fresh = super::load_ca_snapshot(None, Some(&ca_directory)).unwrap();
        assert!(!fresh.matches_metadata(&initial, None, Some(&ca_directory)));
        let fresh_info = super::certificate_info(&fresh.certificates[0]).unwrap();
        let final_metadata = super::TlsMaterialMetadata::capture(&server, Some(&client)).unwrap();
        let reload_after =
            super::TlsMaterialMetadata::capture_for_reload(&server, Some(&client)).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

        for context in [acceptor.context(), connector.context()] {
            let certificates = context.cert_store().all_certificates();
            assert!(certificates
                .iter()
                .any(|cert| cert.to_pem().unwrap() == first));
            assert!(!certificates
                .iter()
                .any(|cert| cert.to_pem().unwrap() == second));
        }
        assert!(connector
            .context()
            .verify_mode()
            .contains(openssl::ssl::SslVerifyMode::PEER));
        assert_ne!(loaded_info.serial, fresh_info.serial);
        assert_eq!(
            reload_before, reload_after,
            "automatic reload retains directory metadata semantics"
        );
        assert_eq!(
            (
                before.ino(),
                before.mtime(),
                before.mtime_nsec(),
                before.len()
            ),
            (after.ino(), after.mtime(), after.mtime_nsec(), after.len())
        );
        assert_eq!(loaded.certificates[0].to_pem().unwrap(), first);
        assert_ne!(openssl::sha::sha256(&first), openssl::sha::sha256(&second));
        assert_ne!(
            initial, final_metadata,
            "CA member contents must fence batch preparation"
        );
        assert!(final_metadata
            .ca_fingerprints
            .iter()
            .any(|(path, hash)| path == &member && hash == &openssl::sha::sha256(&second)));
    }
}
