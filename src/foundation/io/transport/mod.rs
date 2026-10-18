#![forbid(unsafe_code)]

mod api;
pub(crate) mod polling;
mod thread_io;
mod tls;

pub(crate) use self::polling::PollingTransportRuntime;
#[cfg(test)]
pub(crate) use self::thread_io::TransportWake;
pub use self::thread_io::{ThreadIoHandle, TransportWakeHandle};
pub use self::tls::{TlsClientAuth, TlsClientConfig, TlsConfig, TlsContextPolicy};
#[cfg(test)]
pub(crate) use api::TransportTestBackend;
pub use api::{
    ConnectedPeer, Interest, ListenerHandle, OwnedPollSource, PollSourceMode, TransportEvent,
    TransportId, TransportPollState, TransportRuntime, UnixBindOptions,
};
pub(crate) use api::{ReadOutcome, TransportPollMode, TransportWorkerBackend, WriteOutcome};

#[cfg(test)]
mod tls_contract_tests {
    use super::TlsConfig;
    use std::path::PathBuf;

    #[test]
    fn tls_config_rejects_an_unpaired_certificate_or_private_key() {
        let config = TlsConfig {
            cert_file: Some(PathBuf::from("server.crt")),
            key_file: None,
            ca_cert_file: None,
            client_auth: super::TlsClientAuth::Yes,
            ..TlsConfig::default()
        };

        let error = config.server_acceptor().err().unwrap();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error
            .to_string()
            .contains("certificate and private key must be configured together"));
    }
}
