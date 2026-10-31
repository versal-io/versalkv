//! Byte, protocol, storage, and syscall-facing foundation modules.

pub mod compression;
pub mod persistence;
pub mod protocol;
pub mod store;
#[cfg_attr(feature = "wasm", path = "sys/wasm.rs")]
pub mod sys;
#[cfg(not(feature = "wasm"))]
pub mod transport;

#[cfg(all(test, not(feature = "wasm")))]
mod transport_contract_tests {
    #[test]
    fn transport_id_is_opaque_and_nonzero() {
        assert!(super::transport::TransportId::new(0).is_none());
        assert_eq!(super::transport::TransportId::new(7).unwrap().get(), 7);
    }

    #[test]
    fn transport_event_retains_readiness_flags() {
        let event = super::transport::TransportEvent::ready(
            super::transport::TransportId::new(3).unwrap(),
            true,
            false,
            false,
            true,
            true,
        );

        assert!(event.readable);
        assert!(!event.writable);
        assert!(event.write_closed);
        assert!(event.error);
    }

    #[test]
    fn polling_runtime_binds_and_polls_without_events() {
        use super::transport::{PollingTransportRuntime, TransportRuntime};
        use std::time::Duration;

        let mut runtime = PollingTransportRuntime::new().unwrap();
        let listener = runtime.bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
        assert_ne!(listener.id.get(), 0);

        let mut events = Vec::new();
        assert_eq!(runtime.poll(&mut events, Some(Duration::ZERO)).unwrap(), 0);
    }

    #[test]
    fn polling_runtime_accepts_configured_unix_socket_group() {
        use super::transport::{PollingTransportRuntime, TransportRuntime, UnixBindOptions};
        let group = std::process::Command::new("id")
            .arg("-gn")
            .output()
            .unwrap();
        let group = String::from_utf8(group.stdout).unwrap().trim().to_string();
        let path = std::env::temp_dir().join(format!("versal-transport-{}", std::process::id()));
        let mut runtime = PollingTransportRuntime::new().unwrap();
        let result = runtime.bind_unix(
            &path,
            UnixBindOptions {
                perm: 0o600,
                group: Some(group),
            },
        );
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok());
    }
}
