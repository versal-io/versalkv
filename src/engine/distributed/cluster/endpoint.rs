use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

const CLUSTER_NODE_ID_LEN: usize = 40;
const NET_IP_STR_LEN: usize = 46;
const NET_HOST_STR_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(std::sync::Arc<str>);

impl NodeId {
    pub fn parse(value: &[u8]) -> Result<Self, NodeIdError> {
        if value.len() != CLUSTER_NODE_ID_LEN {
            return Err(NodeIdError::WrongLength {
                actual: value.len(),
            });
        }
        if let Some((index, byte)) = value
            .iter()
            .copied()
            .enumerate()
            .find(|(_, byte)| !byte.is_ascii_lowercase() && !byte.is_ascii_digit())
        {
            return Err(NodeIdError::InvalidCharacter { index, byte });
        }
        // The validation above guarantees ASCII and therefore valid UTF-8.
        Ok(Self(std::sync::Arc::from(
            std::str::from_utf8(value)
                .expect("validated cluster node IDs contain only ASCII characters"),
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeIdError {
    WrongLength { actual: usize },
    InvalidCharacter { index: usize, byte: u8 },
}

impl fmt::Display for NodeIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                formatter,
                "cluster node ID must be {CLUSTER_NODE_ID_LEN} bytes, got {actual}"
            ),
            Self::InvalidCharacter { index, byte } => write!(
                formatter,
                "invalid cluster node ID byte 0x{byte:02x} at offset {index}"
            ),
        }
    }
}

impl std::error::Error for NodeIdError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreferredEndpoint {
    Ip,
    Hostname,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientTransport {
    Tcp,
    Tls,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdvertisedEndpoint {
    pub host: String,
    pub port: u16,
}

/// Read-only projection for legacy `node.ip/port/cport` call sites.
/// `ClusterNode` exposes this through `Deref` but deliberately omits
/// `DerefMut`, keeping all endpoint writes centralized in `ClusterEndpoints`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterAddress {
    pub(crate) ip: String,
    pub(crate) port: u16,
    pub(crate) cport: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointError {
    InvalidUtf8,
    InvalidAuxCharacter,
    HostnameTooLong,
    InvalidHostname,
    AnnouncedIpTooLong,
    InvalidAnnouncedIp,
    InvalidIpv4,
    InvalidIpv6,
}

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidUtf8 => "endpoint metadata must be valid UTF-8",
            Self::InvalidAuxCharacter => "endpoint metadata contains an invalid character",
            Self::HostnameTooLong => "hostname must be less than 256 bytes",
            Self::InvalidHostname => {
                "hostname may only contain ASCII alphanumeric characters, hyphens, or dots"
            }
            Self::AnnouncedIpTooLong => "announced IP must be less than 46 bytes",
            Self::InvalidAnnouncedIp => "announced IP must be an IP address or a valid hostname",
            Self::InvalidIpv4 => "invalid IPv4 address",
            Self::InvalidIpv6 => "invalid IPv6 address",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for EndpointError {}

/// Networking metadata advertised by one cluster node.
/// The base address and ports come from the normal Cluster Bus header. The
/// optional client-only addresses, hostname, human name, and availability zone
/// come from validated extension fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterEndpoints {
    address: ClusterAddress,
    base_ip: String,
    client_ipv4: Option<String>,
    client_ipv6: Option<String>,
    hostname: Option<String>,
    tls_port: u16,
    client_port: u16,
    client_tls_port: u16,
    human_name: Option<String>,
    availability_zone: Option<String>,
}

impl ClusterEndpoints {
    pub fn new(ip: String, tcp_port: u16, bus_port: u16) -> Self {
        Self {
            address: ClusterAddress {
                ip: ip.clone(),
                port: tcp_port,
                cport: bus_port,
            },
            base_ip: ip,
            client_ipv4: None,
            client_ipv6: None,
            hostname: None,
            tls_port: 0,
            client_port: 0,
            client_tls_port: 0,
            human_name: None,
            availability_zone: None,
        }
    }

    pub fn ip(&self) -> &str {
        &self.address.ip
    }

    pub(crate) fn address(&self) -> &ClusterAddress {
        &self.address
    }

    pub fn port(&self) -> u16 {
        self.address.port
    }

    pub fn tls_port(&self) -> u16 {
        self.tls_port
    }

    pub fn bus_port(&self) -> u16 {
        self.address.cport
    }

    pub fn hostname(&self) -> Option<&str> {
        self.hostname.as_deref()
    }

    pub fn human_name(&self) -> Option<&str> {
        self.human_name.as_deref()
    }

    pub fn availability_zone(&self) -> Option<&str> {
        self.availability_zone.as_deref()
    }

    pub fn client_ipv4(&self) -> Option<&str> {
        self.client_ipv4.as_deref()
    }

    pub fn client_ipv6(&self) -> Option<&str> {
        self.client_ipv6.as_deref()
    }

    pub fn client_port(&self) -> u16 {
        self.client_port
    }

    pub fn client_tls_port(&self) -> u16 {
        self.client_tls_port
    }

    pub fn set_ip(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.address.ip =
            Self::validate_announced_ip(value)?.unwrap_or_else(|| self.base_ip.clone());
        Ok(())
    }

    pub(crate) fn update_bus_address(&mut self, ip: String, port: u16, bus_port: u16) {
        self.base_ip.clone_from(&ip);
        self.address = ClusterAddress {
            ip,
            port,
            cport: bus_port,
        };
    }

    pub(crate) fn restore_local_runtime_address(
        &mut self,
        runtime_ip: String,
        runtime_port: u16,
        runtime_bus_port: u16,
    ) {
        self.base_ip = runtime_ip.clone();
        if self.address.ip.is_empty() {
            self.address.ip = runtime_ip;
        }
        self.address.port = runtime_port;
        self.address.cport = runtime_bus_port;
    }

    pub fn set_port(&mut self, port: u16) {
        self.address.port = port;
    }

    pub fn set_tls_port(&mut self, port: u16) {
        self.tls_port = port;
    }

    pub fn set_bus_port(&mut self, port: u16) {
        self.address.cport = port;
    }

    pub fn set_client_port(&mut self, port: u16) {
        self.client_port = port;
    }

    pub fn set_client_tls_port(&mut self, port: u16) {
        self.client_tls_port = port;
    }

    pub fn set_hostname(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.hostname = Self::validate_hostname(value)?;
        Ok(())
    }

    pub fn set_client_ipv4(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.client_ipv4 = Self::validate_ipv4(value)?;
        Ok(())
    }

    pub fn set_client_ipv6(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.client_ipv6 = Self::validate_ipv6(value)?;
        Ok(())
    }

    pub fn set_human_name(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.human_name = Self::validate_aux_value(value)?;
        Ok(())
    }

    pub fn set_availability_zone(&mut self, value: &[u8]) -> Result<(), EndpointError> {
        self.availability_zone = if value.is_empty() {
            None
        } else {
            bytes_to_optional_string(value)?
        };
        Ok(())
    }

    pub fn client_endpoint(&self, transport: ClientTransport) -> AdvertisedEndpoint {
        self.client_endpoint_for_family(
            transport,
            matches!(IpAddr::from_str(&self.address.ip), Ok(IpAddr::V6(_))),
        )
    }

    pub fn client_endpoint_for_family(
        &self,
        transport: ClientTransport,
        prefer_ipv6: bool,
    ) -> AdvertisedEndpoint {
        let host = if prefer_ipv6 {
            self.client_ipv6.as_deref().unwrap_or(&self.address.ip)
        } else {
            self.client_ipv4.as_deref().unwrap_or(&self.address.ip)
        };
        let port = match transport {
            ClientTransport::Tcp => nonzero_or(self.client_port, self.address.port),
            ClientTransport::Tls => nonzero_or(self.client_tls_port, self.tls_port),
        };
        AdvertisedEndpoint {
            host: host.to_string(),
            port,
        }
    }

    pub fn redirect_endpoint(&self, preference: PreferredEndpoint) -> AdvertisedEndpoint {
        self.redirect_endpoint_for_transport(preference, ClientTransport::Tcp)
    }

    pub fn redirect_endpoint_for_transport(
        &self,
        preference: PreferredEndpoint,
        transport: ClientTransport,
    ) -> AdvertisedEndpoint {
        let mut endpoint = self.client_endpoint(transport);
        endpoint.host = match preference {
            PreferredEndpoint::Ip => endpoint.host,
            PreferredEndpoint::Hostname => self.hostname.clone().unwrap_or_else(|| "?".into()),
            PreferredEndpoint::Unknown => String::new(),
        };
        endpoint
    }

    pub fn redirect_endpoint_for_client(
        &self,
        preference: PreferredEndpoint,
        transport: ClientTransport,
        prefer_ipv6: bool,
    ) -> AdvertisedEndpoint {
        let mut endpoint = self.client_endpoint_for_family(transport, prefer_ipv6);
        endpoint.host = match preference {
            PreferredEndpoint::Ip => endpoint.host,
            PreferredEndpoint::Hostname => self.hostname.clone().unwrap_or_else(|| "?".into()),
            PreferredEndpoint::Unknown => String::new(),
        };
        endpoint
    }

    pub fn validate_hostname(value: &[u8]) -> Result<Option<String>, EndpointError> {
        if value.is_empty() {
            return Ok(None);
        }
        if value.len() >= NET_HOST_STR_LEN {
            return Err(EndpointError::HostnameTooLong);
        }
        if !value
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.'))
        {
            return Err(EndpointError::InvalidHostname);
        }
        bytes_to_optional_string(value)
    }

    pub fn validate_announced_ip(value: &[u8]) -> Result<Option<String>, EndpointError> {
        if value.is_empty() {
            return Ok(None);
        }
        if value.len() >= NET_IP_STR_LEN {
            return Err(EndpointError::AnnouncedIpTooLong);
        }
        Self::validate_aux_value(value)?;
        let text = std::str::from_utf8(value).map_err(|_| EndpointError::InvalidUtf8)?;
        if IpAddr::from_str(text).is_err() && Self::validate_hostname(value).is_err() {
            return Err(EndpointError::InvalidAnnouncedIp);
        }
        Ok(Some(text.to_string()))
    }

    pub fn validate_ipv4(value: &[u8]) -> Result<Option<String>, EndpointError> {
        validate_ip_literal::<Ipv4Addr>(value, EndpointError::InvalidIpv4)
    }

    pub fn validate_ipv6(value: &[u8]) -> Result<Option<String>, EndpointError> {
        validate_ip_literal::<Ipv6Addr>(value, EndpointError::InvalidIpv6)
    }

    pub fn validate_aux_value(value: &[u8]) -> Result<Option<String>, EndpointError> {
        if value.is_empty() {
            return Ok(None);
        }
        if value.iter().copied().any(|byte| {
            byte <= b','
                || byte == 0x7f
                || matches!(
                    byte,
                    b';' | b'<'
                        | b'='
                        | b'>'
                        | b'?'
                        | b'@'
                        | b'['
                        | b']'
                        | b'^'
                        | b'{'
                        | b'|'
                        | b'}'
                        | b'~'
                        | b'\\'
                )
        }) {
            return Err(EndpointError::InvalidAuxCharacter);
        }
        bytes_to_optional_string(value)
    }
}

fn nonzero_or(value: u16, fallback: u16) -> u16 {
    if value == 0 {
        fallback
    } else {
        value
    }
}

fn bytes_to_optional_string(value: &[u8]) -> Result<Option<String>, EndpointError> {
    let text = std::str::from_utf8(value).map_err(|_| EndpointError::InvalidUtf8)?;
    Ok(Some(text.to_string()))
}

fn validate_ip_literal<T>(
    value: &[u8],
    invalid_error: EndpointError,
) -> Result<Option<String>, EndpointError>
where
    T: FromStr,
{
    if value.is_empty() {
        return Ok(None);
    }
    let text = std::str::from_utf8(value).map_err(|_| invalid_error.clone())?;
    T::from_str(text).map_err(|_| invalid_error)?;
    Ok(Some(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        AdvertisedEndpoint, ClientTransport, ClusterEndpoints, EndpointError, NodeId, NodeIdError,
        PreferredEndpoint,
    };

    #[test]
    fn node_id_accepts_exactly_40_lowercase_alphanumeric_bytes() {
        let id = NodeId::parse(b"0123456789abcdefghijklmnopqrstuvwxyzabcd").unwrap();

        assert_eq!(id.as_str(), "0123456789abcdefghijklmnopqrstuvwxyzabcd");
    }

    #[test]
    fn node_id_rejects_wrong_length_uppercase_and_punctuation() {
        assert_eq!(
            NodeId::parse(b"0123456789abcdefghijklmnopqrstuvwxyzabc"),
            Err(NodeIdError::WrongLength { actual: 39 })
        );
        assert_eq!(
            NodeId::parse(b"0123456789abcdefghijklmnopqrstuvwxyzabcde"),
            Err(NodeIdError::WrongLength { actual: 41 })
        );
        assert_eq!(
            NodeId::parse(b"0123456789abcdefghijklmnopqrstuvwxyzabcD"),
            Err(NodeIdError::InvalidCharacter {
                index: 39,
                byte: b'D',
            })
        );
        assert_eq!(
            NodeId::parse(b"0123456789abcdefghijklmnopqrstuvwxyzabc-"),
            Err(NodeIdError::InvalidCharacter {
                index: 39,
                byte: b'-',
            })
        );
    }

    #[test]
    fn hostname_validation_matches_cluster_wire_limits() {
        assert_eq!(ClusterEndpoints::validate_hostname(b""), Ok(None));
        assert_eq!(
            ClusterEndpoints::validate_hostname(&vec![b'a'; 255]),
            Ok(Some("a".repeat(255)))
        );
        assert_eq!(
            ClusterEndpoints::validate_hostname(&vec![b'a'; 256]),
            Err(EndpointError::HostnameTooLong)
        );
        for invalid in [b"under_score".as_slice(), b"bad,name", b"bad name"] {
            assert_eq!(
                ClusterEndpoints::validate_hostname(invalid),
                Err(EndpointError::InvalidHostname)
            );
        }
    }

    #[test]
    fn announced_ip_validation_distinguishes_generic_ipv4_and_ipv6_inputs() {
        assert_eq!(ClusterEndpoints::validate_announced_ip(b""), Ok(None));
        assert_eq!(
            ClusterEndpoints::validate_announced_ip(b"192.0.2.10"),
            Ok(Some("192.0.2.10".to_string()))
        );
        assert_eq!(
            ClusterEndpoints::validate_announced_ip(b"2001:db8::10"),
            Ok(Some("2001:db8::10".to_string()))
        );
        assert_eq!(
            ClusterEndpoints::validate_announced_ip(b"cache-1.example.com"),
            Ok(Some("cache-1.example.com".to_string()))
        );
        assert_eq!(
            ClusterEndpoints::validate_announced_ip(b"192.0.2.10:6379"),
            Err(EndpointError::InvalidAnnouncedIp)
        );
        assert_eq!(
            ClusterEndpoints::validate_announced_ip(b"under_score"),
            Err(EndpointError::InvalidAnnouncedIp)
        );

        assert_eq!(ClusterEndpoints::validate_ipv4(b""), Ok(None));
        assert_eq!(
            ClusterEndpoints::validate_ipv4(b"192.0.2.11"),
            Ok(Some("192.0.2.11".to_string()))
        );
        assert_eq!(
            ClusterEndpoints::validate_ipv4(b"2001:db8::11"),
            Err(EndpointError::InvalidIpv4)
        );
        assert_eq!(ClusterEndpoints::validate_ipv6(b""), Ok(None));
        assert_eq!(
            ClusterEndpoints::validate_ipv6(b"2001:db8::12"),
            Ok(Some("2001:db8::12".to_string()))
        );
        assert_eq!(
            ClusterEndpoints::validate_ipv6(b"192.0.2.12"),
            Err(EndpointError::InvalidIpv6)
        );
    }

    #[test]
    fn clearing_announced_ip_restores_the_observed_address() {
        let mut endpoints = ClusterEndpoints::new("127.0.0.1".to_string(), 6379, 16379);

        endpoints.set_ip(b"cache.example.com").unwrap();
        assert_eq!(endpoints.ip(), "cache.example.com");
        endpoints.set_ip(b"").unwrap();

        assert_eq!(endpoints.ip(), "127.0.0.1");
    }

    #[test]
    fn aux_values_reject_cluster_config_delimiters_and_control_bytes() {
        assert_eq!(ClusterEndpoints::validate_aux_value(b""), Ok(None));
        assert_eq!(
            ClusterEndpoints::validate_aux_value(b"node-east-1"),
            Ok(Some("node-east-1".to_string()))
        );
        for invalid in [
            b"bad value".as_slice(),
            b"bad,value",
            b"bad=value",
            b"bad\\value",
            b"bad\nvalue",
        ] {
            assert_eq!(
                ClusterEndpoints::validate_aux_value(invalid),
                Err(EndpointError::InvalidAuxCharacter)
            );
        }
    }

    #[test]
    fn client_endpoint_uses_transport_port_overrides_and_zero_fallbacks() {
        let mut endpoints = ClusterEndpoints::new("127.0.0.1".to_string(), 6379, 16379);
        endpoints.set_tls_port(6380);
        endpoints.set_client_port(7000);
        endpoints.set_client_tls_port(7443);

        assert_eq!(
            endpoints.client_endpoint(ClientTransport::Tcp),
            AdvertisedEndpoint {
                host: "127.0.0.1".to_string(),
                port: 7000,
            }
        );
        assert_eq!(
            endpoints.client_endpoint(ClientTransport::Tls),
            AdvertisedEndpoint {
                host: "127.0.0.1".to_string(),
                port: 7443,
            }
        );

        endpoints.set_client_port(0);
        endpoints.set_client_tls_port(0);
        assert_eq!(endpoints.client_endpoint(ClientTransport::Tcp).port, 6379);
        assert_eq!(endpoints.client_endpoint(ClientTransport::Tls).port, 6380);
    }

    #[test]
    fn client_endpoint_prefers_matching_announced_address_family() {
        let mut ipv4 = ClusterEndpoints::new("127.0.0.1".to_string(), 6379, 16379);
        ipv4.set_client_ipv4(b"192.0.2.20").unwrap();
        ipv4.set_client_ipv6(b"2001:db8::20").unwrap();
        assert_eq!(
            ipv4.client_endpoint(ClientTransport::Tcp).host,
            "192.0.2.20"
        );

        let mut ipv6 = ClusterEndpoints::new("::1".to_string(), 6379, 16379);
        ipv6.set_client_ipv4(b"192.0.2.21").unwrap();
        ipv6.set_client_ipv6(b"2001:db8::21").unwrap();
        assert_eq!(
            ipv6.client_endpoint(ClientTransport::Tcp).host,
            "2001:db8::21"
        );
    }

    #[test]
    fn redirect_endpoint_honors_preference_and_hides_unknown_endpoint() {
        let mut endpoints = ClusterEndpoints::new("127.0.0.1".to_string(), 6379, 16379);
        endpoints.set_hostname(b"cache.example.com").unwrap();

        assert_eq!(
            endpoints.redirect_endpoint(PreferredEndpoint::Ip),
            AdvertisedEndpoint {
                host: "127.0.0.1".to_string(),
                port: 6379,
            }
        );
        assert_eq!(
            endpoints.redirect_endpoint(PreferredEndpoint::Hostname),
            AdvertisedEndpoint {
                host: "cache.example.com".to_string(),
                port: 6379,
            }
        );

        endpoints.set_hostname(b"").unwrap();
        assert_eq!(
            endpoints
                .redirect_endpoint(PreferredEndpoint::Hostname)
                .host,
            "?"
        );
        assert_eq!(
            endpoints.redirect_endpoint(PreferredEndpoint::Unknown).host,
            ""
        );
    }
}
