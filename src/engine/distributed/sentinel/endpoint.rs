use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentinelEndpoint<'a> {
    pub host: &'a str,
    pub port: u16,
}

pub fn format_sentinel_endpoint(host: &str, port: u16) -> String {
    let bare_host = strip_ipv6_brackets(host);
    if let Ok(ip) = bare_host.parse::<IpAddr>() {
        SocketAddr::new(ip, port).to_string()
    } else {
        format!("{}:{}", host, port)
    }
}

pub fn split_sentinel_endpoint(addr: &str) -> Option<SentinelEndpoint<'_>> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    Some(SentinelEndpoint {
        host: strip_ipv6_brackets(host),
        port,
    })
}

fn strip_ipv6_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
}

#[cfg(test)]
mod tests {
    use super::{format_sentinel_endpoint, split_sentinel_endpoint, SentinelEndpoint};

    #[test]
    fn formats_ipv4_ipv6_and_hostname_endpoints() {
        assert_eq!(
            format_sentinel_endpoint("127.0.0.1", 6379),
            "127.0.0.1:6379"
        );
        assert_eq!(format_sentinel_endpoint("::1", 6379), "[::1]:6379");
        assert_eq!(format_sentinel_endpoint("[::1]", 6379), "[::1]:6379");
        assert_eq!(
            format_sentinel_endpoint("cache.local", 6379),
            "cache.local:6379"
        );
    }

    #[test]
    fn splits_internal_endpoints_to_bare_host_and_port() {
        assert_eq!(
            split_sentinel_endpoint("[::1]:6379"),
            Some(SentinelEndpoint {
                host: "::1",
                port: 6379
            })
        );
        assert_eq!(
            split_sentinel_endpoint("cache.local:6379"),
            Some(SentinelEndpoint {
                host: "cache.local",
                port: 6379,
            })
        );
    }
}
