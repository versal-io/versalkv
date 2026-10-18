use super::node::ClusterNode;

impl ClusterNode {
    /// Return the human-readable node label used in Cluster log messages.
    pub fn display_name(&self) -> String {
        if let Some(name) = self.endpoints.human_name() {
            return name.to_string();
        }

        let ip = self.endpoints.ip();
        if ip.contains(':') {
            format!("[{ip}]:{}", self.endpoints.port())
        } else {
            format!("{ip}:{}", self.endpoints.port())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::distributed::cluster::node::ClusterNode;

    #[test]
    fn display_name_prefers_human_name() {
        let mut node = ClusterNode::new(
            "node".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        node.endpoints.set_human_name(b"cache-east-1").unwrap();

        assert_eq!(node.display_name(), "cache-east-1");
    }

    #[test]
    fn display_name_formats_ipv4_and_ipv6_fallbacks() {
        let ipv4 = ClusterNode::new(
            "node-v4".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        let ipv6 = ClusterNode::new("node-v6".to_string(), "::1".to_string(), 6379, 16379, true);

        assert_eq!(ipv4.display_name(), "127.0.0.1:6379");
        assert_eq!(ipv6.display_name(), "[::1]:6379");
    }
}
