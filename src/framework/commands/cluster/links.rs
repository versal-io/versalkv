use super::with_cluster_state;
use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterLinkView};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use bytes::Bytes;

pub(super) fn cluster_links(ctx: &mut ServerContext) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(state) => state,
        Err(error) => return error,
    };
    let projection = state.projection();
    let debug = projection.debug_snapshot();
    Value::Array(debug.links.iter().map(link_entry).collect())
}

fn link_entry(link: &ClusterLinkView) -> Value {
    let direction = match link.direction {
        ClusterLinkDirection::To => b"to".as_slice(),
        ClusterLinkDirection::From => b"from".as_slice(),
        ClusterLinkDirection::All => unreachable!("ALL is not a real link direction"),
    };
    Value::Map(vec![
        (
            Value::bulk(Bytes::from_static(b"direction")),
            Value::bulk(Bytes::copy_from_slice(direction)),
        ),
        (
            Value::bulk(Bytes::from_static(b"node")),
            Value::bulk(Bytes::copy_from_slice(link.node_id.as_str().as_bytes())),
        ),
        (
            Value::bulk(Bytes::from_static(b"create-time")),
            Value::integer(link.created_at_ms),
        ),
        (
            Value::bulk(Bytes::from_static(b"events")),
            Value::bulk(Bytes::copy_from_slice(link.events.as_str().as_bytes())),
        ),
        (
            Value::bulk(Bytes::from_static(b"send-buffer-allocated")),
            Value::integer(link.send_buffer_allocated as i64),
        ),
        (
            Value::bulk(Bytes::from_static(b"send-buffer-used")),
            Value::integer(link.send_buffer_used as i64),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::cluster_links;
    use crate::engine::distributed::cluster::{
        ClusterLinkDirection, ClusterLinkView, ClusterState, LinkBufferEvent, LinkEvents, NodeId,
    };
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;

    #[test]
    fn cluster_links_renders_only_real_link_views() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        state.observe_link_open(ClusterLinkView::new(
            node_id.clone(),
            ClusterLinkDirection::To,
            123,
            LinkEvents::READ_WRITE,
        ));
        state.observe_link_buffer(
            node_id.as_str(),
            ClusterLinkDirection::To,
            LinkBufferEvent {
                allocated: 4096,
                used: 17,
                events: LinkEvents::READ_WRITE,
            },
        );
        *context.cluster_state_mut() = Some(Box::new(state));

        let Value::Array(links) = cluster_links(&mut context) else {
            panic!("expected links array");
        };
        assert_eq!(links.len(), 1);
        let Value::Map(fields) = &links[0] else {
            panic!("expected link map");
        };
        let value = |name: &[u8]| {
            fields
                .iter()
                .find_map(|(key, value)| {
                    matches!(key, Value::Bulk(key) if key.as_ref() == name).then_some(value)
                })
                .unwrap()
        };
        assert!(matches!(value(b"direction"), Value::Bulk(v) if v.as_ref() == b"to"));
        assert!(
            matches!(value(b"node"), Value::Bulk(v) if v.as_ref() == node_id.as_str().as_bytes())
        );
        assert_eq!(value(b"create-time"), &Value::integer(123));
        assert!(matches!(value(b"events"), Value::Bulk(v) if v.as_ref() == b"rw"));
        assert_eq!(value(b"send-buffer-allocated"), &Value::integer(4096));
        assert_eq!(value(b"send-buffer-used"), &Value::integer(17));
    }
}
