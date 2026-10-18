//! Runtime-owned Cluster Bus connection state projected into the engine.

use super::{ClusterLinkDirection, ClusterState, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkState {
    Connected,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkEvents {
    pub readable: bool,
    pub writable: bool,
}

impl LinkEvents {
    pub const READABLE: Self = Self {
        readable: true,
        writable: false,
    };
    pub const READ_WRITE: Self = Self {
        readable: true,
        writable: true,
    };

    pub fn as_str(self) -> &'static str {
        match (self.readable, self.writable) {
            (true, true) => "rw",
            (true, false) => "r",
            (false, true) => "w",
            (false, false) => "",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterLinkView {
    pub node_id: NodeId,
    pub direction: ClusterLinkDirection,
    pub created_at_ms: i64,
    pub last_read_ms: i64,
    pub last_write_ms: i64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub send_buffer_allocated: usize,
    pub send_buffer_used: usize,
    pub events: LinkEvents,
    pub state: LinkState,
    pub endpoint: Option<(String, u16)>,
    pub temporary_handshake: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkIoEvent {
    pub read_at_ms: Option<i64>,
    pub wrote_at_ms: Option<i64>,
    pub total_bytes_received: u64,
    pub total_bytes_sent: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkBufferEvent {
    pub allocated: usize,
    pub used: usize,
    pub events: LinkEvents,
}

impl ClusterLinkView {
    pub fn new(
        node_id: NodeId,
        direction: ClusterLinkDirection,
        created_at_ms: i64,
        events: LinkEvents,
    ) -> Self {
        Self {
            node_id,
            direction,
            created_at_ms,
            last_read_ms: 0,
            last_write_ms: 0,
            bytes_received: 0,
            bytes_sent: 0,
            send_buffer_allocated: 0,
            send_buffer_used: 0,
            events,
            state: LinkState::Connected,
            endpoint: None,
            temporary_handshake: false,
        }
    }

    pub fn with_endpoint(mut self, host: String, bus_port: u16) -> Self {
        self.endpoint = Some((host, bus_port));
        self
    }

    pub fn with_temporary_handshake(mut self, temporary_handshake: bool) -> Self {
        self.temporary_handshake = temporary_handshake;
        self
    }
}

impl ClusterState {
    pub fn observe_link_open(&mut self, view: ClusterLinkView) {
        let key = (view.node_id.to_string(), view.direction);
        self.link_views.insert(key, view);
        self.debug_assert_link_invariants();
    }

    pub fn observe_link_identity(&mut self, previous_node_id: Option<&str>, view: ClusterLinkView) {
        if previous_node_id == Some(view.node_id.as_str()) {
            let key = (view.node_id.to_string(), view.direction);
            if let Some(existing) = self.link_views.get_mut(&key) {
                existing.endpoint = view.endpoint;
                existing.temporary_handshake = view.temporary_handshake;
            } else {
                self.link_views.insert(key, view);
            }
            return;
        }
        if let Some(previous_node_id) = previous_node_id {
            self.link_views
                .remove(&(previous_node_id.to_string(), view.direction));
        }
        self.observe_link_open(view);
    }

    pub fn observe_link_io(
        &mut self,
        node_id: &str,
        direction: ClusterLinkDirection,
        event: LinkIoEvent,
    ) {
        let Some(view) = self.link_views.get_mut(&(node_id.to_string(), direction)) else {
            return;
        };
        if let Some(last_read_ms) = event.read_at_ms {
            view.last_read_ms = view.last_read_ms.max(last_read_ms);
        }
        if let Some(last_write_ms) = event.wrote_at_ms {
            view.last_write_ms = view.last_write_ms.max(last_write_ms);
        }
        view.bytes_received = view.bytes_received.max(event.total_bytes_received);
        view.bytes_sent = view.bytes_sent.max(event.total_bytes_sent);
    }

    pub fn observe_link_buffer(
        &mut self,
        node_id: &str,
        direction: ClusterLinkDirection,
        event: LinkBufferEvent,
    ) {
        let Some(view) = self.link_views.get_mut(&(node_id.to_string(), direction)) else {
            return;
        };
        view.send_buffer_allocated = event.allocated;
        view.send_buffer_used = event.used;
        view.events = event.events;
    }

    pub fn observe_link_close(&mut self, node_id: &str, direction: ClusterLinkDirection) {
        match direction {
            ClusterLinkDirection::To | ClusterLinkDirection::From => {
                self.link_views.remove(&(node_id.to_string(), direction));
            }
            ClusterLinkDirection::All => {
                self.link_views
                    .retain(|(known_node_id, _), _| known_node_id != node_id);
            }
        }
        let peer_is_disconnected = !self
            .link_views
            .keys()
            .any(|(known_node_id, _)| known_node_id == node_id);
        if peer_is_disconnected {
            if let Ok(peer) = NodeId::parse(node_id.as_bytes()) {
                let mut manual = std::mem::take(&mut self.manual_failover);
                let actions = manual.on_peer_disconnect(&peer);
                self.manual_failover = manual;
                self.queue_cluster_actions(actions);
            }
        }
        self.debug_assert_link_invariants();
    }

    pub fn link_views(&self) -> impl Iterator<Item = &ClusterLinkView> {
        self.link_views.values()
    }

    pub(crate) fn has_connected_link(
        &self,
        node_id: &str,
        direction: ClusterLinkDirection,
    ) -> bool {
        self.link_views
            .get(&(node_id.to_string(), direction))
            .is_some_and(|view| view.state == LinkState::Connected)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterLinkView, LinkBufferEvent, LinkEvents, LinkIoEvent, LinkState};
    use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState, NodeId};

    #[test]
    fn directional_views_track_independent_lifecycles() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parsed_node_id = NodeId::parse(node_id.as_bytes()).unwrap();

        state.observe_link_open(ClusterLinkView::new(
            parsed_node_id.clone(),
            ClusterLinkDirection::To,
            100,
            LinkEvents::READABLE,
        ));
        state.observe_link_open(ClusterLinkView::new(
            parsed_node_id,
            ClusterLinkDirection::From,
            200,
            LinkEvents::READ_WRITE,
        ));
        state.observe_link_io(
            node_id,
            ClusterLinkDirection::To,
            LinkIoEvent {
                wrote_at_ms: Some(250),
                total_bytes_sent: 17,
                ..LinkIoEvent::default()
            },
        );
        state.observe_link_io(
            node_id,
            ClusterLinkDirection::From,
            LinkIoEvent {
                read_at_ms: Some(300),
                total_bytes_received: 29,
                ..LinkIoEvent::default()
            },
        );
        state.observe_link_buffer(
            node_id,
            ClusterLinkDirection::To,
            LinkBufferEvent {
                allocated: 4096,
                used: 31,
                events: LinkEvents::READ_WRITE,
            },
        );

        let mut views: Vec<_> = state.link_views().collect();
        views.sort_unstable_by_key(|view| view.direction);
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].direction, ClusterLinkDirection::To);
        assert_eq!(views[0].created_at_ms, 100);
        assert_eq!(views[0].last_write_ms, 250);
        assert_eq!(views[0].bytes_sent, 17);
        assert_eq!(views[0].send_buffer_allocated, 4096);
        assert_eq!(views[0].send_buffer_used, 31);
        assert_eq!(views[0].events, LinkEvents::READ_WRITE);
        assert_eq!(views[1].direction, ClusterLinkDirection::From);
        assert_eq!(views[1].created_at_ms, 200);
        assert_eq!(views[1].last_read_ms, 300);
        assert_eq!(views[1].bytes_received, 29);
        assert_eq!(views[1].state, LinkState::Connected);

        state.observe_link_close(node_id, ClusterLinkDirection::To);

        let remaining: Vec<_> = state.link_views().collect();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].direction, ClusterLinkDirection::From);
    }

    #[test]
    fn closing_the_last_peer_link_releases_manual_failover_pause() {
        use crate::engine::distributed::cluster::action::ClusterAction;

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let peer = NodeId::parse(node_id.as_bytes()).unwrap();
        state
            .manual_failover
            .accept_on_primary(peer.clone(), 1_000, 5_000);
        state.observe_link_open(ClusterLinkView::new(
            peer,
            ClusterLinkDirection::To,
            100,
            LinkEvents::READABLE,
        ));

        state.observe_link_close(node_id, ClusterLinkDirection::To);

        assert_eq!(
            state.take_manual_failover_actions(),
            vec![ClusterAction::ResumeWrites { timed_out: false }]
        );
    }
}
