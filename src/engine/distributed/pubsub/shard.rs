use super::PubSubState;
use bytes::Bytes;
use std::collections::HashSet;

impl PubSubState {
    pub fn sunsubscribe_slots(&mut self, slots: &[u16]) -> Vec<(usize, Bytes, usize)> {
        let slots = slots.iter().copied().collect::<HashSet<_>>();
        let mut matches = Vec::new();
        for channel in self.active_shard_channels() {
            if slots.contains(&crate::engine::distributed::cluster::node::key_hash_slot(
                &channel,
            )) {
                for token in self.shard_subscribers(&channel) {
                    matches.push((token, channel.clone()));
                }
            }
        }
        let mut removed = Vec::with_capacity(matches.len());
        for (token, channel) in matches {
            let remaining = self.sunsubscribe(token, &channel);
            removed.push((token, channel, remaining));
        }
        removed.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_only_matching_shard_channels() {
        let mut pubsub = PubSubState::new();
        pubsub.ssubscribe(4, Bytes::from_static(b"ch3"));
        pubsub.ssubscribe(4, Bytes::from_static(b"ch7"));
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"ch3") as usize;

        let removed = pubsub.sunsubscribe_slots(&[slot as u16]);

        assert_eq!(removed, vec![(4, Bytes::from_static(b"ch3"), 1)]);
        assert_eq!(
            pubsub.client_shard_channels(4),
            vec![Bytes::from_static(b"ch7")]
        );
    }
}
