use bytes::Bytes;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct PubSubState {
    channels: HashMap<Bytes, HashSet<usize>>,
    clients: HashMap<usize, HashSet<Bytes>>,
    patterns: HashMap<Bytes, HashSet<usize>>,
    client_patterns: HashMap<usize, HashSet<Bytes>>,
    shard_channels: HashMap<Bytes, HashSet<usize>>,
    shard_clients: HashMap<usize, HashSet<Bytes>>,
}

impl PubSubState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&mut self, token: usize, channel: Bytes) -> usize {
        self.channels
            .entry(channel.clone())
            .or_default()
            .insert(token);
        let set = self.clients.entry(token).or_default();
        set.insert(channel);
        set.len()
    }

    pub fn unsubscribe(&mut self, token: usize, channel: &Bytes) -> usize {
        let remaining = match self.clients.get_mut(&token) {
            Some(set) => {
                set.remove(channel);
                let n = set.len();
                if set.is_empty() {
                    self.clients.remove(&token);
                }
                n
            }
            None => 0,
        };
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.remove(&token);
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
        remaining
    }

    pub fn unsubscribe_all(&mut self, token: usize) -> Vec<Bytes> {
        let channels: Vec<Bytes> = match self.clients.remove(&token) {
            Some(set) => set.into_iter().collect(),
            None => Vec::new(),
        };
        for ch in &channels {
            if let Some(subs) = self.channels.get_mut(ch) {
                subs.remove(&token);
                if subs.is_empty() {
                    self.channels.remove(ch);
                }
            }
        }
        channels
    }

    pub fn remove_client(&mut self, token: usize) {
        self.unsubscribe_all(token);
        self.punsubscribe_all(token);
        self.sunsubscribe_all(token);
    }

    pub fn subscribers(&self, channel: &Bytes) -> Vec<usize> {
        self.channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn psubscribe(&mut self, token: usize, pattern: Bytes) -> usize {
        self.patterns
            .entry(pattern.clone())
            .or_default()
            .insert(token);
        let set = self.client_patterns.entry(token).or_default();
        set.insert(pattern);
        set.len()
    }

    pub fn punsubscribe(&mut self, token: usize, pattern: &Bytes) -> usize {
        let remaining = match self.client_patterns.get_mut(&token) {
            Some(set) => {
                set.remove(pattern);
                let n = set.len();
                if set.is_empty() {
                    self.client_patterns.remove(&token);
                }
                n
            }
            None => 0,
        };
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&token);
            if subs.is_empty() {
                self.patterns.remove(pattern);
            }
        }
        remaining
    }

    pub fn punsubscribe_all(&mut self, token: usize) -> Vec<Bytes> {
        let patterns: Vec<Bytes> = match self.client_patterns.remove(&token) {
            Some(set) => set.into_iter().collect(),
            None => Vec::new(),
        };
        for pat in &patterns {
            if let Some(subs) = self.patterns.get_mut(pat) {
                subs.remove(&token);
                if subs.is_empty() {
                    self.patterns.remove(pat);
                }
            }
        }
        patterns
    }

    pub fn pattern_subscribers(&self, channel: &[u8]) -> Vec<(usize, Bytes)> {
        let mut result = Vec::new();
        for (pattern, tokens) in &self.patterns {
            if pattern_match(pattern, channel) {
                for &token in tokens {
                    result.push((token, pattern.clone()));
                }
            }
        }
        result
    }

    pub fn active_channels(&self) -> Vec<Bytes> {
        self.channels.keys().cloned().collect()
    }

    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn shard_channel_count(&self) -> usize {
        self.shard_channels.len()
    }

    pub fn client_channels(&self, token: usize) -> Vec<Bytes> {
        self.clients
            .get(&token)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn client_patterns_list(&self, token: usize) -> Vec<Bytes> {
        self.client_patterns
            .get(&token)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn client_shard_channels(&self, token: usize) -> Vec<Bytes> {
        self.shard_clients
            .get(&token)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn subscribed_client_count(&self) -> usize {
        let mut clients = HashSet::new();
        clients.extend(self.clients.keys().copied());
        clients.extend(self.client_patterns.keys().copied());
        clients.extend(self.shard_clients.keys().copied());
        clients.len()
    }

    pub fn subscriber_count(&self, channel: &Bytes) -> usize {
        self.channels.get(channel).map(|s| s.len()).unwrap_or(0)
    }

    pub fn ssubscribe(&mut self, token: usize, channel: Bytes) -> usize {
        self.shard_channels
            .entry(channel.clone())
            .or_default()
            .insert(token);
        let set = self.shard_clients.entry(token).or_default();
        set.insert(channel);
        set.len()
    }

    pub fn sunsubscribe(&mut self, token: usize, channel: &Bytes) -> usize {
        let remaining = match self.shard_clients.get_mut(&token) {
            Some(set) => {
                set.remove(channel);
                let n = set.len();
                if set.is_empty() {
                    self.shard_clients.remove(&token);
                }
                n
            }
            None => 0,
        };
        if let Some(subs) = self.shard_channels.get_mut(channel) {
            subs.remove(&token);
            if subs.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
        remaining
    }

    pub fn sunsubscribe_all(&mut self, token: usize) -> Vec<Bytes> {
        let channels: Vec<Bytes> = match self.shard_clients.remove(&token) {
            Some(set) => set.into_iter().collect(),
            None => Vec::new(),
        };
        for ch in &channels {
            if let Some(subs) = self.shard_channels.get_mut(ch) {
                subs.remove(&token);
                if subs.is_empty() {
                    self.shard_channels.remove(ch);
                }
            }
        }
        channels
    }

    pub fn shard_subscribers(&self, channel: &Bytes) -> Vec<usize> {
        self.shard_channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn active_shard_channels(&self) -> Vec<Bytes> {
        self.shard_channels.keys().cloned().collect()
    }

    pub fn shard_subscriber_count(&self, channel: &Bytes) -> usize {
        self.shard_channels
            .get(channel)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

pub fn pattern_match(pattern: &[u8], text: &[u8]) -> bool {
    pattern_match_rec(pattern, 0, text, 0)
}

fn pattern_match_rec(pattern: &[u8], pi: usize, text: &[u8], ti: usize) -> bool {
    let (mut pi, mut ti) = (pi, ti);
    while pi < pattern.len() {
        match pattern[pi] {
            b'*' => {
                let mut pj = pi;
                while pj < pattern.len() && pattern[pj] == b'*' {
                    pj += 1;
                }
                if pj == pattern.len() {
                    return true; //  *
                }
                for tj in ti..=text.len() {
                    if pattern_match_rec(pattern, pj, text, tj) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                if ti >= text.len() {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            b'[' => {
                if ti >= text.len() {
                    return false;
                }
                let c = text[ti];
                pi += 1;
                let mut not = false;
                if pi < pattern.len() && pattern[pi] == b'^' {
                    not = true;
                    pi += 1;
                }
                let mut matched = false;
                loop {
                    if pi >= pattern.len() {
                        return false; //  [
                    }
                    if pattern[pi] == b']' && pi > (if not { 2 } else { 1 }) {
                        pi += 1;
                        break;
                    }
                    let lo = if pattern[pi] == b'\\' && pi + 1 < pattern.len() {
                        pi += 1;
                        pattern[pi]
                    } else {
                        pattern[pi]
                    };
                    pi += 1;
                    let hi =
                        if pi + 1 < pattern.len() && pattern[pi] == b'-' && pattern[pi + 1] != b']'
                        {
                            pi += 1; // consume '-'
                            if pattern[pi] == b'\\' && pi + 1 < pattern.len() {
                                pi += 1;
                            }
                            let ch = pattern[pi];
                            pi += 1;
                            ch
                        } else {
                            lo
                        };
                    if c >= lo && c <= hi {
                        matched = true;
                    }
                }
                if not {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                ti += 1;
            }
            b'\\' => {
                if pi + 1 < pattern.len() {
                    pi += 1;
                }
                if ti >= text.len() || pattern[pi] != text[ti] {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            p => {
                if ti >= text.len() || p != text[ti] {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti == text.len()
}

fn put_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

fn frame_marker(resp3: bool) -> u8 {
    if resp3 {
        b'>'
    } else {
        b'*'
    }
}

pub fn encode_confirm(kind: &[u8], channel: Option<&[u8]>, count: i64, resp3: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(frame_marker(resp3));
    out.extend_from_slice(b"3\r\n");
    put_bulk(&mut out, kind);
    match channel {
        Some(c) => put_bulk(&mut out, c),
        None => out.extend_from_slice(b"$-1\r\n"),
    }
    out.push(b':');
    out.extend_from_slice(count.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

pub fn encode_message(channel: &[u8], payload: &[u8], resp3: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + payload.len());
    out.push(frame_marker(resp3));
    out.extend_from_slice(b"3\r\n");
    put_bulk(&mut out, b"message");
    put_bulk(&mut out, channel);
    put_bulk(&mut out, payload);
    out
}

pub fn encode_pmessage(pattern: &[u8], channel: &[u8], payload: &[u8], resp3: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + payload.len());
    out.push(frame_marker(resp3));
    out.extend_from_slice(b"4\r\n");
    put_bulk(&mut out, b"pmessage");
    put_bulk(&mut out, pattern);
    put_bulk(&mut out, channel);
    put_bulk(&mut out, payload);
    out
}

pub fn encode_smessage(channel: &[u8], payload: &[u8], resp3: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + payload.len());
    out.push(frame_marker(resp3));
    out.extend_from_slice(b"3\r\n");
    put_bulk(&mut out, b"smessage");
    put_bulk(&mut out, channel);
    put_bulk(&mut out, payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn subscribe_counts_are_per_client_and_idempotent() {
        let mut ps = PubSubState::new();
        assert_eq!(ps.subscribe(1, b("chan1")), 1);
        assert_eq!(ps.subscribe(1, b("chan2")), 2);
        assert_eq!(ps.subscribe(1, b("chan1")), 2);
    }

    #[test]
    fn publish_target_set_dedups_by_client() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("chan1"));
        ps.subscribe(1, b("chan1")); //  token
        ps.subscribe(2, b("chan1"));
        let mut subs = ps.subscribers(&b("chan1"));
        subs.sort_unstable();
        assert_eq!(subs, vec![1, 2]);
    }

    #[test]
    fn unsubscribe_updates_counts_and_clears_empty_channels() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("chan1"));
        ps.subscribe(1, b("chan2"));
        assert_eq!(ps.unsubscribe(1, &b("chan1")), 1);
        assert!(ps.subscribers(&b("chan1")).is_empty());
        assert_eq!(ps.subscribers(&b("chan2")), vec![1]);
    }

    #[test]
    fn unsubscribe_non_subscribed_returns_current_count() {
        let mut ps = PubSubState::new();
        assert_eq!(ps.unsubscribe(9, &b("nope")), 0);
    }

    #[test]
    fn unsubscribe_all_returns_all_channels() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("a"));
        ps.subscribe(1, b("b"));
        ps.subscribe(1, b("c"));
        let mut chans = ps.unsubscribe_all(1);
        chans.sort();
        assert_eq!(chans, vec![b("a"), b("b"), b("c")]);
        assert!(ps.subscribers(&b("a")).is_empty());
    }

    #[test]
    fn remove_client_cleans_all_indexes() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("chan1"));
        ps.subscribe(2, b("chan1"));
        ps.remove_client(1);
        assert_eq!(ps.subscribers(&b("chan1")), vec![2]);
    }

    #[test]
    fn subscribed_client_count_dedups_all_subscription_kinds() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("chan"));
        ps.psubscribe(1, b("ch*"));
        ps.ssubscribe(2, b("shard"));

        assert_eq!(ps.subscribed_client_count(), 2);
    }

    #[test]
    fn encode_confirm_resp2_and_resp3() {
        let r2 = encode_confirm(b"subscribe", Some(b"ch"), 1, false);
        assert_eq!(r2, b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");
        let r3 = encode_confirm(b"subscribe", Some(b"ch"), 1, true);
        assert_eq!(r3, b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");
    }

    #[test]
    fn encode_confirm_nil_channel() {
        let out = encode_confirm(b"unsubscribe", None, 0, false);
        assert_eq!(out, b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
    }

    #[test]
    fn encode_message_frame() {
        let out = encode_message(b"chan1", b"hello", false);
        assert_eq!(
            out,
            b"*3\r\n$7\r\nmessage\r\n$5\r\nchan1\r\n$5\r\nhello\r\n"
        );
    }

    // --- pattern_match tests ---

    #[test]
    fn pattern_match_exact() {
        assert!(pattern_match(b"hello", b"hello"));
        assert!(!pattern_match(b"hello", b"world"));
    }

    #[test]
    fn pattern_match_star() {
        assert!(pattern_match(b"h*llo", b"hello"));
        assert!(pattern_match(b"h*o", b"hello"));
        assert!(pattern_match(b"*", b"anything"));
        assert!(pattern_match(b"", b""));
        assert!(!pattern_match(b"h*o", b"hel"));
    }

    #[test]
    fn pattern_match_question() {
        assert!(pattern_match(b"h?llo", b"hello"));
        assert!(pattern_match(b"h?l?o", b"hello"));
        assert!(!pattern_match(b"h??llo", b"hello"));
        assert!(pattern_match(b"?", b"x"));
        assert!(!pattern_match(b"?", b""));
    }

    #[test]
    fn pattern_match_star_and_question() {
        assert!(pattern_match(b"h*l?o", b"hello"));
        assert!(pattern_match(b"h*?o", b"hello"));
    }

    #[test]
    fn pattern_match_keyspace() {
        assert!(pattern_match(b"__keyspace@*__:*", b"__keyspace@0__:foo"));
        assert!(pattern_match(b"__key*@*__:*", b"__keyspace@0__:foo"));
        assert!(!pattern_match(b"__keyspace@*__:*", b"__keyevent@0__:set"));
    }

    #[test]
    fn pattern_match_char_class() {
        assert!(pattern_match(b"h[ae]llo", b"hello"));
        assert!(pattern_match(b"h[ae]llo", b"hallo"));
        assert!(!pattern_match(b"h[ae]llo", b"hbllo"));
        assert!(pattern_match(b"h[a-z]llo", b"hello"));
        assert!(!pattern_match(b"h[a-z]llo", b"hEllo"));
        assert!(pattern_match(b"h[^a-z]llo", b"hEllo"));
        assert!(!pattern_match(b"h[^a-z]llo", b"hello"));
    }

    #[test]
    fn pattern_match_escape() {
        assert!(pattern_match(b"h\\*llo", b"h*llo"));
        assert!(!pattern_match(b"h\\*llo", b"hello"));
        assert!(pattern_match(b"h\\?llo", b"h?llo"));
    }

    #[test]
    fn pattern_match_trailing() {
        assert!(pattern_match(b"hello*", b"hello"));
        assert!(pattern_match(b"hello*", b"hello!"));
        assert!(!pattern_match(b"hello", b"hello!"));
    }

    // --- pattern subscription tests ---

    #[test]
    fn psubscribe_counts_and_idempotent() {
        let mut ps = PubSubState::new();
        assert_eq!(ps.psubscribe(1, b("pat1")), 1);
        assert_eq!(ps.psubscribe(1, b("pat2")), 2);
        assert_eq!(ps.psubscribe(1, b("pat1")), 2);
    }

    #[test]
    fn pattern_subscribers_matches() {
        let mut ps = PubSubState::new();
        ps.psubscribe(1, b("chan*"));
        ps.psubscribe(2, b("chan*"));
        ps.psubscribe(3, b("other*"));

        let mut subs = ps.pattern_subscribers(b"chan1");
        subs.sort_by_key(|(t, _)| *t);
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].0, 1);
        assert_eq!(subs[0].1, b("chan*"));
        assert_eq!(subs[1].0, 2);
        assert_eq!(subs[1].1, b("chan*"));

        let subs2 = ps.pattern_subscribers(b"other100");
        assert_eq!(subs2, vec![(3, b("other*"))]);

        let empty = ps.pattern_subscribers(b"nope");
        assert!(empty.is_empty());
    }

    #[test]
    fn punsubscribe_reduces_count() {
        let mut ps = PubSubState::new();
        ps.psubscribe(1, b("pat1"));
        ps.psubscribe(1, b("pat2"));
        assert_eq!(ps.punsubscribe(1, &b("pat1")), 1);
        assert_eq!(ps.punsubscribe(1, &b("pat2")), 0);
    }

    #[test]
    fn punsubscribe_all_returns_all_patterns() {
        let mut ps = PubSubState::new();
        ps.psubscribe(1, b("a*"));
        ps.psubscribe(1, b("b*"));
        let mut pats = ps.punsubscribe_all(1);
        pats.sort();
        assert_eq!(pats, vec![b("a*"), b("b*")]);
        assert!(ps.pattern_subscribers(&b("apple")).is_empty());
    }

    #[test]
    fn remove_client_cleans_patterns_too() {
        let mut ps = PubSubState::new();
        ps.subscribe(1, b("chan1"));
        ps.psubscribe(1, b("chan*"));
        ps.remove_client(1);
        assert!(ps.subscribers(&b("chan1")).is_empty());
        assert!(ps.pattern_subscribers(&b("chan1")).is_empty());
    }
}
