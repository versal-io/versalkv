use crate::engine::distributed::cluster::node::key_hash_slot;
use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
use std::sync::OnceLock;

const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const MAX_TAG_LEN: usize = 4;

static SLOT_TAGS: OnceLock<Vec<Box<str>>> = OnceLock::new();

pub(super) fn slot_tag(slot: usize) -> &'static str {
    assert!(slot < CLUSTER_SLOTS, "cluster slot out of range: {slot}");
    SLOT_TAGS.get_or_init(build_slot_tags)[slot].as_ref()
}

fn build_slot_tags() -> Vec<Box<str>> {
    let mut tags = vec![None; CLUSTER_SLOTS];
    let mut remaining = CLUSTER_SLOTS;
    let mut candidate = Vec::with_capacity(MAX_TAG_LEN);

    // C uses a generated crc16_slot_table. Build the same O(1) lookup shape
    // once, in shortest-tag order, instead of brute-forcing every response.
    for len in 1..=MAX_TAG_LEN {
        fill_tags(len, &mut candidate, &mut tags, &mut remaining);
        if remaining == 0 {
            break;
        }
    }

    assert_eq!(
        remaining, 0,
        "alphanumeric tags up to length {MAX_TAG_LEN} must cover every cluster slot"
    );
    tags.into_iter()
        .map(|tag| tag.expect("all cluster slots were populated above"))
        .collect()
}

fn fill_tags(
    target_len: usize,
    candidate: &mut Vec<u8>,
    tags: &mut [Option<Box<str>>],
    remaining: &mut usize,
) {
    if *remaining == 0 {
        return;
    }
    if candidate.len() == target_len {
        let slot = key_hash_slot(candidate) as usize;
        if tags[slot].is_none() {
            let tag = String::from_utf8(candidate.clone())
                .expect("slot tag alphabet contains only ASCII")
                .into_boxed_str();
            tags[slot] = Some(tag);
            *remaining -= 1;
        }
        return;
    }

    for &byte in ALPHABET {
        candidate.push(byte);
        fill_tags(target_len, candidate, tags, remaining);
        candidate.pop();
        if *remaining == 0 {
            return;
        }
    }
}
