use libc::{c_char, size_t};
use std::ffi::CString;
use std::sync::OnceLock;

use crate::engine::distributed::cluster::node::key_hash_slot;
use crate::engine::extension::module::ValkeyModuleString;

const CLUSTER_SLOTS: usize = 16_384;
const TAG_ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

static CANONICAL_TAGS: OnceLock<Vec<CString>> = OnceLock::new();

fn canonical_tags() -> &'static [CString] {
    CANONICAL_TAGS.get_or_init(|| {
        let mut tags: Vec<Option<CString>> = (0..CLUSTER_SLOTS).map(|_| None).collect();
        let mut remaining = CLUSTER_SLOTS;
        let mut candidate = [0_u8; 3];

        for &first in TAG_ALPHABET {
            candidate[0] = first;
            record_tag(&candidate[..1], &mut tags, &mut remaining);
        }
        for &first in TAG_ALPHABET {
            candidate[0] = first;
            for &second in TAG_ALPHABET {
                candidate[1] = second;
                record_tag(&candidate[..2], &mut tags, &mut remaining);
            }
        }
        'outer: for &first in TAG_ALPHABET {
            candidate[0] = first;
            for &second in TAG_ALPHABET {
                candidate[1] = second;
                for &third in TAG_ALPHABET {
                    candidate[2] = third;
                    record_tag(&candidate, &mut tags, &mut remaining);
                    if remaining == 0 {
                        break 'outer;
                    }
                }
            }
        }

        tags.into_iter()
            .map(|tag| tag.expect("alphanumeric tags up to three bytes cover every cluster slot"))
            .collect()
    })
}

fn record_tag(candidate: &[u8], tags: &mut [Option<CString>], remaining: &mut usize) {
    let slot = key_hash_slot(candidate) as usize;
    if tags[slot].is_some() {
        return;
    }
    tags[slot] = Some(CString::new(candidate).expect("tag alphabet never contains NUL"));
    *remaining -= 1;
}

// SAFETY: `key` must point to `key_len` readable bytes when `key_len` is non-zero.
pub unsafe extern "C" fn ValkeyModule_ClusterKeySlotC(key: *const c_char, key_len: size_t) -> u32 {
    unsafe {
        if key.is_null() {
            return 0;
        }
        key_hash_slot(std::slice::from_raw_parts(key.cast::<u8>(), key_len)) as u32
    }
}

// SAFETY: `key` must point to a live module string.
pub unsafe extern "C" fn ValkeyModule_ClusterKeySlot(key: *mut ValkeyModuleString) -> u32 {
    unsafe {
        if key.is_null() {
            return 0;
        }
        key_hash_slot((*key).as_bytes()) as u32
    }
}

pub extern "C" fn ValkeyModule_ClusterCanonicalKeyNameInSlot(slot: u32) -> *const c_char {
    canonical_tags()
        .get(slot as usize)
        .map_or(std::ptr::null(), |tag| tag.as_ptr())
}
