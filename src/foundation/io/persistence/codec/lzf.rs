const HLOG: u32 = 16;
const HSIZE: usize = 1 << HLOG;
const MAX_LIT: usize = 1 << 5;
const MAX_OFF: usize = 1 << 13;
const MAX_REF: usize = (1 << 8) + (1 << 3);

// The touched-slot table stores each hash slot in u16, so the table must
// never exceed the u16 index range.
const _: () = assert!(HSIZE <= u16::MAX as usize + 1);

#[inline]
fn frst(input: &[u8], p: usize) -> u32 {
    ((input[p] as u32) << 8) | input[p + 1] as u32
}

#[inline]
fn next(h: u32, input: &[u8], p: usize) -> u32 {
    h.wrapping_shl(8) | input[p + 2] as u32
}

#[inline]
fn idx(h: u32) -> usize {
    ((h >> 8).wrapping_sub(h.wrapping_mul(5)) & (HSIZE as u32 - 1)) as usize
}

/// Sole write entry for the LZF hash table. A slot transitioning from the
/// zero sentinel to a real position is recorded for exact reset.
#[inline]
fn set_htab_slot(htab: &mut [usize], touched: &mut Vec<u16>, slot: usize, position: usize) {
    if position != 0 && htab[slot] == 0 {
        touched.push(slot as u16);
    }
    htab[slot] = position;
}

/// Reusable per-serialization-session LZF compression state. The hash table is
/// allocated once and reset through touched slots between independent strings.
pub(crate) struct LzfWorkspace {
    compression_enabled: bool,
    htab: Vec<usize>,
    touched: Vec<u16>,
    scratch: Vec<u8>,
}

impl LzfWorkspace {
    pub(crate) fn new(compression_enabled: bool) -> Self {
        Self {
            compression_enabled,
            htab: if compression_enabled {
                vec![0; HSIZE]
            } else {
                Vec::new()
            },
            touched: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// Restore all-zero table semantics while keeping every allocation.
    fn reset(&mut self) {
        for &slot in &self.touched {
            self.htab[slot as usize] = 0;
        }
        self.touched.clear();
        self.scratch.clear();
    }

    /// Compress one string. The returned slice borrows the scratch buffer and
    /// stays valid until the next call on this workspace.
    pub(crate) fn compress(&mut self, input: &[u8], out_capacity: usize) -> Option<&[u8]> {
        if !self.compression_enabled {
            return None;
        }
        self.reset();
        let len = compress_into(
            input,
            out_capacity,
            &mut self.scratch,
            &mut self.htab,
            &mut self.touched,
        )?;
        Some(&self.scratch[..len])
    }

    #[cfg(test)]
    fn htab_capacity_for_test(&self) -> usize {
        self.htab.capacity()
    }

    #[cfg(test)]
    fn scratch_capacity_for_test(&self) -> usize {
        self.scratch.capacity()
    }

    #[cfg(test)]
    fn touched_len_for_test(&self) -> usize {
        self.touched.len()
    }

    #[cfg(test)]
    fn htab_is_zero_for_test(&self) -> bool {
        self.htab.iter().all(|&value| value == 0)
    }
}

/// Stateless LZF core operating on caller-owned buffers so the workspace can
/// hold the table and output without overlapping mutable self borrows.
fn compress_into(
    input: &[u8],
    out_capacity: usize,
    out: &mut Vec<u8>,
    htab: &mut [usize],
    touched: &mut Vec<u16>,
) -> Option<usize> {
    let in_len = input.len();
    if in_len == 0 || out_capacity == 0 {
        return None;
    }
    out.resize(out_capacity, 0);

    let mut ip = 0usize;
    let mut op = 0usize;
    let mut hval: u32;
    let mut lit: usize;

    lit = 0;
    op += 1; // start run literal

    hval = frst(input, ip);
    while ip < in_len - 2 {
        hval = next(hval, input, ip);
        let slot = idx(hval);
        let refp = htab[slot];
        set_htab_slot(htab, touched, slot, ip);

        let off = ip.wrapping_sub(refp).wrapping_sub(1);
        if refp != 0
            && off < MAX_OFF
            && input[refp + 2] == input[ip + 2]
            && input[refp] == input[ip]
            && input[refp + 1] == input[ip + 1]
        {
            let mut len = 2usize;
            let mut maxlen = in_len - ip - len;
            if maxlen > MAX_REF {
                maxlen = MAX_REF;
            }

            if op + 4 >= out_capacity {
                let adjusted = if lit == 0 { op - 1 } else { op };
                if adjusted + 4 >= out_capacity {
                    return None;
                }
            }

            if lit == 0 {
                op -= 1; // run
            } else {
                out[op - lit - 1] = (lit - 1) as u8; // stop run
            }

            let mut stepped = 0;
            let mut mismatch = false;
            while maxlen > 16 && stepped < 16 {
                len += 1;
                if input[refp + len] != input[ip + len] {
                    mismatch = true;
                    break;
                }
                stepped += 1;
            }
            if !mismatch {
                loop {
                    len += 1;
                    if len >= maxlen || input[refp + len] != input[ip + len] {
                        break;
                    }
                }
            }

            len -= 2; // len #octets - 1
            ip += 1;

            if len < 7 {
                out[op] = ((off >> 8) + (len << 5)) as u8;
                op += 1;
            } else {
                out[op] = ((off >> 8) + (7 << 5)) as u8;
                op += 1;
                out[op] = (len - 7) as u8;
                op += 1;
            }
            out[op] = (off & 0xff) as u8;
            op += 1;

            lit = 0;
            op += 1; // start run

            ip += len + 1;

            if ip >= in_len - 2 {
                break;
            }

            ip -= 1;
            ip -= 1;
            hval = frst(input, ip);
            hval = next(hval, input, ip);
            set_htab_slot(htab, touched, idx(hval), ip);
            ip += 1;

            hval = next(hval, input, ip);
            set_htab_slot(htab, touched, idx(hval), ip);
            ip += 1;
        } else {
            if op >= out_capacity {
                return None;
            }
            lit += 1;
            out[op] = input[ip];
            op += 1;
            ip += 1;

            if lit == MAX_LIT {
                out[op - lit - 1] = (lit - 1) as u8; // stop run
                lit = 0;
                op += 1; // start run
            }
        }
    }

    if op + 3 > out_capacity {
        return None;
    }
    while ip < in_len {
        lit += 1;
        out[op] = input[ip];
        op += 1;
        ip += 1;

        if lit == MAX_LIT {
            out[op - lit - 1] = (lit - 1) as u8;
            lit = 0;
            op += 1;
        }
    }

    if lit == 0 {
        op -= 1; // run
    } else {
        out[op - lit - 1] = (lit - 1) as u8; // end run
    }

    out.truncate(op);
    Some(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::decoder_compact::lzf_decompress;

    #[test]
    fn workspace_reuse_preserves_encoded_bytes_after_failed_attempts() {
        let mut workspace = LzfWorkspace::new(true);
        for _ in 0..100 {
            let input = vec![b'x'; 60];
            assert_eq!(
                workspace.compress(&input, input.len() - 4),
                Some(&[1, 120, 120, 224, 47, 0, 1, 120, 120][..])
            );
            let input: Vec<u8> = (0..20_000).map(|value| (value * 37) as u8).collect();
            assert!(workspace.compress(&input, 12).is_none());
            assert!(workspace
                .compress(b"member-padding-0000000001", 21)
                .is_none());
            let input = b"abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc";
            assert_eq!(
                workspace.compress(input, input.len() - 4),
                Some(&[3, 97, 98, 99, 97, 224, 39, 2, 1, 98, 99][..])
            );
        }
    }

    #[test]
    fn touched_slots_are_unique_and_clear_to_empty() {
        let mut workspace = LzfWorkspace::new(true);
        for slot in 0..HSIZE {
            assert_eq!(workspace.htab[slot], 0);
            set_htab_slot(&mut workspace.htab, &mut workspace.touched, slot, 0);
            assert_eq!(workspace.htab[slot], 0);
            set_htab_slot(&mut workspace.htab, &mut workspace.touched, slot, slot + 1);
            assert_eq!(workspace.htab[slot], slot + 1);
            set_htab_slot(&mut workspace.htab, &mut workspace.touched, slot, slot + 2);
            assert_eq!(workspace.htab[slot], slot + 2);
        }
        assert_eq!(workspace.touched.len(), HSIZE);
        workspace.reset();
        assert!(workspace.touched.is_empty());
        assert!(workspace.htab.iter().all(|position| *position == 0));
    }

    #[test]
    fn concurrent_compressors_have_independent_workspaces() {
        let threads: Vec<_> = (0..4)
            .map(|index| {
                std::thread::spawn(move || {
                    let mut workspace = LzfWorkspace::new(true);
                    for length in [25, 60, 128, 1024, 20_000] {
                        let input = vec![b'a' + index; length];
                        let compressed = workspace.compress(&input, length + 32).unwrap();
                        assert_eq!(lzf_decompress(compressed, length).unwrap(), input);
                        assert!(workspace.compress(&input, 1).is_none());
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[cfg(all(not(feature = "wasm"), not(target_family = "wasm"), not(miri)))]
    #[test]
    fn warmed_short_compression_has_bounded_allocation_cost() {
        let input = b"member-padding-0000000001";
        let mut workspace = LzfWorkspace::new(true);
        let _ = workspace.compress(input, input.len() - 4);
        let (_, cost) = crate::foundation::memory::util_alloc::measure_allocations(|| {
            for _ in 0..100 {
                let _ = workspace.compress(input, input.len() - 4);
            }
        });
        assert!(
            cost.allocated_bytes <= 100 * 128,
            "short compression allocated too much: {cost:?}"
        );
    }

    #[test]
    fn repeated_bytes_compress_and_roundtrip() {
        let input = vec![b'x'; 60];
        let mut ws = LzfWorkspace::new(true);
        let compressed = ws.compress(&input, input.len() - 4).expect("compresses");
        assert_eq!(compressed.len(), 9);
        assert_eq!(lzf_decompress(compressed, input.len()).unwrap(), input);
    }

    #[test]
    fn incompressible_short_input_returns_none() {
        let input: Vec<u8> = (0..24).map(|i| (i * 37) as u8).collect();
        let mut ws = LzfWorkspace::new(true);
        assert!(ws.compress(&input, input.len() - 4).is_none());
    }

    #[test]
    fn roundtrip_varied_pattern() {
        let mut input = Vec::new();
        for i in 0..1000u32 {
            input.extend_from_slice(format!("item{:04}-", i).as_bytes());
        }
        let mut ws = LzfWorkspace::new(true);
        let compressed = ws.compress(&input, input.len() - 4).expect("compresses");
        assert!(compressed.len() < input.len());
        assert_eq!(lzf_decompress(compressed, input.len()).unwrap(), input);
    }

    fn compress_once(input: &[u8]) -> Vec<u8> {
        // Reference semantics: a brand-new workspace starts from an all-zero table.
        let mut ws = LzfWorkspace::new(true);
        ws.compress(input, input.len() - 4)
            .expect("reference compresses")
            .to_vec()
    }

    #[test]
    fn workspace_reuse_matches_fresh_workspace_for_every_string() {
        let inputs: Vec<Vec<u8>> = (0..64)
            .map(|i| {
                // Adjacent strings share three-byte prefixes to exercise table residue.
                format!("shared-prefix-{i:04}-item-tail-{i:04}")
                    .repeat(8)
                    .into_bytes()
            })
            .collect();
        let mut session = LzfWorkspace::new(true);
        let htab_capacity_before = session.htab_capacity_for_test();
        let scratch_capacity_before = session.scratch_capacity_for_test();
        // The scratch buffer starts empty and is sized lazily by the first string.
        assert_eq!(scratch_capacity_before, 0);
        for input in &inputs {
            let reused = session
                .compress(input, input.len() - 4)
                .expect("compresses");
            assert_eq!(reused, compress_once(input).as_slice());
        }
        // The table is allocated once at construction and reused by every
        // string, so its capacity must stay constant across the session.
        assert_eq!(session.htab_capacity_for_test(), htab_capacity_before);
    }

    #[test]
    fn disabled_workspace_allocates_no_table_and_never_compresses() {
        let mut ws = LzfWorkspace::new(false);
        assert_eq!(ws.htab_capacity_for_test(), 0);
        assert!(ws.compress(b"repeated-bytes-repeated-bytes", 20).is_none());
    }

    #[test]
    fn new_table_starts_zero_and_touched_is_stable_across_repeats() {
        let mut ws = LzfWorkspace::new(true);
        // A freshly allocated table is all zero before any compression.
        assert!(ws.htab_is_zero_for_test());
        let input = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let _ = ws.compress(input, input.len() - 4).expect("compresses");
        let first_touched = ws.touched_len_for_test();
        assert!(first_touched > 0 && first_touched <= HSIZE);
        // The same input touches the same slot set; reset must restore an
        // equivalent table so the second pass records an identical slot count.
        let _ = ws.compress(input, input.len() - 4).expect("compresses");
        assert_eq!(ws.touched_len_for_test(), first_touched);
    }

    #[test]
    fn capacity_failure_leaves_a_clean_table_for_the_next_string() {
        let mut ws = LzfWorkspace::new(true);
        // An out_capacity of zero always rejects compression without poisoning state.
        assert!(ws.compress(b"xxxxxxxxxxxxxxxxxxxxxxxx", 0).is_none());
        // A tiny non-zero capacity passes the entry guard, records touched
        // slots, and only then rejects from the mid-stream capacity guard.
        let crowded = b"x".repeat(64);
        assert!(ws.compress(&crowded, 4).is_none());
        assert!(ws.touched_len_for_test() > 0);
        // reset() must clear the partial failure state so the next string is
        // compressed exactly as it would be on a brand-new workspace.
        let normal = b"repeated-pattern-repeated-pattern";
        let reused = ws.compress(normal, normal.len() - 4).expect("compresses");
        assert_eq!(reused, compress_once(normal).as_slice());
    }
}
