const HLOG: u32 = 16;
const HSIZE: usize = 1 << HLOG;
const MAX_LIT: usize = 1 << 5;
const MAX_OFF: usize = 1 << 13;
const MAX_REF: usize = (1 << 8) + (1 << 3);

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

pub(crate) fn lzf_compress(input: &[u8], out_capacity: usize) -> Option<Vec<u8>> {
    let in_len = input.len();
    if in_len == 0 || out_capacity == 0 {
        return None;
    }
    let mut out = vec![0u8; out_capacity];
    let mut htab = vec![0usize; HSIZE];

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
        let refp = htab[slot]; // BIAS=0
        htab[slot] = ip;

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
                op -= 1; //  run
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

            len -= 2; // len  #octets - 1
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
            htab[idx(hval)] = ip;
            ip += 1;

            hval = next(hval, input, ip);
            htab[idx(hval)] = ip;
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
        op -= 1; //  run
    } else {
        out[op - lit - 1] = (lit - 1) as u8; // end run
    }

    out.truncate(op);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::decoder_compact::lzf_decompress;

    #[test]
    fn repeated_bytes_compress_and_roundtrip() {
        let input = vec![b'x'; 60];
        let compressed = lzf_compress(&input, input.len() - 4).expect("compresses");
        assert_eq!(compressed.len(), 9);
        assert_eq!(lzf_decompress(&compressed, input.len()).unwrap(), input);
    }

    #[test]
    fn incompressible_short_input_returns_none() {
        let input: Vec<u8> = (0..24).map(|i| (i * 37) as u8).collect();
        assert!(lzf_compress(&input, input.len() - 4).is_none());
    }

    #[test]
    fn roundtrip_varied_pattern() {
        let mut input = Vec::new();
        for i in 0..1000u32 {
            input.extend_from_slice(format!("item{:04}-", i).as_bytes());
        }
        let compressed = lzf_compress(&input, input.len() - 4).expect("compresses");
        assert!(compressed.len() < input.len());
        assert_eq!(lzf_decompress(&compressed, input.len()).unwrap(), input);
    }
}
