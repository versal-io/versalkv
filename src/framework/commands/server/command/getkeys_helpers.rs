
use bytes::Bytes;

pub(crate) fn generic_numkeys_get_keys<'a>(
    store_key_ofs: Option<usize>,
    key_count_ofs: usize,
    first_key_ofs: usize,
    key_step: usize,
    args: &'a [Bytes],
    key_flags: &[&'static str],
    store_flags: &[&'static str],
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    if key_step == 0 || key_count_ofs >= args.len() {
        return Vec::new();
    }
    let num = match std::str::from_utf8(&args[key_count_ofs])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => return Vec::new(),
    };
    if first_key_ofs > args.len()
        || num < 1
        || num as usize > (args.len() - first_key_ofs) / key_step
    {
        return Vec::new();
    }
    let num = num as usize;
    let mut result = Vec::with_capacity(num + store_key_ofs.is_some() as usize);
    // Key-specs list the store/destination key first (its begin_search index is
    // lower than the source keys), and COMMAND GETKEYS renders in key-spec order.
    // Mirror that here rather than the legacy genericGetKeys order (store last).
    if let Some(ofs) = store_key_ofs {
        if ofs < args.len() {
            result.push((args[ofs].as_ref(), store_flags.to_vec()));
        }
    }
    for i in 0..num {
        let idx = first_key_ofs + i * key_step;
        if idx < args.len() {
            result.push((args[idx].as_ref(), key_flags.to_vec()));
        }
    }
    result
}

pub(crate) fn eval_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 1, 2, 1, args, &["RW", "access", "update"], &[])
}

pub(crate) fn eval_ro_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 1, 2, 1, args, &["RO", "access"], &[])
}

pub(crate) fn bmpop_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 1, 2, 1, args, &["RW", "access", "delete"], &[])
}

pub(crate) fn xread_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let mut streams_pos: Option<usize> = None;
    let mut i = 0usize;
    while i < args.len() {
        let arg = &args[i];
        if arg.eq_ignore_ascii_case(b"block") || arg.eq_ignore_ascii_case(b"count") {
            i += 1; //
        } else if arg.eq_ignore_ascii_case(b"group") {
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"noack") {
        } else if arg.eq_ignore_ascii_case(b"streams") {
            streams_pos = Some(i);
            break;
        } else {
            break; //
        }
        i += 1;
    }
    let streams_pos = match streams_pos {
        Some(p) => p,
        None => return Vec::new(),
    };
    let num = args.len() - streams_pos - 1;
    if num == 0 || !num.is_multiple_of(2) {
        return Vec::new();
    }
    let num = num / 2; //  key ID
    let mut result = Vec::with_capacity(num);
    for j in 0..num {
        let idx = streams_pos + 1 + j;
        if idx < args.len() {
            result.push((args[idx].as_ref(), vec!["RO", "access"]));
        }
    }
    result
}
