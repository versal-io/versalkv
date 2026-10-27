use bytes::Bytes;

pub(crate) fn set_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let Some(key) = args.first() else {
        return Vec::new();
    };
    let flags = if args
        .iter()
        .skip(2)
        .any(|argument| argument.eq_ignore_ascii_case(b"get"))
    {
        vec!["RW", "access", "update"]
    } else {
        vec!["OW", "update"]
    };
    vec![(key.as_ref(), flags)]
}

pub(crate) fn msetex_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 0, 1, 2, args, &["OW", "update"], &[])
}

pub(crate) fn numkeys_ro_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 0, 1, 1, args, &["RO", "access"], &[])
}

pub(crate) fn zsetstore_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(Some(0), 1, 2, 1, args, &["RO", "access"], &["OW", "update"])
}

pub(crate) fn mpop_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    generic_numkeys_get_keys(None, 0, 1, 1, args, &["RW", "access", "delete"], &[])
}

pub(crate) fn georadius_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let Some(source) = args.first() else {
        return Vec::new();
    };
    let mut result = vec![(source.as_ref(), vec!["RO", "access"])];
    let mut index = 4;
    let mut destination = None;
    while index + 1 < args.len() {
        if args[index].eq_ignore_ascii_case(b"store")
            || args[index].eq_ignore_ascii_case(b"storedist")
        {
            destination = Some(args[index + 1].as_ref());
            index += 2;
        } else {
            index += 1;
        }
    }
    if let Some(destination) = destination {
        result.push((destination, vec!["OW", "update"]));
    }
    result
}

fn generic_numkeys_get_keys<'a>(
    store_key_index: Option<usize>,
    key_count_index: usize,
    first_key_index: usize,
    key_step: usize,
    args: &'a [Bytes],
    key_flags: &[&'static str],
    store_flags: &[&'static str],
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    let Some(count) = args
        .get(key_count_index)
        .and_then(|value| std::str::from_utf8(value).ok()?.parse::<usize>().ok())
        .filter(|count| *count > 0)
    else {
        return Vec::new();
    };
    if first_key_index > args.len()
        || key_step == 0
        || count > (args.len() - first_key_index) / key_step
    {
        return Vec::new();
    }
    let mut keys = Vec::with_capacity(count + usize::from(store_key_index.is_some()));
    if let Some(index) = store_key_index.filter(|index| *index < args.len()) {
        keys.push((args[index].as_ref(), store_flags.to_vec()));
    }
    for offset in 0..count {
        let index = first_key_index + offset * key_step;
        keys.push((args[index].as_ref(), key_flags.to_vec()));
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::zsetstore_get_keys;
    use bytes::Bytes;

    #[test]
    fn zsetstore_preserves_destination_and_source_flags() {
        let args = [
            Bytes::from_static(b"dest"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"left"),
            Bytes::from_static(b"right"),
        ];
        assert_eq!(
            zsetstore_get_keys(&args),
            vec![
                (b"dest".as_slice(), vec!["OW", "update"]),
                (b"left".as_slice(), vec!["RO", "access"]),
                (b"right".as_slice(), vec!["RO", "access"]),
            ]
        );
    }
}
