use bytes::Bytes;

use crate::engine::command::CommandError;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::path_hash::{
    PathHash, PathHashPayload, PathHashPrefixes,
};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;

command_macros::portable_command_support!();

pub fn is_option(value: &[u8], option: &[u8]) -> bool {
    value
        .split(|byte| *byte == 0)
        .next()
        .unwrap_or_default()
        .eq_ignore_ascii_case(option)
}

pub fn parse_long_range(value: &[u8], min: i64) -> Result<usize, Value> {
    let not_integer = || Value::error("ERR value is not an integer or out of range");
    let text = std::str::from_utf8(value).map_err(|_| not_integer())?;
    let parsed = text.parse::<i64>().map_err(|_| not_integer())?;
    if parsed.to_string() != text {
        return Err(not_integer());
    }
    let long_min = std::ffi::c_long::MIN as i128;
    let long_max = std::ffi::c_long::MAX as i128;
    if i128::from(parsed) < long_min || i128::from(parsed) > long_max {
        return Err(Value::error("ERR value is out of range"));
    }
    if parsed < min {
        return Err(Value::error(format!(
            "ERR value is out of range, value must between {min} and {long_max}"
        )));
    }
    usize::try_from(parsed).map_err(|_| Value::error("ERR value is out of range"))
}

pub struct FieldGroup<'a> {
    pub path: &'a Bytes,
    pub fields: &'a [Bytes],
    pub count: usize,
}

pub fn parse_field_groups(args: &[Bytes], has_values: bool) -> Result<Vec<FieldGroup<'_>>, Value> {
    let stride = if has_values { 2 } else { 1 };
    let mut groups = Vec::new();
    let mut index = 1;
    while index < args.len() {
        if args.len() - index < 3 || !is_option(&args[index + 1], b"FIELDS") {
            return Err(CommandError::syntax_error());
        }
        let count = parse_long_range(&args[index + 2], 1)?;
        let fields_index = index + 3;
        if count > (args.len() - fields_index) / stride {
            return Err(CommandError::syntax_error());
        }
        let end = fields_index + count * stride;
        groups.push(FieldGroup {
            path: &args[index],
            fields: &args[fields_index..end],
            count,
        });
        index = end;
    }
    Ok(groups)
}

pub enum ReplyValues<'a> {
    None,
    All,
    Fields(&'a [Bytes]),
}

pub struct MatchOptions<'a> {
    pub has_lengths: bool,
    pub values: ReplyValues<'a>,
    pub count: Option<usize>,
    pub max_path_len: Option<usize>,
}

pub fn parse_match_options(
    args: &[Bytes],
    has_plural_lengths: bool,
    allows_limits: bool,
) -> Result<MatchOptions<'_>, Value> {
    let mut options = MatchOptions {
        has_lengths: false,
        values: ReplyValues::None,
        count: None,
        max_path_len: None,
    };
    let length_option: &[u8] = if has_plural_lengths {
        b"LENGTHS"
    } else {
        b"LENGTH"
    };
    let mut index = 0;
    while index < args.len() {
        let option = &args[index];
        if is_option(option, length_option) && !options.has_lengths {
            options.has_lengths = true;
            index += 1;
        } else if is_option(option, b"WITHVALUES") && matches!(options.values, ReplyValues::None) {
            options.values = ReplyValues::All;
            index += 1;
        } else if is_option(option, b"FIELDS")
            && matches!(options.values, ReplyValues::None)
            && index + 1 < args.len()
        {
            let count = parse_long_range(&args[index + 1], 1)?;
            let fields_index = index + 2;
            if count > args.len() - fields_index {
                return Err(CommandError::syntax_error());
            }
            let end = fields_index + count;
            options.values = ReplyValues::Fields(&args[fields_index..end]);
            index = end;
        } else if allows_limits
            && is_option(option, b"COUNT")
            && options.count.is_none()
            && index + 1 < args.len()
        {
            options.count = Some(parse_long_range(&args[index + 1], 1)?);
            index += 2;
        } else if allows_limits
            && is_option(option, b"MAXPATHLEN")
            && options.max_path_len.is_none()
            && index + 1 < args.len()
        {
            options.max_path_len = Some(parse_long_range(&args[index + 1], 0)?);
            index += 2;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    Ok(options)
}

pub fn read_path_hash<'a>(
    runtime: &'a mut dyn CommandRuntime,
    key: &[u8],
) -> Result<Option<&'a PathHash>, Value> {
    let read_expired_values = runtime.read_expired_values();
    let db = runtime
        .db_mut()
        .ok_or_else(CommandError::db_index_out_of_range)?;
    if read_expired_values {
        match db.get_allow_expired(key) {
            Some(Robj::PathHash(value)) => Ok(Some(value)),
            Some(_) => Err(CommandError::wrong_type()),
            None => Ok(None),
        }
    } else {
        db.get_path_hash(key)
            .map_err(|_| CommandError::wrong_type())
    }
}

pub fn reply_field(payload: Option<&PathHashPayload>, field: &[u8]) -> Value {
    payload
        .and_then(|payload| payload.get(field))
        .map_or_else(Value::null, Value::bulk)
}

pub fn reply_fields(payload: Option<&PathHashPayload>, fields: &[Bytes]) -> Value {
    Value::Array(
        fields
            .iter()
            .map(|field| reply_field(payload, field))
            .collect(),
    )
}

pub fn reply_payload(payload: Option<&PathHashPayload>) -> Value {
    Value::Map(
        payload
            .map(PathHashPayload::entries)
            .unwrap_or_default()
            .into_iter()
            .map(|(field, value)| (Value::bulk(field), Value::bulk(value)))
            .collect(),
    )
}

pub fn reply_match(path: &Bytes, payload: &PathHashPayload, options: &MatchOptions<'_>) -> Value {
    let path_reply = if options.has_lengths {
        Value::integer(path.len() as i64)
    } else {
        Value::bulk(path.clone())
    };
    let values = match &options.values {
        ReplyValues::None => return path_reply,
        ReplyValues::All => reply_payload(Some(payload)),
        ReplyValues::Fields(fields) => reply_fields(Some(payload), fields),
    };
    Value::Array(vec![path_reply, values])
}

pub fn write_outcome(
    db_index: u32,
    key: &Bytes,
    event: &'static str,
    count: usize,
    reply: Value,
) -> CommandOutcome {
    let mut effects = CommandEffects {
        dirty_delta: Some(count as i64),
        skip_propagate: count == 0,
        ..CommandEffects::default()
    };
    if count > 0 {
        effects.changed_keys.push((db_index, key.clone()));
        effects.events.push(CoreEvent {
            kind: event,
            key: key.clone(),
        });
    }
    CommandOutcome { reply, effects }
}

pub struct PrefixMatches<'a> {
    matches: PathHashPrefixes<'a>,
}

impl<'a> PrefixMatches<'a> {
    pub fn new(tree: &'a PathHash, query: &'a [u8], max_path_len: Option<usize>) -> Self {
        Self {
            matches: tree.prefix_matches(query, max_path_len),
        }
    }
}

impl<'a> Iterator for PrefixMatches<'a> {
    type Item = (&'a Bytes, &'a PathHashPayload);

    fn next(&mut self) -> Option<Self::Item> {
        self.matches.next()
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod import_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_apply_length_before_deepest_count() {
        let mut hash = PathHash::new();
        let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
        for path in [b"".as_slice(), b"b", b"bb", b"bbb", b"bb\0", b"ba"] {
            hash.set_pairs(path, &pairs, 512, 64);
        }
        let paths = |query: &[u8], limit, count| {
            PrefixMatches::new(&hash, query, limit)
                .take(count)
                .map(|(path, _)| path.to_vec())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            paths(b"bbbx", None, 2),
            vec![b"bbb".to_vec(), b"bb".to_vec()]
        );
        assert_eq!(paths(b"bbbx", Some(2), 1), vec![b"bb".to_vec()]);
        assert_eq!(paths(b"bbbx", Some(0), 1), vec![Vec::<u8>::new()]);
        assert_eq!(paths(b"bb\0x", None, 1), vec![b"bb\0".to_vec()]);
        assert_eq!(paths(b"", None, 1), vec![Vec::<u8>::new()]);
        assert_eq!(paths(b"x", None, 1), vec![Vec::<u8>::new()]);
    }

    #[test]
    fn prefix_matches_adversarial_growth() {
        for length in [512, 1_024, 2_048] {
            let mut hash = PathHash::new();
            let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
            for index in 0..length {
                let mut path = vec![b'b'; index];
                path.push(b'a');
                hash.set_pairs(&path, &pairs, 512, 64);
            }
            let query = vec![b'b'; length];
            let start = std::time::Instant::now();
            for _ in 0..100 {
                assert!(PrefixMatches::new(&hash, &query, None).next().is_none());
            }
            eprintln!(
                "prefix query: {length}, repeats: 100, elapsed: {:?}",
                start.elapsed()
            );
        }
    }
}
