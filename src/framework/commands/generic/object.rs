use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy)]
pub struct ObjectEncodingLimits {
    pub list_max_listpack_size: i64,
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_value: usize,
}

/// Compute the encoding name as reported by OBJECT ENCODING / DEBUG OBJECT.
/// Lists and legacy hashes use size-based inference. ZSets report their
/// physical encoding so configuration changes cannot downgrade existing trees.
/// Hashes with field TTLs or string references must retain hashtable storage.
pub fn encoding_display_name(
    obj: &Robj,
    limits: ObjectEncodingLimits,
    hash_requires_hashtable: bool,
) -> &'static str {
    match obj {
        Robj::ListQuicklist(l) => l.encoding_name(limits.list_max_listpack_size),
        Robj::HashHashtable(h) => {
            if hash_requires_hashtable {
                return "hashtable";
            }
            crate::foundation::io::store::structure::hash::infer_hash_encoding(
                h,
                limits.hash_max_listpack_entries,
                limits.hash_max_listpack_value,
            )
        }
        _ => obj.encoding_name(),
    }
}

#[command("OBJECT", runtime)]
pub fn object_cmd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(object_reply(runtime, args))
}

fn object_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let (hash_max_listpack_entries, hash_max_listpack_value) = runtime.hash_encoding_limits();
    let limits = ObjectEncodingLimits {
        list_max_listpack_size: runtime.list_max_listpack_size(),
        hash_max_listpack_entries,
        hash_max_listpack_value,
    };
    let sub = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
    match sub.as_str() {
        "encoding" => {
            if args.args.len() != 2 {
                return CommandError::wrong_arity("object|encoding");
            }
            let (obj, hash_requires_hashtable) = {
                let db = match runtime.db_mut() {
                    Some(d) => d,
                    None => return CommandError::db_index_out_of_range(),
                };
                let obj = match db.get(&args.args[1]) {
                    Some(o) => o.clone(),
                    None => return Value::error("ERR no such key"),
                };
                let requires_hashtable = matches!(obj, Robj::HashHashtable(_))
                    && (db.hash_key_has_volatile_fields(&args.args[1])
                        || db.hash_key_has_string_refs(&args.args[1]));
                (obj, requires_hashtable)
            };
            let enc = encoding_display_name(&obj, limits, hash_requires_hashtable);
            Value::bulk(enc.as_bytes().to_vec())
        }
        "refcount" => {
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            match db.get(&args.args[1]) {
                Some(_) => Value::integer(1),
                None => Value::error("ERR no such key"),
            }
        }
        "freq" | "idletime" => {
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            let key = args.args[1].clone();
            let exists = db.exists(&key);
            if !exists {
                return Value::error("ERR no such key");
            }
            let val = if sub == "freq" {
                db.get_lfu_freq(&key) as i64
            } else {
                db.get_lru_idle(&key)
            };
            Value::integer(val)
        }
        "help" => Value::bulk(Bytes::from(
            "OBJECT <subcommand> [<arg> [value] [opt] ...]. See https://valkey.io for details.",
        )),
        _ => Value::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'.",
            sub
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    #[test]
    fn stringref_hash_encoding_reports_hashtable_without_field_ttl() {
        let mut runtime = TestCommandRuntime::new(1, 100);
        runtime
            .db_mut()
            .unwrap()
            .hash_set_with_limits(b"hash", b"field", b"value", 512, 64)
            .unwrap();
        let args = CommandArgs::from_parts(vec!["OBJECT".into(), "ENCODING".into(), "hash".into()]);
        assert_eq!(object_reply(&mut runtime, &args), Value::bulk("listpack"));
        assert!(runtime
            .db_mut()
            .unwrap()
            .set_hash_string_ref(b"hash", b"field", b"value"));
        assert!(!runtime
            .db_mut()
            .unwrap()
            .hash_key_has_volatile_fields(b"hash"));
        assert_eq!(object_reply(&mut runtime, &args), Value::bulk("hashtable"));
        runtime.hash_encoding_limits = (1024, 1024);
        assert_eq!(object_reply(&mut runtime, &args), Value::bulk("hashtable"));
    }
}
