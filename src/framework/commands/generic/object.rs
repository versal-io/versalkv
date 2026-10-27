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
pub fn encoding_display_name(
    obj: &Robj,
    limits: ObjectEncodingLimits,
    hash_has_volatile: bool,
) -> &'static str {
    match obj {
        Robj::ListQuicklist(l) => l.encoding_name(limits.list_max_listpack_size),
        Robj::HashHashtable(h) => {
            let enc = crate::foundation::io::store::structure::hash::infer_hash_encoding(
                h,
                limits.hash_max_listpack_entries,
                limits.hash_max_listpack_value,
            );
            if enc == "listpack" && hash_has_volatile {
                "hashtable"
            } else {
                enc
            }
        }
        _ => obj.encoding_name(),
    }
}

#[command("OBJECT", runtime)]
pub fn object_cmd(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
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
            let (obj, hash_has_volatile) = {
                let db = match runtime.db_mut() {
                    Some(d) => d,
                    None => return CommandError::db_index_out_of_range(),
                };
                let obj = match db.get(&args.args[1]) {
                    Some(o) => o.clone(),
                    None => return Value::error("ERR no such key"),
                };
                let volatile = matches!(obj, Robj::HashHashtable(_))
                    && db.hash_key_has_volatile_fields(&args.args[1]);
                (obj, volatile)
            };
            let enc = encoding_display_name(&obj, limits, hash_has_volatile);
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
