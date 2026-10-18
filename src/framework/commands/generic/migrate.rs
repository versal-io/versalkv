use super::dump::serialize_object_with_hash_field_expires_with_compression;
use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::framework::observability::keyspace::{self, NOTIFY_GENERIC};
use crate::framework::distributed::migration::transport::{Auth, RestoreItem, TransferRequest};
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthArgs {
    Password(Bytes),
    Auth2 { username: Bytes, password: Bytes },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMigrate {
    pub host: Bytes,
    pub port: u16,
    pub key: Bytes,
    pub destination_db: u32,
    pub timeout_ms: u64,
    pub copy: bool,
    pub replace: bool,
    pub auth: Option<AuthArgs>,
    pub keys: Vec<Bytes>,
}

pub fn parse_migrate_args(args: &CommandArgs) -> Result<ParsedMigrate, Value> {
    if args.len() < 5 {
        return Err(CommandError::wrong_arity("migrate"));
    }
    let host = args.args[0].clone();
    let port = parse_utf8::<u16>(&args.args[1], "port")?;
    let key = args.args[2].clone();
    let destination_db = parse_utf8::<u32>(&args.args[3], "destination-db")?;
    let timeout = parse_utf8::<i64>(&args.args[4], "timeout")?;
    let timeout_ms = if timeout > 0 { timeout as u64 } else { 1000 };
    let mut copy = false;
    let mut replace = false;
    let mut auth = None;
    let mut keys = Vec::new();
    let mut i = 5;
    while i < args.len() {
        let option = String::from_utf8_lossy(&args.args[i]).to_ascii_lowercase();
        match option.as_str() {
            "copy" => copy = true,
            "replace" => replace = true,
            "auth" if i + 1 < args.len() => {
                auth = Some(AuthArgs::Password(args.args[i + 1].clone()));
                i += 1;
            }
            "auth2" if i + 2 < args.len() => {
                auth = Some(AuthArgs::Auth2 {
                    username: args.args[i + 1].clone(),
                    password: args.args[i + 2].clone(),
                });
                i += 2;
            }
            "keys" => {
                if !key.is_empty() {
                    return Err(Value::error(
                        "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string",
                    ));
                }
                if i + 1 >= args.len() {
                    return Err(CommandError::syntax_error());
                }
                keys.extend(args.args[i + 1..].iter().cloned());
                break;
            }
            _ => return Err(CommandError::syntax_error()),
        }
        i += 1;
    }
    if keys.is_empty() && key.is_empty() {
        return Err(Value::error(
            "ERR when no keys are specified, the first argument must be a key",
        ));
    }
    Ok(ParsedMigrate {
        host,
        port,
        key,
        destination_db,
        timeout_ms,
        copy,
        replace,
        auth,
        keys,
    })
}

fn parse_utf8<T: std::str::FromStr>(value: &[u8], name: &str) -> Result<T, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| Value::error(format!("ERR {} must be a number", name)))
}

struct PreparedItem {
    key: Bytes,
    ttl_ms: u64,
    payload: Vec<u8>,
}

fn migrate_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let flags = vec!["RW", "access", "delete"];
    if args.len() < 3 {
        return Vec::new();
    }
    let mut first = 2usize;
    let mut num = 1usize;
    if args.len() > 5 {
        let mut i = 5usize;
        while i < args.len() {
            let tok = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
            if tok == "KEYS" {
                if args[2].is_empty() {
                    first = i + 1;
                    num = args.len() - first;
                } else {
                    num = 0;
                }
                break;
            }
            match tok.as_str() {
                "AUTH" => i += 1,
                "AUTH2" => i += 2,
                _ => {}
            }
            i += 1;
        }
    }
    let mut result = Vec::new();
    for k in 0..num {
        let idx = first + k;
        if idx < args.len() {
            result.push((args[idx].as_ref(), flags.clone()));
        }
    }
    result
}

#[command("MIGRATE", get_keys = migrate_get_keys)]
pub fn migrate(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let parsed = match parse_migrate_args(args) {
        Ok(parsed) => parsed,
        Err(error) => return error,
    };
    mark_migrate_parsed(conn);
    let requested = if parsed.keys.is_empty() {
        vec![parsed.key.clone()]
    } else {
        parsed.keys.clone()
    };
    let prepared = match snapshot_items(conn.selected_db(), &requested, ctx) {
        Ok(items) => items,
        Err(error) => return error,
    };
    if prepared.is_empty() {
        return Value::SimpleString(Bytes::from_static(b"NOKEY"));
    }
    let auth = parsed.auth.as_ref().map(|auth| match auth {
        AuthArgs::Password(password) => Auth::Password(password.as_ref()),
        AuthArgs::Auth2 { username, password } => {
            Auth::UserPassword(username.as_ref(), password.as_ref())
        }
    });
    let items: Vec<RestoreItem<'_>> = prepared
        .iter()
        .map(|item| RestoreItem {
            key: item.key.as_ref(),
            ttl_ms: item.ttl_ms,
            payload: item.payload.as_ref(),
        })
        .collect();
    let host = match std::str::from_utf8(&parsed.host) {
        Ok(host) => host,
        Err(_) => return Value::error("ERR host is not valid UTF-8"),
    };
    let request = TransferRequest {
        host,
        port: parsed.port,
        db: parsed.destination_db as i64,
        timeout: Duration::from_millis(parsed.timeout_ms),
        auth,
        replace: parsed.replace,
        items: &items,
    };
    let outcome = match ctx.migrate_sockets_mut().transfer(&request) {
        Ok(outcome) => outcome,
        Err(error) => return Value::error(error),
    };
    let deleted = delete_acknowledged(conn, ctx, &prepared, &outcome.acknowledged, parsed.copy);
    if !deleted.is_empty() {
        conn.set_skip_propagate(false);
        let mut parts: Vec<&[u8]> = vec![b"DEL"];
        parts.extend(deleted.iter().map(Bytes::as_ref));
        conn.record_custom_propagation(encode_command(&parts));
    }
    match outcome.target_error {
        Some(error) => Value::error(format!("ERR Target instance replied with error: {}", error)),
        None => Value::ok(),
    }
}

fn mark_migrate_parsed(conn: &mut dyn CommandConn) {
    conn.set_skip_propagate(true);
}

fn snapshot_items(
    dbid: u32,
    keys: &[Bytes],
    ctx: &mut ServerContext,
) -> Result<Vec<PreparedItem>, Value> {
    let rdb_compression = ctx.config().persistence.rdbcompression;
    let db = ctx
        .dbs_mut()
        .get_mut(dbid)
        .ok_or_else(CommandError::db_index_out_of_range)?;
    let mut prepared = Vec::new();
    for key in keys {
        let (object, _expire, hash_field_expires) = match db.clone_entry(key) {
            Some(entry) => entry,
            None => continue,
        };
        let payload = match serialize_object_with_hash_field_expires_with_compression(
            &object,
            Some(&hash_field_expires),
            rdb_compression,
        ) {
            Some(payload) => payload,
            None => return Err(Value::error("ERR DUMP of this type is not supported yet")),
        };
        let ttl = db.ttl_ms(key);
        if ttl == 0 {
            continue;
        }
        let ttl_ms = if ttl < 0 { 0 } else { (ttl as u64).max(1) };
        prepared.push(PreparedItem {
            key: key.clone(),
            ttl_ms,
            payload,
        });
    }
    Ok(prepared)
}

fn delete_acknowledged(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
    prepared: &[PreparedItem],
    acknowledged: &[bool],
    copy: bool,
) -> Vec<Bytes> {
    if copy {
        return Vec::new();
    }
    let mut deleted = Vec::new();
    let db_idx = conn.selected_db();
    {
        let db = match ctx.db_mut(db_idx) {
            Some(db) => db,
            None => return Vec::new(),
        };
        for (index, item) in prepared.iter().enumerate() {
            if acknowledged.get(index).copied().unwrap_or(false) && db.del(&item.key) {
                conn.record_dirty_key((db_idx, item.key.clone()));
                deleted.push(item.key.clone());
            }
        }
    }
    for key in &deleted {
        keyspace::notify_keyspace_event(conn, ctx, NOTIFY_GENERIC, "del", db_idx, key);
    }
    deleted
}

#[cfg(test)]
mod tests {
    use super::{mark_migrate_parsed, parse_migrate_args, snapshot_items, AuthArgs};
    use crate::framework::context::ServerContext;
    use crate::framework::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use bytes::Bytes;
    use std::net::SocketAddr;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn empty_key_requires_keys_option() {
        assert!(
            parse_migrate_args(&args(&[b"MIGRATE", b"host", b"6379", b"", b"0", b"100",])).is_err()
        );
    }

    #[test]
    fn keys_option_requires_empty_key_argument() {
        assert_eq!(
            parse_migrate_args(&args(&[
                b"MIGRATE", b"host", b"6379", b"key", b"0", b"100", b"KEYS", b"other",
            ])),
            Err(Value::error(
                "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string",
            ))
        );
    }

    #[test]
    fn parses_auth2_credentials() {
        let parsed = parse_migrate_args(&args(&[
            b"MIGRATE",
            b"host",
            b"6379",
            b"key",
            b"0",
            b"100",
            b"AUTH2",
            b"user",
            b"password",
        ]))
        .unwrap();
        assert_eq!(
            parsed.auth,
            Some(AuthArgs::Auth2 {
                username: Bytes::from_static(b"user"),
                password: Bytes::from_static(b"password"),
            })
        );
    }

    #[test]
    fn parses_copy_and_replace_flags() {
        let parsed = parse_migrate_args(&args(&[
            b"MIGRATE", b"host", b"6379", b"key", b"0", b"100", b"COPY", b"REPLACE",
        ]))
        .unwrap();
        assert!(parsed.copy);
        assert!(parsed.replace);
    }

    #[test]
    fn normalizes_non_positive_timeout_to_one_second() {
        let zero =
            parse_migrate_args(&args(&[b"MIGRATE", b"host", b"6379", b"key", b"0", b"0"])).unwrap();
        let negative =
            parse_migrate_args(&args(&[b"MIGRATE", b"host", b"6379", b"key", b"0", b"-1"]))
                .unwrap();
        assert_eq!(zero.timeout_ms, 1000);
        assert_eq!(negative.timeout_ms, 1000);
    }

    #[test]
    fn parsed_migrate_suppresses_default_propagation() {
        let peer: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut conn = Connection::new(Token(0), peer, false);
        mark_migrate_parsed(&mut conn);
        assert!(conn.skip_propagation());
    }

    #[test]
    fn migrate_snapshot_preserves_hash_field_expiry_metadata() {
        let mut ctx = ServerContext::new(1);
        let expires_at = crate::foundation::io::store::kv_engine::now_ms() + 60_000;
        let db = ctx.db_mut(0).unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        db.set_hash_field_expire(b"hash", b"field", expires_at);

        let items = snapshot_items(0, &[Bytes::from_static(b"hash")], &mut ctx).unwrap();
        assert_eq!(items.len(), 1);
        let body = &items[0].payload[..items[0].payload.len() - 10];
        let mut pos = 0;
        let (_, field_expires) =
            crate::foundation::io::persistence::load_typed_object_with_hash_field_expires(
                body, &mut pos, None,
            )
            .unwrap();

        assert_eq!(field_expires.get(b"field".as_slice()), Some(&expires_at));
    }
}
