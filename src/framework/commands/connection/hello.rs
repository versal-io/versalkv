use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::engine::extension::acl::{acl_users, add_acl_log_entry, ACL_DENIED_AUTH, ACL_LOG_CTX_TOPLEVEL};
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use crate::foundation::memory::util_version as version;
use bytes::Bytes;
use command_macros::command;
use std::process;

#[command("HELLO")]
pub fn hello(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let mut proto_version: u8 = conn.resp_version();
    let mut i = 0;
    let mut setname: Option<Bytes> = None;
    let mut auth_user: Option<Bytes> = None;
    let mut auth_pass: Option<Bytes> = None;
    while i < args.args.len() {
        let arg = String::from_utf8_lossy(&args.args[i]).to_ascii_uppercase();
        match arg.as_str() {
            "2" | "3" => {
                if let Ok(v) = std::str::from_utf8(&args.args[i])
                    .unwrap_or("2")
                    .parse::<u8>()
                {
                    if v == 2 || v == 3 {
                        proto_version = v;
                    }
                }
            }
            "AUTH" => {
                if i + 2 < args.args.len() {
                    auth_user = Some(args.args[i + 1].clone());
                    auth_pass = Some(args.args[i + 2].clone());
                    i += 2;
                } else if i + 1 < args.args.len() {
                    auth_pass = Some(args.args[i + 1].clone());
                    i += 1;
                }
            }
            "SETNAME" if i + 1 < args.args.len() => {
                setname = Some(args.args[i + 1].clone());
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    conn.set_resp_version(proto_version);

    // Validate setname character before AUTH (must not contain spaces, newlines, etc.)
    if let Some(ref name) = setname {
        for &b in name.iter() {
            if b == b' ' || b == b'\n' || b == b'\r' || b == b'\0' {
                return Value::error("ERR Client names cannot contain spaces, newlines or special characters.");
            }
        }
    }

    if let Some(pw) = auth_pass {
        let users = acl_users().lock().unwrap();
        let username = auth_user.as_ref().map(|u| String::from_utf8_lossy(u).to_string()).unwrap_or_else(|| "default".to_string());
        let expected = if let Some(user) = users.get(&username) {
            let pw_match = user.check_password(pw.as_ref());
            if pw_match && user.is_on() {
                conn.set_authenticated(true);
                conn.set_ever_authenticated(true);
                conn.set_username(username.clone());
                true
            } else {
                add_acl_log_entry(
                    ACL_DENIED_AUTH,
                    ACL_LOG_CTX_TOPLEVEL,
                    String::from_utf8_lossy(pw.as_ref()).to_string(),
                    username,
                    format!("addr={} cmd=hello", conn.peer_addr()),
                );
                false
            }
        } else {
            add_acl_log_entry(
                ACL_DENIED_AUTH,
                ACL_LOG_CTX_TOPLEVEL,
                String::from_utf8_lossy(pw.as_ref()).to_string(),
                username,
                format!("addr={} cmd=hello", conn.peer_addr()),
            );
            false
        };
        drop(users);
        if !expected {
            return Value::error("WRONGPASS invalid username-password pair or user is disabled.");
        }
    }

    // Apply setname after successful AUTH (so that failed AUTH doesn't change the name)
    if let Some(name) = setname {
        conn.set_name(Some(name));
    }

    let mut arr = vec![Value::bulk(Bytes::from_static(b"server"))];
    if ctx.config().server.extended_redis_compatibility {
        arr.push(Value::bulk(Bytes::from_static(b"redis")));
    } else {
        arr.push(Value::bulk(Bytes::from_static(b"valkey")));
    }
    arr.push(Value::bulk(Bytes::from_static(b"version")));
    let version = if ctx.config().server.extended_redis_compatibility {
        b"7.2.0" as &[u8]
    } else {
        version::REDIS_VERSION.as_bytes()
    };
    arr.push(Value::bulk(Bytes::copy_from_slice(version)));
    arr.push(Value::bulk(Bytes::from_static(b"proto")));
    arr.push(Value::integer(proto_version as i64));
    arr.push(Value::bulk(Bytes::from_static(b"id")));
    arr.push(Value::integer(process::id() as i64));
    arr.push(Value::bulk(Bytes::from_static(b"mode")));
    arr.push(Value::bulk(Bytes::from_static(b"standalone")));
    arr.push(Value::bulk(Bytes::from_static(b"role")));
    arr.push(Value::bulk(Bytes::from_static(b"master")));
    arr.push(Value::bulk(Bytes::from_static(b"modules")));
    arr.push(Value::Array(vec![]));
    if !ctx.config().server.availability_zone.is_empty() {
        arr.push(Value::bulk(Bytes::from_static(b"availability_zone")));
        arr.push(Value::bulk(Bytes::from(
            ctx.config().server.availability_zone.as_bytes().to_vec(),
        )));
    }
    if proto_version >= 3 {
        let mut entries = Vec::with_capacity(arr.len() / 2);
        let mut values = arr.into_iter();
        while let (Some(key), Some(value)) = (values.next(), values.next()) {
            entries.push((key, value));
        }
        Value::Map(entries)
    } else {
        Value::Array(arr)
    }
}
