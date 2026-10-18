use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum TrackingPolicy {
    #[default]
    Default,
    OptIn,
    OptOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrackingRequest {
    enabled: bool,
    broadcast: bool,
    policy: TrackingPolicy,
    noloop: bool,
    redirect: Option<usize>,
    prefixes: Vec<Bytes>,
}

impl TrackingRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 2 {
            return Err(CommandError::wrong_arity("client"));
        }
        let enabled = if args.args[1].eq_ignore_ascii_case(b"on") {
            true
        } else if args.args[1].eq_ignore_ascii_case(b"off") {
            false
        } else {
            return Err(CommandError::syntax_error());
        };
        let mut request = Self {
            enabled,
            broadcast: false,
            policy: TrackingPolicy::Default,
            noloop: false,
            redirect: None,
            prefixes: Vec::new(),
        };
        let mut index = 2;
        while index < args.args.len() {
            index = request.parse_option(args, index)?;
        }
        request.validate()?;
        Ok(request)
    }

    fn parse_option(&mut self, args: &CommandArgs, index: usize) -> Result<usize, Value> {
        let option = &args.args[index];
        if option.eq_ignore_ascii_case(b"redirect") {
            let value = args.args.get(index + 1).ok_or_else(CommandError::syntax_error)?;
            self.redirect = Some(parse_redirect(value)?);
            Ok(index + 2)
        } else if option.eq_ignore_ascii_case(b"prefix") {
            let value = args.args.get(index + 1).ok_or_else(CommandError::syntax_error)?;
            self.prefixes.push(value.clone());
            Ok(index + 2)
        } else {
            self.apply_flag(option)?;
            Ok(index + 1)
        }
    }

    fn apply_flag(&mut self, option: &[u8]) -> Result<(), Value> {
        if option.eq_ignore_ascii_case(b"bcast") {
            self.broadcast = true;
        } else if option.eq_ignore_ascii_case(b"optin") {
            self.set_policy(TrackingPolicy::OptIn)?;
        } else if option.eq_ignore_ascii_case(b"optout") {
            self.set_policy(TrackingPolicy::OptOut)?;
        } else if option.eq_ignore_ascii_case(b"noloop") {
            self.noloop = true;
        } else if !option.eq_ignore_ascii_case(b"noconn")
            && !option.eq_ignore_ascii_case(b"noprefix")
        {
            return Err(CommandError::syntax_error());
        }
        Ok(())
    }

    fn set_policy(&mut self, policy: TrackingPolicy) -> Result<(), Value> {
        if self.policy != TrackingPolicy::Default && self.policy != policy {
            return Err(Value::error("ERR OPTIN and OPTOUT are not compatible"));
        }
        self.policy = policy;
        Ok(())
    }

    fn validate(&self) -> Result<(), Value> {
        if self.broadcast && self.policy != TrackingPolicy::Default {
            return Err(Value::error(
                "ERR OPTIN and OPTOUT are not compatible with BCAST",
            ));
        }
        if !self.broadcast && !self.prefixes.is_empty() {
            return Err(Value::error(
                "ERR PREFIX option requires BCAST mode to be enabled",
            ));
        }
        Ok(())
    }
}

fn parse_redirect(value: &[u8]) -> Result<usize, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))
}

struct ResolvedTracking {
    request: TrackingRequest,
    prefixes: Vec<Bytes>,
}

fn resolve_tracking(
    conn: &dyn CommandConn,
    request: TrackingRequest,
) -> Result<ResolvedTracking, Value> {
    let existing = if conn.tracking() && conn.tracking_bcast() && request.broadcast {
        conn.tracking_prefixes()
    } else {
        &[]
    };
    validate_prefixes(existing, &request.prefixes)?;
    let mut prefixes = existing.to_vec();
    prefixes.extend(request.prefixes.iter().cloned());
    if request.enabled && request.broadcast && prefixes.is_empty() {
        prefixes.push(Bytes::new());
    }
    Ok(ResolvedTracking { request, prefixes })
}

fn validate_prefixes(existing: &[Bytes], added: &[Bytes]) -> Result<(), Value> {
    for prefix in added {
        if let Some(conflict) = existing.iter().find(|value| overlaps(prefix, value)) {
            return Err(overlap_error(prefix, conflict));
        }
    }
    for left in 0..added.len() {
        for right in left + 1..added.len() {
            if overlaps(&added[left], &added[right]) {
                return Err(overlap_error(&added[left], &added[right]));
            }
        }
    }
    Ok(())
}

fn overlaps(left: &[u8], right: &[u8]) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn overlap_error(left: &[u8], right: &[u8]) -> Value {
    Value::error(format!(
        "ERR Prefix '{}' overlaps with '{}'",
        String::from_utf8_lossy(left),
        String::from_utf8_lossy(right)
    ))
}

pub fn client_tracking(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let request = match TrackingRequest::parse(args).and_then(|request| resolve_tracking(conn, request)) {
        Ok(request) => request,
        Err(error) => return error,
    };
    apply_tracking(conn, ctx, request);
    Value::ok()
}

fn apply_tracking(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
    resolved: ResolvedTracking,
) {
    let request = resolved.request;
    let enabled = request.enabled;
    conn.set_tracking(enabled);
    conn.set_tracking_bcast(enabled && request.broadcast);
    conn.set_tracking_optin(enabled && request.policy == TrackingPolicy::OptIn);
    conn.set_tracking_optout(enabled && request.policy == TrackingPolicy::OptOut);
    conn.set_tracking_noloop(enabled && request.noloop);
    conn.set_tracking_caching(false);
    conn.set_tracking_redirect(enabled.then_some(request.redirect).flatten());
    conn.set_tracking_prefixes(if enabled {
        resolved.prefixes.clone()
    } else {
        Vec::new()
    });
    conn.set_tracking_broken_redirect(false);
    if !enabled {
        ctx.tracking_mut().clear_client(conn.token().0);
        return;
    }
    ctx.tracking_mut().clients.insert(conn.token().0);
    if request.broadcast {
        ctx.tracking_mut()
            .prefix_counts
            .insert(conn.token().0, resolved.prefixes.len());
    } else {
        ctx.tracking_mut().prefix_counts.remove(&conn.token().0);
    }
}

pub fn client_trackinginfo(conn: &mut dyn CommandConn) -> Value {
    let mut flags = Vec::new();
    flags.push(Value::bulk(if conn.tracking() {
        Bytes::from_static(b"on")
    } else {
        Bytes::from_static(b"off")
    }));
    for (enabled, name) in [
        (conn.tracking_bcast(), b"bcast".as_slice()),
        (conn.tracking_optin(), b"optin".as_slice()),
        (conn.tracking_optout(), b"optout".as_slice()),
        (conn.tracking_noloop(), b"noloop".as_slice()),
        (conn.tracking_broken_redirect(), b"broken_redirect".as_slice()),
    ] {
        if enabled {
            flags.push(Value::bulk(Bytes::copy_from_slice(name)));
        }
    }
    if conn.tracking_caching() {
        flags.push(Value::bulk(if conn.tracking_optout() {
            Bytes::from_static(b"caching-no")
        } else {
            Bytes::from_static(b"caching-yes")
        }));
    }
    let prefixes = conn
        .tracking_prefixes()
        .iter()
        .cloned()
        .map(Value::bulk)
        .collect();
    Value::Array(vec![
        Value::bulk(Bytes::from_static(b"flags")),
        Value::Array(flags),
        Value::bulk(Bytes::from_static(b"redirect")),
        Value::integer(if conn.tracking() {
            conn.tracking_redirect().map(|id| id as i64).unwrap_or(0)
        } else {
            -1
        }),
        Value::bulk(Bytes::from_static(b"prefixes")),
        Value::Array(prefixes),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_broadcast_prefixes_redirect_and_noloop() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLIENT"),
            Bytes::from_static(b"TRACKING"),
            Bytes::from_static(b"ON"),
            Bytes::from_static(b"BCAST"),
            Bytes::from_static(b"PREFIX"),
            Bytes::from_static(b"cache:"),
            Bytes::from_static(b"REDIRECT"),
            Bytes::from_static(b"42"),
            Bytes::from_static(b"NOLOOP"),
        ]);

        let request = TrackingRequest::parse(&args).unwrap();
        assert!(request.enabled);
        assert!(request.broadcast);
        assert_eq!(request.prefixes, vec![Bytes::from_static(b"cache:")]);
        assert_eq!(request.redirect, Some(42));
        assert!(request.noloop);
    }
}
