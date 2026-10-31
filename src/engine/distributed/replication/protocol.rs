use super::ReplicationOffset;

pub fn generate_replid() -> String {
    // Clock resolution must not determine replication history identity.
    let mut bytes = [0u8; 20];
    crate::foundation::io::sys::rand::fill_os_random_bytes(&mut bytes)
        .expect("OS randomness is required to generate a replication ID");
    hex::encode(bytes)
}

pub fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub fn build_ping() -> Vec<u8> {
    encode_command(&[b"PING"])
}

pub fn build_auth(user: Option<&[u8]>, password: &[u8]) -> Vec<u8> {
    match user {
        Some(user) => encode_command(&[b"AUTH", user, password]),
        None => encode_command(&[b"AUTH", password]),
    }
}

pub fn build_replconf_port(port: u16) -> Vec<u8> {
    encode_command(&[
        b"REPLCONF" as &[u8],
        b"listening-port" as &[u8],
        port.to_string().as_bytes(),
    ])
}

pub fn build_replconf_ip_address(ip: &str) -> Vec<u8> {
    encode_command(&[b"REPLCONF", b"ip-address", ip.as_bytes()])
}

pub fn build_replconf_capa() -> Vec<u8> {
    encode_command(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"])
}

pub fn build_replconf_version() -> Vec<u8> {
    // SHUTDOWN FAILOVER and its internal REPLICAID option were introduced in
    // the 9.0 replication protocol. Advertising that floor records that this
    // implementation understands the handoff frame.
    encode_command(&[b"REPLCONF", b"version", b"9.0.0"])
}

pub fn build_replconf_cluster_node_id(node_id: &str) -> Vec<u8> {
    encode_command(&[b"REPLCONF", b"SET-CLUSTER-NODE-ID", node_id.as_bytes()])
}

pub const DUAL_CHANNEL_CAPABILITY: &str = "dual-channel";
pub const DUAL_CHANNEL_SYNC_REPLY: &[u8] = b"+DUALCHANNELSYNC\r\n";

#[allow(dead_code)] // Parsed by the dual-channel state machine in a later slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndOffset {
    pub offset: ReplicationOffset,
    pub replid: String,
    pub db_id: u32,
    pub client_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullResync {
    pub replid: String,
    pub offset: ReplicationOffset,
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn build_replconf_rdb_only() -> Vec<u8> {
    encode_command(&[b"REPLCONF", b"rdb-only", b"1"])
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn build_replconf_rdb_channel() -> Vec<u8> {
    build_replconf_rdb_channel_with_compression("no")
}

pub fn build_replconf_rdb_channel_with_compression(mode: &str) -> Vec<u8> {
    if compression_is_lz4(mode) {
        return encode_command(&[
            b"REPLCONF",
            b"capa",
            b"eof",
            b"capa",
            b"lz4",
            b"rdb-only",
            b"1",
            b"rdb-channel",
            b"1",
        ]);
    }
    encode_command(&[
        b"REPLCONF",
        b"capa",
        b"eof",
        b"rdb-only",
        b"1",
        b"rdb-channel",
        b"1",
    ])
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn build_replconf_set_rdb_client_id(id: u64) -> Vec<u8> {
    encode_command(&[b"REPLCONF", b"set-rdb-client-id", id.to_string().as_bytes()])
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn parse_dual_channel_sync(line: &[u8]) -> bool {
    line.strip_suffix(b"\r\n")
        .unwrap_or(line)
        .eq(b"+DUALCHANNELSYNC")
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn encode_end_offset(
    offset: ReplicationOffset,
    replid: &str,
    db_id: u32,
    client_id: u64,
) -> Vec<u8> {
    format!("$ENDOFF:{} {replid} {db_id} {client_id}\r\n", offset.get()).into_bytes()
}

#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn parse_end_offset(line: &[u8]) -> Option<EndOffset> {
    let line = line.strip_suffix(b"\r\n").unwrap_or(line);
    let mut fields = line
        .strip_prefix(b"$ENDOFF:")?
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty());
    let offset = parse_live_replication_offset(fields.next()?)?;
    let replid = parse_replid(fields.next()?)?.to_string();
    let db_field = fields.next()?;
    // C uses -1 before the replication stream selects its first database.
    let db_id = if db_field == b"-1" {
        0
    } else {
        parse_utf8::<u32>(db_field)?
    };
    let client_id = parse_utf8::<u64>(fields.next()?)?;
    if fields.next().is_some() {
        return None;
    }
    Some(EndOffset {
        offset,
        replid,
        db_id,
        client_id,
    })
}

#[allow(dead_code)] // Used by the ENDOFF parser once dual-channel is wired.
fn parse_utf8<T: std::str::FromStr>(value: &[u8]) -> Option<T> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Build the replica capability frame, optionally advertising the dual-channel
/// protocol only after the caller has proved its local implementation is ready.
#[allow(dead_code)] // Wired into the dual-channel state machine in a later slice.
pub fn build_replconf_capa_with_dual_channel(enabled: bool) -> Vec<u8> {
    if enabled {
        encode_command(&[
            b"REPLCONF",
            b"capa",
            b"eof",
            b"capa",
            b"psync2",
            b"capa",
            DUAL_CHANNEL_CAPABILITY.as_bytes(),
        ])
    } else {
        build_replconf_capa()
    }
}

pub fn compression_is_lz4(mode: &str) -> bool {
    mode.eq_ignore_ascii_case("yes") || mode.eq_ignore_ascii_case("lz4")
}

pub fn negotiate_lz4(mode: &str, capabilities: &[String]) -> bool {
    compression_is_lz4(mode) && peer_supports_capability(capabilities, "lz4")
}

pub fn build_replconf_capabilities(dual_channel: bool, mode: &str) -> Vec<u8> {
    let mut parts: Vec<&[u8]> = vec![b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"];
    if dual_channel {
        parts.extend([b"capa".as_slice(), DUAL_CHANNEL_CAPABILITY.as_bytes()]);
    }
    if mode.eq_ignore_ascii_case("zstd") {
        parts.extend([b"capa".as_slice(), b"zstd"]);
    }
    if compression_is_lz4(mode) || mode.eq_ignore_ascii_case("zstd") {
        parts.extend([b"capa".as_slice(), b"lz4"]);
    }
    encode_command(&parts)
}

pub fn peer_supports_capability(capabilities: &[String], expected: &str) -> bool {
    capabilities
        .iter()
        .any(|capability| capability.eq_ignore_ascii_case(expected))
}

/// Select dual-channel only when all three protocol preconditions hold:
/// configuration, local state-machine support, and peer capability.
pub fn should_use_dual_channel(
    local_enabled: bool,
    local_implementation_ready: bool,
    peer_capabilities: &[String],
) -> bool {
    local_enabled
        && local_implementation_ready
        && peer_supports_capability(peer_capabilities, "eof")
        && peer_supports_capability(peer_capabilities, "psync2")
        && peer_supports_capability(peer_capabilities, DUAL_CHANNEL_CAPABILITY)
}

/// Primary-side wrapper that additionally requires diskless sync to be
/// locally enabled (C invariant: dual-channel only when primary diskless).
pub fn should_use_dual_channel_primary(
    local_enabled: bool,
    diskless_enabled: bool,
    implementation_ready: bool,
    peer_capabilities: &[String],
) -> bool {
    diskless_enabled
        && should_use_dual_channel(local_enabled, implementation_ready, peer_capabilities)
}

pub fn build_psync_full() -> Vec<u8> {
    encode_command(&[b"PSYNC", b"?", b"-1"])
}

pub fn build_psync(replid: &str, offset: i64) -> Vec<u8> {
    encode_command(&[b"PSYNC", replid.as_bytes(), offset.to_string().as_bytes()])
}

pub fn build_psync_failover(replid: &str, offset: i64) -> Vec<u8> {
    encode_command(&[
        b"PSYNC",
        replid.as_bytes(),
        offset.to_string().as_bytes(),
        b"FAILOVER",
    ])
}

pub fn build_replconf_ack(
    offset: ReplicationOffset,
    aof_offset: Option<ReplicationOffset>,
) -> Vec<u8> {
    let offset = offset.get().to_string();
    let aof_offset = aof_offset.map(|offset| offset.get().to_string());
    if let Some(aof_offset) = aof_offset.as_deref() {
        encode_command(&[
            b"REPLCONF",
            b"ACK",
            offset.as_bytes(),
            b"FACK",
            aof_offset.as_bytes(),
        ])
    } else {
        encode_command(&[b"REPLCONF", b"ACK", offset.as_bytes()])
    }
}

pub fn parse_fullresync(line: &[u8]) -> Option<FullResync> {
    let payload = line.strip_prefix(b"+FULLRESYNC ")?;
    let replid = parse_replid(payload.get(..40)?)?.to_string();
    if payload.get(40) != Some(&b' ') {
        return None;
    }
    let raw_offset = payload.get(41..)?;
    if raw_offset.is_empty() || raw_offset.iter().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }
    let offset = parse_live_replication_offset(raw_offset)?;
    Some(FullResync { replid, offset })
}

fn parse_replid(raw: &[u8]) -> Option<&str> {
    let replid = std::str::from_utf8(raw).ok()?;
    (replid.len() == 40 && replid.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(replid)
}

fn parse_live_replication_offset(raw: &[u8]) -> Option<ReplicationOffset> {
    let offset = ReplicationOffset::try_from(parse_utf8::<i64>(raw)?).ok()?;
    offset.checked_next_request()?;
    Some(offset)
}

/// Parse a `+CONTINUE` or `+CONTINUE <new_replid>` reply.
/// Returns `Some(Option<String>)`:
/// - `Some(None)` for bare `+CONTINUE` (PSYNC1, no replid change)
/// - `Some(Some(replid))` for `+CONTINUE <replid>` (PSYNC2, replica must adopt new replid)
/// - `None` if the line is not a CONTINUE reply.
pub fn parse_continue(line: &[u8]) -> Option<Option<String>> {
    if line == b"+CONTINUE" {
        return Some(None);
    }
    let rest = line.strip_prefix(b"+CONTINUE ")?;
    let replid = parse_replid(rest)?;
    Some(Some(replid.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("test offset must be non-negative")
    }

    #[test]
    fn replid_is_40_hex() {
        let id = generate_replid();
        assert_eq!(id.len(), 40);
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(generate_replid(), id);
    }

    #[test]
    fn replids_are_unique_when_generated_in_a_burst() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1024 {
            assert!(ids.insert(generate_replid()), "duplicate replication ID");
        }
    }

    #[test]
    fn ping_frame() {
        assert_eq!(build_ping(), b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn replconf_ip_address_frame() {
        assert_eq!(
            build_replconf_ip_address("localhost"),
            b"*3\r\n$8\r\nREPLCONF\r\n$10\r\nip-address\r\n$9\r\nlocalhost\r\n"
        );
    }

    #[test]
    fn replconf_port_frame() {
        assert_eq!(
            build_replconf_port(6380),
            b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".to_vec()
        );
    }

    #[test]
    fn replconf_capabilities_include_diskless_eof() {
        assert_eq!(
            build_replconf_capa(),
            b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        );
    }

    #[test]
    fn dual_channel_capability_is_opt_in() {
        assert_eq!(
            build_replconf_capa_with_dual_channel(false),
            build_replconf_capa()
        );
        assert_eq!(
            build_replconf_capa_with_dual_channel(true),
            b"*7\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n$4\r\ncapa\r\n$12\r\ndual-channel\r\n"
        );
    }

    #[test]
    fn dual_channel_requires_all_negotiation_preconditions() {
        let peer = vec![
            "eof".to_string(),
            "psync2".to_string(),
            DUAL_CHANNEL_CAPABILITY.to_string(),
        ];
        assert!(should_use_dual_channel(true, true, &peer));
        assert!(!should_use_dual_channel(false, true, &peer));
        assert!(!should_use_dual_channel(true, false, &peer));
        assert!(!should_use_dual_channel(true, true, &[]));
        assert!(peer_supports_capability(
            &["DUAL-CHANNEL".to_string()],
            DUAL_CHANNEL_CAPABILITY
        ));
    }

    #[test]
    fn dual_channel_falls_back_without_eof_capability() {
        let peer = vec!["psync2".to_string(), DUAL_CHANNEL_CAPABILITY.to_string()];

        assert!(!should_use_dual_channel(true, true, &peer));
    }

    #[test]
    fn dual_channel_falls_back_without_psync2_capability() {
        let peer = vec!["eof".to_string(), DUAL_CHANNEL_CAPABILITY.to_string()];

        assert!(!should_use_dual_channel(true, true, &peer));
    }

    #[test]
    fn dual_channel_falls_back_without_dual_channel_capability() {
        let peer = vec!["eof".to_string(), "psync2".to_string()];

        assert!(!should_use_dual_channel(true, true, &peer));
    }

    #[test]
    fn dual_channel_frames_use_the_expected_resp_shapes() {
        assert_eq!(
            build_replconf_rdb_only(),
            b"*3\r\n$8\r\nREPLCONF\r\n$8\r\nrdb-only\r\n$1\r\n1\r\n"
        );
        assert_eq!(
            build_replconf_rdb_channel(),
            b"*7\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$8\r\nrdb-only\r\n$1\r\n1\r\n$11\r\nrdb-channel\r\n$1\r\n1\r\n"
        );
        assert_eq!(
            build_replconf_set_rdb_client_id(42),
            b"*3\r\n$8\r\nREPLCONF\r\n$17\r\nset-rdb-client-id\r\n$2\r\n42\r\n"
        );
        assert_eq!(DUAL_CHANNEL_SYNC_REPLY, b"+DUALCHANNELSYNC\r\n");
        assert!(parse_dual_channel_sync(b"+DUALCHANNELSYNC"));
        assert!(parse_dual_channel_sync(b"+DUALCHANNELSYNC\r\n"));
        assert!(!parse_dual_channel_sync(b"+DUALCHANNELSYNC extra"));
        assert!(!parse_dual_channel_sync(b"+dualchannelsync"));
    }

    #[test]
    fn end_offset_round_trips_all_snapshot_metadata() {
        let replid = "0123456789abcdef0123456789abcdef01234567";
        let encoded = encode_end_offset(offset(42), replid, 3, 99);
        assert_eq!(
            encoded,
            b"$ENDOFF:42 0123456789abcdef0123456789abcdef01234567 3 99\r\n"
        );
        assert_eq!(
            parse_end_offset(&encoded),
            Some(EndOffset {
                offset: offset(42),
                replid: replid.to_string(),
                db_id: 3,
                client_id: 99,
            })
        );
        let parsed = parse_end_offset(&encoded).unwrap();
        assert_eq!(
            std::any::type_name_of_val(&parsed.offset),
            std::any::type_name::<crate::engine::distributed::replication::ReplicationOffset>()
        );
    }

    #[test]
    fn malformed_end_offset_is_rejected() {
        let valid_replid = b"0123456789abcdef0123456789abcdef01234567";
        for line in [
            b"$ENDOFF:1 0123456789abcdef0123456789abcdef01234567 0".as_slice(),
            b"$ENDOFF:x 0123456789abcdef0123456789abcdef01234567 0 1".as_slice(),
            b"$ENDOFF:1 short 0 1".as_slice(),
            b"$ENDOFF:1 0123456789abcdef0123456789abcdef01234567 nope 1".as_slice(),
            b"$ENDOFF:1 0123456789abcdef0123456789abcdef01234567 0 nope".as_slice(),
            b"$ENDOFF:1 0123456789abcdef0123456789abcdef01234567 0 1 trailing".as_slice(),
            b"$WRONG:1 0123456789abcdef0123456789abcdef01234567 0 1".as_slice(),
        ] {
            assert!(parse_end_offset(line).is_none(), "{line:?}");
        }
        assert!(parse_end_offset(valid_replid).is_none());
    }

    #[test]
    fn end_offset_rejects_non_live_replication_offsets() {
        let replid = "0123456789abcdef0123456789abcdef01234567";
        for offset in [-1, i64::MAX] {
            let frame = format!("$ENDOFF:{offset} {replid} 0 1\r\n");
            assert!(parse_end_offset(frame.as_bytes()).is_none(), "{frame:?}");
        }
    }

    #[test]
    fn psync_frame() {
        assert_eq!(
            build_psync_full(),
            b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_vec()
        );
    }

    #[test]
    fn parse_fullresync_ok() {
        let fullresync =
            parse_fullresync(b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 12").unwrap();
        assert_eq!(fullresync.replid.len(), 40);
        assert_eq!(fullresync.offset.get(), 12);
        assert_eq!(
            std::any::type_name_of_val(&fullresync.offset),
            std::any::type_name::<crate::engine::distributed::replication::ReplicationOffset>()
        );
    }

    #[test]
    fn parse_fullresync_accepts_only_exact_valid_frames() {
        let valid_replid = "0123456789abcdef0123456789abcdef01234567";
        let uppercase_replid = "0123456789ABCDEF0123456789ABCDEF01234567";
        for (reply, expected_offset) in [
            (format!("+FULLRESYNC {valid_replid} 0"), 0),
            (
                format!("+FULLRESYNC {uppercase_replid} {}", i64::MAX - 1),
                i64::MAX - 1,
            ),
        ] {
            let parsed = parse_fullresync(reply.as_bytes()).expect("valid FULLRESYNC frame");
            assert_eq!(parsed.offset.get(), expected_offset, "{reply:?}");
        }

        for reply in [
            "+CONTINUE".to_string(),
            "+FULLRESYNC".to_string(),
            format!("+FULLRESYNC {valid_replid}"),
            "+FULLRESYNC short 0".to_string(),
            format!("+FULLRESYNC {valid_replid}0 0"),
            "+FULLRESYNC 0123456789abcdef0123456789abcdef0123456g 0".to_string(),
            format!("+FULLRESYNC {valid_replid} nope"),
            format!("+FULLRESYNC {valid_replid} -1"),
            format!("+FULLRESYNC {valid_replid} {}", i64::MAX),
            format!("+FULLRESYNC {valid_replid} 9223372036854775808"),
            format!("+FULLRESYNC {valid_replid} 0 trailing"),
            format!("+FULLRESYNC  {valid_replid} 0"),
            format!("+FULLRESYNC {valid_replid}  0"),
            format!("+FULLRESYNC\t{valid_replid} 0"),
            format!("+FULLRESYNC {valid_replid}\t0"),
            format!("+FULLRESYNC {valid_replid} 0 "),
            format!("+FULLRESYNC {valid_replid} 0\t"),
            format!("+FULLRESYNC {valid_replid} 0\r"),
            format!("+FULLRESYNC {valid_replid} 0\n"),
            format!("+FULLRESYNC {valid_replid} 0\r\n"),
            format!("+FULLRESYNC {valid_replid} 1\t2"),
        ] {
            assert!(parse_fullresync(reply.as_bytes()).is_none(), "{reply:?}");
        }
    }

    #[test]
    fn continue_and_ack_frames() {
        assert!(parse_continue(b"+CONTINUE").is_some());
        assert!(parse_continue(b"+CONTINUE 0123456789abcdef0123456789abcdef01234567").is_some());
        assert_eq!(parse_continue(b"-ERR"), None);
        assert_eq!(parse_continue(b"+CONTINUE"), Some(None));
        assert_eq!(
            parse_continue(b"+CONTINUE 0123456789abcdef0123456789abcdef01234567"),
            Some(Some("0123456789abcdef0123456789abcdef01234567".to_string()))
        );
        assert_eq!(parse_continue(b"+CONTINUE short"), None);
        assert_eq!(
            parse_continue(b"+CONTINUE 0123456789abcdef0123456789abcdef01234567 trailing"),
            None
        );
        assert_eq!(parse_continue(b"+FULLRESYNC a 1"), None);
        assert_eq!(
            build_replconf_ack(offset(12), None),
            b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n12\r\n".to_vec()
        );
        assert_eq!(
            build_replconf_ack(offset(12), Some(offset(10))),
            b"*5\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n12\r\n$4\r\nFACK\r\n$2\r\n10\r\n".to_vec()
        );
    }

    #[test]
    fn auth_frame_preserves_binary_credentials() {
        assert_eq!(
            build_auth(None, b"abc\x00def"),
            b"*2\r\n$4\r\nAUTH\r\n$7\r\nabc\x00def\r\n".to_vec()
        );
        assert_eq!(
            build_auth(Some(b"user"), b"pass"),
            b"*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$4\r\npass\r\n".to_vec()
        );
    }

    #[test]
    fn boundary_end_offset_unselected_database_defaults_to_zero() {
        let replid = "0123456789abcdef0123456789abcdef01234567";
        for db_id in ["-2", "-2147483648", "4294967296", "invalid"] {
            let frame = format!("$ENDOFF:123 {replid} {db_id} 91\r\n");
            assert_eq!(
                parse_end_offset(frame.as_bytes()),
                None,
                "invalid database {db_id}"
            );
        }
        for frame in [
            format!("$ENDOFF:-1 {replid} -1 91\r\n"),
            format!("$ENDOFF:9223372036854775808 {replid} -1 91\r\n"),
            "$ENDOFF:123 invalid -1 91\r\n".to_string(),
            format!("$ENDOFF:123 {replid} -1 -1\r\n"),
            format!("$ENDOFF:123 {replid} -1 91 extra\r\n"),
        ] {
            assert_eq!(parse_end_offset(frame.as_bytes()), None, "{frame:?}");
        }
        let frames = [
            format!("$ENDOFF:123 {replid} -1 91\r\n"),
            format!("$ENDOFF:123 {replid} -1 91"),
        ];
        for frame in frames {
            assert_eq!(
                parse_end_offset(frame.as_bytes()),
                Some(EndOffset {
                    offset: offset(123),
                    replid: replid.to_string(),
                    db_id: 0,
                    client_id: 91,
                }),
                "only the exact unselected-database sentinel maps to local DB 0"
            );
        }
    }
}
