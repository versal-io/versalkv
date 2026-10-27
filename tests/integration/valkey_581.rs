use crate::common::TestServer;

#[test]
fn upgrade_581_georadius_extracts_the_effective_destination() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for command in [
        &[b"GEORADIUS".as_slice(), b"src", b"0", b"0", b"1", b"km"][..],
        &[
            b"GEORADIUSBYMEMBER".as_slice(),
            b"src",
            b"member",
            b"1",
            b"km",
        ][..],
    ] {
        for (first, last) in [
            (b"STORE".as_slice(), b"STORE".as_slice()),
            (b"STOREDIST".as_slice(), b"STOREDIST".as_slice()),
            (b"STORE".as_slice(), b"STOREDIST".as_slice()),
            (b"STOREDIST".as_slice(), b"STORE".as_slice()),
        ] {
            let mut args = vec![b"COMMAND".as_slice(), b"GETKEYS"];
            args.extend_from_slice(command);
            args.extend_from_slice(&[first, b"decoy", last, b"effective"]);
            assert_eq!(
                client.command(&args),
                b"*2\r\n$3\r\nsrc\r\n$9\r\neffective\r\n",
                "{args:?}"
            );
        }
    }
}

#[test]
fn upgrade_581_duplicate_geo_store_uses_final_acl_permissions() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut restricted = server.connect();
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"geo-user",
            b"on",
            b"nopass",
            b"-@all",
            b"+georadius",
            b"+georadiusbymember",
            b"%R~geo:*",
            b"%W~scratch:*",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        restricted.command(&[b"AUTH", b"geo-user", b"password"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"SET", b"protected:dst", b"sentinel"]),
        b"+OK\r\n"
    );
    for command in [
        &[
            b"GEORADIUS".as_slice(),
            b"geo:missing",
            b"0",
            b"0",
            b"1",
            b"km",
        ][..],
        &[
            b"GEORADIUSBYMEMBER".as_slice(),
            b"geo:missing",
            b"member",
            b"1",
            b"km",
        ][..],
    ] {
        for (first, last) in [
            (b"STORE".as_slice(), b"STORE".as_slice()),
            (b"STOREDIST".as_slice(), b"STOREDIST".as_slice()),
            (b"STORE".as_slice(), b"STOREDIST".as_slice()),
        ] {
            let mut denied = command.to_vec();
            denied.extend_from_slice(&[first, b"scratch:ok", last, b"protected:dst"]);
            let mut dryrun = vec![b"ACL".as_slice(), b"DRYRUN", b"geo-user"];
            dryrun.extend_from_slice(&denied);
            let reply = admin.command_str(&dryrun);
            assert!(reply.contains("protected:dst"), "{reply}");
            let reply = restricted.command_str(&denied);
            assert!(reply.starts_with("-NOPERM"), "{reply}");

            let mut allowed = command.to_vec();
            allowed.extend_from_slice(&[first, b"protected:dst", last, b"scratch:ok"]);
            dryrun.truncate(3);
            dryrun.extend_from_slice(&allowed);
            assert_eq!(admin.command(&dryrun), b"$2\r\nOK\r\n", "{allowed:?}");
            assert_eq!(restricted.command(&allowed), b":0\r\n", "{allowed:?}");
        }
    }
    assert_eq!(
        admin.command(&[b"GET", b"protected:dst"]),
        b"$8\r\nsentinel\r\n"
    );
}

#[test]
fn upgrade_581_geo_missing_source_preserves_option_errors() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let reply = client.command_str(&[
        b"GEORADIUSBYMEMBER",
        b"missing",
        b"member",
        b"1",
        b"km",
        b"STORE",
        b"dst",
        b"WITHDIST",
    ]);
    assert!(
        reply.starts_with("-ERR STORE option in GEORADIUS is not compatible"),
        "{reply}"
    );
    let reply = client.command_str(&[
        b"GEORADIUSBYMEMBER_RO",
        b"missing",
        b"member",
        b"1",
        b"km",
        b"STORE",
        b"dst",
    ]);
    assert_eq!(reply, "-ERR syntax error\r\n");
}

#[test]
fn upgrade_581_geo_introspection_preserves_variable_destination_specs() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for (command, start_from) in [
        (b"GEORADIUS".as_slice(), 6),
        (b"GEORADIUSBYMEMBER".as_slice(), 5),
    ] {
        let info = client.command_str(&[b"COMMAND", b"INFO", command]);
        assert_eq!(info.matches("variable_flags").count(), 2, "{info}");
        assert!(info.contains("$5\r\nSTORE\r\n"), "{info}");
        assert!(info.contains("$9\r\nSTOREDIST\r\n"), "{info}");
        assert_eq!(
            info.matches(&format!("startfrom\r\n:{start_from}\r\n"))
                .count(),
            2,
            "{info}"
        );
    }
}

#[test]
fn upgrade_581_reset_revokes_import_source_expiration_semantics() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"import-mode", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CLIENT", b"IMPORT-SOURCE", b"on"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SET", b"expired", b"value", b"PX", b"1"]),
        b"+OK\r\n"
    );
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert_eq!(client.command(&[b"GET", b"expired"]), b"$5\r\nvalue\r\n");
    assert_eq!(client.command(&[b"TTL", b"expired"]), b":0\r\n");
    assert_eq!(client.command(&[b"RESET"]), b"+RESET\r\n");
    let info = client.command_str(&[b"CLIENT", b"INFO"]);
    assert!(
        info.split_whitespace().any(|field| field == "flags=N"),
        "{info}"
    );
    assert_eq!(client.command(&[b"GET", b"expired"]), b"$-1\r\n");
    assert_eq!(client.command(&[b"TTL", b"expired"]), b":-2\r\n");
}

#[test]
fn upgrade_581_geo_preserves_compact_reads_and_store_thresholds() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-entries", b"2"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"GEOADD", b"places", b"0", b"0", b"a", b"0.1", b"0.1", b"b"]),
        b":2\r\n"
    );
    for command in [
        &[b"GEOHASH".as_slice(), b"places", b"a"][..],
        &[b"GEOPOS".as_slice(), b"places", b"a"][..],
        &[b"GEODIST".as_slice(), b"places", b"a", b"b"][..],
        &[
            b"GEORADIUSBYMEMBER".as_slice(),
            b"places",
            b"a",
            b"100",
            b"km",
        ][..],
        &[
            b"GEOSEARCH".as_slice(),
            b"places",
            b"FROMMEMBER",
            b"a",
            b"BYRADIUS",
            b"100",
            b"km",
        ][..],
    ] {
        let reply = client.command_str(command);
        assert!(!reply.starts_with('-'), "{reply}");
        assert_eq!(
            client.command(&[b"OBJECT", b"ENCODING", b"places"]),
            b"$8\r\nlistpack\r\n",
            "{command:?}"
        );
    }
    assert_eq!(
        client.command(&[b"GEOADD", b"places", b"0.2", b"0.2", b"c"]),
        b":1\r\n"
    );
    assert_eq!(
        client.command(&[b"OBJECT", b"ENCODING", b"places"]),
        b"$5\r\nbtree\r\n"
    );
    assert_eq!(client.command(&[b"ZREM", b"places", b"c"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"OBJECT", b"ENCODING", b"places"]),
        b"$5\r\nbtree\r\n"
    );
    for command in [
        &[
            b"GEORADIUS".as_slice(),
            b"places",
            b"0",
            b"0",
            b"100",
            b"km",
            b"STORE",
            b"result",
        ][..],
        &[
            b"GEOSEARCHSTORE".as_slice(),
            b"result",
            b"places",
            b"FROMMEMBER",
            b"a",
            b"BYRADIUS",
            b"100",
            b"km",
        ][..],
    ] {
        assert_eq!(client.command(command), b":2\r\n");
        assert_eq!(
            client.command(&[b"OBJECT", b"ENCODING", b"result"]),
            b"$8\r\nlistpack\r\n",
            "{command:?}"
        );
    }
}

#[test]
fn upgrade_581_geoadd_validates_all_coordinates_before_mutation() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let reply = client.command_str(&[
        b"GEOADD", b"places", b"0", b"0", b"valid", b"181", b"0", b"invalid",
    ]);
    assert!(
        reply.starts_with("-ERR invalid longitude,latitude"),
        "{reply}"
    );
    assert_eq!(client.command(&[b"EXISTS", b"places"]), b":0\r\n");
}
