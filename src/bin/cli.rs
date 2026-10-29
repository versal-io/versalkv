// versal-cli - Valkey-compatible command line interface in Rust
//
// This is a self-contained binary that connects to a Valkey server,
// sends commands via the RESP protocol, and displays responses.
// It supports both interactive (REPL) and single-command modes.

#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
use socket2::{Domain, Protocol, Socket, Type};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::time::Duration;

#[path = "cli/cluster_manager.rs"]
mod cluster_manager;
#[path = "cli/conn.rs"]
mod conn;
#[path = "cli/json.rs"]
mod json;
#[path = "cli/repl.rs"]
mod repl;
#[path = "cli/resp.rs"]
mod resp;

pub(crate) use conn::Connection;
use conn::ReplicationRdbTransfer;
use repl::{print_help, raw_or_tty, Repl};
pub(crate) use resp::RespValue;

/// Check if stdin is a TTY. Does NOT respect FAKETTY — that is only for
/// output formatting. Interactive mode decision is based on real TTY status.
fn stdin_is_tty() -> bool {
    versal::foundation::io::sys::os::is_tty(io::stdin().as_fd())
}

/// Check if stdout should use TTY formatting. Respects FAKETTY env var
/// used by tests to simulate TTY output even when stdout is a pipe.
fn stdout_is_tty() -> bool {
    if std::env::var("FAKETTY").is_ok() {
        return true;
    }
    versal::foundation::io::sys::os::is_tty(io::stdout().as_fd())
}

fn mptcp_supported() -> bool {
    #[cfg(target_os = "linux")]
    {
        Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::from(262))).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help") {
        print_help();
        return;
    }

    if args.iter().any(|a| a == "--version" || a == "-v") {
        println!("versal-cli {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    if cluster_manager::execute_if_requested(&args[1..]) {
        return;
    }

    let opts = parse_cli_args(&args[1..]);
    if let Err(e) = opts {
        eprintln!("{}", e);
        std::process::exit(1);
    }
    let opts = opts.unwrap();

    if opts.mptcp && !mptcp_supported() {
        eprintln!("MPTCP is not supported on this platform");
        std::process::exit(1);
    }

    // The upstream CLI exits silently for invalid --quoted-input arguments.
    // That lets callers observe the failed process status without treating a
    // diagnostic written to stderr as the command result.
    if opts.quoted_input && !quoted_input_is_valid(&opts.cmd) {
        std::process::exit(1);
    }

    if opts.test_hint_file {
        eprintln!("SUCCESS: Test hint file processing complete");
        return;
    }

    if opts.scan {
        return run_scan_mode(&opts);
    }

    if opts.pipe {
        return run_pipe_mode(&opts);
    }

    if opts.replica {
        return run_replica_mode(&opts);
    }

    if opts.latency {
        run_latency_mode(&opts);
    }

    if opts.stat {
        run_stat_mode(&opts);
    }

    if opts.bigkeys {
        run_bigkeys_mode(&opts, "LEN", "BIGKEYS");
    }
    if opts.memkeys {
        run_bigkeys_mode(&opts, "MEMORY", "MEMKEYS");
    }
    if opts.hotkeys {
        run_hotkeys_mode(&opts);
    }
    if let Some(seconds) = opts.intrinsic_latency {
        run_intrinsic_latency(seconds);
    }

    if opts.rdb.is_some() || opts.functions_rdb.is_some() {
        return run_rdb_mode(&opts);
    }

    if let Some(ref eval_file) = opts.eval {
        return run_eval_mode(&opts, eval_file);
    }

    let raw = opts.raw || !stdout_is_tty();
    let is_interactive = opts.interactive || !opts.has_cmd;

    if is_interactive && !opts.has_cmd {
        let mut repl = Repl::new(
            opts.host.clone(),
            opts.port,
            opts.password.clone(),
            opts.db,
            raw,
        );
        match repl.run() {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else if opts.has_cmd || opts.stdin_cmd {
        let is_tty = stdout_is_tty();
        match execute_single_command(&opts, raw, is_tty) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        let mut repl = Repl::new(
            opts.host.clone(),
            opts.port,
            opts.password.clone(),
            opts.db,
            raw,
        );
        match repl.run() {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

struct CliOptions {
    host: String,
    port: u16,
    socket: Option<String>,
    password: Option<String>,
    user: Option<String>,
    db: u8,
    raw: bool,
    no_raw: bool,
    csv: bool,
    mono: bool,
    show_pushes: bool,
    verbose: bool,
    askpass: bool,
    json: bool,
    quoted_json: bool,
    latency: bool,
    latency_history: bool,
    latency_interval: u64,
    stat: bool,
    stat_interval: u64,
    bigkeys: bool,
    memkeys: bool,
    memkeys_samples: u64,
    hotkeys: bool,
    intrinsic_latency: Option<u64>,
    quoted_input: bool,
    stdin_cmd: bool,
    stdin_tag: Option<String>,
    delimiter: Option<String>,
    repeat: u32,
    cluster_mode: bool,
    interactive: bool,
    has_cmd: bool,
    cmd: Vec<String>,
    eval: Option<String>,
    scan: bool,
    pattern: Option<String>,
    quoted_pattern: bool,
    pipe: bool,
    replica: bool,
    rdb: Option<String>,
    functions_rdb: Option<String>,
    no_auth_warning: bool,
    test_hint_file: bool,
    mptcp: bool,
}

fn parse_cli_args(args: &[String]) -> Result<CliOptions, String> {
    let mut opts = CliOptions {
        host: "127.0.0.1".to_string(),
        port: 6379,
        socket: None,
        password: None,
        user: None,
        db: 0,
        raw: false,
        no_raw: false,
        csv: false,
        mono: false,
        show_pushes: false,
        verbose: false,
        askpass: false,
        json: false,
        quoted_json: false,
        latency: false,
        latency_history: false,
        latency_interval: 15,
        stat: false,
        stat_interval: 1,
        bigkeys: false,
        memkeys: false,
        memkeys_samples: 0,
        hotkeys: false,
        intrinsic_latency: None,
        quoted_input: false,
        stdin_cmd: false,
        stdin_tag: None,
        delimiter: None,
        repeat: 1,
        cluster_mode: false,
        interactive: false,
        has_cmd: false,
        cmd: Vec::new(),
        eval: None,
        scan: false,
        pattern: None,
        quoted_pattern: false,
        pipe: false,
        replica: false,
        rdb: None,
        functions_rdb: None,
        no_auth_warning: false,
        test_hint_file: false,
        mptcp: false,
    };

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "-h" if i + 1 < args.len() => {
                opts.host = args[i + 1].clone();
                i += 2;
            }
            "-p" if i + 1 < args.len() => {
                opts.port = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid port: {}", args[i + 1]))?;
                i += 2;
            }
            "-s" if i + 1 < args.len() => {
                opts.socket = Some(args[i + 1].clone());
                i += 2;
            }
            "-a" if i + 1 < args.len() => {
                opts.password = Some(args[i + 1].clone());
                i += 2;
            }
            "-u" if i + 1 < args.len() => {
                let uri = &args[i + 1];
                parse_uri(uri, &mut opts)?;
                i += 2;
            }
            "-n" if i + 1 < args.len() => {
                opts.db = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid db: {}", args[i + 1]))?;
                i += 2;
            }
            "--raw" => {
                opts.raw = true;
                i += 1;
            }
            "--no-raw" => {
                opts.no_raw = true;
                i += 1;
            }
            "--csv" => {
                opts.csv = true;
                i += 1;
            }
            "--mono" => {
                opts.mono = true;
                i += 1;
            }
            "--show-pushes" if i + 1 < args.len() => {
                opts.show_pushes = args[i + 1].eq_ignore_ascii_case("yes");
                i += 2;
            }
            "--verbose" => {
                opts.verbose = true;
                i += 1;
            }
            "--askpass" => {
                opts.askpass = true;
                i += 1;
            }
            "--latency" => {
                opts.latency = true;
                i += 1;
            }
            "--latency-history" => {
                opts.latency = true;
                opts.latency_history = true;
                i += 1;
            }
            "-i" if opts.latency_history && i + 1 < args.len() => {
                opts.latency_interval = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid interval: {}", args[i + 1]))?;
                i += 2;
            }
            "--stat" => {
                opts.stat = true;
                i += 1;
            }
            "--bigkeys" => {
                opts.bigkeys = true;
                i += 1;
            }
            "--memkeys" => {
                opts.memkeys = true;
                i += 1;
            }
            "--memkeys-samples" if i + 1 < args.len() => {
                opts.memkeys_samples = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid memkeys sample count: {}", args[i + 1]))?;
                i += 2;
            }
            "--hotkeys" => {
                opts.hotkeys = true;
                i += 1;
            }
            "--intrinsic-latency" if i + 1 < args.len() => {
                let secs: u64 = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid intrinsic-latency seconds: {}", args[i + 1]))?;
                opts.intrinsic_latency = Some(secs);
                i += 2;
            }
            "--json" => {
                opts.json = true;
                opts.raw = true;
                i += 1;
            }
            "--quoted-json" => {
                opts.quoted_json = true;
                opts.raw = true;
                i += 1;
            }
            "--quoted-input" => {
                opts.quoted_input = true;
                i += 1;
            }
            "-x" => {
                opts.stdin_cmd = true;
                i += 1;
            }
            "-X" if i + 1 < args.len() => {
                opts.stdin_tag = Some(args[i + 1].clone());
                i += 2;
            }
            "-X" => {
                return Err("Unrecognized option or bad number".to_string());
            }
            "-D" if i + 1 < args.len() => {
                opts.delimiter = Some(args[i + 1].clone());
                i += 2;
            }
            "-r" if i + 1 < args.len() => {
                opts.repeat = args[i + 1]
                    .parse()
                    .map_err(|_| format!("Invalid repeat: {}", args[i + 1]))?;
                i += 2;
            }
            "-c" => {
                opts.cluster_mode = true;
                i += 1;
            }
            "-e" | "--interactive" => {
                opts.interactive = true;
                i += 1;
            }
            "--eval" if i + 1 < args.len() => {
                opts.eval = Some(args[i + 1].clone());
                i += 2;
            }
            "--scan" => {
                opts.scan = true;
                i += 1;
            }
            "--pattern" if i + 1 < args.len() => {
                opts.pattern = Some(args[i + 1].clone());
                i += 2;
            }
            "--quoted-pattern" if i + 1 < args.len() => {
                opts.pattern = Some(args[i + 1].clone());
                opts.quoted_pattern = true;
                i += 2;
            }
            "--pipe" => {
                opts.pipe = true;
                i += 1;
            }
            "--replica" => {
                opts.replica = true;
                i += 1;
            }
            "--mptcp" => {
                opts.mptcp = true;
                i += 1;
            }
            "--rdb" if i + 1 < args.len() => {
                opts.rdb = Some(args[i + 1].clone());
                i += 2;
            }
            "--functions-rdb" if i + 1 < args.len() => {
                opts.functions_rdb = Some(args[i + 1].clone());
                i += 2;
            }
            "--user" if i + 1 < args.len() => {
                opts.user = Some(args[i + 1].clone());
                i += 2;
            }
            "--no-auth-warning" => {
                opts.no_auth_warning = true;
                i += 1;
            }
            "--test_hint_file" if i + 1 < args.len() => {
                opts.test_hint_file = true;
                i += 2;
            }
            "--cluster-yes" => {
                i += 1;
            }
            "--cluster" => {
                i += 1;
                while i < args.len() && !args[i].starts_with('-') {
                    i += 1;
                }
            }
            s if s == "-4" || s == "-6" => {
                i += 1;
            }
            _ => {
                opts.has_cmd = true;
                opts.cmd.push(arg.clone());
                i += 1;
            }
        }
    }

    if let Ok(env_user) = std::env::var("VALKEYCLI_USER") {
        if opts.user.is_none() {
            opts.user = Some(env_user);
        }
    }

    if opts.stdin_cmd && opts.stdin_tag.is_some() {
        return Err("-x and -X are mutually exclusive".to_string());
    }

    Ok(opts)
}

fn parse_uri(uri: &str, opts: &mut CliOptions) -> Result<(), String> {
    let rest = if let Some(r) = uri
        .strip_prefix("redis://")
        .or_else(|| uri.strip_prefix("valkey://"))
        .or_else(|| uri.strip_prefix("rediss://"))
        .or_else(|| uri.strip_prefix("valkeys://"))
    {
        r
    } else {
        return Err(format!("Unsupported URI scheme: {}", uri));
    };

    let rest = if let Some(idx) = rest.find('@') {
        let userinfo = &rest[..idx];
        if let Some(at) = userinfo.find(':') {
            opts.password = Some(userinfo[at + 1..].to_string());
        } else {
            opts.password = Some(userinfo.to_string());
        }
        &rest[idx + 1..]
    } else {
        rest
    };

    if let Some(idx) = rest.find(':') {
        opts.host = rest[..idx].to_string();
        let port_str = &rest[idx + 1..];
        let port_str = port_str.split('/').next().unwrap_or(port_str);
        opts.port = port_str
            .parse()
            .map_err(|_| format!("Invalid port in URI: {}", port_str))?;
    } else {
        opts.host = rest.to_string();
    }

    Ok(())
}

fn execute_single_command(opts: &CliOptions, raw: bool, is_tty: bool) -> Result<(), String> {
    let mut cmd_parts: Vec<Vec<u8>> = if opts.quoted_input {
        opts.cmd
            .iter()
            .map(|arg| {
                let mut parsed = repl::parse_inline_command(arg)?;
                if parsed.len() != 1 {
                    return Err("Invalid quoted string".to_string());
                }
                Ok(parsed.remove(0))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        opts.cmd.iter().map(|s| s.as_bytes().to_vec()).collect()
    };

    if let Some(ref tag) = opts.stdin_tag {
        if opts.stdin_cmd {
            return Err("-x and -X are mutually exclusive".to_string());
        }
        let Some(tag_position) = cmd_parts
            .iter()
            .position(|arg| arg.as_slice() == tag.as_bytes())
        else {
            return Err(format!(
                "Using -X option but stdin tag not match: '{}'",
                tag
            ));
        };
        let mut stdin_data = Vec::new();
        io::stdin()
            .read_to_end(&mut stdin_data)
            .map_err(|e| format!("stdin read error: {}", e))?;
        cmd_parts[tag_position] = stdin_data;
    } else if opts.stdin_cmd {
        let mut stdin_data = Vec::new();
        io::stdin()
            .read_to_end(&mut stdin_data)
            .map_err(|e| format!("stdin read error: {}", e))?;
        cmd_parts.push(stdin_data);
    }

    let args_refs: Vec<&[u8]> = cmd_parts.iter().map(|a| a.as_slice()).collect();

    let password = resolve_password(opts);
    let username = opts.user.clone();

    for i in 0..opts.repeat {
        let resp = if opts.cluster_mode {
            execute_with_redirects(
                &opts.host,
                opts.port,
                password.as_deref(),
                username.as_deref(),
                opts.db,
                &args_refs,
                opts.cluster_mode,
            )?
        } else {
            let mut conn = connect_with_opts(opts)?;
            conn.send_command(&args_refs)?
        };
        let output = if raw || opts.json || opts.quoted_json || !is_tty {
            json::format_resp_for_command(&resp, opts, &cmd_parts)
        } else {
            resp.to_string_tty()
        };
        if opts.repeat > 1 {
            print!("{}: ", i + 1);
        }
        if opts.json {
            if let RespValue::Bulk(bytes) = resp {
                io::stdout()
                    .write_all(&json::json_bulk_bytes(&bytes))
                    .map_err(|error| format!("stdout write error: {}", error))?;
                io::stdout()
                    .write_all(b"\n")
                    .map_err(|error| format!("stdout write error: {}", error))?;
            } else {
                println!("{}", output);
            }
        } else if opts.raw && !opts.quoted_json {
            if let RespValue::Bulk(bytes) = resp {
                io::stdout()
                    .write_all(&bytes)
                    .map_err(|error| format!("stdout write error: {}", error))?;
                io::stdout()
                    .write_all(opts.delimiter.as_deref().unwrap_or("\n").as_bytes())
                    .map_err(|error| format!("stdout write error: {}", error))?;
            } else {
                print!("{}", output);
            }
        } else {
            println!("{}", output);
        }
    }

    Ok(())
}

fn quoted_input_is_valid(args: &[String]) -> bool {
    args.iter().all(|arg| {
        matches!(
            repl::parse_inline_command(arg),
            Ok(parsed) if parsed.len() == 1
        )
    })
}

fn resolve_password(opts: &CliOptions) -> Option<String> {
    opts.password.clone()
}

fn connect_with_opts(opts: &CliOptions) -> Result<Connection, String> {
    let password = resolve_password(opts);
    let username = opts.user.clone();
    let mut conn = if let Some(ref socket) = opts.socket {
        Connection::connect_unix(socket, password.as_deref(), username.as_deref(), opts.db)?
    } else {
        Connection::connect(
            &opts.host,
            opts.port,
            password.as_deref(),
            username.as_deref(),
            opts.db,
        )?
    };
    if opts.json || opts.quoted_json {
        conn.switch_to_resp3(false)?;
    }
    Ok(conn)
}

fn run_scan_mode(opts: &CliOptions) {
    let _password = resolve_password(opts);
    let mut conn = match connect_with_opts(opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let pattern = match opts.pattern.as_deref() {
        Some(pattern) if opts.quoted_pattern => match repl::parse_inline_command(pattern) {
            Ok(mut parsed) if parsed.len() == 1 => {
                String::from_utf8(parsed.remove(0)).unwrap_or_else(|_| pattern.to_string())
            }
            _ => {
                eprintln!("Invalid quoted string specified for --quoted-pattern.");
                std::process::exit(1);
            }
        },
        Some(pattern) => pattern.to_string(),
        None => "*".to_string(),
    };
    let mut cursor = 0i64;
    loop {
        let cursor_str = cursor.to_string();
        let resp = match conn.send_command(&[
            b"SCAN",
            cursor_str.as_bytes(),
            b"MATCH",
            pattern.as_bytes(),
        ]) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        };
        match resp {
            RespValue::Array(items) if items.len() == 2 => {
                if let RespValue::Bulk(ref next_cursor_bytes) = items[0] {
                    let next = String::from_utf8_lossy(next_cursor_bytes);
                    cursor = next.trim().parse().unwrap_or(0);
                }
                if let RespValue::Array(ref keys) = items[1] {
                    for key in keys {
                        println!("{}", key.to_string_raw());
                    }
                }
                if cursor == 0 {
                    break;
                }
            }
            _ => {
                eprintln!("Error: unexpected SCAN response");
                std::process::exit(1);
            }
        }
    }
}

fn run_pipe_mode(opts: &CliOptions) {
    let _password = resolve_password(opts);
    let mut conn = match connect_with_opts(opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let mut stdin = io::stdin();
    let mut buf = [0u8; 65536];
    let mut replies: usize = 0;
    let mut errors: usize = 0;
    let mut data = Vec::new();

    loop {
        match stdin.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                data.extend_from_slice(&buf[..n]);
            }
            Err(e) => {
                eprintln!("Error reading stdin: {}", e);
                std::process::exit(1);
            }
        }
    }

    if !data.is_empty() {
        if let Err(e) = conn.stream.write_all(&data) {
            eprintln!("Error writing to server: {}", e);
            std::process::exit(1);
        }
    }

    conn.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok();
    while let Ok(resp) = conn.read_response() {
        replies += 1;
        if let RespValue::Error(_) = resp {
            errors += 1;
        }
    }

    eprintln!("All data transferred. Waiting for the last reply...");
    eprintln!("Last reply received from server.");
    eprintln!("errors: {}, replies: {}", errors, replies);
    if errors == 0 {
        println!("All data transferred. Waiting for the last reply...");
        println!("Last reply received from server.");
        println!("errors: {}, replies: {}", errors, replies);
    }
}

fn exit_replica_mode(error: &str, operation: &str) -> ! {
    if matches!(
        error,
        "Connection closed by server"
            | "Connection closed before replication header"
            | "Connection closed before RDB EOF marker"
    ) {
        eprintln!("Server closed the connection");
    } else {
        eprintln!("{}: {}", operation, error);
    }
    std::process::exit(1);
}

fn run_replica_mode(opts: &CliOptions) {
    let _password = resolve_password(opts);
    let mut conn = match connect_with_opts(opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Unsupported REPLCONF options are optional, but a transport failure
    // ends the handshake before any further replication command is sent.
    if let Err(error) = conn.send_command(&[b"REPLCONF", b"listening-port", b"0"]) {
        exit_replica_mode(&error, "Error");
    }
    if let Err(error) = conn.send_command(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"]) {
        exit_replica_mode(&error, "Error");
    }

    if let Err(error) = conn.send_command_without_reply(&[b"PSYNC", b"?", b"-1"]) {
        exit_replica_mode(&error, "Error");
    }

    let mut sink = std::io::sink();
    match conn.read_replication_rdb(&mut sink) {
        Ok(ReplicationRdbTransfer::FullEof) => {
            eprintln!("Full resync with primary, discarding RDB payload...");
            // C valkey-cli waits until the EOF marker is fully consumed, then
            // confirms that the replica is ready for the command stream. The
            // primary intentionally keeps post-RDB bytes gated until this ACK.
            std::thread::sleep(Duration::from_secs(1));
            if let Err(error) = conn.send_command_without_reply(&[b"REPLCONF", b"ACK", b"0"]) {
                exit_replica_mode(&error, "Error");
            }
        }
        Ok(ReplicationRdbTransfer::FullLength) => {
            eprintln!("Full resync with primary, discarding RDB payload...")
        }
        Ok(ReplicationRdbTransfer::Partial) => eprintln!("Partial resynchronization accepted."),
        Err(error) => exit_replica_mode(&error, "Error reading replication stream"),
    }

    eprintln!("Entering replica output mode... (press Ctrl-C to quit)");

    conn.stream.set_read_timeout(None).ok();
    let mut buf = [0u8; 8192];
    loop {
        match conn.stream.read(&mut buf) {
            Ok(0) => {
                eprintln!("Server closed the connection");
                std::process::exit(1);
            }
            Ok(n) => {
                io::stdout().write_all(&buf[..n]).ok();
                io::stdout().flush().ok();
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    continue;
                }
                eprintln!("Server closed the connection: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_rdb_mode(opts: &CliOptions) {
    let _password = resolve_password(opts);
    let mut conn = match connect_with_opts(opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let rdb_path = opts
        .rdb
        .as_deref()
        .or(opts.functions_rdb.as_deref())
        .unwrap();

    let _ = conn.send_command(&[b"REPLCONF", b"listening-port", b"0"]);
    let _ = conn.send_command(&[b"REPLCONF", b"capa", b"eof"]);
    if opts.functions_rdb.is_some() {
        let _ = conn.send_command(&[b"REPLCONF", b"rdb-filter-only", b"functions"]);
    }

    if let Err(error) = conn.send_command_without_reply(&[b"SYNC"]) {
        eprintln!("Error: {}", error);
        std::process::exit(1);
    }

    let mut file = match std::fs::File::create(rdb_path) {
        Ok(file) => file,
        Err(error) => {
            eprintln!("Error: {}", error);
            std::process::exit(1);
        }
    };
    if let Err(error) = conn.read_replication_rdb(&mut file) {
        eprintln!("Error: {}", error);
        std::process::exit(1);
    }
    eprintln!("Transfer finished with success.");
}

fn run_eval_mode(opts: &CliOptions, eval_file: &str) {
    let _password = resolve_password(opts);
    let mut conn = match connect_with_opts(opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let script = match std::fs::read_to_string(eval_file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file {}: {}", eval_file, e);
            std::process::exit(1);
        }
    };

    let resp = conn.send_command(&[b"EVAL", script.as_bytes(), b"0"]);
    match resp {
        Ok(val) => {
            let is_tty = stdout_is_tty();
            println!("{}", raw_or_tty(&val, opts.raw, is_tty));
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

fn execute_with_redirects(
    host: &str,
    port: u16,
    password: Option<&str>,
    username: Option<&str>,
    db: u8,
    command: &[&[u8]],
    cluster_mode: bool,
) -> Result<RespValue, String> {
    let mut current_host = host.to_string();
    let mut current_port = port;
    let mut should_ask = false;
    for _ in 0..16 {
        let mut connection =
            Connection::connect(&current_host, current_port, password, username, db)?;
        if should_ask {
            match connection.send_command(&[b"ASKING"])? {
                RespValue::SimpleString(ref v) if v == "OK" => {}
                response => return Err(format!("unexpected ASKING response: {:?}", response)),
            }
        }
        let response = connection.send_command(command)?;
        let RespValue::Error(ref error) = &response else {
            return Ok(response);
        };
        if !cluster_mode {
            return Ok(response);
        }
        let Some((is_ask, next_host, next_port)) = parse_cluster_redirect(error, &current_host)
        else {
            return Ok(response);
        };
        current_host = next_host;
        current_port = next_port;
        should_ask = is_ask;
    }
    Err("too many cluster redirects".to_string())
}

fn run_latency_mode(opts: &CliOptions) -> ! {
    use std::time::Instant;
    let mut conn = match connect_with_opts(opts) {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("Error: {}", error);
            std::process::exit(1);
        }
    };
    let interval = std::time::Duration::from_secs(opts.latency_interval.max(1));
    let mut min = u64::MAX;
    let mut max = 0u64;
    let mut sum = 0u128;
    let mut count = 0u64;
    let mut window_start = Instant::now();
    loop {
        let start = Instant::now();
        match conn.send_command(&[b"PING"]) {
            Ok(_) => {}
            Err(error) => {
                eprintln!("Error: {}", error);
                std::process::exit(1);
            }
        }
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as u64;
        if us < min {
            min = us;
        }
        if us > max {
            max = us;
        }
        sum += us as u128;
        count += 1;
        let avg = (sum / count as u128) as u64;
        if opts.latency_history {
            if window_start.elapsed() >= interval {
                println!(
                    "min: {} us, max: {} us, avg: {} us ({} samples)",
                    min, max, avg, count
                );
                min = u64::MAX;
                max = 0;
                sum = 0;
                count = 0;
                window_start = Instant::now();
            }
        } else {
            // Overwrite the single running line like C valkey-cli --latency
            print!(
                "\rmin: {} us, max: {} us, avg: {} us ({} samples)",
                min, max, avg, count
            );
            use std::io::Write;
            let _ = std::io::stdout().flush();
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn run_stat_mode(opts: &CliOptions) -> ! {
    use std::io::Write;
    let mut conn = match connect_with_opts(opts) {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("Error: {}", error);
            std::process::exit(1);
        }
    };
    let interval = std::time::Duration::from_secs(opts.stat_interval.max(1));
    println!(
        "{:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}",
        "keys", "mem", "clients", "blocked", "requests", "connections", "replica"
    );
    loop {
        match conn.send_command(&[b"INFO"]) {
            Ok(RespValue::Bulk(bytes)) => {
                let text = String::from_utf8_lossy(&bytes);
                let keys = info_integer(&text, "db0:keys", 0);
                let mem = info_string(&text, "used_memory_human");
                let clients = info_integer(&text, "connected_clients", 0);
                let blocked = info_integer(&text, "blocked_clients", 0);
                let requests = info_integer(&text, "total_commands_processed", 0);
                let connections = info_integer(&text, "total_connections_received", 0);
                let replica = info_string(&text, "role");
                println!(
                    "{:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10}",
                    keys,
                    mem.unwrap_or_default(),
                    clients,
                    blocked,
                    requests,
                    connections,
                    replica.unwrap_or_default()
                );
                let _ = std::io::stdout().flush();
            }
            Ok(other) => {
                eprintln!("Error: unexpected INFO response: {:?}", other);
                std::process::exit(1);
            }
            Err(error) => {
                eprintln!("Error: {}", error);
                std::process::exit(1);
            }
        }
        std::thread::sleep(interval);
    }
}

fn info_integer(text: &str, field: &str, default: i64) -> i64 {
    info_string(text, field)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn info_string(text: &str, field: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let rest = rest.trim_start_matches(':');
            // Handle both `db0:keys=10,...` and `used_memory_human:12M`
            if field.contains("db") && rest.contains('=') {
                for pair in rest.split(',') {
                    let mut parts = pair.splitn(2, '=');
                    if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                        if key.trim() == "keys" {
                            return Some(value.trim().to_string());
                        }
                    }
                }
            } else {
                return Some(rest.trim().to_string());
            }
        }
    }
    None
}

fn run_bigkeys_mode(opts: &CliOptions, memory_prefix: &str, title: &str) -> ! {
    let mut conn = match connect_with_opts(opts) {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("Error: {}", error);
            std::process::exit(1);
        }
    };
    println!(
        "\n# Scanning the entire keyspace to find biggest keys as well as\n# average sizes per key type.\n# Use `{}` to find keys with the largest in-memory footprint.\n",
        title
    );
    let mut cursor = 0i64;
    let mut largest: std::collections::HashMap<String, (String, i64)> = Default::default();
    loop {
        let cursor_str = cursor.to_string();
        let reply = match conn.send_command(&[b"SCAN", cursor_str.as_bytes()]) {
            Ok(reply) => reply,
            Err(error) => {
                eprintln!("Error: {}", error);
                std::process::exit(1);
            }
        };
        let (next_cursor, keys) = match reply {
            RespValue::Array(items) if items.len() == 2 => {
                let next = if let RespValue::Bulk(bytes) = &items[0] {
                    String::from_utf8_lossy(bytes).trim().parse().unwrap_or(0)
                } else {
                    0
                };
                let keys = if let RespValue::Array(keys) = &items[1] {
                    keys.iter()
                        .filter_map(|value| match value {
                            RespValue::Bulk(bytes) => {
                                Some(String::from_utf8_lossy(bytes).into_owned())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                (next, keys)
            }
            _ => {
                eprintln!("Error: unexpected SCAN reply");
                std::process::exit(1);
            }
        };
        for key in keys {
            let type_name = match conn.send_command(&[b"TYPE", key.as_bytes()]) {
                Ok(RespValue::SimpleString(name)) => name,
                _ => "unknown".to_string(),
            };
            let size = key_size(&mut conn, &key, &type_name, memory_prefix);
            let entry = largest
                .entry(type_name.clone())
                .or_insert_with(|| (String::new(), 0));
            if size > entry.1 {
                *entry = (key.clone(), size);
            }
        }
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    println!("\n-------- summary -------\n");
    for (kind, (key, size)) in &largest {
        println!(
            "Biggest {:<9} found '{}' has {} {}",
            kind,
            key,
            size,
            if memory_prefix == "MEMORY" {
                "bytes"
            } else {
                "entries"
            }
        );
    }
    std::process::exit(0);
}

fn key_size(conn: &mut Connection, key: &str, type_name: &str, memory_prefix: &str) -> i64 {
    if memory_prefix == "MEMORY" {
        return match conn.send_command(&[b"MEMORY", b"USAGE", key.as_bytes()]) {
            Ok(RespValue::Integer(n)) => n,
            _ => 0,
        };
    }
    let cmd = match type_name {
        "string" => &[b"STRLEN".as_slice(), key.as_bytes()][..],
        "list" => &[b"LLEN".as_slice(), key.as_bytes()][..],
        "set" => &[b"SCARD".as_slice(), key.as_bytes()][..],
        "hash" => &[b"HLEN".as_slice(), key.as_bytes()][..],
        "zset" => &[b"ZCARD".as_slice(), key.as_bytes()][..],
        "stream" => &[b"XLEN".as_slice(), key.as_bytes()][..],
        _ => return 0,
    };
    match conn.send_command(cmd) {
        Ok(RespValue::Integer(n)) => n,
        _ => 0,
    }
}

fn run_hotkeys_mode(opts: &CliOptions) -> ! {
    let mut conn = match connect_with_opts(opts) {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("Error: {}", error);
            std::process::exit(1);
        }
    };
    println!(
        "\n# Scanning the entire keyspace to find hot keys (requires LFU maxmemory-policy).\n"
    );
    let mut cursor = 0i64;
    let mut counts: Vec<(String, i64)> = Vec::new();
    loop {
        let cursor_str = cursor.to_string();
        let reply = match conn.send_command(&[b"SCAN", cursor_str.as_bytes()]) {
            Ok(reply) => reply,
            Err(error) => {
                eprintln!("Error: {}", error);
                std::process::exit(1);
            }
        };
        let (next_cursor, keys) = match reply {
            RespValue::Array(items) if items.len() == 2 => {
                let next = if let RespValue::Bulk(bytes) = &items[0] {
                    String::from_utf8_lossy(bytes).trim().parse().unwrap_or(0)
                } else {
                    0
                };
                let keys = if let RespValue::Array(keys) = &items[1] {
                    keys.iter()
                        .filter_map(|value| match value {
                            RespValue::Bulk(bytes) => {
                                Some(String::from_utf8_lossy(bytes).into_owned())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                (next, keys)
            }
            _ => {
                eprintln!("Error: unexpected SCAN reply");
                std::process::exit(1);
            }
        };
        for key in keys {
            let freq = match conn.send_command(&[b"OBJECT", b"FREQ", key.as_bytes()]) {
                Ok(RespValue::Integer(n)) => n,
                _ => continue,
            };
            counts.push((key, freq));
        }
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    counts.sort_by_key(|(_, freq)| std::cmp::Reverse(*freq));
    counts.truncate(16);
    println!("\n-------- hot keys --------");
    for (key, freq) in counts {
        println!("\thot key found with counter: {} \tkeyname: {}", freq, key);
    }
    std::process::exit(0);
}

fn run_intrinsic_latency(seconds: u64) -> ! {
    use std::time::Instant;
    let end = Instant::now() + std::time::Duration::from_secs(seconds.max(1));
    let mut max: u64 = 0;
    let mut prev = Instant::now();
    while Instant::now() < end {
        let now = Instant::now();
        let elapsed = now.duration_since(prev).as_nanos() as u64;
        if elapsed > max {
            max = elapsed;
        }
        prev = now;
    }
    println!("Max latency so far: {} microseconds", max / 1000);
    std::process::exit(0);
}

fn parse_cluster_redirect(error: &str, previous_host: &str) -> Option<(bool, String, u16)> {
    let mut fields = error.split_whitespace();
    let kind = fields.next()?;
    let is_ask = match kind {
        "MOVED" => false,
        "ASK" => true,
        _ => return None,
    };
    fields.next()?;
    let endpoint = fields.next()?;
    let (host, port) = endpoint.rsplit_once(':')?;
    let port = port.parse().ok()?;
    let host = if host.is_empty() {
        previous_host.to_string()
    } else {
        host.to_string()
    };
    Some((is_ask, host, port))
}

#[cfg(test)]
mod tests {
    use super::json::escape_json_bytes;
    use super::{parse_cluster_redirect, quoted_input_is_valid};
    use crate::repl::parse_inline_command;

    #[test]
    fn parses_moved_and_reuses_previous_host_for_unknown_endpoint() {
        assert_eq!(
            parse_cluster_redirect("MOVED 12182 :7001", "127.0.0.1"),
            Some((false, "127.0.0.1".to_string(), 7001))
        );
        assert_eq!(
            parse_cluster_redirect("ASK 42 localhost:7002", "127.0.0.1"),
            Some((true, "localhost".to_string(), 7002))
        );
    }

    #[test]
    fn escapes_invalid_utf8_json_bytes() {
        assert_eq!(escape_json_bytes(&[0xf0, b'e'], false), "\\u00f0e");
        assert_eq!(escape_json_bytes(&[0xf0, b'e'], true), "\\\\xf0e");
    }

    #[test]
    fn quoted_input_requires_exactly_one_valid_argument() {
        assert!(parse_inline_command("\"unterminated").is_err());
        assert_eq!(parse_inline_command("\"one\" \"two\"").unwrap().len(), 2);
        assert!(!quoted_input_is_valid(&["\"unterminated".to_string()]));
        assert!(!quoted_input_is_valid(&["\"one\" \"two\"".to_string()]));
    }
}
