use std::collections::VecDeque;
use std::io::{self, BufRead, Write};
use std::time::Duration;

use super::conn::Connection;
use super::resp::RespValue;
use versal::foundation::io::sys::os::{poll_descriptors, PollDescriptor};

pub struct Repl {
    pub history: VecDeque<String>,
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub db: u8,
    pub raw: bool,
    pub subscribed: bool,
    pub subscribed_count: usize,
    pub shard_subscribed_count: usize,
    pub server_addr: (String, u16),
}

impl Repl {
    pub fn new(host: String, port: u16, password: Option<String>, db: u8, raw: bool) -> Self {
        Repl {
            history: VecDeque::new(),
            host: host.clone(),
            port,
            password,
            db,
            raw,
            subscribed: false,
            subscribed_count: 0,
            shard_subscribed_count: 0,
            server_addr: (host, port),
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        let mut conn = Connection::connect(
            &self.host,
            self.port,
            self.password.as_deref(),
            None,
            self.db,
        )?;

        let is_tty = super::stdout_is_tty();
        let show_prompt = super::stdin_is_tty();

        if show_prompt {
            println!("Connected to {}:{}", self.host, self.port);
            println!("Type 'quit' or 'exit' to disconnect.");
        }

        let mut input_buf = String::new();
        loop {
            // When subscribed, poll for push messages and stdin with a timeout
            if self.subscribed {
                self.read_push_messages(&mut conn, is_tty)?;
                let mut stdin_ready = false;
                if is_tty && self.subscribed {
                    print!(
                        "Reading messages... (press Ctrl-C to quit or any key to type command)\r"
                    );
                    io::stdout().flush().ok();
                }

                let server_fd = conn.raw_fd();
                let mut descriptors = [
                    PollDescriptor::new(server_fd, libc::POLLIN),
                    PollDescriptor::new(libc::STDIN_FILENO, libc::POLLIN),
                ];
                if poll_descriptors(&mut descriptors, Duration::from_secs(5)).is_err() {
                    continue;
                }
                if is_tty {
                    print!("\x1b[K");
                    io::stdout().flush().ok();
                }
                if descriptors[0].revents() & libc::POLLIN != 0 {
                    self.read_push_messages(&mut conn, is_tty)?;
                }
                if descriptors[1].revents() & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                    stdin_ready = true;
                }
                if !stdin_ready {
                    continue;
                }
                input_buf.clear();
                match io::stdin().lock().read_line(&mut input_buf) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Read error: {}", e);
                        break;
                    }
                }
            } else {
                if show_prompt {
                    print!("{}:{}> ", self.host, self.port);
                    io::stdout().flush().ok();
                }
                input_buf.clear();
                match io::stdin().lock().read_line(&mut input_buf) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Read error: {}", e);
                        break;
                    }
                }
            }

            let line = input_buf.trim().to_string();
            if line.is_empty() {
                continue;
            }

            if line == "quit" || line == "exit" || line == "q" {
                break;
            }

            if !line.is_empty() {
                if self.history.len() >= 1000 {
                    self.history.pop_front();
                }
                self.history.push_back(line.clone());
            }

            if let Err(e) = self.execute_line(&mut conn, &line) {
                eprintln!("{}", e);
            }
            io::stdout().flush().ok();
        }

        if show_prompt {
            println!("\nDisconnected.");
        }

        Ok(())
    }

    fn execute_line(&mut self, conn: &mut Connection, line: &str) -> Result<(), String> {
        if line.starts_with(':') {
            return self.handle_special_command(conn, line);
        }

        let args = parse_inline_command(line)?;
        if args.is_empty() {
            return Ok(());
        }

        let cmd_upper = String::from_utf8_lossy(&args[0]).to_uppercase();
        let is_subscribe_cmd = matches!(
            cmd_upper.as_str(),
            "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE"
        );
        let is_unsubscribe_cmd = matches!(
            cmd_upper.as_str(),
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE"
        );
        let is_shard_subscription_cmd = matches!(cmd_upper.as_str(), "SSUBSCRIBE" | "SUNSUBSCRIBE");

        let args_refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let is_tty = super::stdout_is_tty();

        if is_subscribe_cmd {
            self.subscribed = true;
            let first_resp = conn.send_command(&args_refs)?;
            let mut all_responses = vec![first_resp];
            conn.stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .ok();
            while let Ok(r) = conn.read_response() {
                all_responses.push(r);
            }
            conn.stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .ok();
            if let Some((_, count)) = all_responses.last().and_then(subscription_confirmation) {
                if is_shard_subscription_cmd {
                    self.shard_subscribed_count = count;
                } else {
                    self.subscribed_count = count;
                }
            }
            self.subscribed = self.subscribed_count > 0 || self.shard_subscribed_count > 0;
            for resp in &all_responses {
                if is_tty {
                    println!("{}", resp.to_string_tty());
                } else if let RespValue::Push(items) = resp {
                    for item in items {
                        println!("{}", item.to_string_raw());
                    }
                } else {
                    println!("{}", resp.to_string_raw());
                }
            }
            io::stdout().flush().ok();
            return Ok(());
        }

        if is_unsubscribe_cmd {
            let first_resp = conn.send_command(&args_refs)?;
            let mut all_responses = vec![first_resp];
            conn.stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .ok();
            while let Ok(r) = conn.read_response() {
                all_responses.push(r);
            }
            conn.stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .ok();
            if let Some((_, count)) = all_responses.last().and_then(subscription_confirmation) {
                if is_shard_subscription_cmd {
                    self.shard_subscribed_count = count;
                } else {
                    self.subscribed_count = count;
                }
            }
            self.subscribed = self.subscribed_count > 0 || self.shard_subscribed_count > 0;
            for resp in &all_responses {
                println!("{}", raw_or_tty(resp, self.raw, is_tty));
            }
            io::stdout().flush().ok();
            return Ok(());
        }

        // Handle RESET command
        if cmd_upper == "RESET" {
            self.subscribed = false;
            let resp = conn.send_command(&args_refs)?;
            println!("{}", raw_or_tty(&resp, self.raw, is_tty));
            io::stdout().flush().ok();
            return Ok(());
        }

        // Handle CONNECT command
        if cmd_upper == "CONNECT" && args.len() >= 3 {
            let new_host = String::from_utf8_lossy(&args[1]).to_string();
            let new_port: u16 = String::from_utf8_lossy(&args[2])
                .parse()
                .map_err(|_| "Invalid port")?;
            *conn =
                Connection::connect(&new_host, new_port, self.password.as_deref(), None, self.db)?;
            self.host = new_host;
            self.port = new_port;
            self.server_addr = (self.host.clone(), self.port);
            println!("Connected to {}:{}", self.host, self.port);
            io::stdout().flush().ok();
            return Ok(());
        }

        let resp = match conn.send_command(&args_refs) {
            Ok(resp) => resp,
            Err(_) => {
                *conn = Connection::connect(
                    &self.host,
                    self.port,
                    self.password.as_deref(),
                    None,
                    self.db,
                )?;
                return Err("Reconnected to server".to_string());
            }
        };
        if cmd_upper == "SELECT"
            && matches!(resp, RespValue::SimpleString(ref value) if value == "OK")
        {
            if let Some(db) = args
                .get(1)
                .and_then(|value| std::str::from_utf8(value).ok())
                .and_then(|value| value.parse().ok())
            {
                self.db = db;
            }
        }
        println!("{}", raw_or_tty(&resp, self.raw, is_tty));
        io::stdout().flush().ok();
        Ok(())
    }

    fn handle_special_command(&mut self, _conn: &mut Connection, line: &str) -> Result<(), String> {
        let cmd = line.trim_start_matches(':').trim();
        if cmd == "get pubsub" {
            if self.subscribed {
                println!("1");
            } else {
                println!("0");
            }
            io::stdout().flush().ok();
            return Ok(());
        }
        Err(format!("Unknown special command: {}", line))
    }

    fn read_push_messages(&mut self, conn: &mut Connection, is_tty: bool) -> Result<(), String> {
        loop {
            conn.stream
                .set_read_timeout(Some(Duration::from_millis(10)))
                .ok();
            match conn.read_response() {
                Ok(resp) => {
                    if is_tty {
                        println!("{}", resp.to_string_tty());
                    } else if let RespValue::Push(ref items) = resp {
                        for item in items {
                            println!("{}", item.to_string_raw());
                        }
                    } else {
                        println!("{}", resp.to_string_raw());
                    }
                    io::stdout().flush().ok();
                }
                Err(_) => break,
            }
        }
        conn.stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .ok();
        Ok(())
    }
}

fn subscription_confirmation(value: &RespValue) -> Option<(&str, usize)> {
    let items = match value {
        RespValue::Array(items) | RespValue::Push(items) => items,
        _ => return None,
    };
    let command = match items.first()? {
        RespValue::Bulk(value) => std::str::from_utf8(value).ok()?,
        RespValue::SimpleString(value) => value.as_str(),
        _ => return None,
    };
    let count = match items.get(2)? {
        RespValue::Integer(value) if *value >= 0 => *value as usize,
        _ => return None,
    };
    Some((command, count))
}

pub fn raw_or_tty(val: &RespValue, raw: bool, tty: bool) -> String {
    if raw || !tty {
        val.to_string_raw()
    } else {
        val.to_string_tty()
    }
}

/// Parse an inline command string into individual arguments,
/// handling double-quoted and single-quoted strings.
pub fn parse_inline_command(line: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut args = Vec::new();
    let mut cur = Vec::new();
    let mut argument_started = false;
    let bytes = line.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b' ' || b == b'\t' {
            if argument_started {
                args.push(std::mem::take(&mut cur));
                argument_started = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => {
                argument_started = true;
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    cur.push(bytes[i]);
                    i += 1;
                }
                if i == bytes.len() {
                    return Err("Invalid quoted string".to_string());
                }
                i += 1;
            }
            b'"' => {
                argument_started = true;
                i += 1;
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        i += 1;
                        closed = true;
                        break;
                    }
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        let next = bytes[i + 1];
                        cur.push(match next {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'\\' => b'\\',
                            b'"' => b'"',
                            b'\'' => b'\'',
                            b'x' => {
                                if i + 3 < bytes.len() {
                                    let hex = &bytes[i + 2..i + 4];
                                    if let Ok(hex_str) = std::str::from_utf8(hex) {
                                        if let Ok(val) = u8::from_str_radix(hex_str, 16) {
                                            i += 4;
                                            cur.push(val);
                                            continue;
                                        }
                                    }
                                }
                                next
                            }
                            _ => next,
                        });
                        i += 2;
                        continue;
                    }
                    cur.push(bytes[i]);
                    i += 1;
                }
                if !closed {
                    return Err("Invalid quoted string".to_string());
                }
            }
            _ => {
                argument_started = true;
                cur.push(b);
                i += 1;
            }
        }
    }

    if argument_started {
        args.push(cur);
    }

    Ok(args)
}

pub fn print_help() {
    println!("Usage: versal-cli [OPTIONS] [COMMAND [ARGS]...]");
    println!();
    println!("Options:");
    println!("  -h <host>        Server hostname (default: 127.0.0.1)");
    println!("  -p <port>        Server port (default: 6379)");
    println!("  -s <socket>      Server socket (overrides hostname and port)");
    println!("  -a <password>    Password to use when connecting to the server");
    println!("  -u <uri>         Server URI (redis://, valkey://, etc.)");
    println!("  -n <db>          Database number");
    println!("  -v, --version    Show version information");
    println!("  --raw            Use raw output formatting");
    println!("  --json           Use JSON output formatting");
    println!("  --quoted-json    Use quoted JSON output formatting");
    println!("  --quoted-input   Enable quoted input handling");
    println!("  -x               Read next argument from STDIN");
    println!("  -X <tag>         Read argument from STDIN with tag");
    println!("  -D <delim>       Delimiter between raw output and input");
    println!("  -r <repeat>      Execute command multiple times");
    println!("  -c               Enable cluster mode (follow redirects)");
    println!("  -e, --interactive  Interactive mode");
    println!("  --eval <file>    Evaluate Lua script file");
    println!("  --scan           Scan keyspace");
    println!("  --pattern <pat>  Pattern for scanning");
    println!("  --quoted-pattern <pat>  Quoted pattern for scanning");
    println!("  --pipe           Transfer raw RESP protocol");
    println!("  --replica        Connect as a replica");
    println!("  --rdb <file>     Transfer RDB dump from server");
    println!("  --functions-rdb <file>  Transfer functions RDB from server");
    println!("  --user <user>    ACL user");
    println!("  --no-auth-warning  Suppress auth warning");
    println!("  --cluster-yes    Yes to cluster commands");
    println!("  --cluster <cmd>  Cluster manager command");
    println!("  --help           Show this help message");
    println!();
    println!("Examples:");
    println!("  versal-cli ping");
    println!("  versal-cli -h 10.0.0.1 -p 6380 get mykey");
    println!("  versal-cli -a mypassword set mykey myvalue");
    println!("  versal-cli  # interactive mode");
}
