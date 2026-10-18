use crate::engine::extension::script::{ScriptError, ScriptFlags};

static NOSCRIPT_SUBCMDS: &[&str] = &[
    "client|getname",
    "client|getredir",
    "client|id",
    "client|info",
    "client|no-evict",
    "client|reply",
    "client|setname",
    "client|tracking",
    "client|trackinginfo",
    "client|unpause",
    "config|get",
    "config|resetstat",
    "debug|object",
    "debug|protocol",
    "debug|sleep",
    "debug|struct",
];

static NOSCRIPT_BLOCK_SUBCMDS: &[&str] = &["cluster|reset"];

pub(crate) fn is_blocked_subcmd(cmd: &str, subcmd: &str) -> bool {
    let key = format!(
        "{}|{}",
        cmd.to_ascii_lowercase(),
        subcmd.to_ascii_lowercase()
    );
    NOSCRIPT_BLOCK_SUBCMDS.binary_search(&key.as_str()).is_ok()
}

pub(crate) fn is_noscript_subcmd(cmd: &str, subcmd: &str) -> bool {
    let key = format!(
        "{}|{}",
        cmd.to_ascii_lowercase(),
        subcmd.to_ascii_lowercase()
    );
    NOSCRIPT_SUBCMDS.binary_search(&key.as_str()).is_ok()
}

#[cfg(test)]
pub(crate) fn extract_function_name(code: &str) -> String {
    let lowered = code.to_ascii_lowercase();
    let marker = "redis.register_function";
    let mut search_start = 0;
    while let Some(pos) = lowered[search_start..].find(marker) {
        let abs_pos = search_start + pos;
        let after = &code[abs_pos + marker.len()..];
        let after = after.trim_start();
        if !after.starts_with('(') {
            search_start = abs_pos + 1;
            continue;
        }
        let after_paren = after[1..].trim_start();
        if let Some(name) = after_paren.strip_prefix('\'') {
            if let Some(end) = name.find('\'') {
                return name[..end].to_string();
            }
        } else if let Some(name) = after_paren.strip_prefix('"') {
            if let Some(end) = name.find('"') {
                return name[..end].to_string();
            }
        }
        search_start = abs_pos + 1;
    }

    let mut name = String::from("f_");
    for b in code.bytes().take(6) {
        if b.is_ascii_alphanumeric() {
            name.push(b as char);
        }
    }
    if name.len() > 2 {
        name
    } else {
        "f_anon".to_string()
    }
}

pub(crate) fn parse_shebang(
    code: &str,
    default_flags: ScriptFlags,
) -> Result<(String, ScriptFlags, bool), ScriptError> {
    let mut flags = default_flags;
    let mut body = code;
    let mut has_shebang = false;

    if let Some(rest) = code.strip_prefix("#!") {
        if let Some(end) = rest.find('\n') {
            has_shebang = true;
            let shebang = &rest[..end];
            let parts: Vec<&str> = shebang.split_whitespace().collect();
            if parts
                .first()
                .map(|engine| !engine.eq_ignore_ascii_case("lua"))
                .unwrap_or(true)
            {
                return Err(ScriptError::Runtime(
                    "ERR Could not find scripting engine".to_string(),
                ));
            }
            if parts.len() > 1 {
                for part in &parts[1..] {
                    if let Some(val) = part.strip_prefix("flags=") {
                        for flag in val.split(',') {
                            match flag.trim() {
                                "" => {}
                                "no-writes" => {
                                    flags |= crate::engine::extension::script::SCRIPT_READ_ONLY
                                        | crate::engine::extension::script::SCRIPT_ALLOW_OOM
                                }
                                "allow-oom" => {
                                    flags |= crate::engine::extension::script::SCRIPT_ALLOW_OOM
                                }
                                "allow-cross-slot" | "allow-cross-slot-keys" => {
                                    flags |=
                                        crate::engine::extension::script::SCRIPT_ALLOW_CROSS_SLOT
                                }
                                "no-cluster" => {
                                    flags |= crate::engine::extension::script::SCRIPT_NO_CLUSTER
                                }
                                "allow-stale" => {
                                    flags |= crate::engine::extension::script::SCRIPT_ALLOW_STALE;
                                }
                                _ => {
                                    return Err(ScriptError::Runtime(
                                        "ERR Unexpected flag in script shebang".to_string(),
                                    ))
                                }
                            }
                        }
                    } else if part.starts_with("name=") {
                        // FUNCTION LOAD uses name= in the same shebang grammar.
                    } else {
                        return Err(ScriptError::Runtime(
                            "ERR Unknown lua shebang option".to_string(),
                        ));
                    }
                }
            }
            body = &rest[end + 1..];
        }
    }

    Ok((body.to_string(), flags, has_shebang))
}

pub(crate) fn extract_shebang_name(code: &str) -> Result<String, String> {
    let shebang = if let Some(rest) = code.strip_prefix("#!") {
        if let Some(end) = rest.find('\n') {
            &rest[..end]
        } else {
            rest
        }
    } else {
        return Err("ERR Library name was not given".to_string());
    };

    let parts: Vec<&str> = shebang.split_whitespace().collect();

    let mut name: Option<String> = None;
    let mut name_count = 0;

    for part in &parts {
        if !part.contains('=') {
            continue;
        }
        if let Some(val) = part.strip_prefix("name=") {
            name_count += 1;
            let stripped = if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
                &val[1..val.len() - 1]
            } else {
                val
            };
            if !stripped.is_empty() {
                name = Some(stripped.to_string());
            }
        } else if part.starts_with("flags=") {
            // flags are handled by parse_shebang; skip here
        } else {
            return Err(format!("ERR Invalid metadata value given: {}", part));
        }
    }

    if name_count > 1 {
        return Err(
            "ERR Invalid metadata value, name argument was given multiple times".to_string(),
        );
    }

    match name {
        Some(n) => Ok(n),
        None => Err("ERR Library name was not given".to_string()),
    }
}
