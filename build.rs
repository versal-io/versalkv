use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[path = "src/framework/flag_names.rs"]
mod flag_names;

fn flag_to_const(flag: &str) -> Option<&'static str> {
    flag_names::flag_to_const(flag)
}

fn group_to_module(group: &str) -> String {
    match group {
        "sorted_set" => "zset".to_string(),
        "transactions" => "transaction".to_string(),
        "bitmap" | "cluster" | "connection" | "generic" | "geo" | "hash" | "hyperloglog"
        | "list" | "pubsub" | "scripting" | "sentinel" | "server" | "set" | "stream" | "string" => {
            group.to_string()
        }
        other => {
            eprintln!(
                "cargo:warning=build.rs: unknown command group '{}', mapping to 'generic'",
                other
            );
            "generic".to_string()
        }
    }
}

fn format_generated_files(paths: &[PathBuf]) {
    const RUSTFMT_BATCH_SIZE: usize = 64;
    for batch in paths.chunks(RUSTFMT_BATCH_SIZE) {
        let status = Command::new("rustfmt")
            .args(["--edition", "2021"])
            .args(batch)
            .status()
            .unwrap_or_else(|error| panic!("failed to run rustfmt: {error}"));
        assert!(status.success(), "rustfmt failed with status {status}");
    }
}

fn write_if_changed(path: &Path, content: &str) -> bool {
    if fs::read_to_string(path).is_ok_and(|existing| existing == content) {
        return false;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|error| panic!("failed to create {}: {error}", parent.display()));
    }
    fs::write(path, content)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
    true
}

fn write_rust_files_if_changed(files: Vec<(PathBuf, String)>) {
    let mut changed_paths = Vec::new();
    for (path, source) in files {
        if write_if_changed(&path, &source) {
            changed_paths.push(path);
        }
    }
    if !changed_paths.is_empty() {
        format_generated_files(&changed_paths);
    }
}

fn remove_stale_rust_files(directory: &Path, expected: &BTreeSet<PathBuf>) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let path = entry
            .expect("failed to read generated directory entry")
            .path();
        if path.extension().is_some_and(|extension| extension == "rs") && !expected.contains(&path)
        {
            fs::remove_file(&path)
                .unwrap_or_else(|error| panic!("failed to remove {}: {error}", path.display()));
        }
    }
}

fn runtime_command_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut remaining = source;
    while let Some(start) = remaining.find("#[command(") {
        let attribute = &remaining[start + "#[command(".len()..];
        let Some(end) = attribute.find(")]") else {
            break;
        };
        let arguments = &attribute[..end];
        let is_runtime = arguments
            .split(',')
            .any(|argument| argument.trim() == "runtime");
        if is_runtime {
            let first_quote = arguments
                .find('"')
                .expect("runtime command attribute must contain a command name");
            let name = &arguments[first_quote + 1..];
            let second_quote = name
                .find('"')
                .expect("runtime command attribute command name must be terminated");
            names.push(name[..second_quote].to_string());
        }
        remaining = &attribute[end + 2..];
    }
    names
}

fn collect_rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| entry.expect("failed to read command source directory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}

fn is_portable_command_module(source: &str) -> bool {
    !runtime_command_names(source).is_empty()
        || source.contains("command_macros::portable_command_support!()")
}

fn runtime_module_path(commands_dir: &Path, source_path: &Path) -> Vec<String> {
    let relative = source_path.strip_prefix(commands_dir).unwrap_or_else(|_| {
        panic!(
            "{} is outside {}",
            source_path.display(),
            commands_dir.display()
        )
    });
    let mut components = relative
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .expect("command source paths must be UTF-8")
                .to_owned()
        })
        .collect::<Vec<_>>();
    let file_name = components
        .pop()
        .expect("command source must have a file name");
    assert_ne!(
        file_name, "mod.rs",
        "wasm runtime handlers must be declared in leaf command files"
    );
    let file_stem = file_name
        .strip_suffix(".rs")
        .expect("runtime command source must be a Rust file");
    components.push(file_stem.to_owned());
    components
}

#[derive(Default)]
struct PortableCommandModule {
    source: Option<PathBuf>,
    children: BTreeMap<String, PortableCommandModule>,
}

impl PortableCommandModule {
    fn insert(&mut self, module_path: &[String], source_path: PathBuf) {
        let (module, rest) = module_path
            .split_first()
            .expect("runtime module path must not be empty");
        let node = self.children.entry(module.clone()).or_default();
        if rest.is_empty() {
            assert!(
                node.children.is_empty(),
                "runtime module {} cannot be both a source file and namespace",
                module_path.join("::")
            );
            assert!(
                node.source.replace(source_path).is_none(),
                "duplicate runtime module {}",
                module_path.join("::")
            );
        } else {
            assert!(
                node.source.is_none(),
                "runtime module {} cannot be both a source file and namespace",
                module_path.join("::")
            );
            node.insert(rest, source_path);
        }
    }

    fn render_children(&self, output: &mut String, depth: usize) {
        let indentation = "    ".repeat(depth);
        for (name, node) in &self.children {
            let module_ident = rust_module_ident(&normalize_module_name(name));
            if let Some(source_path) = &node.source {
                let path_literal = format!("{:?}", source_path.display().to_string());
                output.push_str(&format!("{indentation}#[path = {path_literal}]\n"));
                output.push_str(&format!("{indentation}pub(crate) mod {module_ident};\n"));
            } else {
                output.push_str(&format!("{indentation}pub(crate) mod {module_ident} {{\n"));
                node.render_children(output, depth + 1);
                output.push_str(&format!("{indentation}}}\n"));
            }
        }
    }
}

fn generate_runtime_command_catalog(manifest_dir: &Path) {
    let commands_dir = manifest_dir.join("src/framework/commands");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("Cargo must provide OUT_DIR"));
    let mut registrations = BTreeMap::new();
    let mut sources = Vec::new();
    collect_rust_sources(&commands_dir, &mut sources);
    let mut module_tree = PortableCommandModule::default();
    for source_path in sources {
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", source_path.display()));
        println!("cargo:rerun-if-changed={}", source_path.display());
        if !is_portable_command_module(&source) {
            continue;
        }
        let module_path = runtime_module_path(&commands_dir, &source_path);
        module_tree.insert(&module_path, source_path.clone());
        for name in runtime_command_names(&source) {
            let ident = name.replace("::", "_").replace('-', "_");
            let module = module_path
                .iter()
                .map(|component| rust_module_ident(&normalize_module_name(component)))
                .collect::<Vec<_>>()
                .join("::");
            let value = format!("crate::framework::commands::{module}::{ident}_RUNTIME_SPEC");
            if let Some(previous) = registrations.insert(name.clone(), value) {
                panic!("duplicate runtime command {name}: {previous}");
            }
        }
    }

    let mut catalog = String::from("&[\n");
    for registration in registrations.values() {
        catalog.push_str("    ");
        catalog.push_str(registration);
        catalog.push_str(",\n");
    }
    catalog.push_str("]\n");
    write_if_changed(&out_dir.join("runtime_command_catalog.rs"), &catalog);

    let mut modules = String::new();
    module_tree.render_children(&mut modules, 0);
    write_if_changed(&out_dir.join("portable_command_modules.rs"), &modules);
}

fn normalize_module_name(file_stem: &str) -> String {
    file_stem.replace('-', "_")
}

fn rust_module_ident(module_name: &str) -> String {
    const RUST_KEYWORDS: &[&str] = &[
        "type", "ref", "mut", "move", "match", "mod", "self", "super", "crate", "async", "await",
        "dyn", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "typeof",
        "unsized", "virtual", "yield",
    ];
    if RUST_KEYWORDS.contains(&module_name) {
        format!("r#{module_name}")
    } else {
        module_name.to_string()
    }
}

fn command_dispatch_group(cmd_name: &str) -> &'static str {
    match cmd_name {
        "REPLCONF" | "PSYNC" | "SYNC" => "Transport",
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "SSUBSCRIBE"
        | "SUNSUBSCRIBE" => "PubSubControl",
        "EXEC" | "DISCARD" | "MULTI" => "Transaction",
        "CLIENT" => "ClientAdmin",
        "AUTH" | "HELLO" => "Auth",
        "SHUTDOWN" => "Shutdown",
        _ => "Default",
    }
}

struct ParsedCmd {
    /// The JSON key (uppercase command name, e.g. "CLIENT" or "LIST")
    cmd_key: String,
    /// Lookup name for command_meta(): "CLIENT" or "CLIENT::LIST"
    lookup_name: String,
    /// Lowercase name for CommandSpec.name
    name: String,
    /// Module name (group mapped to Rust module)
    module: String,
    /// Original JSON file stem (e.g. "client", "client-list")
    file_stem: String,
    arity: i32,
    flags: Vec<String>,
    first_key: i32,
    last_key: i32,
    key_step: i32,
    container: Option<String>,
    dispatch_group: String,
    /// Canonical ACL key-access tokens merged across all keyspecs
    /// (subset of access/insert/update/delete). Empty for `RO`-only specs,
    /// which require no specific R/W bit. Mirrors C `ACLSelectorCheckKey`.
    key_flags: Vec<String>,
    acl_categories: Vec<String>,
}

fn compute_movable_keys(cmd: &serde_json::Value) -> bool {
    let key_specs = match cmd.get("key_specs").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return false,
    };
    let all_not_key = key_specs.iter().all(|ks| {
        ks.get("flags")
            .and_then(|f| f.as_array())
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("NOT_KEY")))
            .unwrap_or(false)
    });
    if all_not_key {
        return false;
    }
    for ks in key_specs {
        if let Some(flags) = ks.get("flags").and_then(|f| f.as_array()) {
            if flags.iter().any(|v| v.as_str() == Some("INCOMPLETE")) {
                return true;
            }
        }
        let bs = match ks.get("begin_search") {
            Some(bs) => bs,
            None => continue,
        };
        if bs.get("index").is_none() {
            return true;
        }
        if let Some(fk) = ks.get("find_keys") {
            if fk.get("range").is_none() {
                return true;
            }
        }
    }
    if key_specs.len() > 1 {
        let mut prev_lastkey: Option<i32> = None;
        let mut found_range = false;
        for ks in key_specs {
            let bs = match ks.get("begin_search") {
                Some(bs) if bs.get("index").is_some() => bs,
                _ => continue,
            };
            let range = match ks.get("find_keys").and_then(|fk| fk.get("range")) {
                Some(r) => r,
                _ => continue,
            };
            let pos = bs
                .get("index")
                .and_then(|i| i.get("pos"))
                .and_then(|p| p.as_i64())
                .unwrap_or(0) as i32;
            let st = range.get("step").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            let lk = range.get("lastkey").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            found_range = true;
            if st != 1 {
                return true;
            }
            if let Some(prev) = prev_lastkey {
                if prev != pos - 1 {
                    return true;
                }
            }
            let lastkey_abs = if lk < 0 { lk } else { pos + lk };
            prev_lastkey = Some(lastkey_abs);
        }
        if !found_range {
            return true;
        }
    }
    false
}

fn extract_key_spec(cmd: &serde_json::Value) -> (i32, i32, i32) {
    let key_specs = match cmd.get("key_specs").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return (0, 0, 0),
    };
    let all_not_key = key_specs.iter().all(|ks| {
        ks.get("flags")
            .and_then(|f| f.as_array())
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("NOT_KEY")))
            .unwrap_or(false)
    });
    if all_not_key {
        return (0, 0, 0);
    }
    // Merge multiple key specs into legacy first_key/last_key/key_step.
    // Legacy limitation: only handles specs with "index" begin_search and "range" find_keys.
    // For commands with multiple key specs (e.g. LMOVE src+dest), we compute the union.
    let mut first_key: i32 = 0;
    let mut last_key: i32 = 0;
    let mut key_step: i32 = 0;
    for ks in key_specs {
        let bs = match ks.get("begin_search") {
            Some(bs) if bs.get("index").is_some() => bs,
            _ => continue,
        };
        let range = match ks.get("find_keys").and_then(|fk| fk.get("range")) {
            Some(r) => r,
            _ => continue,
        };
        let pos = bs
            .get("index")
            .and_then(|i| i.get("pos"))
            .and_then(|p| p.as_i64())
            .unwrap_or(0) as i32;
        let lk = range.get("lastkey").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        let st = range.get("step").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        // Convert key-specs range to legacy absolute positions:
        //   key-spec lastkey: -1=to end (KSPEC_ARGNUM_LAST_ARG), 0=same as first, N>0=offset from first
        let legacy_last = match lk {
            -1 => -1,
            0 => pos,
            n if n > 0 => pos + n,
            _ => pos,
        };
        if first_key == 0 || pos < first_key {
            first_key = pos;
        }
        if legacy_last == -1 {
            last_key = -1;
        } else if last_key != -1 && legacy_last > last_key {
            last_key = legacy_last;
        }
        if key_step == 0 {
            key_step = st;
        }
    }
    (first_key, last_key, key_step)
}

/// Collect the ACL-relevant key-access tokens from all keyspecs, mapping the
/// JSON keyspec flags to the canonical lowercase tokens the runtime ACL check
/// understands. Only ACCESS/INSERT/UPDATE/DELETE carry R/W meaning (mirroring
/// C `ACLSelectorCheckKey`); the RO/RW/OW/RM *type* tokens are ignored, so a
/// keyspec of `['RO']` (no ACCESS) yields an empty set — meaning any matching
/// key pattern satisfies the command regardless of its R/W bits.
fn extract_key_flags(cmd: &serde_json::Value) -> Vec<String> {
    let key_specs = match cmd.get("key_specs").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return Vec::new(),
    };
    let mut has_ro = false;
    let mut has_rw = false;
    let mut has_ow = false;
    let mut has_rm = false;
    let mut has_access = false;
    let mut has_insert = false;
    let mut has_update = false;
    let mut has_delete = false;
    for ks in key_specs {
        let flags = match ks.get("flags").and_then(|f| f.as_array()) {
            Some(f) => f,
            None => continue,
        };
        for f in flags {
            match f.as_str() {
                Some("RO") => has_ro = true,
                Some("RW") => has_rw = true,
                Some("OW") => has_ow = true,
                Some("RM") => has_rm = true,
                Some("ACCESS") => has_access = true,
                Some("INSERT") => has_insert = true,
                Some("UPDATE") => has_update = true,
                Some("DELETE") => has_delete = true,
                _ => {}
            }
        }
    }
    let mut out = Vec::new();
    // Category flag first (mirrors C addReplyFlagsForKeyArgs order: RO/RW/OW/RM
    // precede the access flags). ACL bit derivation ignores the category, so
    // including it here is display-only and safe.
    if has_ro {
        out.push("RO".to_string());
    }
    if has_rw {
        out.push("RW".to_string());
    }
    if has_ow {
        out.push("OW".to_string());
    }
    if has_rm {
        out.push("RM".to_string());
    }
    if has_access {
        out.push("access".to_string());
    }
    if has_insert {
        out.push("insert".to_string());
    }
    if has_update {
        out.push("update".to_string());
    }
    if has_delete {
        out.push("delete".to_string());
    }
    out
}

fn command_const_name(cmd: &ParsedCmd) -> String {
    format!(
        "{}_META",
        cmd.lookup_name.replace("::", "_").replace('-', "_")
    )
}

fn command_flags_expr(cmd: &ParsedCmd) -> String {
    if cmd.flags.is_empty() {
        "0".to_string()
    } else {
        format!("({})", cmd.flags.join(" | "))
    }
}

fn string_slice_expr(values: &[String]) -> String {
    if values.is_empty() {
        return "&[]".to_string();
    }
    let items: Vec<String> = values.iter().map(|value| format!("{value:?}")).collect();
    format!("&[{}]", items.join(", "))
}

fn command_subcommand_name<'a>(cmd: &'a ParsedCmd, container: &str) -> &'a str {
    cmd.name
        .strip_prefix(&format!("{}-", container.to_ascii_lowercase()))
        .unwrap_or(&cmd.name)
}

fn generate_command_metadata(all_cmds: &[ParsedCmd], generated_root: &Path) {
    let generated_dir = generated_root.join("commands");
    fs::create_dir_all(&generated_dir)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", generated_dir.display()));
    let legacy_path = generated_root.join("commands.rs");
    if legacy_path.exists() {
        fs::remove_file(&legacy_path)
            .unwrap_or_else(|error| panic!("failed to remove {}: {error}", legacy_path.display()));
    }

    let mut generated_paths = Vec::with_capacity(all_cmds.len() + 2);
    let mut generated_files = Vec::with_capacity(all_cmds.len() + 2);
    let mut facade = String::from(
        "// Auto-generated by build.rs from thirdparty/valkey/src/commands/*.json. Do not edit.\n\n",
    );
    facade.push_str("#![allow(non_upper_case_globals)]\n\n");

    for cmd in all_cmds {
        let module_name = normalize_module_name(&cmd.file_stem);
        let module_ident = rust_module_ident(&module_name);
        let const_name = command_const_name(cmd);
        let path = generated_dir.join(format!("{module_name}.rs"));
        let mut source = format!(
            "// Auto-generated by build.rs from thirdparty/valkey/src/commands/{}.json. Do not edit.\n\n",
            cmd.file_stem
        );
        source.push_str("use crate::framework::{CmdFlags, CommandMeta, DispatchGroup};\n");
        for flag in cmd.flags.iter().collect::<BTreeSet<_>>() {
            source.push_str(&format!("use crate::framework::{flag};\n"));
        }
        source.push('\n');
        source.push_str(&format!(
            "pub const {const_name}: CommandMeta = CommandMeta {{\n\
             name: {:?},\n\
             arity: {},\n\
             flags: {} as CmdFlags,\n\
             dispatch_group: DispatchGroup::{},\n\
             first_key: {},\n\
             last_key: {},\n\
             key_step: {},\n\
             container: {},\n\
             key_flags: {},\n\
             acl_categories: {},\n\
             get_keys: None,\n\
             }};\n",
            cmd.name,
            cmd.arity,
            command_flags_expr(cmd),
            cmd.dispatch_group,
            cmd.first_key,
            cmd.last_key,
            cmd.key_step,
            cmd.container
                .as_ref()
                .map(|container| format!("Some({:?})", container.to_ascii_lowercase()))
                .unwrap_or_else(|| "None".to_string()),
            string_slice_expr(&cmd.key_flags),
            string_slice_expr(&cmd.acl_categories),
        ));
        generated_paths.push(path.clone());
        generated_files.push((path, source));
        facade.push_str(&format!("mod {module_ident};\n"));
        facade.push_str(&format!("pub use {module_ident}::{const_name};\n"));
    }

    facade.push_str("\npub mod catalog;\n");
    facade.push_str("pub use catalog::{acl_command_meta, ALL_COMMAND_META};\n");
    facade.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    facade.push_str(
        "pub use catalog::{subcommand_meta, top_level_meta, BUILTIN_ACL_CATEGORIES, SUBCOMMAND_GATE_FLAGS, SUBCOMMAND_NAMES, TOP_LEVEL_COMMANDS};\n",
    );
    let facade_path = generated_dir.join("mod.rs");
    generated_paths.push(facade_path.clone());
    generated_files.push((facade_path, facade));

    let mut catalog = String::from(
        "// Auto-generated by build.rs from thirdparty/valkey/src/commands/*.json. Do not edit.\n\n",
    );
    catalog.push_str("use crate::framework::CommandMeta;\n");
    catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    catalog.push_str("use crate::framework::CmdFlags;\n");
    let mut used_flags = BTreeSet::new();
    let mut acl_categories = BTreeSet::new();
    for cmd in all_cmds {
        if cmd.container.is_some() {
            used_flags.extend(cmd.flags.iter().cloned());
        }
        acl_categories.extend(cmd.acl_categories.iter().cloned());
    }
    for flag in used_flags {
        catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
        catalog.push_str(&format!("use crate::framework::{flag};\n"));
    }
    catalog.push_str("\npub const ALL_COMMAND_META: &[&CommandMeta] = &[\n");
    for cmd in all_cmds {
        catalog.push_str(&format!("    &super::{},\n", command_const_name(cmd)));
    }
    catalog.push_str("];\n\n");
    catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    catalog.push_str("pub const TOP_LEVEL_COMMANDS: &[&str] = &[\n");
    for cmd in all_cmds.iter().filter(|cmd| cmd.container.is_none()) {
        catalog.push_str(&format!("    {:?},\n", cmd.lookup_name));
    }
    catalog.push_str("];\n\n");
    catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    catalog.push_str("pub const SUBCOMMAND_NAMES: &[&str] = &[\n");
    for cmd in all_cmds.iter().filter(|cmd| cmd.container.is_some()) {
        let container = cmd.container.as_deref().expect("filtered above");
        let name = format!(
            "{}|{}",
            container.to_ascii_lowercase(),
            command_subcommand_name(cmd, container)
        );
        catalog.push_str(&format!("    {name:?},\n"));
    }
    catalog.push_str("];\n\n");
    catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    catalog.push_str("pub const SUBCOMMAND_GATE_FLAGS: &[(&str, &str, CmdFlags)] = &[\n");
    for cmd in all_cmds.iter().filter(|cmd| cmd.container.is_some()) {
        let container = cmd.container.as_deref().expect("filtered above");
        catalog.push_str(&format!(
            "    ({:?}, {:?}, {} as CmdFlags),\n",
            container.to_ascii_lowercase(),
            command_subcommand_name(cmd, container),
            command_flags_expr(cmd)
        ));
    }
    catalog.push_str("];\n\n");
    catalog.push_str("#[cfg(not(feature = \"wasm\"))]\n");
    catalog.push_str("pub const BUILTIN_ACL_CATEGORIES: &[&str] = &[\n");
    for category in acl_categories {
        catalog.push_str(&format!("    {category:?},\n"));
    }
    catalog.push_str("];\n\n");
    catalog.push_str(
        "pub fn top_level_meta(name: &str) -> Option<&'static CommandMeta> {\n\
             ALL_COMMAND_META.iter().copied().find(|meta| {\n\
                 meta.container.is_none() && meta.name.eq_ignore_ascii_case(name)\n\
             })\n\
         }\n\n\
         pub fn subcommand_meta(container: &str, subcommand: &str) -> Option<&'static CommandMeta> {\n\
             ALL_COMMAND_META.iter().copied().find(|meta| {\n\
                 let Some(meta_container) = meta.container else { return false; };\n\
                 meta_container.eq_ignore_ascii_case(container)\n\
                     && meta.name.get(meta_container.len() + 1..).is_some_and(|name| name.eq_ignore_ascii_case(subcommand))\n\
             })\n\
         }\n\n\
         pub fn acl_command_meta(name: &str) -> Option<&'static CommandMeta> {\n\
             match name.split_once('|') {\n\
                 Some((container, subcommand)) => subcommand_meta(container, subcommand),\n\
                 None => top_level_meta(name),\n\
             }\n\
         }\n",
    );
    let catalog_path = generated_dir.join("catalog.rs");
    generated_paths.push(catalog_path.clone());
    generated_files.push((catalog_path, catalog));

    let expected = generated_paths.into_iter().collect::<BTreeSet<_>>();
    remove_stale_rust_files(&generated_dir, &expected);
    write_rust_files_if_changed(generated_files);
}

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    generate_runtime_command_catalog(Path::new(&manifest_dir));

    if env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32") {
        println!("cargo:rerun-if-changed=build.rs");
        return;
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo:rustc-link-arg-bin=versal-server=-rdynamic");
    }

    let c_shim_dir = PathBuf::from(&manifest_dir)
        .join("src")
        .join("engine")
        .join("extension")
        .join("module")
        .join("api");
    let c_shim_path = c_shim_dir.join("module_call_shim.c");
    if c_shim_path.exists() {
        cc::Build::new()
            .file(&c_shim_path)
            .compile("module_call_shim");
        println!("cargo:rerun-if-changed={}", c_shim_path.display());
        eprintln!(
            "cargo:warning=build.rs: compiled C shim: {}",
            c_shim_path.display()
        );
    } else {
        eprintln!(
            "cargo:warning=build.rs: C shim not found at {:?}",
            c_shim_path
        );
    }

    let json_dir = PathBuf::from(&manifest_dir)
        .join("thirdparty")
        .join("valkey")
        .join("src")
        .join("commands");
    let generated_dir = PathBuf::from(&manifest_dir)
        .join("src")
        .join("framework")
        .join("generated");
    let commands_dir = PathBuf::from(&manifest_dir)
        .join("src")
        .join("framework")
        .join("commands");

    println!("cargo:rerun-if-changed=thirdparty/valkey/src/commands");

    let mut all_cmds: Vec<ParsedCmd> = Vec::new();

    let mut json_paths: Vec<PathBuf> = fs::read_dir(&json_dir)
        .unwrap_or_else(|error| {
            panic!(
                "cannot read command metadata directory {}: {error}",
                json_dir.display()
            )
        })
        .map(|entry| {
            entry
                .unwrap_or_else(|error| panic!("cannot read command metadata entry: {error}"))
                .path()
        })
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect();
    json_paths.sort();

    let mut module_sources = BTreeMap::<String, PathBuf>::new();
    for path in json_paths {
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let val: serde_json::Value = serde_json::from_str(&content)
            .unwrap_or_else(|error| panic!("cannot parse {}: {error}", path.display()));
        let object = val
            .as_object()
            .unwrap_or_else(|| panic!("{}: expected a top-level object", path.display()));
        assert_eq!(
            object.len(),
            1,
            "{}: expected exactly one top-level command object",
            path.display()
        );
        let (cmd_key, cmd) = object.iter().next().expect("object length was checked");
        let cmd_key = cmd_key.clone();

        let file_stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_else(|| panic!("{}: command filename is not UTF-8", path.display()))
            .to_string();
        let normalized_module = normalize_module_name(&file_stem);
        if let Some(previous) = module_sources.insert(normalized_module.clone(), path.clone()) {
            panic!(
                "normalized command module collision for {normalized_module}: {} and {}",
                previous.display(),
                path.display()
            );
        }
        let arity = cmd.get("arity").and_then(|a| a.as_i64()).unwrap_or(0) as i32;
        let mut flags = cmd
            .get("command_flags")
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().and_then(flag_to_const))
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !flags.iter().any(|f| f == "CMD_MOVABLE_KEYS") && compute_movable_keys(cmd) {
            flags.push("CMD_MOVABLE_KEYS".to_string());
        }
        let (first_key, last_key, key_step) = extract_key_spec(cmd);
        let key_flags = extract_key_flags(cmd);
        let acl_categories = match cmd.get("acl_categories") {
            None => Vec::new(),
            Some(value) => value
                .as_array()
                .unwrap_or_else(|| {
                    panic!(
                        "{}: acl_categories must be an array of strings",
                        path.display()
                    )
                })
                .iter()
                .map(|category| {
                    category
                        .as_str()
                        .unwrap_or_else(|| {
                            panic!(
                                "{}: acl_categories must be an array of strings",
                                path.display()
                            )
                        })
                        .to_string()
                })
                .collect(),
        };
        let container = cmd
            .get("container")
            .and_then(|c| c.as_str())
            .map(|s| s.to_string());
        let group = cmd
            .get("group")
            .and_then(|g| g.as_str())
            .unwrap_or("generic")
            .to_string();
        let module = group_to_module(&group);

        let lookup_name = if let Some(ref cont) = container {
            format!("{}::{}", cont, cmd_key)
        } else {
            cmd_key.clone()
        };

        let dispatch_group = command_dispatch_group(&cmd_key);

        all_cmds.push(ParsedCmd {
            cmd_key,
            lookup_name,
            name: file_stem.clone(),
            module,
            file_stem,
            arity,
            flags,
            first_key,
            last_key,
            key_step,
            container,
            dispatch_group: dispatch_group.to_string(),
            key_flags,
            acl_categories,
        });
    }

    all_cmds.sort_by(|a, b| a.lookup_name.cmp(&b.lookup_name));

    generate_command_metadata(&all_cmds, &generated_dir);

    let mut mod_out = String::new();
    mod_out.push_str(
        "// Auto-generated by build.rs from src/framework/commands/*.json. Do not edit.\n\n",
    );
    mod_out.push_str("pub(crate) mod command_runtime;\n");
    mod_out.push_str("pub(crate) mod introspection;\n");
    mod_out.push_str("pub(crate) mod key_extractors;\n");
    mod_out.push_str("pub(crate) mod pattern;\n\n");

    let has_handler = |cmd: &ParsedCmd| -> bool {
        let rs_file = commands_dir
            .join(&cmd.module)
            .join(format!("{}.rs", cmd.file_stem));
        let mod_dir = commands_dir
            .join(&cmd.module)
            .join(&cmd.file_stem)
            .join("mod.rs");
        rs_file.exists() || mod_dir.exists()
    };

    let mut groups: BTreeMap<String, Vec<&ParsedCmd>> = BTreeMap::new();
    for cmd in &all_cmds {
        if cmd.container.is_none() && has_handler(cmd) {
            groups.entry(cmd.module.clone()).or_default().push(cmd);
        }
    }

    for group in groups.keys() {
        mod_out.push_str("#[rustfmt::skip]\n");
        mod_out.push_str(&format!("pub mod {};\n", group));
    }

    mod_out.push_str("\nuse crate::framework::CommandRegistry;\n\n");
    mod_out.push_str("pub fn register_all(reg: &mut CommandRegistry) {\n");
    for group in groups.keys() {
        mod_out.push_str(&format!("    {}::register_all(reg);\n", group));
    }
    mod_out.push_str("}\n");

    let mut command_module_files = vec![(commands_dir.join("mod.rs"), mod_out)];

    let extra_modules: std::collections::HashMap<&str, &[&str]> = [
        ("hash", &["runtime_support"] as &[&str]),
        ("zset", &["basic"] as &[&str]),
        ("string", &["strops"] as &[&str]),
        ("scripting", &["script_cache", "script_cmd"] as &[&str]),
        ("geo", &["geohash_util"] as &[&str]),
        ("hyperloglog", &["common"] as &[&str]),
        ("stream", &["runtime_support"] as &[&str]),
        (
            "cluster",
            &["slots", "slot_stats", "replicas", "failover"] as &[&str],
        ),
        (
            "sentinel",
            &["admin_cmds", "control_cmds", "info_cmds"] as &[&str],
        ),
    ]
    .iter()
    .cloned()
    .collect();

    let extra_registers: std::collections::HashMap<&str, &[&str]> =
        [("server", &["config::register_all(reg)"] as &[&str])]
            .iter()
            .cloned()
            .collect();

    let rust_keywords: std::collections::HashSet<&str> = [
        "type", "ref", "mut", "move", "match", "mod", "self", "super", "crate", "async", "await",
        "dyn", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "typeof",
        "unsized", "virtual", "yield",
    ]
    .iter()
    .cloned()
    .collect();

    let escape_ident = |ident: &str| -> String {
        if rust_keywords.contains(ident) {
            format!("r#{}", ident)
        } else {
            ident.to_string()
        }
    };

    const GENERATED_MOD_RS_HARD_LIMIT: usize = 100;

    for (group, cmds) in &groups {
        let group_dir = commands_dir.join(group);
        fs::create_dir_all(&group_dir).unwrap();

        let mut group_out = String::new();
        group_out.push_str(
            "// Auto-generated by build.rs from src/framework/commands/*.json. Do not edit.\n\n",
        );

        for cmd in cmds {
            group_out.push_str("#[rustfmt::skip]\n");
            if cmd.file_stem == group.as_str() {
                group_out.push_str("#[allow(clippy::module_inception)]\n");
            }
            group_out.push_str(&format!("pub mod {};\n", escape_ident(&cmd.file_stem)));
        }
        if let Some(extra) = extra_modules.get(group.as_str()) {
            for m in extra.iter() {
                group_out.push_str("#[rustfmt::skip]\n");
                if *m == group.as_str() {
                    group_out.push_str("#[allow(clippy::module_inception)]\n");
                }
                group_out.push_str(&format!("pub mod {};\n", m));
            }
        }

        let mut register_body = String::new();
        register_body.push_str("pub fn register_all(reg: &mut CommandRegistry) {\n");

        for cmd in cmds {
            let spec_const = format!("{}_SPEC", cmd.cmd_key.replace('-', "_"));
            register_body.push_str(&format!(
                "    reg.register({}::{});\n",
                escape_ident(&cmd.file_stem),
                spec_const
            ));
        }

        if let Some(extra) = extra_registers.get(group.as_str()) {
            for call in extra.iter() {
                register_body.push_str(&format!("    {};\n", call));
            }
        }

        register_body.push_str("}\n");

        let inline_register = format!(
            "\nuse crate::framework::CommandRegistry;\n\n{}",
            register_body
        );
        let projected_inline_lines = group_out.lines().count() + inline_register.lines().count();
        if projected_inline_lines > GENERATED_MOD_RS_HARD_LIMIT {
            group_out.push_str("mod register;\n\n");
            group_out.push_str("pub use register::register_all;\n");

            let mut register_out = String::new();
            register_out.push_str(
                "// Auto-generated by build.rs from src/framework/commands/*.json. Do not edit.\n\n",
            );
            register_out.push_str("use crate::framework::CommandRegistry;\n");
            register_out.push_str(&format!("use super::{{{}}};\n\n", {
                let modules: Vec<String> = cmds
                    .iter()
                    .map(|cmd| escape_ident(&cmd.file_stem))
                    .collect();
                modules.join(", ")
            }));
            register_out.push_str(&register_body);
            command_module_files.push((group_dir.join("register.rs"), register_out));
        } else {
            group_out.push_str(&inline_register);
        }

        command_module_files.push((group_dir.join("mod.rs"), group_out));
    }

    write_rust_files_if_changed(command_module_files);

    eprintln!(
        "cargo:warning=build.rs: generated {} command entries",
        all_cmds.len()
    );
}
