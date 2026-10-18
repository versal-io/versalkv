use super::Server;
use crate::engine::extension::acl::acl_users;
use crate::framework::commands::server::config::{apply_config_set_one, config_registry};
use crate::framework::config::Config;
use crate::runtime::server::worker::ServerWorkerState;

impl Server {
    pub fn new(config: Config) -> Self {
        let systemd_supervised = config.supervised_active;
        Self {
            config,
            systemd_supervised,
            startup_args: std::env::args_os().skip(1).collect(),
        }
    }

    pub(super) fn normalize_paths(&mut self, backend: &mut ServerWorkerState) {
        if !self.config.config_file.is_empty() {
            if let Ok(abs_cfg) = std::fs::canonicalize(&self.config.config_file) {
                self.config.config_file = abs_cfg.to_string_lossy().to_string();
            }
        }
        if let Err(e) = std::env::set_current_dir(&self.config.dir) {
            crate::server_log!(warning, "Could not chdir to '{}': {}", self.config.dir, e);
        }
        if let Ok(cwd) = std::env::current_dir() {
            let abs_dir = cwd.to_string_lossy().to_string();
            self.config.dir.clone_from(&abs_dir);
            backend.ctx.config_mut().persistence.dir = abs_dir;
        }
    }

    pub(super) fn apply_config(&mut self, backend: &mut ServerWorkerState) {
        for (setting, value) in [
            ("appendfilename", self.config.appendfilename.as_str()),
            ("appenddirname", self.config.appenddirname.as_str()),
        ] {
            if let Err(error) =
                crate::engine::extension::config::persistence::validate_aof_basename(value, setting)
            {
                eprintln!("Bad directive '{setting}': {error}");
                std::process::exit(1);
            }
        }
        // Sentinel mode: always disable cluster (must run before apply_to
        // so that apply_to sees the correct cluster_enabled value).
        if self.config.sentinel {
            self.config.cluster_enabled = false;
        }
        self.config.apply_to(backend.ctx.config_mut());
        crate::engine::extension::acl::set_default_user_requirepass(
            backend.ctx.config().security.requirepass.as_deref(),
        );
        // Apply CLI-only `--flag value` directives that clap did not
        // recognise. C valkey accepts *any* config-file directive as a CLI
        // flag, so we run them through ConfigRegistry after RuntimeConfig
        // exists. Unknown keys are silently ignored, matching C's behaviour
        // of loading unknown config-file directives with a warning only.
        //
        // `ConfigRegistry::try_set` returns `None` for `ConfigType::Special`
        // specs (e.g. `maxmemory`) whose `set` closure is intentionally a
        // no-op in the registry because CONFIG SET semantics require extra
        // side effects (mem-unit parsing, `needs_eviction`, OOM score, etc.).
        // Those codepaths live under `framework::commands::server::config::set`
        // alongside the command handler; we re-dispatch any `None` result
        // there so CLI directives match CONFIG SET behaviour exactly.
        let registry = config_registry();
        let extra = std::mem::take(&mut self.config.extra_cli_configs);
        for (key, value) in extra {
            let result = match registry.try_set_startup(backend.ctx.config_mut(), &key, &value) {
                Some(r) => r,
                None => {
                    // ConfigType::Special (e.g. maxmemory): re-dispatch into
                    // the same helper that backs `CONFIG SET` so mem-unit
                    // parsing, `needs_eviction`, and similar side effects
                    // apply to CLI directives too.
                    let val_trimmed = value.trim_matches(|c| c == '\'' || c == '"');
                    apply_config_set_one(&mut backend.ctx, &key, val_trimmed, value.as_bytes())
                }
            };
            if let Err(e) = result {
                // C valkey (config.c `loadServerConfigFromString`) exits with
                // "Bad directive or wrong number of arguments" for a directive
                // it does not recognise. Directives applied through the
                // config-file path (`parse_config_string`) — rename-command,
                // loadmodule, sentinel, user, include — and module configs
                // (name contains '.') are handled elsewhere, so a registry miss
                // for them is expected and must stay non-fatal.
                let recognized = registry.lookup(&key).is_some()
                    || key.contains('.')
                    || matches!(
                        key.as_str(),
                        "rename-command" | "loadmodule" | "sentinel" | "user" | "include"
                    );
                if !recognized {
                    eprintln!("Bad directive or wrong number of arguments");
                    std::process::exit(1);
                }
                eprintln!("Bad directive '{}': {}", key, e);
                std::process::exit(1);
            }
        }
        crate::engine::extension::acl::set_acl_log_max_len(
            backend.ctx.config().server.acllog_max_len,
        );
        let hash_seed = if backend.ctx.config().server.hash_seed.is_empty() {
            crate::foundation::io::store::structure::hash_seed::random_seed()
        } else {
            crate::foundation::io::store::structure::hash_seed::derive_seed(
                backend.ctx.config().server.hash_seed.as_bytes(),
            )
        };
        crate::foundation::io::store::structure::hash_seed::install_process_seed(hash_seed);
        if !backend.ctx.config_mut().server.aclfile.is_empty() {
            if let Err(e) = Self::load_acl_file(&backend.ctx.config_mut().server.aclfile) {
                crate::server_log!(
                    warning,
                    "Could not load ACL file '{}': {}",
                    backend.ctx.config_mut().server.aclfile,
                    e
                );
            }
        }
    }

    pub(super) fn load_acl_file(path: &str) -> Result<(), String> {
        let content = std::fs::read_to_string(path).map_err(|e| format!("{}", e))?;
        let users = acl_users();
        let mut users = users.lock().unwrap();
        users.clear();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "user" {
                continue;
            }
            let username = parts[1].to_string();
            let mut user = crate::engine::extension::acl::AclUser::new_disabled();
            let rules = match crate::engine::extension::acl::merge_acl_tokens(&parts[2..]) {
                Ok(r) => r,
                Err(()) => {
                    crate::server_log!(
                        warning,
                        "ACL file '{}': unmatched parenthesis in rules for user '{}'",
                        path,
                        username
                    );
                    continue;
                }
            };
            for rule in &rules {
                if let Err(e) = user.apply_rule(rule) {
                    crate::server_log!(
                        warning,
                        "ACL file '{}': invalid rule '{}' for user '{}': {}",
                        path,
                        rule,
                        username,
                        e
                    );
                }
            }
            users.insert(username, user);
        }
        // Ensure the default user always exists (C valkey guarantees this)
        if !users.contains_key("default") {
            let mut default_user = crate::engine::extension::acl::AclUser::new_disabled();
            let _ = default_user.apply_rule("on");
            let _ = default_user.apply_rule("+@all");
            let _ = default_user.apply_rule("~*");
            let _ = default_user.apply_rule("&*");
            let _ = default_user.apply_rule("nopass");
            users.insert("default".to_string(), default_user);
        }
        Ok(())
    }
}
