use super::Server;
use crate::engine::extension::acl::{
    acl_users, parse_acl_declarations, parse_acl_file, replace_acl_state,
};
use crate::framework::commands::server::acl::validate_acl_file;
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
        if let Err(error) = backend.ctx.config().persistence.validate_bgsave_method() {
            eprintln!("{error}");
            std::process::exit(1);
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
    }

    pub(super) fn load_configured_acl(
        &mut self,
        backend: &ServerWorkerState,
    ) -> std::io::Result<()> {
        let server = &backend.ctx.config().server;
        let default_allchannels = server.acl_pubsub_default == "allchannels";
        let declarations = std::mem::take(&mut self.config.acl_declarations);
        let result = if !server.aclfile.is_empty() {
            if !declarations.is_empty() {
                Err("Declaring users or roles in valkey.conf while setting an ACL file path is invalid".to_string())
            } else {
                Self::load_acl_file(&server.aclfile, default_allchannels, &backend.ctx)
            }
        } else if declarations.is_empty() {
            Ok(())
        } else {
            Self::load_acl_declarations(&declarations, default_allchannels, &backend.ctx)
        };
        result.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }

    fn load_acl_declarations(
        declarations: &[String],
        default_allchannels: bool,
        ctx: &crate::framework::context::ServerContext,
    ) -> Result<(), String> {
        let lines = declarations
            .iter()
            .map(|line| {
                let mut arguments = crate::framework::config::parse_config_arguments(line)?;
                if let Some(directive) = arguments.first_mut() {
                    directive.make_ascii_lowercase();
                }
                Ok(arguments)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut state = parse_acl_declarations(lines, default_allchannels)
            .map_err(|error| format!("Error in role declaration or user declaration: {error}"))?;
        validate_acl_file(&state, ctx)
            .map_err(|error| format!("Error in role declaration or user declaration: {error}"))?;
        if !state.has_default_user {
            let users = acl_users()
                .lock()
                .map_err(|_| "ACL user registry is unavailable".to_string())?;
            if let Some(user) = users.get("default") {
                state.users.insert("default".to_string(), user.clone());
            }
        }
        replace_acl_state(state).map(|_| ()).map_err(str::to_string)
    }

    fn load_acl_file(
        path: &str,
        default_allchannels: bool,
        ctx: &crate::framework::context::ServerContext,
    ) -> Result<(), String> {
        let content = std::fs::read(path).map_err(|error| error.to_string())?;
        let state = parse_acl_file(&content, default_allchannels)?;
        validate_acl_file(&state, ctx)?;
        replace_acl_state(state).map(|_| ()).map_err(str::to_string)
    }
}
