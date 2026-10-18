mod args;

use args::ServerArgPlan;

pub fn run_from_env() {
    let raw_args = std::env::args().collect();
    let executable_name = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
        .unwrap_or_default();
    let mut plan = match ServerArgPlan::parse(raw_args, &executable_name) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    let config_file = plan.config().config_file.clone();
    if let Err(e) = plan.config_mut().load_from_file(&config_file) {
        if e.kind() == std::io::ErrorKind::InvalidData {
            eprintln!("Fatal error loading config file: {}", e);
            std::process::exit(1);
        }
        eprintln!("Warning: could not load config file: {}", e);
    }
    let mut config = plan.apply();
    let supervision_mode = config.supervised.to_ascii_lowercase();
    let should_try_upstart = supervision_mode == "upstart"
        || (supervision_mode == "auto" && std::env::var_os("UPSTART_JOB").is_some());
    if should_try_upstart {
        if let Ok(true) = crate::foundation::io::sys::process::activate_upstart_supervision() {
            config.supervised_active = true;
        }
    } else if supervision_mode == "systemd"
        || (supervision_mode == "auto" && std::env::var_os("NOTIFY_SOCKET").is_some())
    {
        if let Ok(true) =
            crate::foundation::io::sys::process::notify_systemd("STATUS=Valkey is loading...\n")
        {
            config.supervised_active = true;
            // This is set before logger/runtime threads exist and records
            // the successful sd_notify handshake for the later ready and
            // stopping lifecycle notifications.
            crate::foundation::io::sys::process::set_systemd_supervised(true);
        }
    }
    if config.daemonize && !config.supervised_active {
        match crate::foundation::io::sys::process::daemonize() {
            Ok(crate::foundation::io::sys::process::DaemonizeOutcome::Parent) => return,
            Ok(crate::foundation::io::sys::process::DaemonizeOutcome::Child) => {}
            Err(error) => {
                eprintln!("Fatal: could not daemonize: {error}");
                std::process::exit(1);
            }
        }
    }
    let logfile = if config.logfile.is_empty() {
        None
    } else {
        let path = std::path::Path::new(&config.logfile);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::path::Path::new(&config.dir).join(path)
        };
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => Some(file),
            Err(err) => {
                eprintln!(
                    "Warning: could not open logfile '{}': {}",
                    path.display(),
                    err
                );
                None
            }
        }
    };
    if let Err(error) = crate::foundation::io::sys::log::configure_syslog(
        config.syslog_enabled,
        &config.syslog_ident,
        &config.syslog_facility,
    ) {
        eprintln!("Fatal: could not configure syslog: {error}");
        std::process::exit(1);
    }
    crate::foundation::io::sys::log::init_logger_with_format(
        config.log_level(),
        logfile,
        crate::foundation::io::sys::log::LogFormat::parse(&config.log_format),
        crate::foundation::io::sys::log::LogTimestampFormat::parse(&config.log_timestamp_format),
    );
    crate::foundation::io::sys::log::install_panic_hook();
    let mut srv = crate::server::Server::new(config);
    if let Err(e) = srv.run() {
        eprintln!("Fatal: {}", e);
        std::process::exit(1);
    }
}
