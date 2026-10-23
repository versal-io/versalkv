use super::Server;
use crate::foundation::io::persistence::journal::AofJournal;
use crate::foundation::io::persistence::{AofAppender, AofLoadError, AofLoadStats, AofOpenMode};
use crate::runtime::server::worker::ServerWorkerState;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

fn warning_is_ignored(configured: &str, warning: &str) -> bool {
    configured
        .split_whitespace()
        .any(|candidate| candidate.eq_ignore_ascii_case(warning))
}

fn install_startup_backlog_replication_info(
    backlog: &mut crate::engine::distributed::replication::backlog::ReplicationBacklog,
    is_replica: bool,
    replid: &str,
    offset: i64,
) -> Result<(), crate::engine::distributed::replication::InvalidReplicationPosition> {
    let reported_offset =
        crate::engine::distributed::replication::ReplicationOffset::try_from(offset)?;
    if is_replica {
        backlog.adopt_primary_from_rdb(replid, reported_offset)
    } else {
        backlog.restore_primary_from_rdb(replid, reported_offset)
    }
}

fn append_startup_select_if_changed(
    backlog: &mut crate::engine::distributed::replication::backlog::ReplicationBacklog,
    selected_db: &mut i64,
    db: u32,
) -> Result<(), crate::engine::distributed::replication::InvalidReplicationPosition> {
    let db_id = i64::from(db);
    if *selected_db == db_id {
        return Ok(());
    }
    let db = db.to_string();
    let select = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SELECT",
        db.as_bytes(),
    ]);
    backlog.append(&select)?;
    *selected_db = db_id;
    Ok(())
}

fn startup_expired_keys_use_unlink(
    config: &crate::engine::extension::config::server::ServerConfig,
) -> bool {
    config.lazyfree_lazy_expire
}

fn startup_rdb_decode_policy(
    strict: bool,
    key_load_delay_us: i64,
    role: crate::engine::distributed::replication::ReplicationRole,
) -> crate::foundation::io::persistence::RdbDecodePolicy {
    if role == crate::engine::distributed::replication::ReplicationRole::Replica {
        crate::foundation::io::persistence::RdbDecodePolicy::replication(strict, key_load_delay_us)
    } else {
        crate::foundation::io::persistence::RdbDecodePolicy::persisted(strict, key_load_delay_us)
    }
}

fn configured_startup_replication_role(
    primary_host: Option<&str>,
) -> crate::engine::distributed::replication::ReplicationRole {
    if primary_host.is_some() {
        crate::engine::distributed::replication::ReplicationRole::Replica
    } else {
        crate::engine::distributed::replication::ReplicationRole::Primary
    }
}

fn validate_startup_rdb_role(
    load_role: crate::engine::distributed::replication::ReplicationRole,
    current_role: crate::engine::distributed::replication::ReplicationRole,
) -> Result<(), crate::engine::host::HostError> {
    if current_role == load_role {
        Ok(())
    } else {
        Err(crate::engine::host::HostError::Invariant(
            "replication role changed during startup RDB load",
        ))
    }
}

fn append_startup_rdb_expiry_events(
    backlog: &mut crate::engine::distributed::replication::backlog::ReplicationBacklog,
    mut selected_db: i64,
    use_unlink: bool,
    expiry_events: Vec<crate::foundation::io::persistence::RdbExpiryEvent>,
) -> Result<i64, crate::engine::distributed::replication::InvalidReplicationPosition> {
    let key_command: &[u8] = if use_unlink { b"UNLINK" } else { b"DEL" };
    for event in expiry_events {
        match event {
            crate::foundation::io::persistence::RdbExpiryEvent::Key { db, key } => {
                append_startup_select_if_changed(backlog, &mut selected_db, db)?;
                let frame = crate::engine::distributed::replication::protocol::encode_command(&[
                    key_command,
                    &key,
                ]);
                backlog.append(&frame)?;
            }
            crate::foundation::io::persistence::RdbExpiryEvent::HashFields(event) => {
                append_startup_select_if_changed(backlog, &mut selected_db, event.db)?;
                for field in event.fields {
                    let frame =
                        crate::engine::distributed::replication::protocol::encode_command(&[
                            b"HDEL",
                            event.key.as_ref(),
                            field.as_ref(),
                        ]);
                    backlog.append(&frame)?;
                }
            }
        }
    }
    Ok(selected_db)
}

fn append_startup_rdb_expiry_events_for_role(
    backlog: &mut crate::engine::distributed::replication::backlog::ReplicationBacklog,
    selected_db: i64,
    role: crate::engine::distributed::replication::ReplicationRole,
    use_unlink: bool,
    expiry_events: Vec<crate::foundation::io::persistence::RdbExpiryEvent>,
) -> Result<i64, crate::engine::distributed::replication::InvalidReplicationPosition> {
    if role == crate::engine::distributed::replication::ReplicationRole::Replica {
        return Ok(selected_db);
    }
    append_startup_rdb_expiry_events(backlog, selected_db, use_unlink, expiry_events)
}

impl Server {
    pub fn run(&mut self) -> std::io::Result<()> {
        self.install_signals();
        self.enforce_arm64_cow_safety()?;
        let mut backend = self.create_backend()?;
        backend.ctx.set_startup_args(self.startup_args.clone());
        self.normalize_paths(&mut backend);
        self.apply_config(&mut backend);
        crate::foundation::io::sys::log::set_debug_context(
            backend.ctx.config().server.debug_context.clone(),
        );
        crate::foundation::io::sys::log::set_crash_log_enabled(
            backend.ctx.config().server.crash_log_enabled,
        );
        crate::foundation::io::sys::log::set_use_exit_on_panic(
            backend.ctx.config().server.use_exit_on_panic,
        );
        if self.systemd_supervised
            || crate::foundation::io::sys::process::systemd_notify_available()
        {
            backend.ctx.config_mut().server.supervised_active = true;
        }
        backend.configure_tcp_backlog_from_config()?;
        backend.configure_io_threads_from_config()?;
        if let Err(error) = backend.configure_bio_cpulist_from_config() {
            crate::server_log!(warning, "Failed to set BIO CPU affinity: {}", error);
        }
        let configured_role = configured_startup_replication_role(
            backend.ctx.config().replication.master_host.as_deref(),
        );
        backend.ctx.replication_mut().role = configured_role;
        if configured_role == crate::engine::distributed::replication::ReplicationRole::Replica {
            crate::foundation::io::sys::log::set_log_role_replica();
        }
        self.register_commands(&mut backend);
        self.load_modules(&mut backend)?;
        self.init_cluster(&mut backend)
            .map_err(Self::report_listener_error)?;
        self.init_observability(&mut backend);
        self.init_sentinel_identity(&mut backend);
        self.init_sentinel_directives(&mut backend);
        let addrs = self
            .bind_listeners(&mut backend)
            .map_err(Self::report_listener_error)?;
        let created_pidfile = Self::create_pidfile(&backend.ctx.config().server.pidfile);
        crate::engine::extension::config::server::refresh_proc_title(
            &backend.ctx.config_mut().server,
        );
        Self::print_initialized();
        self.log_startup_banner(&backend);
        self.load_persistence(&mut backend);
        self.log_ready(&backend, &addrs);
        if let Err(error) = crate::foundation::io::sys::process::apply_cpu_affinity(
            &backend.ctx.config().server.server_cpulist,
        ) {
            crate::server_log!(warning, "Failed to set server CPU affinity: {}", error);
        }
        let result = backend.run();
        Self::remove_created_pidfile(created_pidfile.as_deref());
        if self.systemd_supervised
            || crate::foundation::io::sys::process::systemd_notify_available()
        {
            let _ = crate::foundation::io::sys::process::notify_systemd("STOPPING=1\n");
        }
        if result.is_ok() && backend.ctx.is_shutdown_requested() {
            // Match C `shutdownCommand` / `serverCron`: terminate while the
            // runtime still owns every client socket, so the requester sees
            // process EOF without a success reply or a pre-exit drop window.
            process::exit(0);
        }
        result
    }

    fn enforce_arm64_cow_safety(&self) -> std::io::Result<()> {
        use crate::foundation::io::sys::arm64_cow::Arm64CowCheck;

        let check = crate::foundation::io::sys::arm64_cow::check_madv_free_fork_safety();
        let message = match check {
            Arm64CowCheck::NotApplicable | Arm64CowCheck::Safe => return Ok(()),
            Arm64CowCheck::Affected => {
                "Your kernel has a bug that could lead to data corruption during background save. Please upgrade to the latest stable kernel."
            }
            Arm64CowCheck::Inconclusive(reason) => {
                crate::server_log!(warning, "WARNING {reason}");
                "Failed to test the kernel for a bug that could lead to data corruption during background save. Your system could be affected, please report this error."
            }
        };
        crate::server_log!(warning, "WARNING {message}");
        if warning_is_ignored(&self.config.ignore_warnings, "ARM64-COW-BUG") {
            return Ok(());
        }
        Err(std::io::Error::other(
            "ARM64 COW safety check failed; set ignore-warnings ARM64-COW-BUG only when the risk is understood",
        ))
    }

    fn create_pidfile(configured_path: &str) -> Option<PathBuf> {
        if configured_path.is_empty() {
            return None;
        }

        let path = PathBuf::from(configured_path);
        match std::fs::write(&path, format!("{}\n", process::id())) {
            Ok(()) => Some(path),
            Err(error) => {
                crate::server_log!(
                    warning,
                    "Failed to write PID file '{}': {}",
                    path.display(),
                    error
                );
                None
            }
        }
    }

    fn remove_created_pidfile(path: Option<&Path>) {
        let Some(path) = path else {
            return;
        };

        if let Err(error) = std::fs::remove_file(path) {
            crate::server_log!(
                warning,
                "Failed to remove PID file '{}': {}",
                path.display(),
                error
            );
        }
    }

    fn install_signals(&self) {
        crate::foundation::io::sys::signal::install_shutdown_signals();
        crate::server_log!(notice, "Signal handlers installed for SIGTERM/SIGINT.");
    }

    pub(crate) fn print_initialized() {
        let mut stdout = std::io::stdout();
        let _ = writeln!(
            stdout,
            "                PID: {} Server initialized",
            process::id()
        );
        let _ = stdout.flush();
    }

    fn log_startup_banner(&self, backend: &ServerWorkerState) {
        use std::io::IsTerminal;

        let config = &backend.ctx.config().server;
        let mode = if config.cluster_enabled {
            "cluster"
        } else if config.sentinel_mode {
            "sentinel"
        } else {
            "standalone"
        };
        // C only prints its ASCII logo by default when the logger targets a
        // terminal. Syslog is not a supported logging backend yet, so it
        // cannot suppress this branch until that capability is introduced.
        let show_logo = config.always_show_logo
            || (self.config.logfile.is_empty() && std::io::stdout().is_terminal());
        if show_logo {
            crate::server_log!(
                notice,
                "VersalKV startup banner\nmode={} port={} pid={}",
                mode,
                config.listening_port,
                process::id()
            );
        } else {
            crate::server_log!(
                notice,
                "Running mode={}, port={}",
                mode,
                config.listening_port
            );
        }
    }

    fn exit_after_aof_load_error(&self) -> ! {
        process::exit(1);
    }

    pub(super) fn report_listener_error(error: std::io::Error) -> std::io::Error {
        if error.kind() == std::io::ErrorKind::AddrInUse {
            println!("Failed listening on port: {}", error);
        }
        error
    }

    fn load_persistence(&mut self, backend: &mut ServerWorkerState) {
        if backend.ctx.config_mut().persistence.aof_enabled {
            backend.ctx.aof_mut().journal = self.load_aof(backend);
            backend.ctx.aof_mut().started_this_session = true;
            // Mirror C's `server.fsynced_reploff = aof_enabled ? 0 : -1`: with
            // AOF on from boot the base is considered durable at offset 0, so the
            // confirmed two-phase offset starts at 0 rather than the `-1`/None
            // "never ack" sentinel.
            backend.ctx.aof_mut().fsynced_repl_offset = Some(0);
            backend.ctx.aof_mut().fsynced_repl_offset_pending = 0;
        } else {
            self.load_rdb(backend);
        }
    }

    fn log_ready(&self, _backend: &ServerWorkerState, addrs: &[String]) {
        crate::server_log!(
            notice,
            "Ready to accept connections on {}",
            addrs.join(", ")
        );
        crate::server_log!(notice, "VersalKV M1 (polling level-triggered runtime)");
        if self.systemd_supervised
            || crate::foundation::io::sys::process::systemd_notify_available()
        {
            if let Err(error) = crate::foundation::io::sys::process::notify_systemd(
                "STATUS=Ready to accept connections\n",
            ) {
                crate::server_log!(
                    warning,
                    "systemd ready status notification failed: {}",
                    error
                );
            }
            if let Err(error) = crate::foundation::io::sys::process::notify_systemd("READY=1\n") {
                crate::server_log!(warning, "systemd ready notification failed: {}", error);
            }
        }
    }

    fn load_rdb(&self, backend: &mut ServerWorkerState) {
        let dir = backend.ctx.config().persistence.dir.clone();
        let dbfilename = backend.ctx.config().persistence.dbfilename.clone();
        let path = Path::new(&dir).join(&dbfilename);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                self.exit_after_rdb_task_error(crate::framework::host_tasks::RdbTaskError::Load(
                    crate::foundation::io::persistence::LoadError::Io(e),
                ))
            }
        };
        let strict = !backend.ctx.config().persistence.rdb_version_check_relaxed;
        let key_load_delay_us = self.startup_key_load_delay(backend);
        let role = backend.ctx.replication().role;
        let dataset = backend.ctx.dbs().empty_like();
        let mut task = crate::framework::host_tasks::RdbLoadTask::new(
            bytes,
            dataset,
            startup_rdb_decode_policy(strict, key_load_delay_us, role),
            crate::framework::host_tasks::DatasetVisibility::Visible,
            backend
                .ctx
                .config()
                .persistence
                .loading_process_events_interval_bytes,
        );
        backend.ctx.set_host_task_loading(true);
        let result = backend.run_background_rdb_task(&mut task);
        if result.is_ok() {
            let current_role = backend.ctx.replication().role;
            if let Err(error) = validate_startup_rdb_role(role, current_role) {
                backend.ctx.set_host_task_loading(false);
                self.exit_after_rdb_task_error(crate::framework::host_tasks::RdbTaskError::Host(
                    error,
                ));
            }
        }
        let dataset = task.into_dataset();
        match result {
            Ok(metadata) => {
                *backend.ctx.dbs_mut() = dataset;
                backend.ctx.set_host_task_loading(false);
                self.commit_startup_rdb_metadata(backend, metadata, role);
            }
            Err(error) => {
                backend.ctx.set_host_task_loading(false);
                self.exit_after_rdb_task_error(error);
            }
        }
    }

    fn startup_key_load_delay(&self, backend: &ServerWorkerState) -> i64 {
        // `Config::apply_to` and the registry-based CLI fallback have already
        // resolved precedence into RuntimeConfig.  Re-parsing and numeric max
        // would turn C's valid negative debug delays into zero or a huge u64.
        backend.ctx.config().persistence.key_load_delay
    }

    fn commit_startup_rdb_metadata(
        &self,
        backend: &mut ServerWorkerState,
        metadata: crate::foundation::io::persistence::RdbDecodedMetadata,
        load_role: crate::engine::distributed::replication::ReplicationRole,
    ) {
        if crate::framework::host_tasks::replace_function_libraries(
            &mut backend.ctx,
            &metadata.function_codes,
        )
        .is_err()
        {
            self.exit_after_rdb_task_error(crate::framework::host_tasks::RdbTaskError::Load(
                crate::foundation::io::persistence::LoadError::Corrupt,
            ));
        }
        let aux_fields = metadata
            .replication
            .as_ref()
            .map(|info| info.aux_fields.as_slice())
            .unwrap_or_default();
        if let Err(error) = backend.ctx.replace_slot_imports_from_rdb_aux(aux_fields) {
            crate::server_log!(warning, "Fatal error restoring slot imports: {}", error);
            process::exit(1);
        }
        self.fail_stale_startup_imports(backend);
        if let Some(replication) = metadata.replication {
            if self
                .install_startup_replication_info(backend, replication)
                .is_err()
            {
                self.exit_after_rdb_task_error(crate::framework::host_tasks::RdbTaskError::Load(
                    crate::foundation::io::persistence::LoadError::Corrupt,
                ));
            }
        }
        let use_unlink = startup_expired_keys_use_unlink(&backend.ctx.config().server);
        let current_selected_db = backend.ctx.repl_seldb();
        let selected_db = append_startup_rdb_expiry_events_for_role(
            &mut backend.ctx.replication_mut().backlog,
            current_selected_db,
            load_role,
            use_unlink,
            metadata.expiry_events,
        )
        .unwrap_or_else(|_| {
            self.exit_after_rdb_task_error(crate::framework::host_tasks::RdbTaskError::Load(
                crate::foundation::io::persistence::LoadError::Corrupt,
            ))
        });
        backend.ctx.set_repl_seldb(selected_db);
        backend
            .ctx
            .persistence_mut()
            .record_rdb_save((crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64);
        backend
            .ctx
            .persistence_mut()
            .record_rdb_load(metadata.loaded, metadata.expired);
        backend.ctx.mem_stats().update_startup_snapshot();
        crate::server_log!(notice, "DB loaded from disk: {} keys", metadata.loaded);
    }

    fn fail_stale_startup_imports(&self, backend: &mut ServerWorkerState) {
        let is_primary = backend
            .ctx
            .cluster_state()
            .as_deref()
            .is_some_and(|cluster| cluster.myself.is_primary());
        if !is_primary {
            return;
        }
        let actions = backend
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .map(|cluster| {
                cluster.fail_import_migrations(
                    "Process restarted",
                    crate::foundation::io::sys::time::unixtime(),
                )
            })
            .unwrap_or_default();
        if let Some(cluster) = backend.ctx.cluster_state_mut().as_deref_mut() {
            cluster.queue_migration_actions(actions);
        }
    }

    fn install_startup_replication_info(
        &self,
        backend: &mut ServerWorkerState,
        info: crate::foundation::io::persistence::RdbReplicationInfo,
    ) -> Result<(), crate::engine::distributed::replication::InvalidReplicationPosition> {
        let is_replica = backend.ctx.config().replication.master_host.is_some();
        install_startup_backlog_replication_info(
            &mut backend.ctx.replication_mut().backlog,
            is_replica,
            &info.repl_id,
            info.repl_offset,
        )?;
        if is_replica {
            let resume_offset =
                crate::engine::distributed::replication::ReplicationOffset::try_from(
                    info.repl_offset,
                )?;
            resume_offset
                .checked_next_request()
                .ok_or(crate::engine::distributed::replication::InvalidReplicationPosition)?;
            if let Some(host) = backend.ctx.config().replication.master_host.clone() {
                let master = crate::framework::distributed::replication::MasterInfo {
                    host,
                    port: backend.ctx.config().replication.master_port,
                };
                backend.install_startup_replica_resume(
                    master,
                    info.repl_id.clone(),
                    resume_offset,
                    info.repl_stream_db.max(0) as u32,
                );
            }
            backend.ctx.replication_mut().slave_repl_offset = resume_offset;
            backend.ctx.replication_mut().slave_read_repl_offset = resume_offset;
        }
        let capacity = backend.ctx.config().server.repl_backlog_size.max(0) as usize;
        backend.ctx.replication_mut().backlog.set_capacity(capacity);
        backend.ctx.set_repl_seldb(info.repl_stream_db);
        backend.mark_initial_rdb_replication_info_installed();
        Ok(())
    }

    fn exit_after_rdb_task_error(&self, error: crate::framework::host_tasks::RdbTaskError) -> ! {
        if matches!(
            &error,
            crate::framework::host_tasks::RdbTaskError::Aborted(
                crate::engine::host::HostAbort::Shutdown
            )
        ) {
            process::exit(0);
        }
        crate::server_log!(
            warning,
            "Terminating server after rdb file reading failure."
        );
        match error {
            crate::framework::host_tasks::RdbTaskError::Load(error) => {
                Self::log_rdb_load_error(&error);
            }
            crate::framework::host_tasks::RdbTaskError::Host(error) => {
                crate::server_log!(warning, "RDB host error: {}", error);
            }
            crate::framework::host_tasks::RdbTaskError::Aborted(reason) => {
                crate::server_log!(warning, "RDB load aborted: {:?}", reason);
            }
        }
        crate::server_log!(
            warning,
            "Fatal error loading the DB, check server logs. Exiting."
        );
        process::exit(1);
    }

    fn log_rdb_load_error(error: &crate::foundation::io::persistence::LoadError) {
        match error {
            crate::foundation::io::persistence::LoadError::ForeignType { type_byte, version } => {
                crate::server_log!(
                    warning,
                    "Can't handle foreign type or opcode {} in RDB with version {}",
                    type_byte,
                    version
                )
            }
            crate::foundation::io::persistence::LoadError::UnknownType => crate::server_log!(
                warning,
                "Unknown type or opcode when loading DB. Unrecoverable error, aborting now."
            ),
            crate::foundation::io::persistence::LoadError::CrcError => {
                crate::server_log!(warning, "CRC error")
            }
            crate::foundation::io::persistence::LoadError::Corrupt => {
                crate::server_log!(warning, "RDB integrity check failed");
                crate::server_log!(
                    warning,
                    "RDB integrity check failed while loading DB from disk"
                );
            }
            crate::foundation::io::persistence::LoadError::IncompatibleVersion(version) => {
                crate::server_log!(warning, "Can't handle RDB format version {}", version)
            }
            crate::foundation::io::persistence::LoadError::Io(error) => {
                crate::server_log!(warning, "RDB I/O error: {}", error)
            }
        }
    }

    fn replay_startup_aof_file(
        &self,
        backend: &mut ServerWorkerState,
        replay_conn: &mut crate::framework::capabilities::connection::Connection,
        path: &Path,
        bytes: Vec<u8>,
        is_last: bool,
    ) {
        let result = backend
            .run_aof_file_task(bytes, replay_conn, None)
            .unwrap_or_else(|error| self.exit_after_aof_task_error(error));
        self.handle_truncated_aof(
            backend,
            path,
            result.tail_start,
            result.tail_len,
            result.stats,
            is_last,
        );
        crate::server_log!(
            notice,
            "AOF loaded: {} commands replayed",
            result.stats.cmd_count
        );
        if result.tail_start > 0 {
            crate::server_log!(
                notice,
                "AOF RDB preamble loaded ({} bytes)",
                result.tail_start
            );
        }
        backend
            .ctx
            .persistence_mut()
            .record_rdb_save((crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64);
        backend.ctx.mem_stats().update_startup_snapshot();
    }

    fn handle_truncated_aof(
        &self,
        backend: &ServerWorkerState,
        path: &Path,
        tail_start: usize,
        tail_len: usize,
        stats: AofLoadStats,
        is_last: bool,
    ) {
        let result = crate::framework::host_tasks::AofFileResult {
            stats,
            tail_start,
            tail_len,
        };
        match crate::framework::host_tasks::apply_aof_truncation(
            path,
            &result,
            backend.ctx.config().persistence.aof_load_truncated,
            is_last,
        ) {
            Ok(false) => {}
            Ok(true) => {
                crate::server_log!(
                    warning,
                    "Unexpected end of file reading the append only file"
                );
            }
            Err(error) => self.exit_after_aof_task_error(error),
        }
    }

    fn exit_after_aof_task_error(
        &self,
        error: crate::framework::host_tasks::AofFileTaskError,
    ) -> ! {
        if matches!(
            &error,
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::Aborted(
                    crate::engine::host::HostAbort::Shutdown
                )
            ) | crate::framework::host_tasks::AofFileTaskError::Rdb(
                crate::framework::host_tasks::RdbTaskError::Aborted(
                    crate::engine::host::HostAbort::Shutdown
                )
            )
        ) {
            process::exit(0);
        }
        match error {
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::Load(AofLoadError::Corrupt),
            ) => {
                crate::server_log!(warning, "Bad file format reading the append only file");
            }
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::Load(error),
            ) => {
                crate::server_log!(warning, "Bad file format reading the AOF: {:?}", error);
            }
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::Host(error),
            ) => {
                crate::server_log!(warning, "AOF host error: {}", error);
            }
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::Aborted(reason),
            ) => {
                crate::server_log!(warning, "AOF load aborted: {:?}", reason);
            }
            crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::PropagationError { command },
            ) => {
                crate::server_log!(
                    warning,
                    "This server panicked applying an error during AOF replay after command '{}'",
                    command
                );
            }
            crate::framework::host_tasks::AofFileTaskError::Rdb(error) => {
                crate::server_log!(warning, "Error loading RDB preamble in AOF: {:?}", error);
            }
            crate::framework::host_tasks::AofFileTaskError::InvalidRdbMetadata => {
                crate::server_log!(warning, "Invalid RDB preamble metadata in AOF");
            }
            crate::framework::host_tasks::AofFileTaskError::Truncated => {
                crate::server_log!(
                    warning,
                    "Unexpected end of file reading the append only file"
                );
            }
            crate::framework::host_tasks::AofFileTaskError::TruncatedBeforeFinal => {
                crate::server_log!(
                    warning,
                    "Unexpected end of file reading the append only file"
                );
                crate::server_log!(
                    warning,
                    "Fatal error: the truncated file is not the last file"
                );
            }
            crate::framework::host_tasks::AofFileTaskError::Truncate(error) => {
                crate::server_log!(
                    warning,
                    "Unexpected end of file reading the append only file"
                );
                crate::server_log!(
                    warning,
                    "Failed to truncate AOF after unexpected end: {}",
                    error
                );
            }
        }
        crate::server_log!(warning, "Fatal error loading the DB. Exiting.");
        self.exit_after_aof_load_error();
    }

    fn load_aof(&self, backend: &mut ServerWorkerState) -> Option<AofJournal> {
        let dir = backend.ctx.config_mut().persistence.dir.clone();
        let filename = backend.ctx.config_mut().persistence.aof_filename.clone();
        let appenddirname = backend
            .ctx
            .config_mut()
            .persistence
            .aof_appenddirname
            .clone();
        let fsync_mode = backend.ctx.config_mut().persistence.aof_fsync;
        if !appenddirname.is_empty() {
            let manifest_path = Path::new(&dir)
                .join(&appenddirname)
                .join(format!("{}.manifest", filename));
            let legacy_path = Path::new(&dir).join(&filename);
            if !manifest_path.exists() && !legacy_path.exists() {
                let persistence = &backend.ctx.config().persistence;
                let options =
                    crate::foundation::io::persistence::aof_rewrite::AofRewriteFileOptions {
                        dir: Path::new(&dir).to_path_buf(),
                        filename: filename.clone(),
                        appenddirname: appenddirname.clone(),
                        use_rdb_preamble: persistence.aof_use_rdb_preamble,
                        timestamp_enabled: persistence.aof_timestamp_enabled,
                        key_delay_us: persistence.rdb_key_save_delay,
                        rdb_compression: persistence.rdbcompression,
                        incremental_fsync: persistence.aof_rewrite_incremental_fsync,
                    };
                let pid = process::id() as i32;
                let initialize_result =
                    crate::foundation::io::persistence::aof_rewrite::write_rewrite_temp(
                        backend.ctx.dbs(),
                        &[],
                        &[],
                        &options,
                        pid,
                    )
                    .and_then(|_| {
                        crate::foundation::io::persistence::aof_rewrite::finalize_rewrite(
                            &options,
                            pid,
                            &[],
                            true,
                            false,
                            false,
                        )
                    });
                if let Err(error) = initialize_result {
                    crate::server_log!(warning, "Can't initialize multipart AOF: {}", error);
                    self.exit_after_aof_load_error();
                }
            }
        }
        let paths = match crate::foundation::io::persistence::resolve_aof_read_paths_checked(
            &dir,
            &filename,
            &appenddirname,
        ) {
            Ok(paths) => paths,
            Err(error) => {
                crate::server_log!(warning, "{}", error);
                crate::server_log!(warning, "Fatal error loading the DB. Exiting.");
                self.exit_after_aof_load_error();
            }
        };
        // Bind has already completed. Keep LOADING set while the same L5 host
        // dispatcher replays every file so allowed commands remain serviceable.
        let mut loaded_any = false;
        let mut replay_conn = crate::framework::capabilities::connection::Connection::new_fake();
        replay_conn.import_source = true;
        backend.ctx.flush_all_dbs();
        backend.ctx.script_mut().function_registry.flush();
        backend.ctx.prepare_slot_migration_load();
        backend.ctx.set_host_task_loading(true);
        backend.ctx.config_mut().server.import_mode = true;
        let last_path_index = paths.len().saturating_sub(1);
        for (path_index, path) in paths.iter().enumerate() {
            match std::fs::read(path) {
                Ok(bytes) if !bytes.is_empty() => {
                    loaded_any = true;
                    crate::server_log!(notice, "Loading AOF from disk ({} bytes)...", bytes.len());
                    self.replay_startup_aof_file(
                        backend,
                        &mut replay_conn,
                        path,
                        bytes,
                        path_index == last_path_index,
                    );
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    crate::server_log!(warning, "Fatal error opening AOF file. Exiting.");
                    self.exit_after_aof_load_error();
                }
            }
        }
        backend.ctx.set_host_task_loading(false);
        backend.ctx.config_mut().server.import_mode = false;
        if !loaded_any {
            crate::server_log!(notice, "Creating new AOF {}", filename);
        }
        backend.ctx.finish_slot_migration_load();
        if !appenddirname.is_empty() {
            let manifest_path = Path::new(&dir)
                .join(&appenddirname)
                .join(format!("{}.manifest", filename));
            if manifest_path.exists() {
                if let Err(error) = crate::foundation::io::persistence::ensure_aof_increment(
                    &dir,
                    &filename,
                    &appenddirname,
                ) {
                    crate::server_log!(warning, "Can't create AOF increment: {}", error);
                    self.exit_after_aof_load_error();
                }
            }
        }
        let path = crate::foundation::io::persistence::resolve_aof_read_path(
            &dir,
            &filename,
            &appenddirname,
        );

        let mode = if path.exists() {
            AofOpenMode::AppendExisting
        } else {
            AofOpenMode::CreateNew
        };
        match AofAppender::open(&dir, &filename, &appenddirname, mode, fsync_mode) {
            Ok(mut appender) => {
                backend
                    .ctx
                    .persistence_mut()
                    .record_aof_size(appender.current_size());
                let _ = appender.force_fsync();
                backend.ctx.aof_mut().last_fsync_sec =
                    (crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64;
                backend.ctx.aof_mut().last_write_status =
                    crate::engine::persistence::OperationStatus::Ok;
                crate::server_log!(
                    notice,
                    "AOF append-only log initialized (fsync={})",
                    fsync_mode.as_str()
                );
                Some(AofJournal::new(appender))
            }
            Err(e) => {
                crate::server_log!(warning, "Can't open AOF for append: {}", e);
                process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::engine::distributed::replication::backlog::ReplicationBacklog;
    use crate::engine::distributed::replication::{
        BacklogCursor, InvalidReplicationPosition, ReplicationRole,
    };
    use crate::foundation::io::persistence::{ExpiredHashFields, RdbExpiryEvent};

    #[test]
    fn ignore_warnings_uses_case_insensitive_whitespace_tokens() {
        assert!(super::warning_is_ignored(
            "ARM64-COW-BUG other",
            "arm64-cow-bug"
        ));
        assert!(super::warning_is_ignored(
            "  arm64-cow-bug\t",
            "ARM64-COW-BUG"
        ));
        assert!(!super::warning_is_ignored("ARM64-COW", "ARM64-COW-BUG"));
        assert!(!super::warning_is_ignored("", "ARM64-COW-BUG"));
    }

    #[test]
    fn startup_replication_metadata_rejects_invalid_offsets() {
        for is_replica in [false, true] {
            for invalid in [-1, i64::MAX] {
                let mut backlog = ReplicationBacklog::new("local".to_string(), 64);

                assert_eq!(
                    super::install_startup_backlog_replication_info(
                        &mut backlog,
                        is_replica,
                        "persisted",
                        invalid,
                    ),
                    Err(InvalidReplicationPosition)
                );
                assert_eq!(backlog.replid(), "local");
            }
        }
    }

    #[test]
    fn startup_rdb_expiry_events_append_key_and_hash_field_deletions() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 4096);

        let selected_db = super::append_startup_rdb_expiry_events_for_role(
            &mut backlog,
            -1,
            ReplicationRole::Primary,
            false,
            vec![
                RdbExpiryEvent::HashFields(ExpiredHashFields {
                    db: 1,
                    key: Bytes::from_static(b"hash"),
                    fields: vec![
                        Bytes::from_static(b"expired-field-1"),
                        Bytes::from_static(b"expired-field-2"),
                    ],
                }),
                RdbExpiryEvent::Key {
                    db: 1,
                    key: b"expired-key".to_vec(),
                },
                RdbExpiryEvent::HashFields(ExpiredHashFields {
                    db: 0,
                    key: Bytes::from_static(b"other-hash"),
                    fields: vec![Bytes::from_static(b"other-field")],
                }),
            ],
        )
        .expect("startup expiry frames should fit in the replication backlog");

        assert_eq!(selected_db, 0);
        assert_eq!(
            backlog.read_from(BacklogCursor::ZERO),
            Some(
                b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n\
                  *3\r\n$4\r\nHDEL\r\n$4\r\nhash\r\n$15\r\nexpired-field-1\r\n\
                  *3\r\n$4\r\nHDEL\r\n$4\r\nhash\r\n$15\r\nexpired-field-2\r\n\
                  *2\r\n$3\r\nDEL\r\n$11\r\nexpired-key\r\n\
                  *2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n\
                  *3\r\n$4\r\nHDEL\r\n$10\r\nother-hash\r\n$11\r\nother-field\r\n"
                    .to_vec()
            )
        );
    }

    #[test]
    fn startup_expired_keys_follow_lazyfree_expire_not_server_del() {
        let mut config = crate::engine::extension::config::server::ServerConfig::new();
        config.lazyfree_lazy_expire = false;
        config.lazyfree_lazy_server_del = true;
        assert!(!super::startup_expired_keys_use_unlink(&config));

        config.lazyfree_lazy_expire = true;
        config.lazyfree_lazy_server_del = false;
        assert!(super::startup_expired_keys_use_unlink(&config));
    }

    #[test]
    fn startup_rdb_expired_key_uses_unlink_when_lazyfree_expire_is_enabled() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 1024);

        let selected_db = super::append_startup_rdb_expiry_events_for_role(
            &mut backlog,
            0,
            ReplicationRole::Primary,
            true,
            vec![RdbExpiryEvent::Key {
                db: 0,
                key: b"expired-key".to_vec(),
            }],
        )
        .expect("startup expiry frame should fit in the replication backlog");

        assert_eq!(selected_db, 0);
        assert_eq!(
            backlog.read_from(BacklogCursor::ZERO),
            Some(b"*2\r\n$6\r\nUNLINK\r\n$11\r\nexpired-key\r\n".to_vec())
        );
    }

    #[test]
    fn startup_rdb_decode_policy_retains_expiry_on_replica() {
        assert_eq!(
            super::startup_rdb_decode_policy(true, 0, ReplicationRole::Primary).expired_keys,
            crate::foundation::io::persistence::ExpiredKeyPolicy::Skip
        );
        assert_eq!(
            super::startup_rdb_decode_policy(true, 0, ReplicationRole::Replica).expired_keys,
            crate::foundation::io::persistence::ExpiredKeyPolicy::Retain
        );
        assert_eq!(
            super::configured_startup_replication_role(Some("127.0.0.1")),
            ReplicationRole::Replica
        );
        assert_eq!(
            super::configured_startup_replication_role(None),
            ReplicationRole::Primary
        );

        let mut source = crate::foundation::io::store::kv_engine::DbSet::new(1);
        let source_db = source.get_mut(0).expect("DB 0 exists");
        let hash = source_db
            .get_or_create_hash(b"hash")
            .expect("new hash can be created");
        hash.insert(Bytes::from_static(b"expired"), Bytes::from_static(b"old"));
        hash.insert(Bytes::from_static(b"live"), Bytes::from_static(b"new"));
        source_db.set_hash_field_expire(b"hash", b"expired", 1);
        source_db.set_no_lazy_expire(true);
        let future_key_expiry =
            crate::foundation::io::store::kv_engine::now_ms().saturating_add(60_000);
        source_db.set_expire(b"hash", future_key_expiry);
        assert_eq!(source_db.get_expire(b"hash"), Some(future_key_expiry));
        assert_eq!(
            source_db.get_hash_field_expire(b"hash", b"expired"),
            Some(1)
        );
        let mut snapshot = crate::foundation::io::persistence::serialize_all_with_options(
            &source,
            0,
            crate::foundation::io::persistence::RdbSerializeOptions {
                compression: true,
                checksum: false,
            },
        );
        let encoded_expiry = future_key_expiry.to_le_bytes();
        let expiry_offset = snapshot
            .windows(encoded_expiry.len())
            .position(|window| window == encoded_expiry)
            .expect("snapshot contains the top-level key expiry");
        assert_eq!(
            snapshot[expiry_offset - 1],
            crate::foundation::io::persistence::types::RDB_OPCODE_EXPIRETIME_MS
        );
        snapshot[expiry_offset..expiry_offset + encoded_expiry.len()]
            .copy_from_slice(&1u64.to_le_bytes());

        let mut restored = crate::foundation::io::store::kv_engine::DbSet::new(1);
        let metadata = crate::foundation::io::persistence::load_rdb_bytes_with_policy(
            &snapshot,
            &mut restored,
            super::startup_rdb_decode_policy(true, 0, ReplicationRole::Replica),
        )
        .expect("replica startup RDB should decode");

        assert!(metadata.expiry_events.is_empty());
        let restored_db = restored.get(0).expect("DB 0 exists");
        assert_eq!(restored_db.get_expire(b"hash"), Some(1));
        assert_eq!(
            restored_db.get_hash_field_expire(b"hash", b"expired"),
            Some(1)
        );
        let restored_hash = restored_db
            .get_allow_expired(b"hash")
            .expect("replica retains the expired key");
        assert!(restored_hash.hash_contains(b"expired"));
        assert!(restored_hash.hash_contains(b"live"));
    }

    #[test]
    fn replica_startup_expiry_events_do_not_advance_adopted_history() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 1024);
        super::install_startup_backlog_replication_info(
            &mut backlog,
            true,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            100,
        )
        .expect("replica should adopt persisted replication metadata");
        let cursor = backlog.next_stream_cursor();
        let reported_offset = backlog.reported_offset();

        let selected_db = super::append_startup_rdb_expiry_events_for_role(
            &mut backlog,
            3,
            ReplicationRole::Replica,
            false,
            vec![RdbExpiryEvent::Key {
                db: 1,
                key: b"must-not-be-synthesized".to_vec(),
            }],
        )
        .expect("replica startup ignores local expiry events");

        assert_eq!(selected_db, 3);
        assert_eq!(backlog.next_stream_cursor(), cursor);
        assert_eq!(backlog.reported_offset(), reported_offset);
        assert_eq!(backlog.history_len(), 0);
    }

    #[test]
    fn startup_rdb_load_rejects_a_replication_role_change() {
        assert!(super::validate_startup_rdb_role(
            ReplicationRole::Primary,
            ReplicationRole::Primary
        )
        .is_ok());
        assert_eq!(
            super::validate_startup_rdb_role(ReplicationRole::Primary, ReplicationRole::Replica,),
            Err(crate::engine::host::HostError::Invariant(
                "replication role changed during startup RDB load"
            ))
        );
    }
}
