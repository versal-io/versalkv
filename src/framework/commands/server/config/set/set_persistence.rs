use crate::framework::context::ServerContext;

use super::super::parse_bool_strict;

fn _enable_aof_with<F>(
    ctx: &mut ServerContext,
    in_exec: bool,
    start_rewrite: F,
) -> Result<(), String>
where
    F: FnOnce(&mut ServerContext) -> Result<(), String>,
{
    if ctx.aof().journal.is_some() {
        ctx.persistence_mut().set_aof_rewrite_scheduled(false);
        ctx.config_mut().persistence.aof_enabled = true;
        return Ok(());
    }

    // OFF -> ON transition enters C's AOF_WAIT_REWRITE state: the confirmed
    // fsynced offset must stay frozen until the initial rewrite completes.
    ctx.aof_mut().aof_wait_rewrite = true;

    if ctx.persistence().has_active_child() {
        ctx.persistence_mut().set_aof_rewrite_scheduled(true);
        ctx.config_mut().persistence.aof_enabled = true;
        crate::server_log!(
            notice,
            "AOF was enabled but there is already another background operation. An AOF background was scheduled to start when possible."
        );
        return Ok(());
    }

    if in_exec {
        ctx.persistence_mut().set_aof_rewrite_scheduled(true);
        ctx.config_mut().persistence.aof_enabled = true;
        crate::server_log!(
            notice,
            "AOF was enabled during a transaction. An AOF background was scheduled to start when possible."
        );
        return Ok(());
    }

    ctx.config_mut().persistence.aof_enabled = true;
    let result = start_rewrite(ctx);
    if result.is_ok()
        && (ctx.aof().journal.is_some() || ctx.persistence().aof_rewrite_child_pid().is_some())
    {
        ctx.persistence_mut().set_aof_rewrite_scheduled(false);
        return Ok(());
    }

    ctx.config_mut().persistence.aof_enabled = false;
    ctx.persistence_mut().set_aof_rewrite_scheduled(false);
    ctx.persistence_mut().reset_aof_selected_db();
    ctx.aof_mut().reset_fsynced_disabled();
    match result {
        Ok(()) => Err("ERR initial AOF rewrite did not establish a write sink".to_string()),
        Err(error) if error.starts_with("ERR ") => Err(error),
        Err(error) => Err(format!("ERR {error}")),
    }
}

pub(super) fn config_set_persistence(
    ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    _val_bytes: &[u8],
    in_exec: bool,
) -> Result<(), String> {
    match param {
        "rdb-key-save-delay" => {
            if let Ok(n) = val_trimmed.parse::<i64>() {
                if !(i32::MIN as i64..=i32::MAX as i64).contains(&n) {
                    return Err("rdb-key-save-delay out of range".to_string());
                }
                ctx.config_mut().persistence.rdb_key_save_delay = n;
                ctx.persistence().set_forkless_key_delay(n);
                if n == 0 {
                    if let Some(pid) = ctx.persistence().rdb_child_pid() {
                        let _ = crate::foundation::io::sys::process::send_signal(
                            pid,
                            crate::foundation::io::sys::process::ProcessSignal::User2,
                        );
                    }
                }
            }
        }
        "appendonly" => {
            let new_val = parse_bool_strict(val_trimmed)?;
            let old_val = ctx.config().persistence.aof_enabled;
            if new_val && !old_val {
                return _enable_aof_with(ctx, in_exec, |ctx| {
                    crate::framework::commands::server::bgrewriteaof::start_aof_rewrite(ctx)
                });
            }
            if !new_val {
                ctx.config_mut().persistence.aof_enabled = false;
            }
            if !new_val && old_val {
                ctx.persistence_mut().set_aof_rewrite_scheduled(false);
                if let Some(pid) = ctx.persistence().aof_rewrite_child_pid() {
                    crate::server_log!(notice, "Killing AOF child pid {}", pid);
                    let _ = crate::foundation::io::sys::process::send_signal(
                        pid,
                        crate::foundation::io::sys::process::ProcessSignal::Kill,
                    );
                    let _ = crate::foundation::io::sys::process::wait_blocking(pid);
                    ctx.persistence_mut().abort_child();
                }
                if let Some(mut journal) = ctx.aof_mut().journal.take() {
                    let _ = journal.appender.shutdown();
                }
                // Mirror C's stopAppendOnly: once AOF is off the confirmed
                // fsynced offset reverts to the "-1"/None sentinel (never ack)
                // and the optimistic pending offset resets to 0.
                ctx.aof_mut().reset_fsynced_disabled();
                ctx.persistence_mut().reset_aof_selected_db();
            }
        }
        "aof-disable-auto-gc" => {
            if !ctx.config().persistence.aof_disable_auto_gc {
                let config = &ctx.config().persistence;
                if let Err(error) = crate::foundation::io::persistence::gc_aof_history(
                    &config.dir,
                    &config.aof_filename,
                    &config.aof_appenddirname,
                ) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        return Err(error.to_string());
                    }
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabling_aof_auto_gc_removes_existing_history_files() {
        let dir = std::env::temp_dir().join(format!(
            "versal-aof-auto-gc-{}-{}",
            std::process::id(),
            crate::foundation::io::sys::time::mstime()
        ));
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).expect("create AOF directory");
        let history = app_dir.join("appendonly.aof.1.base.aof");
        std::fs::write(&history, b"history").expect("write history file");
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.2.base.aof seq 2 type b\n\
             file appendonly.aof.1.base.aof seq 1 type h\n",
        )
        .expect("write manifest");

        let mut ctx = ServerContext::new(16);
        ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        ctx.config_mut().persistence.aof_appenddirname = "appendonlydir".to_string();
        ctx.config_mut().persistence.aof_disable_auto_gc = true;
        config_set_persistence(&mut ctx, "aof-disable-auto-gc", "yes", b"yes", false)
            .expect("disabled auto GC is accepted");
        assert!(history.exists());

        ctx.config_mut().persistence.aof_disable_auto_gc = false;
        config_set_persistence(&mut ctx, "aof-disable-auto-gc", "no", b"no", false)
            .expect("enabling auto GC succeeds");
        assert!(!history.exists());
        let manifest = std::fs::read_to_string(app_dir.join("appendonly.aof.manifest"))
            .expect("read rewritten manifest");
        assert!(!manifest.contains("type h"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn enabling_aof_rolls_back_when_initial_rewrite_start_fails() {
        let mut ctx = ServerContext::new(16);
        ctx.set_repl_seldb(9);
        ctx.persistence_mut().set_aof_selected_db(7);
        ctx.persistence_mut().set_aof_rewrite_scheduled(true);

        let error =
            _enable_aof_with(&mut ctx, false, |_| Err("fork failed".to_string())).unwrap_err();

        assert_eq!(error, "ERR fork failed");
        assert!(!ctx.config().persistence.aof_enabled);
        assert!(!ctx.persistence().aof_rewrite_scheduled());
        assert_eq!(ctx.persistence().aof_selected_db(), -1);
        assert_eq!(ctx.repl_seldb(), 9);
        assert!(ctx.aof().journal.is_none());
        assert!(!ctx.persistence().has_active_child());
    }

    #[test]
    fn config_set_schedules_aof_rewrite_while_rdb_child_is_active() {
        let mut ctx = ServerContext::new(16);
        ctx.set_repl_seldb(9);
        ctx.persistence_mut().begin_rdb_child(41, 100).unwrap();

        config_set_persistence(&mut ctx, "appendonly", "yes", b"yes", false).unwrap();

        assert!(ctx.config().persistence.aof_enabled);
        assert!(ctx.persistence().aof_rewrite_scheduled());
        assert_eq!(ctx.repl_seldb(), 9);
        assert!(ctx.aof().journal.is_none());
        assert_eq!(ctx.persistence().rdb_child_pid(), Some(41));
        ctx.persistence_mut().abort_child();
    }

    #[test]
    fn config_set_schedules_aof_rewrite_while_exec_is_active() {
        let mut ctx = ServerContext::new(16);

        _enable_aof_with(&mut ctx, true, |_| {
            panic!("transactional AOF enable must not fork before EXEC completes")
        })
        .unwrap();

        assert!(ctx.config().persistence.aof_enabled);
        assert!(ctx.persistence().aof_rewrite_scheduled());
        assert!(!ctx.persistence().has_active_child());
    }

    #[test]
    fn enabling_aof_returns_only_after_rewrite_buffer_is_available() {
        let mut ctx = ServerContext::new(16);

        _enable_aof_with(&mut ctx, false, |ctx| {
            ctx.persistence_mut()
                .begin_aof_rewrite_child(42, 100)
                .map_err(|error| format!("{error:?}"))
        })
        .unwrap();

        assert!(ctx.config().persistence.aof_enabled);
        assert_eq!(ctx.persistence().aof_rewrite_child_pid(), Some(42));
        ctx.persistence_mut().feed_aof(b"frame");
        assert_eq!(ctx.aof().rewrite.buffer, b"frame");
        ctx.persistence_mut().abort_child();
    }

    #[test]
    fn disabling_aof_resets_only_aof_cursor_even_without_open_journal() {
        let mut ctx = ServerContext::new(16);
        ctx.config_mut().persistence.aof_enabled = true;
        ctx.set_repl_seldb(9);
        ctx.persistence_mut().set_aof_selected_db(7);

        config_set_persistence(&mut ctx, "appendonly", "no", b"no", false).unwrap();

        assert!(!ctx.config().persistence.aof_enabled);
        assert_eq!(ctx.repl_seldb(), 9);
        assert_eq!(ctx.persistence().aof_selected_db(), -1);
    }
}
