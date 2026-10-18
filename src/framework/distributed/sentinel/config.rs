use std::os::unix::fs::PermissionsExt;

use crate::engine::distributed::sentinel::config::{
    SentinelConfigScriptKind, SentinelConfigScriptValidator,
};
use crate::engine::distributed::sentinel::SentinelState;

struct FilesystemScriptValidator;

impl SentinelConfigScriptValidator for FilesystemScriptValidator {
    fn validate_script(&self, kind: SentinelConfigScriptKind, path: &str) -> Result<(), String> {
        let executable = std::fs::metadata(path)
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false);
        if executable {
            return Ok(());
        }
        Err(match kind {
            SentinelConfigScriptKind::Notification => {
                "Notification script seems non existing or non executable."
            }
            SentinelConfigScriptKind::ClientReconfig => {
                "Client reconfiguration script seems non existing or non executable."
            }
        }
        .to_string())
    }
}

pub(crate) fn load_config_directives(
    state: &mut SentinelState,
    directives: &[String],
) -> Result<(), String> {
    state.load_config_directives(directives, &FilesystemScriptValidator)
}

#[cfg(test)]
mod tests {
    use super::FilesystemScriptValidator;
    use crate::engine::distributed::sentinel::config::{
        SentinelConfigScriptKind, SentinelConfigScriptValidator,
    };
    use crate::engine::distributed::sentinel::script::{ScriptType, SentinelScriptEngine};
    use std::fs;

    #[test]
    fn filesystem_validator_uses_c_compatible_errors() {
        assert!(FilesystemScriptValidator
            .validate_script(SentinelConfigScriptKind::Notification, "/bin/sh")
            .is_ok());
        assert_eq!(
            FilesystemScriptValidator.validate_script(
                SentinelConfigScriptKind::Notification,
                "/path/that/does/not/exist"
            ),
            Err("Notification script seems non existing or non executable.".to_string())
        );
        assert_eq!(
            FilesystemScriptValidator.validate_script(
                SentinelConfigScriptKind::ClientReconfig,
                "/path/that/does/not/exist"
            ),
            Err("Client reconfiguration script seems non existing or non executable.".to_string())
        );
    }

    #[test]
    fn script_engine_discards_child_stdout_and_stderr() {
        let unique = format!(
            "versal-sentinel-script-{}-{}",
            std::process::id(),
            crate::foundation::io::sys::time::mstime()
        );
        let dir = std::env::temp_dir().join(unique);
        fs::create_dir(&dir).expect("create temp script directory");
        let out_path = dir.join("fds.txt");
        let mut engine = SentinelScriptEngine::new();
        engine.enqueue(
            ScriptType::Notification,
            "/bin/sh",
            vec![
                "-c".to_string(),
                "test /dev/fd/1 -ef /dev/null && test /dev/fd/2 -ef /dev/null && printf 'stdio-is-null\\n' > \"$1\"".to_string(),
                "stdio-probe".to_string(),
                out_path.to_str().expect("temp path is utf-8").to_string(),
            ],
        );
        engine.run_pending();
        let spawned = engine.running.len();
        wait_until_empty(&mut engine);
        assert_eq!(spawned, 1, "stdio probe must spawn successfully");
        let contents = fs::read_to_string(&out_path).expect("read fd report");
        assert_eq!(contents, "stdio-is-null\n");
        fs::remove_dir_all(&dir).expect("remove temp script directory");
    }

    fn wait_until_empty(engine: &mut SentinelScriptEngine) {
        use std::time::{Duration, Instant};

        let mut deadline = Instant::now() + Duration::from_secs(5);
        let mut has_timed_out = false;
        let mut has_failed = false;
        while !engine.running.is_empty() {
            let mut has_all_exited = true;
            for (_, child) in &mut engine.running {
                match child.try_wait().expect("wait for stdio probe") {
                    Some(status) => has_failed |= !status.success(),
                    None => has_all_exited = false,
                }
            }
            if has_all_exited {
                engine.collect_terminated();
                assert!(engine.running.is_empty());
                break;
            }
            if Instant::now() >= deadline {
                assert!(!has_timed_out, "timed out reaping killed stdio probe");
                for (_, child) in &mut engine.running {
                    let _ = child.kill();
                }
                has_timed_out = true;
                deadline = Instant::now() + Duration::from_secs(5);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!has_timed_out, "stdio probe exceeded its deadline");
        assert!(!has_failed, "stdio probe exited unsuccessfully");
    }
}
