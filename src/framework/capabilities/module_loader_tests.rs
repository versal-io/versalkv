use super::{load_module, unload_module};
use crate::engine::extension::module::lifecycle_test_support::{snapshot, Drops};
use crate::engine::extension::module::{ModuleRef, ModuleRegistry};
use crate::framework::context::ServerContext;
use crate::framework::CommandRegistry;
use std::cell::RefCell;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

thread_local! {
    static EVENTS: RefCell<Vec<(i32, Drops)>> = const { RefCell::new(Vec::new()) };
}

const DUPLICATE_ACL_REPARSE_PROBE_ENV: &str = "VERSAL_LOADER_DUPLICATE_ACL_REPARSE_PROBE";
const LOADER_REWRITE_LIFECYCLE_CHILD_ENV: &str = "VERSAL_LOADER_REWRITE_LIFECYCLE_CHILD";

extern "C" fn observe(phase: i32) {
    let _ = EVENTS.try_with(|events| events.borrow_mut().push((phase, snapshot())));
}

fn events() -> Vec<(i32, Drops)> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "versal-loader-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn compile(&self, flavor: &str, define: Option<&str>) -> String {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let path = self.0.join(format!("{flavor}.so"));
        let mut cc = Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()));
        cc.args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"]);
        if cfg!(target_os = "macos") {
            cc.arg("-Wl,-undefined,dynamic_lookup");
        }
        if let Some(define) = define {
            cc.arg(format!("-D{define}"));
        }
        let output = cc
            .arg("-I")
            .arg(root.join("thirdparty/valkey/src"))
            .arg(root.join("tests/fixtures/module_ffi/loader_lifecycle.c"))
            .arg("-o")
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        path.to_string_lossy().into_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn names(registry: &ModuleRegistry) -> Vec<String> {
    registry.list().map(|module| module.name.clone()).collect()
}

fn ping(ctx: &mut ServerContext, name: &str) {
    let mut conn =
        crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
            0, None,
        );
    let args = crate::engine::command::CommandArgs::from_parts(vec![bytes::Bytes::from(format!(
        "{name}.ping"
    ))]);
    assert_eq!(
        super::module_cmd_dispatcher(&mut conn, &args, ctx),
        crate::foundation::io::protocol::Value::ok()
    );
}

fn run_current_test_child(test_name: &str, env_key: &str) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(env_key, "1")
        .output()
        .unwrap()
}

fn assert_loader_child_completed(output: &Output, label: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let ran_one = stdout.contains("running 1 test") || stderr.contains("running 1 test");
    let passed_one = stdout.contains("test result: ok. 1 passed; 0 failed")
        || stderr.contains("test result: ok. 1 passed; 0 failed");
    assert!(
        output.status.success() && ran_one && passed_one,
        "{label} did not complete exactly one passing child test\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout,
        stderr
    );
}

fn current_module_test_name(test_name: &str) -> String {
    let module = module_path!()
        .strip_prefix(concat!(env!("CARGO_CRATE_NAME"), "::"))
        .unwrap_or(module_path!());
    format!("{module}::{test_name}")
}

fn install_acl_user(username: &str) {
    let mut user = crate::engine::extension::acl::AclUser::new_disabled();
    user.apply_rule("on").unwrap();
    user.apply_rule("nopass").unwrap();
    user.apply_rule("+@all").unwrap();
    crate::engine::extension::acl::acl_users()
        .lock()
        .unwrap()
        .insert(username.to_string(), user);
}

fn run_duplicate_acl_inline_reparse_probe() {
    use crate::framework::commands::server::config::config_rewrite;
    use crate::framework::config::Config;
    use clap::Parser;

    install_acl_user("loader-duplicate-acl-probe");
    let fixture = Fixture::new();
    let config_path = fixture.0.join("loader.conf");
    std::fs::write(&config_path, "").unwrap();
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().server.config_file = Some(config_path.to_string_lossy().into_owned());
    assert_eq!(
        config_rewrite(&mut ctx),
        crate::foundation::io::protocol::Value::ok()
    );
    let mut parsed = Config::parse_from(["versal-server"]);
    parsed
        .load_from_file(config_path.to_str().unwrap())
        .unwrap();
}

#[test]
fn loader_inline_reparse_with_existing_acl_user_exits_duplicate_probe() {
    if std::env::var_os(DUPLICATE_ACL_REPARSE_PROBE_ENV).is_some() {
        run_duplicate_acl_inline_reparse_probe();
        return;
    }

    let test_name = current_module_test_name(
        "loader_inline_reparse_with_existing_acl_user_exits_duplicate_probe",
    );
    let output = run_current_test_child(&test_name, DUPLICATE_ACL_REPARSE_PROBE_ENV);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "inline reparse unexpectedly survived duplicate ACL user\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr
    );
    assert!(
        stderr.contains("Duplicate user found"),
        "duplicate-user diagnostic missing\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        stderr
    );
    assert!(
        stderr.contains("loader-duplicate-acl-probe"),
        "duplicate username missing\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        stderr
    );
}

fn run_loader_rewrite_reload_tail_and_reparse_recreate_real_modules_in_order() {
    use crate::foundation::io::protocol::Value;
    use crate::framework::commands::server::config::config_rewrite;
    use crate::framework::config::Config;
    use clap::Parser;

    fn rewrite_order(ctx: &mut ServerContext, path: &Path, expected: &[&str]) {
        assert_eq!(config_rewrite(ctx), Value::ok());
        let mut parsed = Config::parse_from(["versal-server"]);
        parsed.load_from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(
            parsed
                .load_modules
                .iter()
                .map(|(_, args)| args[0].as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }

    let fixture = Fixture::new();
    let info_path = fixture.compile("info", None);
    let data_path = fixture.compile("data", None);
    let config_path = fixture.0.join("loader.conf");
    std::fs::write(&config_path, "").unwrap();
    let mut commands = Box::new(CommandRegistry::new());
    let mut ctx = ServerContext::new(1);
    ctx.set_command_registry_snapshot(&commands);
    ctx.set_module_command_registry(&mut *commands);
    ctx.config_mut().server.config_file = Some(config_path.to_string_lossy().into_owned());
    ctx.module_registry_mut()
        .register(ModuleRef::new("lua", std::ptr::null_mut(), 1, true))
        .unwrap();
    for (path, name, mode) in [
        (&info_path, "infotest", "normal"),
        (&data_path, "datatype", "type"),
    ] {
        load_module(path, &[name.as_bytes(), mode.as_bytes()], &mut ctx).unwrap();
        assert!(ctx.module_registry_mut().set_load_path(
            name,
            path.clone(),
            vec![name.to_string(), mode.to_string()]
        ));
    }
    rewrite_order(&mut ctx, &config_path, &["infotest", "datatype"]);
    assert!(unload_module("datatype", &mut ctx).is_err());
    rewrite_order(&mut ctx, &config_path, &["infotest", "datatype"]);
    unload_module("infotest", &mut ctx).unwrap();
    rewrite_order(&mut ctx, &config_path, &["datatype"]);
    load_module(&info_path, &[b"infotest", b"normal"], &mut ctx).unwrap();
    assert!(ctx.module_registry_mut().set_load_path(
        "infotest",
        info_path.clone(),
        vec!["infotest".to_string(), "normal".to_string()]
    ));
    rewrite_order(&mut ctx, &config_path, &["datatype", "infotest"]);
    let handles: Vec<_> = ctx
        .module_registry()
        .list()
        .filter(|module| !module.is_static)
        .map(|module| module.handle)
        .collect();
    ctx.set_module_command_registry(std::ptr::null_mut());
    drop(ctx);
    drop(commands);
    for handle in handles {
        // SAFETY: the first runtime has been destroyed, with no live module values.
        unsafe { assert_eq!(libc::dlclose(handle), 0) };
    }
    let mut parsed = Config::parse_from(["versal-server"]);
    parsed
        .load_from_file(config_path.to_str().unwrap())
        .unwrap();
    let mut commands = Box::new(CommandRegistry::new());
    let mut restarted = ServerContext::new(1);
    restarted.set_command_registry_snapshot(&commands);
    restarted.set_module_command_registry(&mut *commands);
    for (path, args) in parsed.load_modules {
        let args: Vec<_> = args.iter().map(String::as_bytes).collect();
        load_module(&path, &args, &mut restarted).unwrap();
    }
    assert_eq!(names(restarted.module_registry()), ["datatype", "infotest"]);
    ping(&mut restarted, "datatype");
    ping(&mut restarted, "infotest");
    let data_handle = restarted
        .module_registry()
        .lookup("datatype")
        .unwrap()
        .handle;
    unload_module("infotest", &mut restarted).unwrap();
    restarted.set_module_command_registry(std::ptr::null_mut());
    drop(restarted);
    // SAFETY: restarted runtime and its type registry are gone; no type values existed.
    unsafe { assert_eq!(libc::dlclose(data_handle), 0) };
}

fn run_loader_rewrite_lifecycle_child() {
    let test_name = current_module_test_name(
        "loader_rewrite_reload_tail_and_reparse_recreate_real_modules_in_order",
    );
    let output = run_current_test_child(&test_name, LOADER_REWRITE_LIFECYCLE_CHILD_ENV);
    assert_loader_child_completed(&output, "loader rewrite lifecycle child");
}

#[cfg(unix)]
#[test]
fn loader_child_output_assertion_rejects_zero_test_success() {
    let output = Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: b"running 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored\n".to_vec(),
        stderr: Vec::new(),
    };
    let result = std::panic::catch_unwind(|| {
        assert_loader_child_completed(&output, "synthetic zero-test child");
    });
    assert!(result.is_err());
}

#[test]
fn loader_rewrite_reload_tail_and_reparse_recreate_real_modules_in_order() {
    if std::env::var_os(LOADER_REWRITE_LIFECYCLE_CHILD_ENV).is_some() {
        run_loader_rewrite_reload_tail_and_reparse_recreate_real_modules_in_order();
        return;
    }

    run_loader_rewrite_lifecycle_child();
}
#[test]
fn loader_registry_keeps_registration_order_through_rejection_rollback_and_reload() {
    let mut registry = ModuleRegistry::new();
    let mut expected = Vec::new();
    for index in 0..32 {
        let name = format!("ordered-{index}");
        registry
            .register(ModuleRef::new(&name, std::ptr::null_mut(), 1, true))
            .unwrap();
        expected.push(name);
    }
    assert_eq!(names(&registry), expected);
    assert!(registry
        .register(ModuleRef::new("ordered-0", std::ptr::null_mut(), 1, true))
        .is_err());
    assert_eq!(names(&registry), expected);
    registry.abort_load("ordered-1").unwrap();
    expected.remove(1);
    assert!(registry.prepare_unload("ordered-0").unwrap().is_none());
    registry.finish_unload("ordered-0").unwrap();
    expected.remove(0);
    registry
        .register(ModuleRef::new("ordered-0", std::ptr::null_mut(), 2, true))
        .unwrap();
    expected.push("ordered-0".to_string());
    assert_eq!(names(&registry), expected);
}

#[test]
fn loader_real_onunload_has_identity_nested_api_and_collects_before_close() {
    let fixture = Fixture::new();
    let path = fixture.compile("normal", None);
    let mut commands = Box::new(CommandRegistry::new());
    let mut ctx = ServerContext::new(1);
    ctx.set_command_registry_snapshot(&commands);
    ctx.set_module_command_registry(&mut *commands);
    let probe = format!("{:x}", observe as *const () as usize);
    load_module(
        &path,
        &[b"loader-normal", b"normal", probe.as_bytes()],
        &mut ctx,
    )
    .unwrap();
    assert!(ctx
        .module_registry()
        .lookup("loader-normal")
        .unwrap()
        .onunload
        .is_some());
    assert_eq!(
        ctx.module_registry()
            .lookup("loader-normal")
            .unwrap()
            .options
            .get(),
        2
    );
    let options = ctx
        .module_registry()
        .lookup("loader-normal")
        .unwrap()
        .options
        .clone();
    let resolved = commands.resolve(b"loader-normal.ping").unwrap();
    let before = snapshot();
    unload_module("loader-normal", &mut ctx).unwrap();
    assert_eq!(options.get(), 4);
    let observed = events();
    assert_eq!(
        observed.iter().map(|event| event.0).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(observed[2].1, snapshot());
    assert!(observed[2].1.strings >= before.strings + 2);
    assert!(observed[2].1.replies > observed[1].1.replies);
    assert_eq!(observed[2].1.keys - before.keys, 1);
    assert_eq!(observed[1].1.keys, before.keys);
    assert!(ctx.module_registry().lookup("loader-normal").is_none());
    assert!(commands.lookup(b"loader-normal.ping").is_none());
    assert!(ctx
        .command_registry()
        .lookup(b"loader-normal.ping")
        .is_none());
    assert!(commands.executor(resolved).is_none());
    ctx.set_module_command_registry(std::ptr::null_mut());
}

#[test]
fn loader_refused_onunload_preserves_library_commands_order_and_collects_context() {
    let fixture = Fixture::new();
    let path = fixture.compile("refused", None);
    let mut commands = Box::new(CommandRegistry::new());
    let mut ctx = ServerContext::new(1);
    ctx.set_command_registry_snapshot(&commands);
    ctx.set_module_command_registry(&mut *commands);
    let probe = format!("{:x}", observe as *const () as usize);
    load_module(
        &path,
        &[b"loader-refused", b"refuse", probe.as_bytes()],
        &mut ctx,
    )
    .unwrap();
    let handle = ctx
        .module_registry()
        .lookup("loader-refused")
        .unwrap()
        .handle;
    let order = names(ctx.module_registry());
    let before = snapshot();
    assert!(unload_module("loader-refused", &mut ctx).is_err());
    assert_eq!(names(ctx.module_registry()), order);
    assert_eq!(
        ctx.module_registry()
            .lookup("loader-refused")
            .unwrap()
            .handle,
        handle
    );
    assert!(ctx.module_registry().command_exists("loader-refused.ping"));
    assert!(commands.lookup(b"loader-refused.ping").is_some());
    assert!(ctx
        .command_registry()
        .lookup(b"loader-refused.ping")
        .is_some());
    assert_eq!(
        events().iter().map(|event| event.0).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(snapshot().strings >= before.strings + 2);
    assert_eq!(snapshot().keys - before.keys, 1);
    assert_eq!(
        ctx.module_registry()
            .lookup("loader-refused")
            .unwrap()
            .options
            .get(),
        4
    );
    ping(&mut ctx, "loader-refused");
    assert_eq!(
        ctx.module_registry()
            .lookup("loader-refused")
            .unwrap()
            .options
            .get(),
        3
    );
    ctx.set_module_command_registry(std::ptr::null_mut());
    drop(ctx);
    // SAFETY: the refused module remains loaded; all runtime contexts are gone.
    unsafe { assert_eq!(libc::dlclose(handle), 0) };
    events();
}

#[test]
fn loader_type_and_timer_guards_preserve_state_without_running_onunload() {
    for mode in ["type", "timer"] {
        let fixture = Fixture::new();
        let path = fixture.compile(mode, None);
        let mut commands = Box::new(CommandRegistry::new());
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&commands);
        ctx.set_module_command_registry(&mut *commands);
        let probe = format!("{:x}", observe as *const () as usize);
        load_module(
            &path,
            &[b"loader-guard", mode.as_bytes(), probe.as_bytes()],
            &mut ctx,
        )
        .unwrap();
        let handle = ctx.module_registry().lookup("loader-guard").unwrap().handle;
        let order = names(ctx.module_registry());
        let type_id = ctx
            .module_registry()
            .lookup("loader-guard")
            .unwrap()
            .types
            .first()
            .map(|mt| mt.id);
        ping(&mut ctx, "loader-guard");
        let error = unload_module("loader-guard", &mut ctx)
            .unwrap_err()
            .to_string();
        assert!(error.contains(if mode == "type" {
            "the module exports one or more module-side data types, can't unload"
        } else {
            "the module holds timer that is not fired"
        }));
        assert!(events().is_empty());
        assert_eq!(names(ctx.module_registry()), order);
        assert_eq!(
            ctx.module_registry().lookup("loader-guard").unwrap().handle,
            handle
        );
        assert!(ctx.module_registry().command_exists("loader-guard.ping"));
        assert!(commands.lookup(b"loader-guard.ping").is_some());
        assert!(ctx
            .command_registry()
            .lookup(b"loader-guard.ping")
            .is_some());
        if let Some(type_id) = type_id {
            assert!(ctx.module_registry().lookup_type_by_id(type_id).is_some());
        }
        ping(&mut ctx, "loader-guard");
        ctx.set_module_command_registry(std::ptr::null_mut());
        drop(ctx);
        // SAFETY: no module value or timer callback is alive after context drop.
        unsafe { assert_eq!(libc::dlclose(handle), 0) };
        events();
    }
}

#[test]
fn loader_legacy_symbol_and_missing_onunload_are_distinguished() {
    let fixture = Fixture::new();
    for (flavor, define, has_callback) in [
        ("legacy", "LEGACY_LOADER", true),
        ("missing", "NO_LOADER_UNLOAD", false),
    ] {
        let path = fixture.compile(flavor, Some(define));
        let mut commands = Box::new(CommandRegistry::new());
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&commands);
        ctx.set_module_command_registry(&mut *commands);
        load_module(&path, &[flavor.as_bytes(), b"normal"], &mut ctx).unwrap();
        assert_eq!(
            ctx.module_registry()
                .lookup(flavor)
                .unwrap()
                .onunload
                .is_some(),
            has_callback
        );
        unload_module(flavor, &mut ctx).unwrap();
        ctx.set_module_command_registry(std::ptr::null_mut());
    }
}

#[test]
fn loader_failed_onload_has_no_command_or_order_residue() {
    let fixture = Fixture::new();
    let path = fixture.compile("rollback", None);
    let mut commands = Box::new(CommandRegistry::new());
    let mut ctx = ServerContext::new(1);
    ctx.set_command_registry_snapshot(&commands);
    ctx.set_module_command_registry(&mut *commands);
    ctx.module_registry_mut()
        .register(ModuleRef::new("lua", std::ptr::null_mut(), 1, true))
        .unwrap();
    for _ in 0..2 {
        assert!(load_module(&path, &[b"loader-rollback", b"fail"], &mut ctx).is_err());
        assert_eq!(names(ctx.module_registry()), ["lua"]);
        assert!(!ctx.module_registry().command_exists("loader-rollback.ping"));
        assert!(commands.lookup(b"loader-rollback.ping").is_none());
        assert!(ctx
            .command_registry()
            .lookup(b"loader-rollback.ping")
            .is_none());
    }
    ctx.set_module_command_registry(std::ptr::null_mut());
}
