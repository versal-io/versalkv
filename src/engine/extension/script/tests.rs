use super::{
    PropagationOp, PropagationTargets, ScriptControlHandle, ScriptError, ScriptKind, ScriptRunCtx,
    SCRIPT_ALLOW_CROSS_SLOT, SCRIPT_ALLOW_OOM, SCRIPT_EVAL_MODE, SCRIPT_NO_WRITES,
};
use crate::engine::command::context::CommandConn;
use crate::engine::command::{CommandArgs, CommandDescriptor};
use crate::engine::extension::script::ScriptRuntime;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use std::net::SocketAddr;

#[derive(Default)]
struct FakeScriptRuntime {
    script_disable_deny_scripts: bool,
}

fn test_ctx() -> ScriptRunCtx {
    ScriptRunCtx {
        flags: SCRIPT_EVAL_MODE,
        timeout_ms: 0,
        start_time: None,
        control: ScriptControlHandle::new(ScriptKind::Eval),
        slow_log_emitted: false,
        propagate_dirty: false,
        needs_propagation: true,
        deferred_error_replies: Vec::new(),
        argv_scratch: Vec::new(),
        lua_argument_cache: Vec::new(),
        propagation_ops: Vec::new(),
        dirty_keys_seen: Default::default(),
        propagation_targets: PropagationTargets::Both,
        has_enough_replicas_to_write: true,
        lua_enable_insecure_api: false,
        skip_propagate: false,
        resp: 2,
        func_name: String::new(),
        caller_args: Vec::new(),
        script_slot: None,
    }
}

impl ScriptRuntime for FakeScriptRuntime {
    fn lookup_command(&self, _name: &[u8]) -> Option<CommandDescriptor> {
        None
    }

    fn execute_command(
        &mut self,
        _name: &[u8],
        _conn: &mut dyn CommandConn,
        _args: &CommandArgs,
    ) -> Value {
        unreachable!("guard test does not execute commands")
    }

    fn num_dbs(&self) -> usize {
        1
    }

    fn has_maxmemory_limit(&self) -> bool {
        false
    }

    fn has_enough_replicas_to_write(&self) -> bool {
        true
    }

    fn is_readonly_replica(&self) -> bool {
        false
    }

    fn is_stale_replica(&self) -> bool {
        false
    }

    fn has_persistence_write_error(&self) -> bool {
        false
    }

    fn script_disable_deny_scripts(&self) -> bool {
        self.script_disable_deny_scripts
    }

    fn end_command(&mut self, _name: &str, _rejected: bool) {}

    fn record_command_call(&mut self, _name: &str) {}

    fn record_command_failure(&mut self, _name: &str) {}

    fn remember_tracked_key(&mut self, _token: usize, _key: &[u8]) {}

    fn record_slow_command(
        &mut self,
        _argv: Vec<Bytes>,
        _duration_us: u64,
        _peer_addr: &SocketAddr,
        _client_name: Option<Bytes>,
    ) {
    }

    fn record_internal_monitor(
        &mut self,
        _command: CommandDescriptor,
        _db: u32,
        _argv: Vec<Bytes>,
    ) {
    }

    fn pending_script_kill(&self) -> bool {
        false
    }

    fn set_script_busy(&mut self, _is_function: bool, _write_dirty: bool) {}
}

#[test]
fn fake_script_runtime_defaults_debug_noscript_override_off() {
    let runtime = FakeScriptRuntime::default();

    assert!(!runtime.script_disable_deny_scripts());
}

#[test]
fn script_context_round_trips_db_aware_propagation_operation() {
    let mut ctx = test_ctx();
    let operation = PropagationOp::new(10, PropagationTargets::Both, b"frame".to_vec());

    ctx.propagation_ops.push(operation.clone());

    assert_eq!(ctx.propagation_ops[0].db_id, 10);
    assert_eq!(ctx.propagation_ops, vec![operation]);
}

#[test]
fn script_context_reuses_common_lua_argument_bytes() {
    let mut ctx = test_ctx();

    let first = ctx.intern_lua_argument(b"sadd");
    let second = ctx.intern_lua_argument(b"sadd");

    assert_eq!(first, second);
    assert_eq!(first.as_ptr(), second.as_ptr());
}

#[test]
fn script_control_handle_shares_kill_and_busy_state_without_a_run_pointer() {
    let control = ScriptControlHandle::new(ScriptKind::Function);
    let observer = control.clone();

    control.mark_timed_out();
    control.mark_write_dirty();
    assert!(!control.request_kill(super::KillSource::FunctionKill));
    assert!(observer.is_timed_out());
    assert!(observer.is_write_dirty());
    assert!(!observer.is_killed());

    let clean = ScriptControlHandle::new(ScriptKind::Eval);
    let clean_observer = clean.clone();
    assert!(clean.request_kill(super::KillSource::ScriptKill));
    assert!(clean_observer.is_killed());
    assert_eq!(clean_observer.kill_source(), super::KillSource::ScriptKill);
}

#[test]
fn script_cluster_slot_guard_rejects_second_slot_unless_allowed() {
    let mut ctx = test_ctx();
    ctx.script_slot = Some(u16::MAX);

    assert!(ctx
        .check_cluster_slots(&[Bytes::from_static(b"foo")])
        .is_ok());
    assert!(ctx
        .check_cluster_slots(&[Bytes::from_static(b"{foo}bar")])
        .is_ok());
    assert!(ctx
        .check_cluster_slots(&[Bytes::from_static(b"bar")])
        .is_err());

    ctx.flags |= SCRIPT_ALLOW_CROSS_SLOT;
    assert!(ctx
        .check_cluster_slots(&[Bytes::from_static(b"bar")])
        .is_ok());
}

#[test]
fn script_error_compile_into_value() {
    let err = ScriptError::Compile("syntax error".to_string());
    let val = err.into_value();
    match val {
        Value::Error(msg) => {
            assert!(msg.starts_with(b"ERR Error compiling script: syntax error"));
        }
        _ => panic!("expected error value"),
    }
}

#[test]
fn script_error_runtime_with_known_code() {
    let err = ScriptError::Runtime("WRONGTYPE wrong type".to_string());
    let val = err.into_value();
    match val {
        Value::Error(msg) => {
            assert_eq!(msg.as_ref(), b"WRONGTYPE wrong type");
        }
        _ => panic!("expected error value"),
    }
}

#[test]
fn script_error_runtime_with_callback_prefix() {
    let err = ScriptError::Runtime("callback error: something failed".to_string());
    let val = err.into_value();
    match val {
        Value::Error(msg) => {
            assert!(msg.starts_with(b"ERR Error running script: something failed"));
        }
        _ => panic!("expected error value"),
    }
}

#[test]
fn script_error_runtime_with_traceback() {
    let err = ScriptError::Runtime(
        "runtime error: oops\nstack traceback:\n\t[C]: in function 'error'".to_string(),
    );
    let val = err.into_value();
    match val {
        Value::Error(msg) => {
            assert!(msg.starts_with(b"ERR Error running script: oops"));
            assert!(!msg.contains(&b'\n'));
        }
        _ => panic!("expected error value"),
    }
}

#[test]
fn script_error_kill() {
    let kill = ScriptError::KillError("Script killed by user with SCRIPT KILL.");
    match kill.into_value() {
        Value::Error(msg) => {
            assert_eq!(msg.as_ref(), b"ERR Script killed by user with SCRIPT KILL.")
        }
        _ => panic!("expected error"),
    }
}

#[test]
fn script_error_restore() {
    let err = ScriptError::Restore("bad payload".to_string());
    let val = err.into_value();
    match val {
        Value::Error(msg) => {
            assert!(msg.starts_with(b"ERR Error restoring script: bad payload"));
        }
        _ => panic!("expected error value"),
    }
}

#[test]
fn script_error_display() {
    assert_eq!(
        format!("{}", ScriptError::Compile("x".to_string())),
        "compile error: x"
    );
    assert_eq!(
        format!("{}", ScriptError::Runtime("y".to_string())),
        "runtime error: y"
    );
    assert_eq!(
        format!("{}", ScriptError::KillError("foo")),
        "script killed"
    );
    assert_eq!(
        format!("{}", ScriptError::Restore("z".to_string())),
        "restore error: z"
    );
}

#[test]
fn script_flag_constants_are_distinct() {
    assert_ne!(SCRIPT_ALLOW_OOM, SCRIPT_EVAL_MODE);
    assert_ne!(SCRIPT_ALLOW_CROSS_SLOT, SCRIPT_NO_WRITES);
}
