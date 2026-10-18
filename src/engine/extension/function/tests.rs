use super::*;

fn no_command_host(
    request: crate::engine::host::HostRequest<'_>,
) -> Result<crate::engine::host::HostReply, crate::engine::host::HostError> {
    match request {
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
        _ => Err(crate::engine::host::HostError::Runtime(
            "unexpected host request".to_string(),
        )),
    }
}
use crate::engine::extension::lua::LUA_ENGINE;

#[test]
fn test_parse_engine_name_lua() {
    assert_eq!(parse_engine_name("#!lua flags=no-writes\nreturn 42"), "lua");
    assert_eq!(parse_engine_name("#!lua\nreturn 42"), "lua");
    assert_eq!(parse_engine_name("return 42"), "LUA");
}

#[test]
fn test_engine_manager_default() {
    let mgr = EngineManager::new(&LUA_ENGINE);
    assert!(mgr.get("LUA").is_some());
    assert!(mgr.get("lua").is_some());
    assert!(mgr.get("JS").is_none());
}

#[test]
fn test_function_registry_load_and_find() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    assert_eq!(reg.library_count(), 1);
    assert!(reg.function_count() > 0);
    assert!(reg.find_function("test").is_some());
    assert!(reg.find_function("TEST").is_some());
}

#[test]
fn test_function_registry_delete() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    assert_eq!(reg.library_count(), 1);
    assert!(reg.delete_library("testlib"));
    assert_eq!(reg.library_count(), 0);
    assert!(!reg.delete_library("nonexistent"));
}

#[test]
fn test_function_registry_flush() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code1 = "#!lua flags=no-writes\nserver.register_function('test1', function(keys, args) return #keys end)";
    let code2 = "#!lua flags=no-writes\nserver.register_function('test2', function(keys, args) return #keys end)";
    reg.load_library("lib1", code1).unwrap();
    reg.load_library("lib2", code2).unwrap();
    assert_eq!(reg.library_count(), 2);
    reg.flush();
    assert_eq!(reg.library_count(), 0);
    assert_eq!(reg.function_count(), 0);
}

#[test]
fn test_flush_clears_lua_vm_function_table() {
    use crate::engine::extension::lua::registered_function_count;
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('vmtest', function(keys, args) return #keys end)";
    reg.load_library("vmlib", code).unwrap();
    assert!(
        registered_function_count() >= 1,
        "VM should hold the registered callback after load"
    );
    reg.flush();
    assert_eq!(
        registered_function_count(),
        0,
        "flush() must clear the Lua VM function table"
    );
}

#[test]
fn test_fcall_self_heals_after_lua_runtime_reset() {
    use crate::engine::extension::lua::reset_lua_runtime;
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua name=test\nserver.register_function('healme', function(keys, args) return 'hello1' end)";
    reg.load_library("healme_lib", code).unwrap();

    reset_lua_runtime();

    let compiled = reg
        .find_function("healme")
        .expect("function still in registry")
        .clone();
    let engine = reg.function_engine(&compiled).expect("engine present");
    let mut ctx = crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
    let mut host = no_command_host;
    let result = engine.call(&compiled, &mut ctx, &[], &[], &mut host);
    assert_eq!(
        result.expect("FCALL after VM reset must self-heal, not error nil-to-table"),
        crate::foundation::io::protocol::Value::Bulk(bytes::Bytes::from_static(b"hello1")),
    );
}

#[test]
fn test_function_registry_dump_restore() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    let dumped = reg.dump();
    assert!(!dumped.is_empty());

    let mut reg2 = FunctionRegistry::new(&LUA_ENGINE);
    let count = reg2.restore(&dumped).unwrap();
    assert_eq!(count, 1);
    assert_eq!(reg2.library_count(), 1);
}
