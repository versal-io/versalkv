
use command_macros::command;

use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::framework::commands::server::command::getkeys_helpers::eval_get_keys;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;

#[command("EVAL", get_keys = eval_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn eval(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    match crate::framework::host_tasks::prepare_lua_task(conn, args, ctx) {
        Ok(crate::framework::host_tasks::PreparedHostTask::Lua(task)) => {
            crate::framework::host_tasks::run_lua_task_inline(task, conn, ctx)
        }
        Ok(_) => Value::error("ERR invalid hosted task for EVAL"),
        Err(reply) => reply,
    }
}

#[cfg(test)]
mod tests {
    use super::eval;
    use crate::engine::extension::acl::{
        acl_log, acl_users, AclUser, ACL_DENIED_KEY, ACL_LOG_CTX_LUA,
    };
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::state::{ClusterState, CLUSTER_OK};
    use crate::engine::distributed::replication::protocol::encode_command;
    use crate::engine::extension::script::PropagationTargets;
    use crate::engine::command::context::CommandConn;
    use crate::framework::commands::scripting::evalsha::evalsha;
    use crate::framework::commands::scripting::fcall::fcall;
    use crate::framework::commands::scripting::function::function;
    use crate::framework::context::ServerContext;
    use crate::framework::commands::scripting::script_cache::ScriptCache;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::{CommandArgs, CommandRegistry};
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn connection() -> Connection {
        Connection::new(
            Token(1),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            false,
        )
    }

    fn registry() -> CommandRegistry {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        registry
    }

    fn eval_args(script: &'static [u8]) -> CommandArgs {
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(script),
            Bytes::from_static(b"0"),
        ])
    }

    fn load_function(
        conn: &mut Connection,
        ctx: &mut ServerContext,
        code: &'static [u8],
    ) {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"LOAD"),
            Bytes::from_static(code),
        ]);
        assert!(matches!(function(conn, &args, ctx), Value::Bulk(_)));
        conn.clear_custom_propagation();
    }

    #[test]
    fn real_lua_writes_capture_db_ids_without_select_operation() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('SET','db0','v0'); redis.call('SELECT',10); return redis.call('SET','db10','v10')",
        );

        let result = eval(&mut conn, &args, &mut ctx);

        assert_eq!(result, crate::foundation::io::protocol::Value::ok());
        let ops = conn.script_propagation();
        assert_eq!(ops.iter().map(|op| op.db_id).collect::<Vec<_>>(), [0, 10]);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].bytes, encode_command(&[b"SET", b"db0", b"v0"]));
        assert_eq!(ops[1].bytes, encode_command(&[b"SET", b"db10", b"v10"]));
        assert!(ops.iter().all(|op| !op.bytes.windows(6).any(|w| w == b"SELECT")));
    }

    #[test]
    fn eval_preserves_binary_keys_and_arguments() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let binary_key = Bytes::from_static(b"key\xff\x00\"\\");
        let binary_arg = Bytes::from_static(b"value\x80\x00\n\r\t\"\\");
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"return {KEYS[1],ARGV[1]}"),
            Bytes::from_static(b"1"),
            binary_key.clone(),
            binary_arg.clone(),
        ]);

        let result = eval(&mut conn, &args, &mut ctx);

        assert_eq!(
            result,
            Value::Array(vec![Value::Bulk(binary_key), Value::Bulk(binary_arg)])
        );
    }

    #[test]
    fn real_lua_failed_nested_write_preserves_prior_effect_only() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('SET','wrong-type','value'); return redis.pcall('LPUSH','wrong-type','item')",
        );

        let result = eval(&mut conn, &args, &mut ctx);

        assert!(matches!(
            result,
            crate::foundation::io::protocol::Value::Error(_)
        ));
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].db_id, 0);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"wrong-type", b"value"])
        );
    }

    #[test]
    fn eval_rejects_nested_command_for_a_slot_owned_by_another_cluster_node() {
        const REMOTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.config_mut().server.cluster_enabled = true;
        ctx.set_command_registry_snapshot(&registry);

        let key = Bytes::from_static(b"{remote-script-slot}key");
        let slot = key_hash_slot(&key) as usize;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut remote = ClusterNode::new(
            REMOTE.to_string(),
            "192.0.2.1".to_string(),
            6380,
            16380,
            true,
        );
        remote.flags &= !CLUSTER_NODE_MYSELF;
        remote.slots[slot] = true;
        remote.numslots = 1;
        state.nodes.insert(REMOTE.to_string(), remote);
        state.state = CLUSTER_OK;
        *ctx.cluster_state_mut() = Some(Box::new(state));

        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"return redis.call('RPUSH', KEYS[1], 'value')"),
            Bytes::from_static(b"1"),
            key.clone(),
        ]);

        let result = eval(&mut conn, &args, &mut ctx);

        assert!(matches!(result, Value::Error(ref message) if String::from_utf8_lossy(message).contains("Attempted to access a non local key in a cluster node")));
        assert!(ctx.db(0).is_some_and(|db| db.get_no_expire(&key).is_none()));
    }

    #[test]
    fn eval_allows_nested_read_when_cluster_is_down_and_reads_are_enabled() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.config_mut().server.cluster_enabled = true;
        ctx.config_mut().server.cluster_allow_reads_when_down = true;
        ctx.set_command_registry_snapshot(&registry);

        let key = Bytes::from_static(b"{down-read}key");
        let slot = key_hash_slot(&key) as usize;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.myself.slots[slot] = true;
        state.myself.numslots = 1;
        state.slots[slot] = true;
        *ctx.cluster_state_mut() = Some(Box::new(state));
        ctx.db_mut(0)
            .unwrap()
            .set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string(b"value".as_slice()),
            );

        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"return redis.call('GET', KEYS[1])"),
            Bytes::from_static(b"1"),
            key,
        ]);

        assert_eq!(
            eval(&mut conn, &args, &mut ctx),
            Value::Bulk(Bytes::from_static(b"value"))
        );
    }

    #[test]
    fn nested_acl_key_denial_preserves_error_and_log_object() {
        const USERNAME: &str = "lua-allocation-acl-test";

        let registry = registry();
        let mut conn = connection();
        conn.set_username(USERNAME.to_string());
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let mut user = AclUser::new_disabled();
        user.apply_rule("on").unwrap();
        user.apply_rule("nopass").unwrap();
        user.apply_rule("+set").unwrap();
        acl_users()
            .lock()
            .unwrap()
            .insert(USERNAME.to_string(), user);
        let args = eval_args(b"return redis.pcall('SET','denied-key','value')");

        let result = eval(&mut conn, &args, &mut ctx);
        acl_users().lock().unwrap().remove(USERNAME);
        let entry = {
            let mut log = acl_log().lock().unwrap();
            let entry = log
                .iter()
                .find(|entry| entry.username == USERNAME)
                .cloned()
                .expect("nested ACL denial should be logged");
            log.retain(|entry| entry.username != USERNAME);
            entry
        };

        assert_eq!(
            result,
            Value::Error(Bytes::from_static(
                b"ACL failure in script: No permissions to access a key"
            ))
        );
        assert_eq!(entry.reason, ACL_DENIED_KEY);
        assert_eq!(entry.context, ACL_LOG_CTX_LUA);
        assert_eq!(entry.object, "denied-key");
    }

    #[test]
    fn repeated_nested_writes_report_one_dirty_key() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('SET','same-key','one'); return redis.call('SET','same-key','two')",
        );

        assert_eq!(eval(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(
            conn.dirty_keys(),
            &[(0, Bytes::from_static(b"same-key"))]
        );
    }

    #[test]
    fn nested_custom_propagation_frames_retain_order() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('INCRBYFLOAT','counter','0.5'); return redis.call('INCRBYFLOAT','counter','0.25')",
        );

        assert_eq!(
            eval(&mut conn, &args, &mut ctx),
            Value::Bulk(Bytes::from_static(b"0.75"))
        );
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        let expected = [
            encode_command(&[b"SET", b"counter", b"0.5", b"KEEPTTL"]),
            encode_command(&[b"SET", b"counter", b"0.75", b"KEEPTTL"]),
        ]
        .concat();
        assert_eq!(
            ops[0].bytes,
            expected
        );
        assert_eq!(ops[0].command_count(), 2);
    }

    #[test]
    fn real_eval_repl_mode_changes_apply_only_to_subsequent_commands() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('SET','before','1'); redis.set_repl(redis.REPL_NONE); redis.call('SET','suppressed','2'); redis.set_repl(redis.REPL_ALL); return redis.call('SET','after','3')",
        );

        let result = eval(&mut conn, &args, &mut ctx);

        assert_eq!(result, crate::foundation::io::protocol::Value::ok());
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].bytes, encode_command(&[b"SET", b"before", b"1"]));
        assert_eq!(ops[0].targets, PropagationTargets::Both);
        assert_eq!(
            ops[1].bytes,
            encode_command(&[b"SET", b"suppressed", b"2"])
        );
        assert_eq!(ops[1].targets, PropagationTargets::None);
        assert_eq!(ops[2].bytes, encode_command(&[b"SET", b"after", b"3"]));
        assert_eq!(ops[2].targets, PropagationTargets::Both);
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn real_eval_write_before_repl_none_exports_typed_op_without_outer_frame() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.call('SET','local-only','value'); redis.set_repl(redis.REPL_NONE); return redis.call('GET','local-only')",
        );

        let result = eval(&mut conn, &args, &mut ctx);

        assert_eq!(
            result,
            crate::foundation::io::protocol::Value::Bulk(Bytes::from_static(b"value"))
        );
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"local-only", b"value"])
        );
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn real_eval_repl_none_write_then_repl_all_exports_no_effects() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let args = eval_args(
            b"redis.set_repl(redis.REPL_NONE); redis.call('SET','suppressed','value'); redis.set_repl(redis.REPL_ALL); return redis.call('GET','suppressed')",
        );

        let result = eval(&mut conn, &args, &mut ctx);

        assert_eq!(
            result,
            crate::foundation::io::protocol::Value::Bulk(Bytes::from_static(b"value"))
        );
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"suppressed", b"value"])
        );
        assert_eq!(ops[0].targets, PropagationTargets::None);
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn real_evalsha_exports_typed_db_effects_without_outer_frame() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        let script =
            "redis.call('SET','sha-db0','0'); redis.call('SELECT',10); return redis.call('SET','sha-db10','10')";
        let sha1 = ScriptCache::compute_sha1(script);
        ctx.script_mut()
            .script_cache
            .store(sha1.clone(), script.to_string());
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVALSHA"),
            Bytes::from(sha1),
            Bytes::from_static(b"0"),
        ]);

        let result = evalsha(&mut conn, &args, &mut ctx);

        assert_eq!(result, Value::ok());
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].db_id, 0);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"sha-db0", b"0"])
        );
        assert_eq!(ops[1].db_id, 10);
        assert_eq!(
            ops[1].bytes,
            encode_command(&[b"SET", b"sha-db10", b"10"])
        );
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn real_fcall_error_restores_caller_db_and_exports_completed_effects() {
        let registry = registry();
        let mut conn = connection();
        conn.set_selected_db(4);
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        load_function(
            &mut conn,
            &mut ctx,
            b"#!lua name=task3_error\nserver.register_function('task3_write_then_error', function(keys, args) redis.call('SELECT', 10); redis.call('SET', 'fcall-done', '1'); error('task3 boom') end)",
        );
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FCALL"),
            Bytes::from_static(b"task3_write_then_error"),
            Bytes::from_static(b"0"),
        ]);

        let result = fcall(&mut conn, &args, &mut ctx);

        match result {
            Value::Error(message) => {
                assert!(String::from_utf8_lossy(&message).contains("task3 boom"));
            }
            other => panic!("expected function runtime error, got {other:?}"),
        }
        assert_eq!(conn.selected_db(), 4);
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].db_id, 10);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"fcall-done", b"1"])
        );
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn real_fcall_repl_none_effect_is_not_replayed_by_final_repl_all() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        load_function(
            &mut conn,
            &mut ctx,
            b"#!lua name=task3_repl_none\nserver.register_function('task3_suppressed', function(keys, args) redis.set_repl(redis.REPL_NONE); redis.call('SET', 'fcall-suppressed', '1'); redis.set_repl(redis.REPL_ALL); return 'done' end)",
        );
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FCALL"),
            Bytes::from_static(b"task3_suppressed"),
            Bytes::from_static(b"0"),
        ]);

        let result = fcall(&mut conn, &args, &mut ctx);

        assert_eq!(result, Value::Bulk(Bytes::from_static(b"done")));
        let ops = conn.script_propagation();
        assert_eq!(ops.len(), 1);
        assert_eq!(
            ops[0].bytes,
            encode_command(&[b"SET", b"fcall-suppressed", b"1"])
        );
        assert_eq!(ops[0].targets, PropagationTargets::None);
        assert!(conn.custom_propagation().is_empty());
    }

    #[test]
    fn fcall_allow_stale_flag_reaches_the_script_runtime() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&registry);
        load_function(
            &mut conn,
            &mut ctx,
            b"#!lua name=allow_stale_regression\nserver.register_function{function_name='allow_stale_fn', callback=function() return 'hello' end, flags={'allow-stale', 'no-writes'}}",
        );
        ctx.replication_mut().role =
            crate::engine::distributed::replication::ReplicationRole::Replica;
        ctx.config_mut().replication.master_link_status = "down".to_string();
        ctx.config_mut().replication.replica_serve_stale_data = false;
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FCALL"),
            Bytes::from_static(b"allow_stale_fn"),
            Bytes::from_static(b"0"),
        ]);

        assert_eq!(
            fcall(&mut conn, &args, &mut ctx),
            Value::Bulk(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn fcall_allow_stale_reports_nested_command_rejection() {
        let registry = registry();
        let mut conn = connection();
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&registry);
        load_function(
            &mut conn,
            &mut ctx,
            b"#!lua name=allow_stale_nested\nserver.register_function{function_name='allow_stale_get', callback=function() return redis.call('get', 'x') end, flags={'allow-stale', 'no-writes'}}",
        );
        ctx.replication_mut().role =
            crate::engine::distributed::replication::ReplicationRole::Replica;
        ctx.config_mut().replication.master_link_status = "down".to_string();
        ctx.config_mut().replication.replica_serve_stale_data = false;
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FCALL"),
            Bytes::from_static(b"allow_stale_get"),
            Bytes::from_static(b"0"),
        ]);

        let result = fcall(&mut conn, &args, &mut ctx);

        let Value::Error(error) = result else {
            panic!("expected nested stale-command error, got {result:?}");
        };
        assert!(
            String::from_utf8_lossy(&error)
                .contains("Can not execute the command on a stale replica"),
            "unexpected error: {}",
            String::from_utf8_lossy(&error)
        );
    }
}
