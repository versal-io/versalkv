use std::time::Instant;

use crate::engine::command::{CommandArgs, CommandKeyIndices};

use super::{PropagationTargets, ScriptControlHandle, ScriptKind, ScriptRunCtx, SCRIPT_EVAL_MODE};

impl ScriptRunCtx {
    pub fn kill_message(&self) -> &'static str {
        match self.control.kill_source() {
            super::KillSource::FunctionKill => "Script killed by user with FUNCTION KILL.",
            super::KillSource::Shutdown => "Script interrupted by SHUTDOWN.",
            _ => "Script killed by user with SCRIPT KILL.",
        }
    }

    pub fn is_killed(&self) -> bool {
        self.control.is_killed()
    }

    pub fn is_timed_out(&self) -> bool {
        self.control.is_timed_out()
    }

    pub fn is_write_dirty(&self) -> bool {
        self.control.is_write_dirty()
    }

    pub fn mark_write_dirty(&self) {
        self.control.mark_write_dirty();
    }

    pub fn new_eval_detached(
        allow_oom: bool,
        needs_propagation: bool,
        has_enough_replicas_to_write: bool,
        lua_enable_insecure_api: bool,
    ) -> Self {
        let mut flags = SCRIPT_EVAL_MODE;
        if allow_oom {
            flags |= super::SCRIPT_ALLOW_OOM;
        }
        Self::new_detached(
            flags,
            needs_propagation,
            has_enough_replicas_to_write,
            lua_enable_insecure_api,
        )
    }

    fn new_detached(
        flags: super::ScriptFlags,
        needs_propagation: bool,
        has_enough_replicas_to_write: bool,
        lua_enable_insecure_api: bool,
    ) -> Self {
        Self {
            flags,
            timeout_ms: 0,
            start_time: None,
            control: ScriptControlHandle::new(ScriptKind::Eval),
            slow_log_emitted: false,
            propagate_dirty: false,
            needs_propagation,
            deferred_error_replies: Vec::new(),
            argv_scratch: Vec::new(),
            lua_argument_cache: Vec::new(),
            propagation_ops: Vec::new(),
            dirty_keys_seen: Default::default(),
            propagation_targets: PropagationTargets::Both,
            has_enough_replicas_to_write,
            lua_enable_insecure_api,
            skip_propagate: false,
            resp: 2,
            func_name: String::new(),
            caller_args: Vec::new(),
            script_slot: None,
            env: None,
        }
    }

    pub fn new_fcall_detached(
        flags: super::ScriptFlags,
        needs_propagation: bool,
        has_enough_replicas_to_write: bool,
        lua_enable_insecure_api: bool,
    ) -> Self {
        let mut run = Self::new_detached(
            flags,
            needs_propagation,
            has_enough_replicas_to_write,
            lua_enable_insecure_api,
        );
        run.control = ScriptControlHandle::new(ScriptKind::Function);
        run
    }

    #[cfg(test)]
    pub fn new_bare_for_test(flags: super::ScriptFlags) -> Self {
        let kind = if flags & SCRIPT_EVAL_MODE != 0 {
            ScriptKind::Eval
        } else {
            ScriptKind::Function
        };
        let mut run = Self::new_detached(flags, true, true, false);
        run.control = ScriptControlHandle::new(kind);
        run
    }

    pub fn check_cluster_slots(&mut self, keys: &[bytes::Bytes]) -> Result<(), &'static str> {
        self.check_cluster_slots_iter(keys.iter().map(bytes::Bytes::as_ref))
    }

    pub(crate) fn check_cluster_arg_slots(
        &mut self,
        args: &CommandArgs,
        indices: CommandKeyIndices,
    ) -> Result<(), &'static str> {
        self.check_cluster_slots_iter(indices.map(|index| args.args[index].as_ref()))
    }

    fn check_cluster_slots_iter<'a>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
    ) -> Result<(), &'static str> {
        if self.script_slot.is_none() || self.flags & super::SCRIPT_ALLOW_CROSS_SLOT != 0 {
            return Ok(());
        }
        for key in keys {
            let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
            match self.script_slot {
                Some(first_slot) if first_slot != slot => {
                    if first_slot == u16::MAX {
                        self.script_slot = Some(slot);
                        continue;
                    }
                    return Err(
                        "ERR Script attempted to access keys that do not hash to the same slot",
                    );
                }
                None => return Ok(()),
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Captures the server facts that govern nested-command admission for the
    /// whole invocation. Called once on the first `redis.call`; the event loop
    /// cannot change configuration, ACL users, MONITOR registration or
    /// replica role while a script executes, so the snapshot stays valid.
    pub(crate) fn ensure_env_snapshot(
        &mut self,
        runtime: &dyn super::ScriptRuntime,
        username: &str,
    ) {
        if self.env.is_some() {
            return;
        }
        let privileged_user = {
            // Single global-lock acquisition for the entire invocation; nested
            // commands of the default user skip per-command ACL work entirely.
            let users = crate::engine::extension::acl::acl_users().lock().unwrap();
            users
                .get(username)
                .map(|user| user.is_factory_default_privileged())
                .unwrap_or(false)
        };
        self.env = Some(super::ScriptEnvSnapshot {
            privileged_user,
            readonly_replica: runtime.is_readonly_replica(),
            stale_replica: runtime.is_stale_replica(),
            persistence_write_error: runtime.has_persistence_write_error(),
            maxmemory_limit: runtime.has_maxmemory_limit(),
            log_gate: runtime.script_log_gate(),
        });
    }

    pub fn check_timeout(&mut self) {
        if self.timeout_ms > 0 && !self.control.is_timed_out() {
            if let Some(start) = self.start_time {
                if start.elapsed().as_millis() >= self.timeout_ms as u128 {
                    self.control.mark_timed_out();
                }
            }
        }
    }

    pub fn start_timer(&mut self) {
        self.start_time = Some(Instant::now());
        self.control.set_invocation_info(
            self.func_name.clone(),
            self.caller_args.clone(),
            self.start_time,
        );
    }
}
