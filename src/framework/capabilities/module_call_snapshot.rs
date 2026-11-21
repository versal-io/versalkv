//! Shared snapshot classification with bounded owner-thread preparation.

use bytes::Bytes;

use crate::engine::command::{CmdFlags, CommandArgs, CommandDescriptor};
use crate::framework::context::ServerContext;

pub(crate) enum ModuleCallSnapshotPlan {
    Ready,
    ResolveTargets(Vec<(u32, Bytes)>),
    RequiresFullCapture,
    WaitForSharedKeys,
}

pub(crate) fn is_forkless_capture_control(name: &[u8]) -> bool {
    [
        b"bgsave".as_slice(),
        b"save",
        b"flushdb",
        b"flushall",
        b"shutdown",
    ]
    .iter()
    .any(|control| name.eq_ignore_ascii_case(control))
}

pub(crate) fn forkless_command_needs_full_capture(
    ctx: &ServerContext,
    flags: CmdFlags,
    args: &CommandArgs,
    targets: &[(u32, Vec<u8>)],
) -> bool {
    targets.is_empty()
        || flags & crate::engine::command::CMD_TOUCHES_ARBITRARY_KEYS != 0
        || [b"eval".as_slice(), b"evalsha", b"fcall"]
            .iter()
            .any(|name| args.name.eq_ignore_ascii_case(name))
        || targets.iter().any(|(db_idx, key)| {
            !ctx.blocking_registry()
                .waiters_for_key(*db_idx, key)
                .is_empty()
        })
}

pub(crate) fn plan_module_call_snapshot(
    ctx: &ServerContext,
    command: &CommandDescriptor,
    args: &CommandArgs,
    db_idx: u32,
) -> ModuleCallSnapshotPlan {
    if !crate::engine::command::command_may_mutate_dataset(command.name.as_str(), command.flags) {
        return ModuleCallSnapshotPlan::Ready;
    }
    let persistence = ctx.persistence();
    if !persistence.forkless_save_in_progress() && !persistence.forkless_capture_in_progress() {
        return ModuleCallSnapshotPlan::Ready;
    }
    let targets = crate::framework::spec::normalize_forkless_key_targets(
        command.name.as_str(),
        args,
        db_idx,
        command
            .extract_keys(args)
            .unwrap_or_default()
            .into_iter()
            .map(|key| (db_idx, key.to_vec()))
            .collect(),
    );
    if targets
        .iter()
        .any(|(db_idx, _)| *db_idx as usize >= ctx.num_dbs())
    {
        return ModuleCallSnapshotPlan::Ready;
    }
    if persistence.forkless_capture_in_progress()
        && !is_forkless_capture_control(&args.name)
        && !args.name.eq_ignore_ascii_case(b"swapdb")
    {
        if forkless_command_needs_full_capture(ctx, command.flags, args, &targets) {
            return ModuleCallSnapshotPlan::RequiresFullCapture;
        }
        let unresolved: Vec<_> = targets
            .iter()
            .filter(|(db_idx, key)| !persistence.forkless_key_captured(*db_idx, key))
            .map(|(db_idx, key)| (*db_idx, Bytes::copy_from_slice(key)))
            .collect();
        if !unresolved.is_empty() {
            return ModuleCallSnapshotPlan::ResolveTargets(unresolved);
        }
    }
    if targets
        .iter()
        .any(|(db_idx, key)| persistence.forkless_key_blocked(*db_idx, key))
    {
        ModuleCallSnapshotPlan::WaitForSharedKeys
    } else {
        ModuleCallSnapshotPlan::Ready
    }
}

pub(crate) fn prepare_module_call_snapshot(
    ctx: &mut ServerContext,
    command: &CommandDescriptor,
    args: &CommandArgs,
    db_idx: u32,
) -> bool {
    let plan = plan_module_call_snapshot(ctx, command, args, db_idx);
    let ModuleCallSnapshotPlan::ResolveTargets(targets) = plan else {
        return matches!(plan, ModuleCallSnapshotPlan::Ready);
    };
    for (db_idx, key) in &targets {
        ctx.prioritize_forkless_keys(*db_idx, std::iter::once(key.to_vec()));
    }
    let budget = targets.len().clamp(1, 16);
    for _ in 0..budget {
        let unresolved_before = targets
            .iter()
            .filter(|(db_idx, key)| !ctx.persistence().forkless_key_captured(*db_idx, key))
            .count();
        // Only advance snapshot storage. Client release and continuation
        // replay remain deferred until the owner releases its lease.
        ctx.drive_forkless_feed();
        match plan_module_call_snapshot(ctx, command, args, db_idx) {
            ModuleCallSnapshotPlan::Ready => return true,
            ModuleCallSnapshotPlan::ResolveTargets(unresolved)
                if unresolved.len() < unresolved_before => {}
            _ => return false,
        }
    }
    false
}
