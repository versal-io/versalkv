pub(crate) mod blocking;
pub(crate) mod dispatch;
pub(crate) mod execution;
pub(crate) mod invocation;
pub(crate) mod outcome;
mod runtime_command;
pub(crate) mod transaction;

use crate::engine::command::CommandArgs;
use bytes::Bytes;

pub(crate) const UNAUTH_MBULK_MAX: usize = 8;
pub(crate) const UNAUTH_BULK_MAX: usize = 32 * 1024;

pub(crate) fn extract_write_keys(
    spec: &crate::framework::CommandSpec,
    args: &CommandArgs,
    db_id: u32,
) -> Option<Vec<(u32, Bytes)>> {
    extract_key_args(spec, args).map(|keys| keys.into_iter().map(|key| (db_id, key)).collect())
}

pub(crate) fn extract_key_args(
    spec: &crate::framework::CommandSpec,
    args: &CommandArgs,
) -> Option<Vec<Bytes>> {
    if spec.first_key <= 0 {
        return None;
    }
    let first = spec.first_key as usize;
    let last = if spec.last_key == 0 {
        first - 1
    } else if spec.last_key < 0 {
        let step = spec.key_step.unsigned_abs() as usize;
        let n = args.args.len();
        if step == 0 || n == 0 {
            return None;
        }
        ((n - 1) / step) * step
    } else {
        spec.last_key as usize - 1
    };
    let step = spec.key_step.unsigned_abs() as usize;
    if step == 0 {
        return None;
    }
    let mut keys = Vec::new();
    let mut i = first - 1;
    while i <= last && i < args.args.len() {
        keys.push(args.args[i].clone());
        i += step;
    }
    Some(keys)
}
