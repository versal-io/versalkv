#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReplicaDiskWriteErrorAction {
    Continue,
    Fatal,
}

pub(super) fn replica_disk_write_error_action(
    is_replica: bool,
    ignore_disk_write_errors: bool,
) -> ReplicaDiskWriteErrorAction {
    if is_replica && !ignore_disk_write_errors {
        ReplicaDiskWriteErrorAction::Fatal
    } else {
        ReplicaDiskWriteErrorAction::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::{replica_disk_write_error_action, ReplicaDiskWriteErrorAction};

    #[test]
    fn replica_disk_error_policy_matches_valkey_default_and_override() {
        assert_eq!(
            replica_disk_write_error_action(true, false),
            ReplicaDiskWriteErrorAction::Fatal
        );
        assert_eq!(
            replica_disk_write_error_action(true, true),
            ReplicaDiskWriteErrorAction::Continue
        );
        assert_eq!(
            replica_disk_write_error_action(false, false),
            ReplicaDiskWriteErrorAction::Continue
        );
    }
}
