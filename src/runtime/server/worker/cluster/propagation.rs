use crate::engine::command::CommandArgs;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::engine::extension::script::PropagationBatch;

pub(crate) struct SequencedPropagation {
    pub replication_frames: Vec<Vec<u8>>,
    pub aof_frames: Vec<Vec<u8>>,
    pub replication_selected_db: i64,
    pub aof_selected_db: i64,
}

pub(crate) fn sequence_propagation(
    ops: &[PropagationBatch],
    replication_selected_db: i64,
    aof_selected_db: i64,
) -> SequencedPropagation {
    let wrap = ops
        .iter()
        .map(PropagationBatch::command_count)
        .sum::<usize>()
        > 1;
    let mut sequenced = SequencedPropagation {
        replication_frames: Vec::new(),
        aof_frames: Vec::new(),
        replication_selected_db,
        aof_selected_db,
    };

    if wrap {
        let multi = encode_command(&[b"MULTI"]);
        sequenced.replication_frames.push(multi.clone());
        sequenced.aof_frames.push(multi);
    }

    for op in ops {
        if op.targets.includes_replication() {
            _append_op(
                &mut sequenced.replication_frames,
                &mut sequenced.replication_selected_db,
                op,
            );
        }
        if op.targets.includes_aof() {
            _append_op(
                &mut sequenced.aof_frames,
                &mut sequenced.aof_selected_db,
                op,
            );
        }
    }

    if wrap {
        let exec = encode_command(&[b"EXEC"]);
        sequenced.replication_frames.push(exec.clone());
        sequenced.aof_frames.push(exec);
    }

    sequenced
}

pub(crate) fn encode_command_args(args: &CommandArgs) -> Vec<u8> {
    let mut parts = Vec::with_capacity(args.args.len() + 1);
    parts.push(args.name.as_ref());
    parts.extend(args.args.iter().map(AsRef::as_ref));
    encode_command(&parts)
}

fn _append_op(frames: &mut Vec<Vec<u8>>, selected_db: &mut i64, op: &PropagationBatch) {
    let db_id = i64::from(op.db_id);
    if *selected_db != db_id {
        let db = op.db_id.to_string();
        frames.push(encode_command(&[b"SELECT", db.as_bytes()]));
        *selected_db = db_id;
    }
    frames.push(op.bytes.clone());
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{encode_command_args, sequence_propagation};
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::replication::protocol::encode_command;
    use crate::engine::extension::script::{PropagationOp, PropagationTargets};

    fn _frame(parts: &[&[u8]]) -> Vec<u8> {
        encode_command(parts)
    }

    #[test]
    fn one_batch_with_multiple_commands_keeps_atomic_wrapping() {
        let first = _frame(&[b"SET", b"a", b"1"]);
        let second = _frame(&[b"INCR", b"a"]);
        let mut batch = PropagationOp::both(0, first.clone());
        batch.append_frame(second.clone());

        let sequenced = sequence_propagation(&[batch], 0, 0);
        let expected_batch = [first, second].concat();

        assert_eq!(
            sequenced.replication_frames,
            vec![_frame(&[b"MULTI"]), expected_batch, _frame(&[b"EXEC"])]
        );
        assert_eq!(sequenced.aof_frames, sequenced.replication_frames);
    }

    #[test]
    fn both_targets_sequence_db_changes_identically() {
        let ops = vec![
            PropagationOp::both(0, _frame(&[b"SET", b"foo1", b"bar1"])),
            PropagationOp::both(10, _frame(&[b"INCR", b"x"])),
            PropagationOp::both(11, _frame(&[b"INCR", b"z"])),
        ];

        let sequenced = sequence_propagation(&ops, 0, 0);
        let expected = vec![
            _frame(&[b"MULTI"]),
            _frame(&[b"SET", b"foo1", b"bar1"]),
            _frame(&[b"SELECT", b"10"]),
            _frame(&[b"INCR", b"x"]),
            _frame(&[b"SELECT", b"11"]),
            _frame(&[b"INCR", b"z"]),
            _frame(&[b"EXEC"]),
        ];

        assert_eq!(sequenced.replication_frames, expected);
        assert_eq!(sequenced.aof_frames, sequenced.replication_frames);
        assert_eq!(sequenced.replication_frames.len(), 7);
        assert_eq!(
            sequenced
                .replication_frames
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            152
        );
        assert_eq!(sequenced.replication_selected_db, 11);
        assert_eq!(sequenced.aof_selected_db, 11);
    }

    #[test]
    fn nonzero_cursors_select_db_zero_inside_multi() {
        let ops = vec![
            PropagationOp::both(0, _frame(&[b"SET", b"foo1", b"bar1"])),
            PropagationOp::both(10, _frame(&[b"INCR", b"x"])),
            PropagationOp::both(11, _frame(&[b"INCR", b"z"])),
        ];

        let sequenced = sequence_propagation(&ops, 7, 7);

        assert_eq!(sequenced.replication_frames.len(), 8);
        assert_eq!(sequenced.replication_frames[1], _frame(&[b"SELECT", b"0"]));
        assert_eq!(sequenced.aof_frames, sequenced.replication_frames);
        assert_eq!(
            sequenced
                .replication_frames
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            175
        );
        assert_eq!(sequenced.replication_selected_db, 11);
        assert_eq!(sequenced.aof_selected_db, 11);
    }

    #[test]
    fn mixed_targets_wrap_both_sinks_before_filtering() {
        let replication_set = _frame(&[b"SET", b"a", b"1"]);
        let aof_set = _frame(&[b"SET", b"b", b"2"]);
        let ops = vec![
            PropagationOp::new(1, PropagationTargets::Replication, replication_set.clone()),
            PropagationOp::new(2, PropagationTargets::Aof, aof_set.clone()),
        ];

        let sequenced = sequence_propagation(&ops, 0, 0);

        assert_eq!(
            sequenced.replication_frames,
            vec![
                _frame(&[b"MULTI"]),
                _frame(&[b"SELECT", b"1"]),
                replication_set,
                _frame(&[b"EXEC"]),
            ]
        );
        assert_eq!(
            sequenced.aof_frames,
            vec![
                _frame(&[b"MULTI"]),
                _frame(&[b"SELECT", b"2"]),
                aof_set,
                _frame(&[b"EXEC"]),
            ]
        );
        assert_eq!(sequenced.replication_selected_db, 1);
        assert_eq!(sequenced.aof_selected_db, 2);
    }

    #[test]
    fn global_wrappers_reach_sink_without_targeted_effects() {
        let first = _frame(&[b"SET", b"a", b"1"]);
        let second = _frame(&[b"INCR", b"b"]);
        let ops = vec![
            PropagationOp::new(3, PropagationTargets::Replication, first.clone()),
            PropagationOp::new(7, PropagationTargets::Replication, second.clone()),
        ];

        let sequenced = sequence_propagation(&ops, 11, 13);

        assert_eq!(
            sequenced.replication_frames,
            vec![
                _frame(&[b"MULTI"]),
                _frame(&[b"SELECT", b"3"]),
                first,
                _frame(&[b"SELECT", b"7"]),
                second,
                _frame(&[b"EXEC"]),
            ]
        );
        assert_eq!(
            sequenced.aof_frames,
            vec![_frame(&[b"MULTI"]), _frame(&[b"EXEC"])]
        );
        assert_eq!(sequenced.replication_selected_db, 7);
        assert_eq!(sequenced.aof_selected_db, 13);
    }

    #[test]
    fn single_target_only_advances_its_sink() {
        let replication = sequence_propagation(
            &[PropagationOp::new(
                3,
                PropagationTargets::Replication,
                _frame(&[b"INCR", b"x"]),
            )],
            0,
            9,
        );
        assert_eq!(
            replication.replication_frames,
            vec![_frame(&[b"SELECT", b"3"]), _frame(&[b"INCR", b"x"])]
        );
        assert!(replication.aof_frames.is_empty());
        assert_eq!(replication.replication_selected_db, 3);
        assert_eq!(replication.aof_selected_db, 9);

        let aof = sequence_propagation(
            &[PropagationOp::new(
                4,
                PropagationTargets::Aof,
                _frame(&[b"INCR", b"y"]),
            )],
            8,
            0,
        );
        assert!(aof.replication_frames.is_empty());
        assert_eq!(
            aof.aof_frames,
            vec![_frame(&[b"SELECT", b"4"]), _frame(&[b"INCR", b"y"])]
        );
        assert_eq!(aof.replication_selected_db, 8);
        assert_eq!(aof.aof_selected_db, 4);
    }

    #[test]
    fn empty_ops_preserve_cursors_without_frames() {
        let sequenced = sequence_propagation(&[], 5, 6);

        assert!(sequenced.replication_frames.is_empty());
        assert!(sequenced.aof_frames.is_empty());
        assert_eq!(sequenced.replication_selected_db, 5);
        assert_eq!(sequenced.aof_selected_db, 6);
    }

    #[test]
    fn rewrite_buffer_first_db10_write_begins_with_select10() {
        let mut state = crate::engine::persistence::PersistenceState::new();
        state.set_aof_selected_db(10);
        state.begin_aof_rewrite_child(123, 1_000).unwrap();
        let op = PropagationOp::both(10, _frame(&[b"SET", b"during-rewrite", b"1"]));

        let sequenced = sequence_propagation(&[op], 0, state.aof_selected_db());
        for frame in &sequenced.aof_frames {
            state.feed_aof(frame);
        }

        assert!(state
            .aof()
            .rewrite
            .buffer
            .starts_with(&_frame(&[b"SELECT", b"10"])));
    }

    #[test]
    fn command_args_adapter_uses_command_name_and_arguments() {
        let args = CommandArgs {
            name: Bytes::from_static(b"SET"),
            args: vec![Bytes::from_static(b"key"), Bytes::from_static(b"value")],
        };

        assert_eq!(
            encode_command_args(&args),
            _frame(&[b"SET", b"key", b"value"])
        );
    }
}
