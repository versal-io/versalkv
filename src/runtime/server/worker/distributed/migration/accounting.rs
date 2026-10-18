use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::node::key_hash_slot;
#[cfg(test)]
use crate::framework::observability::slot_metrics::SlotMetrics;
use crate::framework::CommandRegistry;
use bytes::Bytes;

#[derive(Clone, Copy)]
pub(crate) struct SlotMetricSample {
    pub cpu_usec: u64,
    pub network_bytes_in: u64,
    pub network_bytes_out: u64,
}

#[cfg(test)]
struct CommandMetricInput<'a> {
    pub registry: &'a CommandRegistry,
    pub args: &'a CommandArgs,
    pub observed_keys: &'a [(u32, Bytes)],
}

pub(crate) fn slots_for_command(
    registry: &CommandRegistry,
    args: &CommandArgs,
    observed_keys: &[(u32, Bytes)],
) -> Vec<u16> {
    let is_script = args.name.eq_ignore_ascii_case(b"eval")
        || args.name.eq_ignore_ascii_case(b"evalsha")
        || args.name.eq_ignore_ascii_case(b"eval_ro")
        || args.name.eq_ignore_ascii_case(b"evalsha_ro")
        || args.name.eq_ignore_ascii_case(b"fcall")
        || args.name.eq_ignore_ascii_case(b"fcall_ro");
    let keys = if is_script && !observed_keys.is_empty() {
        observed_keys
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>()
    } else if args.name.eq_ignore_ascii_case(b"ssubscribe") {
        crate::framework::commands::pubsub::ssubscribe::route_channels(args)
            .into_iter()
            .map(Bytes::from)
            .collect()
    } else if args.name.eq_ignore_ascii_case(b"spublish") {
        crate::framework::commands::pubsub::spublish::route_channels(args)
            .into_iter()
            .map(Bytes::from)
            .collect()
    } else {
        registry
            .lookup(&args.name)
            .and_then(|spec| {
                crate::runtime::server::worker::flows::request::extract_key_args(spec, args)
            })
            .unwrap_or_default()
    };
    common_slot(&keys).into_iter().collect()
}

fn common_slot(keys: &[Bytes]) -> Option<u16> {
    let first = key_hash_slot(keys.first()?);
    keys.iter()
        .all(|key| key_hash_slot(key) == first)
        .then_some(first)
}

impl crate::runtime::server::worker::ServerWorkerState {
    pub(crate) fn command_metric_slots(
        &self,
        args: &CommandArgs,
        observed_keys: &[(u32, Bytes)],
    ) -> Vec<u16> {
        if !self.ctx.config().server.cluster_enabled
            || !self.ctx.config().server.cluster_slot_stats_enabled
            || script_allows_cross_slot(&self.ctx, args)
        {
            return Vec::new();
        }
        slots_for_command(&self.registry, args, observed_keys)
    }

    pub(crate) fn record_command_slot_metrics(
        &mut self,
        args: &CommandArgs,
        observed_keys: &[(u32, Bytes)],
        sample: SlotMetricSample,
    ) {
        let slots = self.command_metric_slots(args, observed_keys);
        self.ctx.slot_metrics_mut().record(
            &slots,
            sample.cpu_usec,
            sample.network_bytes_in,
            sample.network_bytes_out,
        );
    }
}

fn script_allows_cross_slot(
    ctx: &crate::framework::context::ServerContext,
    args: &CommandArgs,
) -> bool {
    use crate::engine::extension::script::SCRIPT_ALLOW_CROSS_SLOT;

    let script =
        if args.name.eq_ignore_ascii_case(b"eval") || args.name.eq_ignore_ascii_case(b"eval_ro") {
            args.args
                .first()
                .and_then(|code| std::str::from_utf8(code).ok())
        } else if args.name.eq_ignore_ascii_case(b"evalsha")
            || args.name.eq_ignore_ascii_case(b"evalsha_ro")
        {
            args.args
                .first()
                .and_then(|sha| std::str::from_utf8(sha).ok())
                .and_then(|sha| ctx.script().script_cache.peek(sha))
        } else {
            None
        };
    if let Some(script) = script {
        return crate::engine::extension::lua::parse_shebang(script, 0)
            .is_ok_and(|(_, flags, _)| flags & SCRIPT_ALLOW_CROSS_SLOT != 0);
    }
    if args.name.eq_ignore_ascii_case(b"fcall") || args.name.eq_ignore_ascii_case(b"fcall_ro") {
        return args
            .args
            .first()
            .and_then(|name| std::str::from_utf8(name).ok())
            .and_then(|name| ctx.script().function_registry.find_function(name))
            .is_some_and(|function| function.flags & SCRIPT_ALLOW_CROSS_SLOT != 0);
    }
    false
}

#[cfg(test)]
fn record_command_metrics(
    metrics: &mut SlotMetrics,
    input: CommandMetricInput<'_>,
    sample: SlotMetricSample,
) {
    let slots = slots_for_command(input.registry, input.args, input.observed_keys);
    metrics.record(
        &slots,
        sample.cpu_usec,
        sample.network_bytes_in,
        sample.network_bytes_out,
    );
}

#[cfg(test)]
mod tests {
    use super::{
        record_command_metrics, script_allows_cross_slot, slots_for_command, CommandMetricInput,
        SlotMetricSample,
    };
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::framework::{commands, CommandRegistry};
    use bytes::Bytes;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    fn registry() -> CommandRegistry {
        let mut registry = CommandRegistry::new();
        commands::register_all(&mut registry);
        registry
    }

    #[test]
    fn command_slots_deduplicate_one_slot_and_reject_cross_slot() {
        let registry = registry();
        let same = args(&[b"MGET", b"{same}one", b"{same}two"]);
        let cross = args(&[b"MGET", b"{one}key", b"{two}key"]);

        assert_eq!(
            slots_for_command(&registry, &same, &[]),
            vec![key_hash_slot(b"{same}one")]
        );
        assert!(slots_for_command(&registry, &cross, &[]).is_empty());
    }

    #[test]
    fn command_slots_use_shard_channels_and_observed_script_keys() {
        let registry = registry();
        let channel = b"{shard}channel";
        let spublish = args(&[b"SPUBLISH", channel, b"payload"]);
        let eval = args(&[b"EVAL", b"return 1", b"0"]);
        let observed = vec![(0, Bytes::from_static(b"{script}key"))];

        assert_eq!(
            slots_for_command(&registry, &spublish, &[]),
            vec![key_hash_slot(channel)]
        );
        assert_eq!(
            slots_for_command(&registry, &eval, &observed),
            vec![key_hash_slot(b"{script}key")]
        );
        let flushall = args(&[b"FLUSHALL"]);
        assert!(slots_for_command(&registry, &flushall, &observed).is_empty());
    }

    #[test]
    fn command_accounting_records_once_only_for_a_single_slot() {
        let registry = registry();
        let same = args(&[b"MGET", b"{same}one", b"{same}two"]);
        let cross = args(&[b"MGET", b"{one}key", b"{two}key"]);
        let slot = key_hash_slot(b"{same}one") as usize;
        let mut metrics = crate::framework::observability::slot_metrics::SlotMetrics::new();

        record_command_metrics(
            &mut metrics,
            CommandMetricInput {
                registry: &registry,
                args: &same,
                observed_keys: &[],
            },
            SlotMetricSample {
                cpu_usec: 3,
                network_bytes_in: 5,
                network_bytes_out: 7,
            },
        );
        record_command_metrics(
            &mut metrics,
            CommandMetricInput {
                registry: &registry,
                args: &cross,
                observed_keys: &[],
            },
            SlotMetricSample {
                cpu_usec: 11,
                network_bytes_in: 13,
                network_bytes_out: 17,
            },
        );

        let counters = metrics.counters(slot).unwrap();
        assert_eq!(counters.cpu_usec, 3);
        assert_eq!(counters.network_bytes_in, 5);
        assert_eq!(counters.network_bytes_out, 7);
    }

    #[test]
    fn readonly_eval_with_cross_slot_flag_is_not_accounted() {
        let ctx = crate::framework::context::ServerContext::new(1);
        let eval_ro = args(&[
            b"EVAL_RO",
            b"#!lua flags=allow-cross-slot-keys\nreturn 1",
            b"0",
        ]);

        assert!(script_allows_cross_slot(&ctx, &eval_ro));
    }
}
