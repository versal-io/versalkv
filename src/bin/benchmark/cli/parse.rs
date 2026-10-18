use super::apply::apply_options;
use super::option::parse_benchmark_options;
use super::types::{BenchConfig, FuzzerArgs};

pub fn parse_fuzzer_args(raw_args: &[String]) -> Option<FuzzerArgs> {
    if !raw_args.iter().any(|arg| arg == "--fuzz") {
        return None;
    }

    let mut args = FuzzerArgs {
        host: "127.0.0.1".to_string(),
        port: 6379,
        num_clients: 50,
        total_commands: 100_000,
        num_keys: 0,
        loglevel: "info".to_string(),
        fuzz_flags: 0,
    };

    let mut i = 1;
    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "-h" if i + 1 < raw_args.len() => {
                args.host = raw_args[i + 1].clone();
                i += 1;
            }
            "-p" if i + 1 < raw_args.len() => {
                args.port = raw_args[i + 1].parse().unwrap_or(args.port);
                i += 1;
            }
            "-c" if i + 1 < raw_args.len() => {
                args.num_clients = raw_args[i + 1].parse().unwrap_or(args.num_clients);
                i += 1;
            }
            "-n" if i + 1 < raw_args.len() => {
                args.total_commands = raw_args[i + 1].parse().unwrap_or(args.total_commands);
                i += 1;
            }
            "--fuzz-numkeys" if i + 1 < raw_args.len() => {
                args.num_keys = raw_args[i + 1].parse().unwrap_or(args.num_keys);
                i += 1;
            }
            "--fuzz-loglevel" if i + 1 < raw_args.len() => {
                args.loglevel = raw_args[i + 1].clone();
                i += 1;
            }
            "--fuzz-flags" if i + 1 < raw_args.len() => {
                args.fuzz_flags = raw_args[i + 1].parse().unwrap_or(args.fuzz_flags);
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    Some(args)
}

pub fn parse_args(raw_args: Vec<String>) -> Result<BenchConfig, String> {
    apply_options(parse_benchmark_options(raw_args)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{BenchMode, KeyPattern, Ratio};

    #[test]
    fn repeated_commands_apply_dependent_options_to_most_recent_definition() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--command".into(),
            "SET __key__ __data__".into(),
            "--command-ratio".into(),
            "3".into(),
            "--command".into(),
            "GET __key__".into(),
            "--command-ratio".into(),
            "7".into(),
            "--command-key-pattern".into(),
            "R".into(),
        ])
        .unwrap();
        assert_eq!(cfg.memtier.command_specs[0].ratio, 3);
        assert_eq!(
            cfg.memtier.command_specs[0].key_pattern,
            KeyPattern::Sequential
        );
        assert_eq!(cfg.memtier.command_specs[1].ratio, 7);
        assert_eq!(cfg.memtier.command_specs[1].key_pattern, KeyPattern::Random);
    }

    #[test]
    fn missing_command_dependency_error_precedes_later_parse_error() {
        let cases = [
            (
                vec!["versal-benchmark", "--command-ratio", "2", "--ratio", "bad"],
                "--command-ratio requires a preceding --command",
            ),
            (
                vec![
                    "versal-benchmark",
                    "--command-key-pattern",
                    "R",
                    "--unsupported",
                ],
                "--command-key-pattern requires a preceding --command",
            ),
        ];
        for (args, expected) in cases {
            let result = parse_args(args.into_iter().map(str::to_string).collect());
            assert_eq!(result.unwrap_err(), expected);
        }
    }

    #[test]
    fn dependent_value_errors_precede_missing_command_dependency() {
        let cases = [
            (
                vec!["versal-benchmark", "--command-ratio", "invalid"],
                "Invalid value for --command-ratio: invalid",
            ),
            (
                vec!["versal-benchmark", "--command-ratio"],
                "Option --command-ratio requires an argument",
            ),
            (
                vec!["versal-benchmark", "--command-key-pattern", "X"],
                "Invalid key pattern X",
            ),
        ];
        for (args, expected) in cases {
            let result = parse_args(args.into_iter().map(str::to_string).collect());
            assert_eq!(result.unwrap_err(), expected);
        }
    }

    #[test]
    fn later_parse_error_wins_after_valid_command_dependency() {
        let cases = [
            (
                vec![
                    "versal-benchmark",
                    "--command",
                    "SET __key__ __data__",
                    "--command-ratio",
                    "2",
                    "--ratio",
                    "bad",
                ],
                "Invalid ratio bad, expected <writes>:<reads>",
            ),
            (
                vec![
                    "versal-benchmark",
                    "--command",
                    "GET __key__",
                    "--command-key-pattern",
                    "R",
                    "--unsupported",
                ],
                "Unsupported option --unsupported",
            ),
        ];
        for (args, expected) in cases {
            let result = parse_args(args.into_iter().map(str::to_string).collect());
            assert_eq!(result.unwrap_err(), expected);
        }
    }

    #[test]
    fn command_tail_preserves_separator_and_first_positional_behavior() {
        let separated = parse_args(vec![
            "versal-benchmark".into(),
            "--".into(),
            "-n".into(),
            "literal".into(),
        ])
        .unwrap();
        assert_eq!(
            separated.command,
            vec!["-n".to_string(), "literal".to_string()]
        );
        let positional = parse_args(vec![
            "versal-benchmark".into(),
            "PING".into(),
            "--ratio=bad".into(),
        ])
        .unwrap();
        assert_eq!(
            positional.command,
            vec!["PING".to_string(), "--ratio".to_string(), "bad".to_string()]
        );
    }

    #[test]
    fn equals_forms_apply_values_and_empty_values_keep_exact_error() {
        let config = parse_args(vec![
            "versal-benchmark".into(),
            "--command=SET __key__ \"hello world\"".into(),
            "--command-ratio=2".into(),
        ])
        .unwrap();
        assert_eq!(
            config.memtier.command_specs[0].argv,
            vec![
                "SET".to_string(),
                "__key__".to_string(),
                "hello world".to_string()
            ]
        );
        assert_eq!(config.memtier.command_specs[0].ratio, 2);
        assert_eq!(
            parse_args(vec!["versal-benchmark".into(), "--ratio=".into()]).unwrap_err(),
            "Option --ratio requires an argument"
        );
    }

    #[test]
    fn mode_and_conflicts_only_use_valkey_request_and_duration_options() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "-n".into(),
            "10".into(),
            "--requests".into(),
            "3".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(cfg.requests, 10);
        assert_eq!(cfg.memtier.requests, Some(3));
        let err = parse_args(vec![
            "versal-benchmark".into(),
            "-n".into(),
            "10".into(),
            "--duration".into(),
            "1".into(),
        ])
        .unwrap_err();
        assert_eq!(err, "Options -n and --duration are mutually exclusive");
    }

    #[test]
    fn compatibility_options_are_ignored_and_consume_required_values() {
        let config = parse_args(vec![
            "versal-benchmark".into(),
            "--tls".into(),
            "--cert".into(),
            "client.pem".into(),
            "--cluster".into(),
            "--precision".into(),
            "3".into(),
            "-n".into(),
            "7".into(),
        ])
        .unwrap();
        assert_eq!(config.requests, 7);
        assert_eq!(config.mode, BenchMode::Valkey);
        assert_eq!(
            parse_args(vec!["versal-benchmark".into(), "--cert".into()]).unwrap_err(),
            "Option --cert requires an argument"
        );
        assert_eq!(
            parse_args(vec!["versal-benchmark".into(), "--cert=client.pem".into(),]).unwrap_err(),
            "Unsupported option --cert=client.pem"
        );
    }

    #[test]
    fn parse_errors_are_exact_and_never_return_partially_applied_config() {
        let cases = [
            (
                vec!["versal-benchmark", "-h", "changed", "--ratio", "bad"],
                "Invalid ratio bad, expected <writes>:<reads>",
            ),
            (
                vec!["versal-benchmark", "--protocol", "resp4"],
                "Unsupported Redis protocol resp4",
            ),
            (
                vec!["versal-benchmark", "--unknown"],
                "Unsupported option --unknown",
            ),
        ];
        for (args, expected) in cases {
            let result = parse_args(args.into_iter().map(str::to_string).collect());
            assert_eq!(result.unwrap_err(), expected);
        }
    }

    #[test]
    fn parses_c_style_request_and_keyspace_options() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "-n".into(),
            "10".into(),
            "-r".into(),
            "50".into(),
        ])
        .unwrap();
        assert_eq!(cfg.requests, 10);
        assert_eq!(cfg.keyspace_len, 50);
    }

    #[test]
    fn parses_upstream_unix_socket_custom_command() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "-q".into(),
            "-s".into(),
            "/tmp/valkey.sock".into(),
            "-c".into(),
            "20".into(),
            "-n".into(),
            "20000".into(),
            "incr".into(),
            "foo".into(),
        ])
        .unwrap();
        assert_eq!(cfg.unix_socket.as_deref(), Some("/tmp/valkey.sock"));
        assert_eq!(cfg.clients, 20);
        assert_eq!(cfg.requests, 20_000);
        assert_eq!(cfg.command, vec!["incr", "foo"]);
    }

    #[test]
    fn parses_uri_user_password() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "-u".into(),
            "valkey://default:pass@127.0.0.1:6380".into(),
        ])
        .unwrap();
        assert_eq!(cfg.user.as_deref(), Some("default"));
        assert_eq!(cfg.password.as_deref(), Some("pass"));
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 6380);
    }

    #[test]
    fn valkey_mode_is_default_and_keeps_short_options() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "-n".into(),
            "10".into(),
            "-r".into(),
            "50".into(),
            "-P".into(),
            "4".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::Valkey);
        assert_eq!(cfg.requests, 10);
        assert_eq!(cfg.keyspace_len, 50);
        assert_eq!(cfg.pipeline, 4);
    }

    #[test]
    fn memtier_mode_is_enabled_by_ratio() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--ratio".into(),
            "1:10".into(),
            "--requests".into(),
            "100".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(
            cfg.memtier.ratio,
            Some(Ratio {
                writes: 1,
                reads: 10
            })
        );
        assert_eq!(cfg.memtier.requests, Some(100));
    }

    #[test]
    fn memtier_protocol_uses_long_option_to_avoid_short_p_conflict() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--protocol".into(),
            "resp3".into(),
            "--ratio".into(),
            "0:1".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert!(cfg.resp3);
    }

    #[test]
    fn parses_memtier_json_output_and_percentiles() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--json-out-file".into(),
            "/tmp/bench.json".into(),
            "--print-percentiles".into(),
            "50,95,99.9".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(
            cfg.memtier.json_out_file.as_deref(),
            Some("/tmp/bench.json")
        );
        assert_eq!(cfg.memtier.percentiles, vec![50.0, 95.0, 99.9]);
    }

    #[test]
    fn parses_memtier_key_and_data_options() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--key-prefix".into(),
            "bench:".into(),
            "--key-minimum".into(),
            "2".into(),
            "--key-maximum".into(),
            "9".into(),
            "--data-size".into(),
            "64".into(),
            "--random-data".into(),
            "--data-size-range".into(),
            "16-32".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(cfg.memtier.key_prefix, "bench:");
        assert_eq!(cfg.memtier.key_minimum, 2);
        assert_eq!(cfg.memtier.key_maximum, 9);
        assert_eq!(cfg.memtier.data_size, Some(64));
        assert!(cfg.memtier.random_data);
        assert_eq!(cfg.memtier.data_size_range, Some((16, 32)));
    }

    #[test]
    fn memtier_defaults_use_redis_ratio_values() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--ratio".into(),
            "1:10".into(),
        ])
        .unwrap();
        assert_eq!(cfg.memtier.key_prefix, "memtier-");
        assert_eq!(cfg.memtier.key_minimum, 0);
        assert_eq!(cfg.memtier.key_maximum, 10_000_000);
        assert_eq!(cfg.memtier.data_size, Some(32));
    }

    #[test]
    fn parses_repeatable_memtier_commands_with_options() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--command".into(),
            "SET __key__ \"hello world\"".into(),
            "--command-ratio".into(),
            "3".into(),
            "--command-key-pattern".into(),
            "R".into(),
            "--command".into(),
            "GET __key__".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(cfg.memtier.command_specs.len(), 2);
        assert_eq!(
            cfg.memtier.command_specs[0].argv,
            vec!["SET", "__key__", "hello world"]
        );
        assert_eq!(cfg.memtier.command_specs[0].ratio, 3);
        assert_eq!(cfg.memtier.command_specs[0].key_pattern, KeyPattern::Random);
        assert_eq!(cfg.memtier.command_specs[1].argv, vec!["GET", "__key__"]);
        assert_eq!(cfg.memtier.command_specs[1].ratio, 1);
        assert_eq!(
            cfg.memtier.command_specs[1].key_pattern,
            KeyPattern::Sequential
        );
    }

    #[test]
    fn command_specific_options_require_a_command() {
        let err = parse_args(vec![
            "versal-benchmark".into(),
            "--command-ratio".into(),
            "2".into(),
        ])
        .unwrap_err();
        assert!(err.contains("requires a preceding --command"));
    }

    #[test]
    fn parses_memtier_equals_forms() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--ratio=1:1".into(),
            "--requests=3".into(),
            "--command=SET __key__ __data__".into(),
            "--rate-limiting=100".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(
            cfg.memtier.ratio,
            Some(Ratio {
                writes: 1,
                reads: 1
            })
        );
        assert_eq!(cfg.memtier.requests, Some(3));
        assert_eq!(cfg.memtier.command_specs.len(), 1);
        assert_eq!(
            cfg.memtier.command_specs[0].argv,
            vec!["SET", "__key__", "__data__"]
        );
        assert_eq!(cfg.memtier.rate_limit_per_connection, Some(100));
    }

    #[test]
    fn parses_rate_limiting_separated_form() {
        let cfg = parse_args(vec![
            "versal-benchmark".into(),
            "--rate-limiting".into(),
            "250".into(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, BenchMode::MemtierRedis);
        assert_eq!(cfg.memtier.rate_limit_per_connection, Some(250));
    }
}
