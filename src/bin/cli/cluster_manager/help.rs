/// Implement `--cluster help` and `cluster help` by printing the list of
/// supported cluster-manager commands with the same layout the C variant
/// uses (`clusterManagerCommandHelp` in `thirdparty/valkey/src/valkey-cli.c`).
pub(super) fn execute() -> Result<(), String> {
    println!("Cluster Manager Commands:");
    const COMMANDS: &[(&str, &str, &[&str])] = &[
        ("create", "host1:port1 ... hostN:portN", &["replicas"]),
        (
            "check",
            "<host:port> or <host> <port> - separated by either colon or space",
            &["search-multiple-owners"],
        ),
        (
            "info",
            "<host:port> or <host> <port> - separated by either colon or space",
            &[],
        ),
        (
            "fix",
            "<host:port> or <host> <port> - separated by either colon or space",
            &["search-multiple-owners", "fix-with-unreachable-primaries"],
        ),
        (
            "reshard",
            "<host:port> or <host> <port> - separated by either colon or space",
            &[
                "from", "to", "slots", "yes", "timeout", "pipeline", "replace",
            ],
        ),
        (
            "rebalance",
            "<host:port> or <host> <port> - separated by either colon or space",
            &[
                "weight",
                "use-empty-primaries",
                "timeout",
                "simulate",
                "pipeline",
                "threshold",
                "replace",
            ],
        ),
        (
            "add-node",
            "new_host:new_port existing_host:existing_port",
            &["replica", "primaries-id"],
        ),
        ("del-node", "host:port node_id", &[]),
        (
            "call",
            "host:port command arg arg .. arg",
            &["only-primaries", "only-replicas"],
        ),
        ("set-timeout", "host:port milliseconds", &[]),
        (
            "import",
            "host:port",
            &[
                "from",
                "from-user",
                "from-pass",
                "from-askpass",
                "copy",
                "replace",
            ],
        ),
        ("backup", "host:port backup_directory", &[]),
        ("help", "", &[]),
    ];
    const PADDING: usize = 15;
    for (name, args, options) in COMMANDS {
        println!("  {:<width$}{}", name, args, width = PADDING - 2);
        for option in *options {
            println!("  {:<width$}--cluster-{}", "", option, width = PADDING - 2);
        }
    }
    println!(
        "\nFor check, fix, reshard, del-node, set-timeout, info, rebalance, call, import, \
         backup you can specify the host and port of any working node in the cluster."
    );
    println!(
        "\nCluster Manager Options:\n  --cluster-yes  Automatic yes to cluster commands prompts"
    );
    Ok(())
}
