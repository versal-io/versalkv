#
#

start_server {tags {"aof"}} {

    test {AOF: enable appendonly and write data} {
        r config set appendonly yes
        waitForBgrewriteaof r
        r set foo bar
        r set baz 42
        assert_equal {bar} [r get foo]
        assert_equal {42} [r get baz]
    }

    test {AOF: rewrite produces valid AOF and data survives restart} {
        r config set appendonly yes
        waitForBgrewriteaof r
        r set a 1
        r set b 2
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        assert_equal {1} [r get a]
        assert_equal {2} [r get b]
    }

    test {AOF: rewrite with rdb preamble} {
        r config set aof-use-rdb-preamble yes
        r config set appendonly yes
        waitForBgrewriteaof r
        r set k1 v1
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        assert_equal {v1} [r get k1]
    }

    test {AOF: rewrite without rdb preamble} {
        r config set aof-use-rdb-preamble no
        r config set appendonly yes
        waitForBgrewriteaof r
        r set k2 v2
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        assert_equal {v2} [r get k2]
    }

    test {AOF: disable appendonly kills rewrite child} {
        r config set appendonly yes
        waitForBgrewriteaof r
        # Start a slow BGREWRITEAOF
        r config set rdb-key-save-delay 10000000
        r debug populate 1000 rewrite-key 100
        assert_equal {Background append only file rewriting started} [r bgrewriteaof]
        wait_for_condition 50 100 {
            [string match {*aof_rewrite_in_progress:1*} [r info persistence]]
        } else {
            fail "AOF rewrite child did not start in time"
        }
        # Disable AOF should kill the child
        set loglines [count_log_lines 0]
        r config set appendonly no
        wait_for_log_messages 0 {"*Killing*AOF*child*"} $loglines 100 100
        r config set rdb-key-save-delay 0
    }
}
