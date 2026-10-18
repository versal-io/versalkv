# Versal-owned MONITOR compatibility tests.
#
# These cases freeze behavior that is implicit in C Valkey's monitorCommand,
# call, replicationFeedMonitors, and clearClientConnectionState paths.

start_server {tags {"monitor network"}} {
    test {MONITOR suppresses administrative commands} {
        set rd [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        r config get port
        r set monitor-admin-visible value
        set event [$rd read]

        assert_match {*"set" "monitor-admin-visible" "value"*} $event
        assert_no_match {*"config"*} $event
        $rd close
    }

    test {MONITOR repeat is ignored and RESET clears monitor state} {
        set rd [valkey_deferring_client]
        $rd client id
        set monitor_id [$rd read]
        $rd monitor
        assert_equal OK [$rd read]
        assert_match {*flags=O *} [r client list id $monitor_id]

        # C ignores the second MONITOR without producing another reply. RESET
        # is therefore the next response on this pipelined connection.
        $rd monitor
        $rd reset
        assert_equal RESET [$rd read]
        assert_no_match {*flags=O *} [r client list id $monitor_id]

        # A later command must not leave monitor data queued on this client.
        r set monitor-reset-visible value
        $rd ping
        assert_equal PONG [$rd read]
        $rd close
    }

    test {MONITOR delivers earlier pipeline events before RESET takes effect} {
        set rd [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        # Force both commands into one server read turn. C feeds the PING
        # event while this client is still a monitor, before RESET removes it.
        $rd write "*1\r\n\$4\r\nPING\r\n*1\r\n\$5\r\nRESET\r\n"
        $rd flush
        set ping_reply [$rd read]
        set ping_event [$rd read]
        set reset_reply [$rd read]
        $rd close

        assert_equal PONG $ping_reply
        assert_match {*"PING"*} $ping_event
        assert_equal RESET $reset_reply
    }

    test {MONITOR clients cannot interact with the keyspace} {
        set rd [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        $rd get monitor-keyspace-key
        catch {$rd read} err
        assert_match {*Replica can't interact with the keyspace*} $err

        $rd reset
        assert_equal RESET [$rd read]
        $rd close
    }

    test {MONITOR redacts only HELLO and MIGRATE credentials} {
        set rd [valkey_deferring_client]
        set auth_client [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        $auth_client hello 2 AUTH missing-user missing-password SETNAME visible-name
        catch {$auth_client read} _
        set hello_event [$rd read]
        assert_match {*"hello" "2" "AUTH" "(redacted)" "(redacted)" "SETNAME" "visible-name"*} $hello_event

        catch {
            r migrate 127.0.0.1 1 "" 0 1 AUTH2 visible-user hidden-password KEYS visible-key
        } _
        set migrate_event [$rd read]
        assert_match {*"migrate"*"AUTH2" "(redacted)" "(redacted)" "KEYS" "visible-key"*} $migrate_event

        $auth_client close
        $rd close
    }

    test {MONITOR filters administrative commands inside MULTI EXEC} {
        set rd [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        assert_equal OK [r multi]
        assert_equal QUEUED [r config get port]
        assert_equal QUEUED [r set monitor-multi-visible value]
        r exec

        assert_match {*"multi"*} [$rd read]
        assert_match {*"set" "monitor-multi-visible" "value"*} [$rd read]
        assert_match {*"exec"*} [$rd read]
        $rd close
    }

    test {MONITOR orders EVAL and FCALL before their nested commands} {
        r function flush
        r function load replace {#!lua name=monitorlib
            server.register_function('monitorfn', function()
                return redis.call('set', 'monitor-function-key', 'function-value')
            end)
        }

        set rd [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]

        r eval {return redis.call('set', KEYS[1], ARGV[1])} 1 monitor-eval-key eval-value
        assert_match {*"eval"*"monitor-eval-key" "eval-value"*} [$rd read]
        assert_match {*lua*"set" "monitor-eval-key" "eval-value"*} [$rd read]

        r fcall monitorfn 0
        assert_match {*"fcall" "monitorfn" "0"*} [$rd read]
        assert_match {*lua*"set" "monitor-function-key" "function-value"*} [$rd read]
        $rd close
    }

    test {MONITOR exposes script events while a timed out script is still busy} {
        set rd [valkey_deferring_client]
        set script_client [valkey_deferring_client]
        $rd monitor
        assert_equal OK [$rd read]
        r config set lua-time-limit 10

        $script_client eval {redis.call('ping'); while true do end} 0
        wait_for_condition 50 100 {
            [catch {r ping} busy_error] == 1 && [string match {BUSY*} $busy_error]
        } else {
            fail "script did not enter BUSY state"
        }

        set monitor_fd [$rd channel]
        fconfigure $monitor_fd -blocking 0
        after 50
        set events [read $monitor_fd]
        fconfigure $monitor_fd -blocking 1

        assert_equal OK [r script kill]
        catch {$script_client read} _
        r config set lua-time-limit 5000
        $script_client close
        $rd close

        assert_match {*"eval"*} $events
        assert_match {*lua*"ping"*} $events
    }
}
