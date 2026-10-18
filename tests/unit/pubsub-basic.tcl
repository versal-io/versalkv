#
#

start_server {tags {"pubsub network"}} {
    test "PUBLISH/SUBSCRIBE basics" {
        set rd1 [valkey_deferring_client]

        # subscribe to two channels
        assert_equal {1 2} [subscribe $rd1 {chan1 chan2}]
        assert_equal 1 [r publish chan1 hello]
        assert_equal 1 [r publish chan2 world]
        assert_equal {message chan1 hello} [$rd1 read]
        assert_equal {message chan2 world} [$rd1 read]

        # unsubscribe from one of the channels
        unsubscribe $rd1 {chan1}
        assert_equal 0 [r publish chan1 hello]
        assert_equal 1 [r publish chan2 world]
        assert_equal {message chan2 world} [$rd1 read]

        # unsubscribe from the remaining channel
        unsubscribe $rd1 {chan2}
        assert_equal 0 [r publish chan1 hello]
        assert_equal 0 [r publish chan2 world]

        $rd1 close
    }

    test "PUBLISH/SUBSCRIBE with two clients" {
        set rd1 [valkey_deferring_client]
        set rd2 [valkey_deferring_client]

        assert_equal {1} [subscribe $rd1 {chan1}]
        assert_equal {1} [subscribe $rd2 {chan1}]
        assert_equal 2 [r publish chan1 hello]
        assert_equal {message chan1 hello} [$rd1 read]
        assert_equal {message chan1 hello} [$rd2 read]

        $rd1 close
        $rd2 close
    }

    test "SUBSCRIBE to one channel more than once" {
        set rd1 [valkey_deferring_client]
        assert_equal {1 1 1} [subscribe $rd1 {chan1 chan1 chan1}]
        assert_equal 1 [r publish chan1 hello]
        assert_equal {message chan1 hello} [$rd1 read]

        $rd1 close
    }

    test "UNSUBSCRIBE from non-subscribed channels" {
        set rd1 [valkey_deferring_client]
        assert_equal {0 0 0} [unsubscribe $rd1 {foo bar quux}]

        $rd1 close
    }

    test "PUBLISH/SUBSCRIBE after UNSUBSCRIBE without arguments" {
        set rd1 [valkey_deferring_client]
        assert_equal {1 2 3} [subscribe $rd1 {chan1 chan2 chan3}]
        unsubscribe $rd1
        # wait for the unsubscribe to take effect
        wait_for_condition 50 100 {
            [r publish chan1 hello] eq 0
        } else {
            fail "unsubscribe did not take effect"
        }
        assert_equal 0 [r publish chan1 hello]
        assert_equal 0 [r publish chan2 hello]
        assert_equal 0 [r publish chan3 hello]

        $rd1 close
    }

    test "Pub/Sub PING on RESP2" {
        set rd1 [valkey_deferring_client]
        subscribe $rd1 somechannel
        # While subscribed to non-zero channels PING works in Pub/Sub mode.
        $rd1 ping
        $rd1 ping "foo"
        assert_equal {pong {}} [$rd1 read]
        assert_equal {pong foo} [$rd1 read]
        unsubscribe $rd1 somechannel
        # Now we are unsubscribed, PING should just return PONG.
        $rd1 ping
        assert_equal {PONG} [$rd1 read]

        $rd1 close
    }
}
