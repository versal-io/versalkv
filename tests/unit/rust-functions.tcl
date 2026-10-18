
proc get_rust_function_code {args} {
    return [format "#!%s name=%s\nserver.register_function('%s', function(KEYS, ARGV)\n %s \nend)" \
            [lindex $args 0] [lindex $args 1] [lindex $args 2] [lindex $args 3]]
}

proc get_rust_no_writes_function_code {args} {
    return [format "#!%s name=%s\nserver.register_function{function_name='%s', callback=function(KEYS, ARGV)\n %s \nend, flags={'no-writes'}}" \
            [lindex $args 0] [lindex $args 1] [lindex $args 2] [lindex $args 3]]
}

start_server {tags {"scripting"}} {
    test {RUST-FUNCTION - Basic load and fcall} {
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        r fcall test 0
    } {hello}

    test {RUST-FUNCTION - Load with REPLACE} {
        r function load REPLACE [get_rust_function_code LUA test test {return 'hello1'}]
        r fcall test 0
    } {hello1}

    test {RUST-FUNCTION - FCALL is case insensitive} {
        r fcall TEST 0
    } {hello1}

    test {RUST-FUNCTION - Create existing library raises error} {
        catch {
            r function load [get_rust_function_code LUA test test {return 'hello2'}]
        } e
        set _ $e
    } {*already exists*}

    test {RUST-FUNCTION - Delete library} {
        r function delete test
        catch { r fcall test 0 } e
        set _ $e
    } {*not found*}

    test {RUST-FUNCTION - Delete nonexistent library} {
        catch { r function delete nonexistent } e
        set _ $e
    } {*not found*}

    test {RUST-FUNCTION - Wrong subcommand} {
        catch { r function bad_subcommand } e
        set _ $e
    } {*unknown subcommand*}

    test {RUST-FUNCTION - Function kill when not running} {
        catch { r function kill } e
        set _ $e
    } {*NOTBUSY*}

    test {RUST-FUNCTION - Unknown engine} {
        catch {
            r function load [get_rust_function_code bad_engine test test {return 'hello'}]
        } e
        set _ $e
    } {*Engine*not found*}

    test {RUST-FUNCTION - Function list} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        set result [r function list]
        assert_match {*library_name*test*engine*LUA*} $result
    }

    test {RUST-FUNCTION - Function list withcode} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        set result [r function list withcode]
        assert_match {*test*} $result
        assert_match {*LUA*} $result
        assert_match {*return 'hello'*} $result
    }

    test {RUST-FUNCTION - Function flush} {
        r function flush
        r function list
    } {}

    test {RUST-FUNCTION - Function dump and restore} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        set e [r function dump]
        r function delete test
        assert_match {} [r function list]
        r function restore $e
        set result [r function list]
        assert_match {*library_name*test*engine*LUA*} $result
    }

    test {RUST-FUNCTION - Function dump and restore with FLUSH} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        set e [r function dump]
        r function flush
        assert_match {} [r function list]
        r function restore $e FLUSH
        set result [r function list]
        assert_match {*library_name*test*engine*LUA*} $result
    }

    test {RUST-FUNCTION - FCALL with keys and args} {
        r function load REPLACE [get_rust_function_code LUA test test {return redis.call('set', KEYS[1], ARGV[1])}]
        r fcall test 1 x foo
        r get x
    } {foo}

    test {RUST-FUNCTION - FCALL_RO with read only command} {
        r function load REPLACE [get_rust_no_writes_function_code LUA test test {return redis.call('get', 'x')}]
        r set x 1
        r fcall_ro test 1 x
    } {1}

    test {RUST-FUNCTION - FCALL_RO rejects write} {
        r function load REPLACE [get_rust_no_writes_function_code LUA test test {return redis.call('set', 'x', '1')}]
        catch { r fcall_ro test 1 x } e
        set _ $e
    } {*Write commands are not allowed from read-only scripts*}

    test {RUST-FUNCTION - Ping after fcall error} {
        r ping
    } {PONG}

    test {RUST-FUNCTION - Function stats} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 1}]
        set result [r function stats]
        assert_match {*running_script*} $result
        assert_match {*engines*} $result
    }

    test {RUST-FUNCTION - Function help} {
        set result [r function help]
        assert_match {*FUNCTION*} $result
    }

    test {RUST-FUNCTION - Bad number of keys} {
        catch { r fcall test 10 key1 } e
        set _ $e
    } {*Number of keys can't be greater than number of args*}

    test {RUST-FUNCTION - Negative number of keys} {
        catch { r fcall test -1 key1 } e
        set _ $e
    } {*Number of keys can't be negative*}

    test {RUST-FUNCTION - Bad payload on restore} {
        r function flush
        r function load [get_rust_function_code LUA test test {return 'hello'}]
        catch { r function restore bad_payload } e
        set _ $e
    } {*DUMP payload version or checksum are wrong*}
}