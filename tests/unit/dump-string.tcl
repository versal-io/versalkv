#
#

start_server {tags {"dump"}} {
    test {DUMP / RESTORE are able to serialize / unserialize a simple key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r get foo]
    } {0 OK -1 bar}

    test {RESTORE can set an arbitrary expire to the materialized key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 5000 $encoded
        set ttl [r pttl foo]
        assert_range $ttl 3000 5000
        r get foo
    } {bar}

    test {RESTORE can set an expire that overflows a 32 bit integer} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 2569591501 $encoded
        set ttl [r pttl foo]
        assert_range $ttl [expr 2569591501-3000] 2569591501
        r get foo
    } {bar}

    test {RESTORE can set an absolute expire} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        set now [clock milliseconds]
        r restore foo [expr $now+3000] $encoded absttl
        set ttl [r pttl foo]
        assert_range $ttl 2000 3100
        r get foo
    } {bar}

    test {RESTORE returns an error of the key already exists} {
        r set foo bar
        set e {}
        catch {r restore foo 0 "..."} e
        set e
    } {*BUSYKEY*}

    test {RESTORE can overwrite an existing key with REPLACE} {
        r set foo bar1
        set encoded1 [r dump foo]
        r set foo bar2
        set encoded2 [r dump foo]
        r del foo
        r restore foo 0 $encoded1
        r restore foo 0 $encoded2 replace
        r get foo
    } {bar2}

    test {RESTORE can detect a syntax error for unrecognized options} {
        catch {r restore foo 0 "..." invalid-option} e
        set e
    } {*syntax*}

    test {RESTORE key with future RDB version, strict version check} {
        #              str len  "bar"      RDB 222 CRC64 checksum
        set bar_dump "\x00\x03\x62\x61\x72\xde\x00\x0fYUza\xd3\xec\xe0"
        assert_error {ERR DUMP payload version or checksum are wrong} {r restore foo 0 $bar_dump replace}
    }

    test {DUMP / RESTORE round trip an integer-encoded value} {
        r set counter 12345
        assert_equal {int} [r object encoding counter]
        set encoded [r dump counter]
        r del counter
        r restore counter 0 $encoded
        list [r get counter] [r object encoding counter]
    } {12345 int}

    test {DUMP of non existing key returns nil} {
        r dump nonexisting_key
    } {}
}
