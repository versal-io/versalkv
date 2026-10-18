#
#

start_server {tags {"rdb"}} {
    test {SAVE writes an RDB file with the VALKEY magic} {
        r flushall
        r set foo bar
        r set counter 12345
        assert_equal {OK} [r save]
        set dir [lindex [r config get dir] 1]
        set dbfile [lindex [r config get dbfilename] 1]
        set path [file join $dir $dbfile]
        assert {[file exists $path]}
        set fd [open $path rb]
        set magic [read $fd 9]
        close $fd
        set magic
    } {VALKEY080}

    test {BGSAVE replies with the expected status} {
        r set k v
        assert_equal {Background saving started} [r bgsave]
        wait_for_condition 50 100 {
            [string match {*rdb_bgsave_in_progress:0*} [r info persistence]]
        } else {
            fail "BGSAVE child did not complete in time"
        }
    }

    test {BGSAVE actually forks a child that writes the RDB file} {
        r flushall
        r set persisted yes
        set dir [lindex [r config get dir] 1]
        set dbfile [lindex [r config get dbfilename] 1]
        set path [file join $dir $dbfile]
        catch {file delete $path}
        assert_equal {Background saving started} [r bgsave]
        wait_for_condition 50 100 {
            [file exists $path] &&
            [string match {*rdb_bgsave_in_progress:0*} [r info persistence]] &&
            [string match {*rdb_last_bgsave_status:ok*} [r info persistence]]
        } else {
            fail "BGSAVE child did not complete in time"
        }
        set fd [open $path rb]
        set magic [read $fd 9]
        close $fd
        set magic
    } {VALKEY080}

    test {INFO persistence exposes rdb fields} {
        set info [r info persistence]
        assert {[string match {*rdb_last_save_time:*} $info]}
        assert {[string match {*rdb_last_bgsave_status:ok*} $info]}
    }

    test {BGSAVE SCHEDULE also saves in the simplified model} {
        assert_equal {Background saving started} [r bgsave schedule]
    }

    test {LASTSAVE returns a timestamp updated by SAVE} {
        set before [r lastsave]
        after 1100
        r set another value
        r save
        set after [r lastsave]
        assert {$after >= $before}
        assert {$after > 0}
    }

    test {BGSAVE with unknown option is a syntax error} {
        assert_error "*syntax*" {r bgsave bogus}
    }

    test {BGSAVE CANCEL with nothing in progress reports an error} {
        assert_error "*not in progress*" {r bgsave cancel}
    }

    test {The RDB file trailer carries a CRC64 checksum} {
        r flushall
        r set x y
        r save
        set dir [lindex [r config get dir] 1]
        set dbfile [lindex [r config get dbfilename] 1]
        set path [file join $dir $dbfile]
        set sz [file size $path]
        assert {$sz >= 18}
    }

    test {SAVE persists all five data types to the RDB file} {
        r flushall
        r set mystr hello
        r rpush mylist a b c
        r sadd myset x y z
        r hset myhash f1 v1 f2 v2
        r zadd myzset 1.5 m1 2 m2
        assert_equal {OK} [r save]
        set dir [lindex [r config get dir] 1]
        set dbfile [lindex [r config get dbfilename] 1]
        set path [file join $dir $dbfile]
        set fd [open $path rb]
        set data [read $fd]
        close $fd
        foreach token {mystr mylist myset myhash myzset hello a b c x y z f1 v1 f2 v2 m1 m2} {
            assert {[string first $token $data] >= 0}
        }
    }
}
