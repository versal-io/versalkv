use super::rng::FastRng;

pub(super) const DEFAULT_INT_MAX: i64 = 50;
pub(super) const OPTIONAL_SKIP_PROBABILITY: usize = 2;

pub(super) struct KeyGenerator {
    rng: FastRng,
    max_keys: usize,
    slot_tag: Option<String>,
}

impl KeyGenerator {
    pub fn new(parent_rng: &mut FastRng, max_keys: usize) -> Self {
        let seed = parent_rng.next_u64().wrapping_add(0x9E3779B97F4A7C15);
        KeyGenerator {
            rng: FastRng::new(seed),
            max_keys,
            slot_tag: None,
        }
    }

    pub fn gen_key(&mut self, prefix: &str) -> Vec<u8> {
        let knum = self.rng.next_usize(self.max_keys);
        if let Some(tag) = &self.slot_tag {
            format!("{}{}:{}", tag, prefix, knum).into_bytes()
        } else {
            format!("{}:{}", prefix, knum).into_bytes()
        }
    }

    pub fn gen_slot_tag(&mut self) {
        let slot_num = self.rng.next_usize(21);
        self.slot_tag = Some(format!("{{slot-{}}}", slot_num));
    }

    pub fn clear_slot_tag(&mut self) {
        self.slot_tag = None;
    }
}

pub(super) enum ArgSpec {
    Key(&'static str),
    Int(i64, i64),
    Double(f64, f64),
    Fixed(&'static [u8]),
    Token(&'static [u8]),
    Optional(Box<ArgSpec>),
    Multiple(Box<ArgSpec>, usize, usize),
    OneOf(Vec<ArgSpec>),
    Seconds,
    Field,
    Member,
    Channel,
    Score,
    RandomStr(usize, usize),
    RandomValue(usize, usize),
    ScoreMemberPair,
    KeyValuePairs(usize, usize),
}

impl ArgSpec {
    pub fn generate(&self, rng: &mut FastRng, key_gen: &mut KeyGenerator) -> Vec<Vec<u8>> {
        match self {
            ArgSpec::Key(prefix) => vec![key_gen.gen_key(prefix)],
            ArgSpec::Int(min, max) => vec![rng.next_i64(*min, *max).to_string().into_bytes()],
            ArgSpec::Double(min, max) => {
                let range = max - min;
                let val = min + (rng.next_u64() as f64 / u64::MAX as f64) * range;
                vec![format!("{}", val).into_bytes()]
            }
            ArgSpec::Fixed(b) => vec![b.to_vec()],
            ArgSpec::Token(t) => vec![t.to_vec()],
            ArgSpec::Optional(inner) => {
                if rng.next_usize(OPTIONAL_SKIP_PROBABILITY) == 0 {
                    vec![]
                } else {
                    inner.generate(rng, key_gen)
                }
            }
            ArgSpec::Multiple(inner, min_c, max_c) => {
                let count = min_c + rng.next_usize(max_c - min_c + 1);
                let mut result = Vec::new();
                for _ in 0..count {
                    result.extend(inner.generate(rng, key_gen));
                }
                result
            }
            ArgSpec::OneOf(options) => {
                let idx = rng.next_usize(options.len());
                options[idx].generate(rng, key_gen)
            }
            ArgSpec::Seconds => {
                let secs = 1 + rng.next_usize(3600);
                vec![secs.to_string().into_bytes()]
            }
            ArgSpec::Field => {
                let fnum = rng.next_usize(20);
                vec![format!("field:{}", fnum).into_bytes()]
            }
            ArgSpec::Member => {
                let mnum = rng.next_usize(50);
                vec![format!("member:{}", mnum).into_bytes()]
            }
            ArgSpec::Channel => {
                let cnum = rng.next_usize(5);
                vec![format!("channel:{}", cnum).into_bytes()]
            }
            ArgSpec::Score => vec![rng.next_i64(0, DEFAULT_INT_MAX).to_string().into_bytes()],
            ArgSpec::RandomStr(min_len, max_len) => {
                let len = min_len + rng.next_usize(max_len - min_len + 1);
                let s: Vec<u8> = (0..len)
                    .map(|_| b'a' + (rng.next_u64() % 26) as u8)
                    .collect();
                vec![s]
            }
            ArgSpec::RandomValue(min_len, max_len) => {
                let len = min_len + rng.next_usize(max_len - min_len + 1);
                let s: Vec<u8> = (0..len)
                    .map(|_| b'x' + (rng.next_u64() % 26) as u8)
                    .collect();
                vec![s]
            }
            ArgSpec::ScoreMemberPair => {
                let score = rng.next_i64(0, DEFAULT_INT_MAX);
                let mnum = rng.next_usize(50);
                vec![
                    score.to_string().into_bytes(),
                    format!("member:{}", mnum).into_bytes(),
                ]
            }
            ArgSpec::KeyValuePairs(min_pairs, max_pairs) => {
                let count = min_pairs + rng.next_usize(max_pairs - min_pairs + 1);
                let mut result = Vec::new();
                for _ in 0..count {
                    result.push(key_gen.gen_key("string"));
                    let vlen = 1 + rng.next_usize(50);
                    let val: Vec<u8> = (0..vlen)
                        .map(|_| b'x' + (rng.next_u64() % 26) as u8)
                        .collect();
                    result.push(val);
                }
                result
            }
        }
    }
}

pub(super) struct CommandTemplate {
    pub name: &'static str,
    pub args: Vec<ArgSpec>,
}

pub(super) fn generate_command_from_template(
    tmpl: &CommandTemplate,
    rng: &mut FastRng,
    key_gen: &mut KeyGenerator,
) -> Vec<Vec<u8>> {
    let mut args: Vec<Vec<u8>> = vec![tmpl.name.as_bytes().to_vec()];
    for arg_spec in &tmpl.args {
        args.extend(arg_spec.generate(rng, key_gen));
    }
    args
}

pub(super) fn get_command_templates() -> Vec<CommandTemplate> {
    vec![
        CommandTemplate {
            name: "PING",
            args: vec![],
        },
        CommandTemplate {
            name: "ECHO",
            args: vec![ArgSpec::RandomStr(1, 20)],
        },
        CommandTemplate {
            name: "SET",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::RandomValue(1, 200),
                ArgSpec::Optional(Box::new(ArgSpec::OneOf(vec![
                    ArgSpec::Token(b"EX"),
                    ArgSpec::Token(b"PX"),
                ]))),
                ArgSpec::Optional(Box::new(ArgSpec::Seconds)),
            ],
        },
        CommandTemplate {
            name: "GET",
            args: vec![ArgSpec::Key("string")],
        },
        CommandTemplate {
            name: "GETSET",
            args: vec![ArgSpec::Key("string"), ArgSpec::RandomValue(1, 100)],
        },
        CommandTemplate {
            name: "SETEX",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::Int(1, 3600),
                ArgSpec::RandomValue(1, 100),
            ],
        },
        CommandTemplate {
            name: "SETNX",
            args: vec![ArgSpec::Key("string"), ArgSpec::RandomValue(1, 100)],
        },
        CommandTemplate {
            name: "INCR",
            args: vec![ArgSpec::Key("string")],
        },
        CommandTemplate {
            name: "DECR",
            args: vec![ArgSpec::Key("string")],
        },
        CommandTemplate {
            name: "INCRBY",
            args: vec![ArgSpec::Key("string"), ArgSpec::Int(1, 100)],
        },
        CommandTemplate {
            name: "DECRBY",
            args: vec![ArgSpec::Key("string"), ArgSpec::Int(1, 100)],
        },
        CommandTemplate {
            name: "INCRBYFLOAT",
            args: vec![ArgSpec::Key("string"), ArgSpec::Double(0.0, 100.0)],
        },
        CommandTemplate {
            name: "APPEND",
            args: vec![ArgSpec::Key("string"), ArgSpec::RandomStr(1, 50)],
        },
        CommandTemplate {
            name: "STRLEN",
            args: vec![ArgSpec::Key("string")],
        },
        CommandTemplate {
            name: "GETRANGE",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::Int(0, 10),
                ArgSpec::Int(10, 100),
            ],
        },
        CommandTemplate {
            name: "SETRANGE",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::Int(0, 100),
                ArgSpec::RandomStr(1, 20),
            ],
        },
        CommandTemplate {
            name: "MGET",
            args: vec![ArgSpec::Multiple(Box::new(ArgSpec::Key("string")), 1, 5)],
        },
        CommandTemplate {
            name: "MSET",
            args: vec![ArgSpec::KeyValuePairs(1, 3)],
        },
        CommandTemplate {
            name: "LPUSH",
            args: vec![
                ArgSpec::Key("list"),
                ArgSpec::Multiple(Box::new(ArgSpec::RandomValue(1, 50)), 1, 3),
            ],
        },
        CommandTemplate {
            name: "RPUSH",
            args: vec![
                ArgSpec::Key("list"),
                ArgSpec::Multiple(Box::new(ArgSpec::RandomValue(1, 50)), 1, 3),
            ],
        },
        CommandTemplate {
            name: "LPOP",
            args: vec![ArgSpec::Key("list")],
        },
        CommandTemplate {
            name: "RPOP",
            args: vec![ArgSpec::Key("list")],
        },
        CommandTemplate {
            name: "LLEN",
            args: vec![ArgSpec::Key("list")],
        },
        CommandTemplate {
            name: "LRANGE",
            args: vec![
                ArgSpec::Key("list"),
                ArgSpec::Int(0, 0),
                ArgSpec::Int(10, 100),
            ],
        },
        CommandTemplate {
            name: "LINDEX",
            args: vec![ArgSpec::Key("list"), ArgSpec::Int(0, 50)],
        },
        CommandTemplate {
            name: "LSET",
            args: vec![
                ArgSpec::Key("list"),
                ArgSpec::Int(0, 50),
                ArgSpec::RandomValue(1, 50),
            ],
        },
        CommandTemplate {
            name: "LPUSHX",
            args: vec![ArgSpec::Key("list"), ArgSpec::RandomValue(1, 50)],
        },
        CommandTemplate {
            name: "RPUSHX",
            args: vec![ArgSpec::Key("list"), ArgSpec::RandomValue(1, 50)],
        },
        CommandTemplate {
            name: "LREM",
            args: vec![
                ArgSpec::Key("list"),
                ArgSpec::Int(-5, 5),
                ArgSpec::RandomStr(1, 20),
            ],
        },
        CommandTemplate {
            name: "SADD",
            args: vec![
                ArgSpec::Key("set"),
                ArgSpec::Multiple(Box::new(ArgSpec::Member), 1, 3),
            ],
        },
        CommandTemplate {
            name: "SREM",
            args: vec![
                ArgSpec::Key("set"),
                ArgSpec::Multiple(Box::new(ArgSpec::Member), 1, 3),
            ],
        },
        CommandTemplate {
            name: "SCARD",
            args: vec![ArgSpec::Key("set")],
        },
        CommandTemplate {
            name: "SISMEMBER",
            args: vec![ArgSpec::Key("set"), ArgSpec::Member],
        },
        CommandTemplate {
            name: "SMEMBERS",
            args: vec![ArgSpec::Key("set")],
        },
        CommandTemplate {
            name: "SRANDMEMBER",
            args: vec![ArgSpec::Key("set")],
        },
        CommandTemplate {
            name: "SPOP",
            args: vec![ArgSpec::Key("set")],
        },
        CommandTemplate {
            name: "HSET",
            args: vec![
                ArgSpec::Key("hash"),
                ArgSpec::Field,
                ArgSpec::RandomValue(1, 100),
            ],
        },
        CommandTemplate {
            name: "HGET",
            args: vec![ArgSpec::Key("hash"), ArgSpec::Field],
        },
        CommandTemplate {
            name: "HDEL",
            args: vec![ArgSpec::Key("hash"), ArgSpec::Field],
        },
        CommandTemplate {
            name: "HLEN",
            args: vec![ArgSpec::Key("hash")],
        },
        CommandTemplate {
            name: "HKEYS",
            args: vec![ArgSpec::Key("hash")],
        },
        CommandTemplate {
            name: "HVALS",
            args: vec![ArgSpec::Key("hash")],
        },
        CommandTemplate {
            name: "HGETALL",
            args: vec![ArgSpec::Key("hash")],
        },
        CommandTemplate {
            name: "HEXISTS",
            args: vec![ArgSpec::Key("hash"), ArgSpec::Field],
        },
        CommandTemplate {
            name: "HINCRBY",
            args: vec![ArgSpec::Key("hash"), ArgSpec::Field, ArgSpec::Int(1, 100)],
        },
        CommandTemplate {
            name: "HSETNX",
            args: vec![
                ArgSpec::Key("hash"),
                ArgSpec::Field,
                ArgSpec::RandomValue(1, 50),
            ],
        },
        CommandTemplate {
            name: "HSTRLEN",
            args: vec![ArgSpec::Key("hash"), ArgSpec::Field],
        },
        CommandTemplate {
            name: "HMGET",
            args: vec![
                ArgSpec::Key("hash"),
                ArgSpec::Multiple(Box::new(ArgSpec::Field), 1, 3),
            ],
        },
        CommandTemplate {
            name: "ZADD",
            args: vec![
                ArgSpec::Key("zset"),
                ArgSpec::Multiple(Box::new(ArgSpec::ScoreMemberPair), 1, 3),
            ],
        },
        CommandTemplate {
            name: "ZREM",
            args: vec![
                ArgSpec::Key("zset"),
                ArgSpec::Multiple(Box::new(ArgSpec::Member), 1, 3),
            ],
        },
        CommandTemplate {
            name: "ZCARD",
            args: vec![ArgSpec::Key("zset")],
        },
        CommandTemplate {
            name: "ZSCORE",
            args: vec![ArgSpec::Key("zset"), ArgSpec::Member],
        },
        CommandTemplate {
            name: "ZRANGE",
            args: vec![
                ArgSpec::Key("zset"),
                ArgSpec::Int(0, 0),
                ArgSpec::Int(10, 50),
            ],
        },
        CommandTemplate {
            name: "ZREVRANGE",
            args: vec![
                ArgSpec::Key("zset"),
                ArgSpec::Int(0, 0),
                ArgSpec::Int(10, 50),
            ],
        },
        CommandTemplate {
            name: "ZRANK",
            args: vec![ArgSpec::Key("zset"), ArgSpec::Member],
        },
        CommandTemplate {
            name: "ZREVRANK",
            args: vec![ArgSpec::Key("zset"), ArgSpec::Member],
        },
        CommandTemplate {
            name: "ZINCRBY",
            args: vec![ArgSpec::Key("zset"), ArgSpec::Int(1, 100), ArgSpec::Member],
        },
        CommandTemplate {
            name: "ZRANGEBYSCORE",
            args: vec![ArgSpec::Key("zset"), ArgSpec::Score, ArgSpec::Score],
        },
        CommandTemplate {
            name: "DEL",
            args: vec![ArgSpec::Multiple(Box::new(ArgSpec::Key("key")), 1, 3)],
        },
        CommandTemplate {
            name: "EXISTS",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "EXPIRE",
            args: vec![ArgSpec::Key("key"), ArgSpec::Seconds],
        },
        CommandTemplate {
            name: "PEXPIRE",
            args: vec![ArgSpec::Key("key"), ArgSpec::Int(1000, 60000)],
        },
        CommandTemplate {
            name: "PERSIST",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "TTL",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "PTTL",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "TYPE",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "RANDOMKEY",
            args: vec![],
        },
        CommandTemplate {
            name: "DBSIZE",
            args: vec![],
        },
        CommandTemplate {
            name: "KEYS",
            args: vec![ArgSpec::Fixed(b"*")],
        },
        CommandTemplate {
            name: "PUBLISH",
            args: vec![ArgSpec::Channel, ArgSpec::RandomStr(1, 50)],
        },
        CommandTemplate {
            name: "SELECT",
            args: vec![ArgSpec::Int(0, 15)],
        },
        CommandTemplate {
            name: "TIME",
            args: vec![],
        },
        CommandTemplate {
            name: "DISCARD",
            args: vec![],
        },
        CommandTemplate {
            name: "UNWATCH",
            args: vec![],
        },
        CommandTemplate {
            name: "TOUCH",
            args: vec![ArgSpec::Key("key")],
        },
        CommandTemplate {
            name: "GETBIT",
            args: vec![ArgSpec::Key("string"), ArgSpec::Int(0, 1000)],
        },
        CommandTemplate {
            name: "SETBIT",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::Int(0, 1000),
                ArgSpec::Int(0, 1),
            ],
        },
        CommandTemplate {
            name: "BITCOUNT",
            args: vec![ArgSpec::Key("string")],
        },
        CommandTemplate {
            name: "BITPOS",
            args: vec![ArgSpec::Key("string"), ArgSpec::Int(0, 1)],
        },
        CommandTemplate {
            name: "SUBSTR",
            args: vec![
                ArgSpec::Key("string"),
                ArgSpec::Int(0, 10),
                ArgSpec::Int(20, 100),
            ],
        },
    ]
}
