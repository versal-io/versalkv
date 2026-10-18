use super::commands::{generate_command_from_template, CommandTemplate, KeyGenerator};
use super::rng::FastRng;

pub(super) const MALFORMED_PROBABILITY: usize = 20;
pub(super) const LUA_PROBABILITY: usize = 100;
pub(super) const MAX_COMMANDS_PER_LUA: usize = 5;

fn create_corrupted_arg(rng: &mut FastRng) -> Vec<u8> {
    match rng.next_usize(5) {
        0 => {
            let len = 10 + rng.next_usize(7);
            (0..len).map(|_| rng.next_u64() as u8).collect()
        }
        1 => {
            let len = 1024 + rng.next_usize(129);
            vec![b'A' + (rng.next_u64() % 26) as u8; len]
        }
        2 => vec![],
        3 => b"\n\r\t\"\\'$%^&*(){}[]<>".to_vec(),
        _ => {
            let invalid = ["123abc", "-+123", "12.34.56", "NaN", "Infinity"];
            invalid[rng.next_usize(invalid.len())].as_bytes().to_vec()
        }
    }
}

pub(super) fn generate_malformed_command(
    templates: &[CommandTemplate],
    rng: &mut FastRng,
    key_gen: &mut KeyGenerator,
) -> Vec<Vec<u8>> {
    let idx = rng.next_usize(templates.len());
    let mut cmd = generate_command_from_template(&templates[idx], rng, key_gen);
    if cmd.len() <= 1 {
        return cmd;
    }
    let corruption_count = 1 + rng.next_usize(3);
    for _ in 0..corruption_count {
        match rng.next_usize(4) {
            0 => {
                if cmd.len() > 2 {
                    for _ in 0..2 {
                        let i1 = 1 + rng.next_usize(cmd.len() - 1);
                        let i2 = 1 + rng.next_usize(cmd.len() - 1);
                        if i1 != i2 {
                            cmd.swap(i1, i2);
                        }
                    }
                }
            }
            1 => {
                let mut idx = rng.next_usize(cmd.len());
                if idx == 0 && rng.next_usize(10) < 7 {
                    idx = rng.next_usize(cmd.len() - 1) + 1;
                }
                cmd[idx] = create_corrupted_arg(rng);
            }
            2 => {
                if cmd.len() > 1 {
                    let idx = 1 + rng.next_usize(cmd.len() - 1);
                    cmd.remove(idx);
                }
            }
            _ => {
                let new_arg = match rng.next_usize(3) {
                    0 => b"random_value".to_vec(),
                    1 => rng.next_u64().to_string().into_bytes(),
                    _ => b"\n\r\t\"\\'$%^&*(){}[]<>".to_vec(),
                };
                let pos = rng.next_usize(cmd.len() + 1);
                cmd.insert(pos, new_arg);
            }
        }
    }
    cmd
}

pub(super) fn generate_lua_script(
    templates: &[CommandTemplate],
    rng: &mut FastRng,
    key_gen: &mut KeyGenerator,
) -> Vec<Vec<u8>> {
    let num_cmds = 1 + rng.next_usize(MAX_COMMANDS_PER_LUA);
    let mut script = String::from("local result = {} ");
    for i in 0..num_cmds {
        let sub = generate_command_from_template(
            &templates[rng.next_usize(templates.len())],
            rng,
            key_gen,
        );
        script.push_str(&format!("result[{}] = redis.call(", i + 1));
        for (j, arg) in sub.iter().enumerate() {
            if j > 0 {
                script.push(',');
            }
            let s = String::from_utf8_lossy(arg);
            script.push('"');
            for c in s.chars() {
                if c == '"' || c == '\\' {
                    script.push('\\');
                }
                script.push(c);
            }
            script.push('"');
        }
        script.push_str(") ");
    }
    script.push_str("return result");
    vec![b"EVAL".to_vec(), script.into_bytes(), b"0".to_vec()]
}

pub(super) fn generate_command(
    templates: &[CommandTemplate],
    rng: &mut FastRng,
    key_gen: &mut KeyGenerator,
    in_lua: bool,
) -> Vec<Vec<u8>> {
    if !in_lua && rng.next_usize(MALFORMED_PROBABILITY) == 0 {
        return generate_malformed_command(templates, rng, key_gen);
    }
    if !in_lua && rng.next_usize(LUA_PROBABILITY) == 0 {
        return generate_lua_script(templates, rng, key_gen);
    }
    if rng.next_usize(5) == 0 {
        key_gen.gen_slot_tag();
    }
    let idx = rng.next_usize(templates.len());
    let cmd = generate_command_from_template(&templates[idx], rng, key_gen);
    key_gen.clear_slot_tag();
    cmd
}

pub(super) fn cmd_to_string(cmd: &[Vec<u8>]) -> String {
    let parts: Vec<String> = cmd
        .iter()
        .map(|a| {
            if a.len() > 50 {
                format!("{}...", String::from_utf8_lossy(&a[..50]))
            } else {
                String::from_utf8_lossy(a).to_string()
            }
        })
        .collect();
    parts.join(" ")
}
