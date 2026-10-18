use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("LCS", runtime)]
pub fn lcs(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(lcs_reply(runtime, args))
}

fn lcs_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("lcs");
    }
    let key1 = &args.args[0];
    let key2 = &args.args[1];
    let mut len_mode = false;
    let mut idx_mode = false;
    let mut withmatchlen = false;
    let mut minmatchlen: usize = 0;
    let mut i = 2;
    while i < args.args.len() {
        let opt = String::from_utf8_lossy(&args.args[i]).to_ascii_lowercase();
        match opt.as_str() {
            "len" => {
                len_mode = true;
                i += 1;
            }
            "idx" => {
                idx_mode = true;
                i += 1;
            }
            "withmatchlen" => {
                withmatchlen = true;
                i += 1;
            }
            "minmatchlen" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                minmatchlen = match std::str::from_utf8(&args.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) if n > 0 => n,
                    _ => return Value::error("ERR value is not an integer or out of range"),
                };
                i += 2;
            }
            _ => return CommandError::syntax_error(),
        }
    }

    let max_bulk_len = runtime.proto_max_bulk_len() as i64;
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let s1 = match db.get_string(key1) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => Vec::new(),
        Ok(Some(b)) => b.to_vec(),
    };
    let s2 = match db.get_string(key2) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => Vec::new(),
        Ok(Some(b)) => b.to_vec(),
    };

    if !lcs_transient_memory_allowed(s1.len(), s2.len(), max_bulk_len) {
        return Value::error(
            "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
        );
    }

    if key1 == key2 && !idx_mode {
        return lcs_same_key_bytes(&s1, len_mode);
    }

    if idx_mode {
        let matches = compute_lcs_matches(&s1, &s2, minmatchlen);
        let lcs_len: usize = matches.iter().map(|m| m.len).sum();
        build_idx_response(matches, lcs_len, withmatchlen)
    } else if len_mode {
        let lcs_len = compute_lcs_len(&s1, &s2);
        Value::integer(lcs_len as i64)
    } else {
        let lcs_str = compute_lcs_string(&s1, &s2);
        Value::bulk(lcs_str)
    }
}

fn lcs_same_key_bytes(bytes: &[u8], len_mode: bool) -> Value {
    if len_mode {
        Value::integer(bytes.len() as i64)
    } else {
        Value::bulk(bytes.to_vec())
    }
}

fn lcs_transient_memory_allowed(len1: usize, len2: usize, proto_max_bulk_len: i64) -> bool {
    let Some(rows) = len1.checked_add(1) else {
        return false;
    };
    let Some(cols) = len2.checked_add(1) else {
        return false;
    };
    let Some(cells) = rows.checked_mul(cols) else {
        return false;
    };
    let Some(bytes) = cells.checked_mul(std::mem::size_of::<u32>()) else {
        return false;
    };
    proto_max_bulk_len <= 0 || bytes <= proto_max_bulk_len as usize
}

fn compute_lcs_len(s1: &[u8], s2: &[u8]) -> usize {
    let m = s1.len();
    let n = s2.len();
    if m == 0 || n == 0 {
        return 0;
    }
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    dp[m][n]
}

fn compute_lcs_string(s1: &[u8], s2: &[u8]) -> Vec<u8> {
    let m = s1.len();
    let n = s2.len();
    if m == 0 || n == 0 {
        return Vec::new();
    }
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    let mut result = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            result.push(s1[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    result.reverse();
    result
}

#[derive(Debug, Clone)]
struct LcsMatch {
    s1_start: usize,
    s1_end: usize,
    s2_start: usize,
    s2_end: usize,
    len: usize,
}

fn compute_lcs_matches(s1: &[u8], s2: &[u8], minmatchlen: usize) -> Vec<LcsMatch> {
    let m = s1.len();
    let n = s2.len();
    if m == 0 || n == 0 {
        return Vec::new();
    }
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    let mut matches: Vec<LcsMatch> = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            let mut run_len = 1;
            let mut ii = i - 1;
            let mut jj = j - 1;
            while ii > 0 && jj > 0 && s1[ii - 1] == s2[jj - 1] {
                run_len += 1;
                ii -= 1;
                jj -= 1;
            }
            if run_len >= minmatchlen {
                matches.push(LcsMatch {
                    s1_start: ii,
                    s1_end: i - 1,
                    s2_start: jj,
                    s2_end: j - 1,
                    len: run_len,
                });
            }
            i = ii;
            j = jj;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    matches
}

fn build_idx_response(matches: Vec<LcsMatch>, lcs_len: usize, withmatchlen: bool) -> Value {
    let mut result: Vec<Value> = Vec::new();
    result.push(Value::bulk(b"matches".to_vec()));
    let mut matches_arr: Vec<Value> = Vec::new();
    for m in &matches {
        let mut match_arr: Vec<Value> = Vec::new();
        let mut s1_pair = vec![Value::integer(m.s1_start as i64)];
        s1_pair.push(Value::integer(m.s1_end as i64));
        match_arr.push(Value::Array(s1_pair));
        let mut s2_pair = vec![Value::integer(m.s2_start as i64)];
        s2_pair.push(Value::integer(m.s2_end as i64));
        match_arr.push(Value::Array(s2_pair));
        if withmatchlen {
            match_arr.push(Value::integer(m.len as i64));
        }
        matches_arr.push(Value::Array(match_arr));
    }
    result.push(Value::Array(matches_arr));
    result.push(Value::bulk(b"len".to_vec()));
    result.push(Value::integer(lcs_len as i64));
    Value::Array(result)
}

#[cfg(test)]
mod tests {
    use super::lcs_transient_memory_allowed;

    #[test]
    fn lcs_transient_memory_guard_matches_proto_bulk_limit() {
        assert!(lcs_transient_memory_allowed(8 * 1024, 8 * 1024, 512 * 1024 * 1024));
        assert!(!lcs_transient_memory_allowed(423_325, 423_325, 512 * 1024 * 1024));
    }

    #[test]
    fn lcs_transient_memory_guard_allows_disabled_limit() {
        assert!(lcs_transient_memory_allowed(423_325, 423_325, 0));
    }
}
