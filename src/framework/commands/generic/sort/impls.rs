use bytes::Bytes;

use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;

pub(super) struct SortOptions {
    pub(super) desc: bool,
    pub(super) alpha: bool,
    pub(super) limit_start: i64,
    pub(super) limit_count: i64,
    pub(super) by_nosort: bool,
    pub(super) by_pattern: Option<Bytes>,
    pub(super) get_patterns: Vec<Bytes>,
    pub(super) store_key: Option<Bytes>,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            desc: false,
            alpha: false,
            limit_start: 0,
            limit_count: -1,
            by_nosort: false,
            by_pattern: None,
            get_patterns: Vec::new(),
            store_key: None,
        }
    }
}

pub(super) struct SortEntry {
    pub(super) element: Vec<u8>,
    pub(super) score: f64,
    pub(super) cmp_obj: Option<Vec<u8>>,
}

pub(super) fn parse_sort_double(v: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(v).ok()?;
    let f = s.parse::<f64>().ok()?;
    if f.is_nan() {
        None
    } else {
        Some(f)
    }
}

pub(super) fn sort_entries(entries: &mut [SortEntry], alpha: bool, desc: bool) {
    if alpha {
        entries.sort_by(|a, b| {
            let left = a.cmp_obj.as_deref().unwrap_or(&a.element);
            let right = b.cmp_obj.as_deref().unwrap_or(&b.element);
            let cmp = left.cmp(right);
            if desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
    } else {
        entries.sort_by(|a, b| {
            let cmp = a
                .score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal);
            if cmp == std::cmp::Ordering::Equal {
                let ecmp = a.element.cmp(&b.element);
                if desc {
                    ecmp.reverse()
                } else {
                    ecmp
                }
            } else if desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }
}

pub(super) fn apply_limit(entries: &[SortEntry], start: i64, count: i64) -> &[SortEntry] {
    let len = entries.len() as i64;
    let s = start.max(0).min(len);
    let count = count.max(-1).min(len);
    if s >= len || count == 0 {
        return &[];
    }
    let e = if count < 0 { len } else { (s + count).min(len) };
    &entries[s as usize..e as usize]
}

pub(super) fn lookup_pattern_value(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    pattern: &[u8],
    element: &[u8],
) -> Result<Option<Vec<u8>>, ()> {
    let (key_pattern, hash_field) = split_hash_field_pattern(pattern);
    let key = substitute_pattern(key_pattern, element);
    if let Some(field_pattern) = hash_field {
        let field = substitute_pattern(field_pattern, element);
        match db.get_hash(&key) {
            Ok(Some(hash)) => Ok(hash.get(field.as_slice()).map(|v| v.to_vec())),
            Ok(None) => Ok(None),
            Err(()) => Err(()),
        }
    } else {
        match db.get_string(&key) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(()) => Err(()),
        }
    }
}

pub(super) fn split_hash_field_pattern(pattern: &[u8]) -> (&[u8], Option<&[u8]>) {
    for i in 0..pattern.len().saturating_sub(1) {
        if pattern[i] == b'-' && pattern[i + 1] == b'>' && i + 2 < pattern.len() {
            return (&pattern[..i], Some(&pattern[i + 2..]));
        }
    }
    (pattern, None)
}

pub(super) fn substitute_pattern(pattern: &[u8], element: &[u8]) -> Vec<u8> {
    if pattern.contains(&b'*') {
        let mut out = Vec::new();
        for &b in pattern {
            if b == b'*' {
                out.extend_from_slice(element);
            } else {
                out.push(b);
            }
        }
        out
    } else {
        pattern.to_vec()
    }
}

pub(super) fn resolve_get_pattern(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    pattern: &[u8],
    element: &[u8],
) -> Value {
    if pattern == b"#" {
        return Value::bulk(element.to_vec());
    }
    match lookup_pattern_value(db, pattern, element) {
        Ok(Some(v)) => Value::bulk(v),
        Ok(None) | Err(()) => Value::Null,
    }
}

pub(super) fn extract_elements(obj: &Robj) -> Vec<Vec<u8>> {
    match obj {
        Robj::ListQuicklist(list) => list.iter().map(|b| b.to_vec()).collect(),
        Robj::SetHashtable(set) => {
            let mut v: Vec<Vec<u8>> = set.data.keys().map(|b| b.to_vec()).collect();
            v.sort();
            v
        }
        Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => obj
            .zset_entries()
            .into_iter()
            .map(|(member, _)| member.to_vec())
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn build_entries(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    elements: &[Vec<u8>],
    opts: &SortOptions,
) -> Result<Vec<SortEntry>, Value> {
    let mut out = Vec::with_capacity(elements.len());
    for e in elements {
        if opts.by_nosort {
            out.push(SortEntry {
                element: e.clone(),
                score: 0.0,
                cmp_obj: None,
            });
            continue;
        }
        let by_value = match &opts.by_pattern {
            Some(pattern) => lookup_pattern_value(db, pattern, e).unwrap_or_default(),
            None => Some(e.clone()),
        };
        let (score, cmp_obj) = if opts.alpha {
            (0.0, by_value)
        } else if let Some(v) = by_value {
            let Some(score) = parse_sort_double(&v) else {
                return Err(Value::error(
                    "ERR One or more scores can't be converted into double",
                ));
            };
            (score, None)
        } else {
            (0.0, None)
        };
        out.push(SortEntry {
            element: e.clone(),
            score,
            cmp_obj,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::extract_elements;
    use crate::foundation::io::store::structure::zset::ZSetLp;
    use crate::foundation::io::store::Robj;

    #[test]
    fn extract_elements_reads_listpack_zset_in_score_order() {
        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        zset.zset_add(b"b", 5.0);
        zset.zset_add(b"c", 2.0);
        zset.zset_add(b"a", 1.0);

        assert_eq!(
            extract_elements(&zset),
            vec![b"a".to_vec(), b"c".to_vec(), b"b".to_vec()]
        );
    }
}
