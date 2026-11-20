use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::hyperloglog::common::{
    c_format, empty_hll, error_value, hll_format, parse_hll, serialize_hll, HllError, HllFormat,
    HllState, HLL_REGISTERS,
};
use crate::framework::commands::key_extractors::pfmerge_get_keys;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PfMergeRequest {
    destination: Bytes,
    sources: Vec<Bytes>,
}

impl PfMergeRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        let Some(destination) = args.args.first() else {
            return Err(CommandError::wrong_arity("pfmerge"));
        };
        Ok(Self {
            destination: destination.clone(),
            sources: args.args[1..].to_vec(),
        })
    }
}

struct PfMergeOutcome {
    serialized: Vec<u8>,
    format: HllFormat,
}

fn execute_pfmerge(
    db: &mut Db,
    request: &PfMergeRequest,
    sparse_max_bytes: i64,
) -> Result<PfMergeOutcome, Value> {
    let mut has_standard = false;
    let mut has_legacy = false;
    for key in std::iter::once(&request.destination).chain(&request.sources) {
        match db.get_string(key) {
            Err(_) => return Err(CommandError::wrong_type()),
            Ok(Some(data)) => match hll_format(data).map_err(error_value)? {
                HllFormat::Standard => has_standard = true,
                HllFormat::Legacy => has_legacy = true,
            },
            Ok(None) => {}
        }
    }
    if has_standard {
        // Register indices use different hashes in the two formats. A mixed
        // union cannot be reconstructed without the original input elements.
        if has_legacy {
            return Err(error_value(HllError::InvalidObject));
        }
        return merge_standard_hll(db, request, sparse_max_bytes);
    }
    let destination = load_hll(db, &request.destination)?;
    let mut result = if request.sources.is_empty() {
        destination
    } else {
        HllState::empty()
    };
    for source in &request.sources {
        let Some(state) = load_optional_hll(db, source)? else {
            continue;
        };
        result.merge(&state);
    }
    let serialized = if result.cardinality() == 0 {
        empty_hll()
    } else {
        serialize_hll(&result, sparse_max_bytes, true)
    };
    Ok(PfMergeOutcome {
        serialized,
        format: HllFormat::Legacy,
    })
}

fn merge_standard_hll(
    db: &mut Db,
    request: &PfMergeRequest,
    sparse_max_bytes: i64,
) -> Result<PfMergeOutcome, Value> {
    let mut registers = vec![0u8; HLL_REGISTERS];
    let mut use_dense = false;
    // Validate all inputs before publishing any change, including destination.
    for key in std::iter::once(&request.destination).chain(&request.sources) {
        if let Some(data) = db.get_string(key).map_err(|_| CommandError::wrong_type())? {
            use_dense |= data[4] == 0;
            for (dst, rank) in registers
                .iter_mut()
                .zip(c_format::decode(data).map_err(error_value)?)
            {
                *dst = (*dst).max(rank);
            }
        }
    }
    let mut serialized = db
        .get_string(&request.destination)
        .map_err(|_| CommandError::wrong_type())?
        .map_or_else(c_format::empty, <[u8]>::to_vec);
    if use_dense && serialized[4] == 1 {
        serialized = c_format::to_dense(&serialized).map_err(error_value)?;
    }
    for (index, rank) in registers.iter().copied().enumerate() {
        if rank > 0 {
            c_format::set_register(&mut serialized, index, rank, sparse_max_bytes)
                .map_err(error_value)?;
        }
    }
    serialized[15] |= 0x80;
    Ok(PfMergeOutcome {
        serialized,
        format: HllFormat::Standard,
    })
}

fn load_hll(db: &mut Db, key: &[u8]) -> Result<HllState, Value> {
    Ok(load_optional_hll(db, key)?.unwrap_or_else(HllState::empty))
}

fn load_optional_hll(db: &mut Db, key: &[u8]) -> Result<Option<HllState>, Value> {
    match db.get_string(key) {
        Err(_) => Err(CommandError::wrong_type()),
        Ok(Some(data)) => parse_hll(data).map(Some).map_err(error_value),
        Ok(None) => Ok(None),
    }
}

#[command("PFMERGE", runtime, get_keys = pfmerge_get_keys)]
pub fn pfmerge(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match PfMergeRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let sparse_max_bytes = runtime.hll_sparse_max_bytes();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(db) => db,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let outcome = match execute_pfmerge(db, &request, sparse_max_bytes) {
        Ok(outcome) => outcome,
        Err(error) => return CommandOutcome::reply(error),
    };
    let value = Robj::string_raw(outcome.serialized);
    match outcome.format {
        HllFormat::Standard => db.set_keep_ttl(request.destination.to_vec(), value),
        HllFormat::Legacy => db.set(request.destination.to_vec(), value),
    }
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, request.destination)],
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_owns_destination_and_sources() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"PFMERGE"),
            Bytes::from_static(b"destination"),
            Bytes::from_static(b"source-a"),
            Bytes::from_static(b"source-b"),
        ]);

        let request = PfMergeRequest::parse(&args).unwrap();
        assert_eq!(request.destination, Bytes::from_static(b"destination"));
        assert_eq!(request.sources.len(), 2);
    }
}
