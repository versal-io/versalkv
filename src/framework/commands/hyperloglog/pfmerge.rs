use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::store::Robj;
use crate::framework::commands::hyperloglog::common::{
    empty_hll, error_value, parse_hll, serialize_hll, HllState,
};
use crate::framework::commands::command_runtime::CommandRuntime;
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
}

fn execute_pfmerge(
    db: &mut Db,
    request: &PfMergeRequest,
    sparse_max_bytes: i64,
) -> Result<PfMergeOutcome, Value> {
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
    Ok(PfMergeOutcome { serialized })
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

#[command("PFMERGE", runtime)]
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
    db.set(
        request.destination.to_vec(),
        Robj::string_raw(outcome.serialized),
    );
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
