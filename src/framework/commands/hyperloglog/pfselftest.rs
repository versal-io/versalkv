use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::hyperloglog::common::{parse_hll, serialize_hll_as, HllEncoding, HllState};
use crate::framework::CommandArgs;
use command_macros::command;

#[command("PFSELFTEST", runtime)]
pub fn pfselftest(_runtime: &mut dyn CommandRuntime, _args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(pfselftest_reply())
}

fn pfselftest_reply() -> Value {
    // Run a simple self-test to verify the HLL implementation works

    // Test 1: Empty set should produce empty serialization
    let empty = HllState::empty();
    let serialized = serialize_hll_as(&empty, HllEncoding::Sparse, true);
    let deserialized = match parse_hll(&serialized) {
        Ok(state) => state,
        Err(_) => return Value::error("ERR PFSELFTEST: empty state failed to parse"),
    };
    if deserialized.cardinality() != 0 {
        return Value::error("ERR PFSELFTEST: empty set serialization/deserialization failed");
    }

    // Test 2: Single element
    let mut state1 = HllState::empty();
    state1.add(b"test_element");
    let serialized = serialize_hll_as(&state1, HllEncoding::Sparse, true);
    let deserialized = match parse_hll(&serialized) {
        Ok(state) => state,
        Err(_) => return Value::error("ERR PFSELFTEST: single element failed to parse"),
    };
    if deserialized.cardinality() != 1 {
        return Value::error("ERR PFSELFTEST: single element round-trip failed");
    }

    // Test 3: Multiple elements
    let mut state2 = HllState::empty();
    state2.add(b"a");
    state2.add(b"b");
    state2.add(b"c");
    let serialized = serialize_hll_as(&state2, HllEncoding::Sparse, true);
    let deserialized = match parse_hll(&serialized) {
        Ok(state) => state,
        Err(_) => return Value::error("ERR PFSELFTEST: multiple elements failed to parse"),
    };
    if deserialized.cardinality() != 3 {
        return Value::error("ERR PFSELFTEST: multiple elements round-trip failed");
    }

    // Test 4: Binary data (elements with null bytes)
    let mut state3 = HllState::empty();
    state3.add(b"binary\x00data");
    state3.add(b"plain");
    let serialized = serialize_hll_as(&state3, HllEncoding::Sparse, true);
    let deserialized = match parse_hll(&serialized) {
        Ok(state) => state,
        Err(_) => return Value::error("ERR PFSELFTEST: binary data failed to parse"),
    };
    if deserialized.cardinality() != 2 {
        return Value::error("ERR PFSELFTEST: binary data round-trip failed");
    }

    // Test 5: Large elements
    let mut state4 = HllState::empty();
    let large = vec![0x42u8; 1024];
    state4.add(&large);
    let serialized = serialize_hll_as(&state4, HllEncoding::Dense, true);
    let deserialized = match parse_hll(&serialized) {
        Ok(state) => state,
        Err(_) => return Value::error("ERR PFSELFTEST: large element failed to parse"),
    };
    if deserialized.cardinality() != 1 {
        return Value::error("ERR PFSELFTEST: large element round-trip failed");
    }

    // Test 6: Duplicate elements should not increase count
    let mut state5 = HllState::empty();
    if !state5.add(b"dup") || state5.add(b"dup") {
        return Value::error("ERR PFSELFTEST: duplicate detection failed");
    }

    Value::ok()
}
