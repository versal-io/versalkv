use super::*;
use bytes::Bytes;

#[test]
fn test_simple_string() {
    assert_eq!(
        serialize_to_vec(&Value::SimpleString(Bytes::from_static(b"OK"))),
        b"+OK\r\n"
    );
}
#[test]
fn test_error() {
    assert_eq!(
        serialize_to_vec(&Value::Error(Bytes::from_static(b"ERR x"))),
        b"-ERR x\r\n"
    );
}
#[test]
fn test_integer() {
    assert_eq!(serialize_to_vec(&Value::Integer(42)), b":42\r\n");
}
#[test]
fn test_bulk() {
    assert_eq!(
        serialize_to_vec(&Value::Bulk(Bytes::from_static(b"hi"))),
        b"$2\r\nhi\r\n"
    );
}
#[test]
fn test_null() {
    assert_eq!(serialize_to_vec(&Value::Null), b"$-1\r\n");
}
#[test]
fn test_null_array() {
    assert_eq!(serialize_to_vec(&Value::NullArray), b"*-1\r\n");
}
#[test]
fn test_array() {
    let v = Value::Array(vec![Value::bulk(Bytes::from_static(b"ping"))]);
    assert_eq!(serialize_to_vec(&v), b"*1\r\n$4\r\nping\r\n");
}
#[test]
fn test_bool_resp2() {
    assert_eq!(serialize_to_vec_resp2(&Value::Boolean(true)), b":1\r\n");
    assert_eq!(serialize_to_vec_resp2(&Value::Boolean(false)), b":0\r\n");
}
#[test]
fn test_bool_resp3() {
    assert_eq!(serialize_to_vec_resp3(&Value::Boolean(true)), b"#t\r\n");
    assert_eq!(serialize_to_vec_resp3(&Value::Boolean(false)), b"#f\r\n");
}
#[test]
#[allow(clippy::approx_constant)]
fn test_double_uses_shortest_representation() {
    assert_eq!(format_double(3.141), "3.141");
    assert_eq!(format_double(42.0), "42");
    assert_eq!(format_double(f64::INFINITY), "inf");
    assert_eq!(format_double(f64::NEG_INFINITY), "-inf");
    assert_eq!(format_double(f64::NAN), "nan");
    assert_eq!(
        serialize_to_vec_resp2(&Value::double(f64::INFINITY)),
        b"$3\r\ninf\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp3(&Value::double(f64::NEG_INFINITY)),
        b",-inf\r\n"
    );
}
#[test]
fn test_bignum_resp3() {
    assert_eq!(
        serialize_to_vec_resp3(&Value::bignum("12345")),
        b"(12345\r\n"
    );
}
#[test]
fn test_verbatim_resp3() {
    let v = Value::verbatim(*b"txt", "hello");
    assert_eq!(serialize_to_vec_resp3(&v), b"=9\r\ntxt:hello\r\n");
}
#[test]
fn test_attribute_resp2_strips() {
    let v = Value::attribute(
        vec![(Value::bulk("ttl"), Value::integer(3600))],
        Value::bulk("hi"),
    );
    assert_eq!(serialize_to_vec_resp2(&v), b"$2\r\nhi\r\n");
}
#[test]
fn test_attribute_resp3_keeps_attrs_before_inner_value() {
    let v = Value::attribute(
        vec![(Value::bulk("ttl"), Value::integer(3600))],
        Value::bulk("hi"),
    );
    assert_eq!(
        serialize_to_vec_resp3(&v),
        b"|1\r\n$3\r\nttl\r\n:3600\r\n$2\r\nhi\r\n"
    );
}
#[test]
fn test_push_resp3() {
    let v = Value::push(vec![
        Value::bulk("message"),
        Value::bulk("ch"),
        Value::bulk("pay"),
    ]);
    assert!(serialize_to_vec_resp3(&v).starts_with(b">3\r\n"));
}
#[test]
fn test_resp2_downgrades_resp3_container_types_to_arrays_or_bulk() {
    assert_eq!(
        serialize_to_vec_resp2(&Value::bignum("12345")),
        b"$5\r\n12345\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp2(&Value::verbatim(*b"txt", "hello")),
        b"$5\r\nhello\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp2(&Value::push(vec![Value::bulk("p")])),
        b"*1\r\n$1\r\np\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp2(&Value::map(vec![(Value::bulk("k"), Value::bulk("v"))])),
        b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp2(&Value::set(vec![Value::bulk("a"), Value::bulk("b")])),
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
}
#[test]
fn test_resp3_map_set_null_and_scalar_shapes() {
    assert_eq!(
        serialize_to_vec_resp3(&Value::SimpleString(Bytes::from_static(b"OK"))),
        b"+OK\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp3(&Value::Error(Bytes::from_static(b"ERR x"))),
        b"-ERR x\r\n"
    );
    assert_eq!(serialize_to_vec_resp3(&Value::Integer(-7)), b":-7\r\n");
    assert_eq!(serialize_to_vec_resp3(&Value::NullArray), b"_\r\n");
    assert_eq!(
        serialize_to_vec_resp3(&Value::map(vec![(Value::bulk("k"), Value::bulk("v"))])),
        b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
    );
    assert_eq!(
        serialize_to_vec_resp3(&Value::set(vec![Value::bulk("a"), Value::bulk("b")])),
        b"~2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
}
