use versal::foundation::io::protocol::parser::Parser;
use versal::foundation::io::store::structure::dict::Dict;
use versal::run_from_env;
use versal::{ClusterRouteDecision, RouteRequest};

#[test]
fn library_exports_core_test_targets() {
    let _: fn() = run_from_env;
    let _: Option<ClusterRouteDecision> = None;
    let _: Option<RouteRequest<'static>> = None;

    let mut dict = Dict::new();
    dict.insert("key", "value");
    assert_eq!(dict.get("key"), Some(&"value"));

    let mut parser = Parser::new();
    parser.feed(b"PING\r\n");
    assert_eq!(parser.try_parse().unwrap().unwrap()[0].as_ref(), b"PING");
}
