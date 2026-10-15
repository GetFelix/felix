use felix_client::Seed;

use super::parse_addrs;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

/// Names reach the client as names, so it can resolve them again later.
#[test]
fn names_are_kept_as_names() {
    let seeds = parse_addrs("127.0.0.1:5000, localhost:5000").expect("parse");
    assert_eq!(
        seeds,
        [
            Seed::Addr("127.0.0.1:5000".parse().unwrap()),
            Seed::Name("localhost:5000".to_string()),
        ]
    );
}

/// A name that does not resolve is still the caller's bad argument.
#[test]
fn a_name_that_does_not_resolve_is_an_invalid_argument() {
    let failure = parse_addrs("felix.invalid:5000").unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(
        failure.message.contains("could not resolve"),
        "{}",
        failure.message
    );
    let failure = parse_addrs("localhost").unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}
