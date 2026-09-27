use super::*;
use crate::Action;

#[test]
fn wildcard_match_exact() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/orders",
        "stream:tenant-a/payments/orders"
    ));
    assert!(!wildcard_match(
        "stream:tenant-a/payments/orders",
        "stream:tenant-a/payments/orders.v2"
    ));
}

#[test]
fn wildcard_match_suffix() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/orders"
    ));
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/orders.v2"
    ));
    assert!(!wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:accounts/orders"
    ));
}

#[test]
fn wildcard_match_any() {
    assert!(wildcard_match("*", "anything"));
}

#[test]
fn wildcard_match_backtrack() {
    assert!(wildcard_match(
        "cache:*:read",
        "cache:tenant-a/payments:read"
    ));
    assert!(!wildcard_match(
        "cache:*:read",
        "cache:tenant-a/payments:write"
    ));
}

#[test]
fn wildcard_match_trailing_star() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/"
    ));
}

#[test]
fn matcher_allows() {
    let matcher = PermissionMatcher::new(vec![PermissionPattern::new(
        Action::StreamPublish,
        "stream:tenant-a/payments/orders.*",
    )]);
    assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v2"));
    assert!(!matcher.allows(
        Action::StreamSubscribe,
        "stream:tenant-a/payments/orders.v2"
    ));
}

#[test]
fn matcher_from_strings_and_patterns() {
    let patterns = vec![
        "stream.publish:stream:tenant-a/payments/orders.*".to_string(),
        "cache.read:cache:tenant-a/payments/session/*".to_string(),
    ];
    let matcher = PermissionMatcher::from_strings(&patterns).expect("parse patterns");
    assert_eq!(matcher.patterns().len(), 2);
    assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v1"));
    assert!(matcher.allows(Action::CacheRead, "cache:tenant-a/payments/session/abc"));
}

fn matcher(patterns: &[&str]) -> PermissionMatcher {
    let patterns: Vec<String> = patterns.iter().map(|p| p.to_string()).collect();
    PermissionMatcher::from_strings(&patterns).expect("patterns")
}

fn may_consume(matcher: &PermissionMatcher, stream: &str, group: &str) -> bool {
    matcher.allows_group(
        Action::GroupConsume,
        &crate::TenantId::new("t1"),
        &crate::Namespace::new("ns"),
        &crate::StreamName::new(stream),
        &crate::GroupName::new(group),
    )
}

#[test]
fn a_stream_grant_covers_every_group_on_the_stream() {
    let reader = matcher(&["stream.subscribe:stream:t1/ns/orders"]);
    assert!(may_consume(&reader, "orders", "workers"));
    assert!(may_consume(&reader, "orders", "billing"));
    assert!(!may_consume(&reader, "refunds", "workers"));

    let scoped = matcher(&["group.consume:stream:t1/ns/orders"]);
    assert!(may_consume(&scoped, "orders", "anything"));
}

#[test]
fn a_group_grant_allows_that_group_alone() {
    let worker = matcher(&["group.consume:group:t1/ns/orders/workers"]);
    assert!(may_consume(&worker, "orders", "workers"));
    assert!(!may_consume(&worker, "orders", "billing"));
    assert!(!may_consume(&worker, "refunds", "workers"));
}

/// Once a principal is granted particular groups on a stream, its stream grant
/// no longer reaches the others there. It still reaches other streams.
#[test]
fn a_group_grant_narrows_the_stream_grant_on_that_stream() {
    let worker = matcher(&[
        "stream.subscribe:stream:t1/ns/*",
        "group.consume:group:t1/ns/orders/workers",
    ]);
    assert!(may_consume(&worker, "orders", "workers"));
    assert!(!may_consume(&worker, "orders", "billing"));
    assert!(may_consume(&worker, "refunds", "billing"));

    // A wildcard group grant narrows every stream it could reach.
    let wide = matcher(&[
        "stream.subscribe:stream:t1/ns/*",
        "group.consume:group:t1/ns/*/workers",
    ]);
    assert!(may_consume(&wide, "orders", "workers"));
    assert!(!may_consume(&wide, "orders", "billing"));
    assert!(!may_consume(&wide, "refunds", "billing"));
}

/// Narrowing is per action: a consume grant on one group says nothing about
/// who may manage the stream's dead letters.
#[test]
fn narrowing_one_action_leaves_the_others() {
    let operator = matcher(&[
        "stream.manage:stream:t1/ns/orders",
        "group.consume:group:t1/ns/orders/workers",
    ]);
    assert!(operator.allows_group(
        Action::GroupManage,
        &crate::TenantId::new("t1"),
        &crate::Namespace::new("ns"),
        &crate::StreamName::new("orders"),
        &crate::GroupName::new("billing"),
    ));
}

#[test]
fn some_extension_follows_wildcards() {
    let prefix = "group:t1/ns/orders/";
    assert!(matches_some_extension("group:t1/ns/orders/workers", prefix));
    assert!(matches_some_extension("group:t1/*", prefix));
    assert!(matches_some_extension("group:*/orders/*", prefix));
    assert!(matches_some_extension("*", prefix));
    assert!(!matches_some_extension("group:t1/ns/refunds/*", prefix));
    assert!(!matches_some_extension("group:t2/*", prefix));
    assert!(!matches_some_extension("group:t1/ns/orders", prefix));
}
