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

fn may_read(matcher: &PermissionMatcher, ns: &str, cache: &str, keys: CacheKeys<'_>) -> bool {
    matcher.allows_cache_keys(
        Action::CacheRead,
        &crate::TenantId::new("t1"),
        &crate::Namespace::new(ns),
        &crate::CacheScope::new(cache),
        keys,
    )
}

#[test]
fn a_whole_cache_grant_covers_every_key_and_prefix() {
    for grant in [
        "cache.read:cache:t1/ns/rooms",
        "cache.read:cache:t1/ns/*",
        "cache.read:cache:t1/*/*",
    ] {
        let reader = matcher(&[grant]);
        assert!(
            may_read(&reader, "ns", "rooms", CacheKeys::Key("a")),
            "{grant}"
        );
        assert!(
            may_read(&reader, "ns", "rooms", CacheKeys::Prefix("")),
            "{grant}"
        );
        assert!(
            may_read(&reader, "ns", "rooms", CacheKeys::Prefix("room1/")),
            "{grant}"
        );
    }
    let reader = matcher(&["cache.read:cache:t1/ns/rooms"]);
    assert!(!may_read(&reader, "ns", "other", CacheKeys::Key("a")));
    assert!(!may_read(&reader, "ns2", "rooms", CacheKeys::Key("a")));
}

#[test]
fn an_exact_key_grant_allows_that_key_alone() {
    let reader = matcher(&["cache.read:cache:t1/ns/rooms/user:1"]);
    assert!(may_read(&reader, "ns", "rooms", CacheKeys::Key("user:1")));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Key("user:10")));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Key("user:")));
    assert!(!may_read(&reader, "ns", "other", CacheKeys::Key("user:1")));
    // A prefix watch on exactly the key would still read `user:10`.
    assert!(!may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Prefix("user:1")
    ));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Prefix("")));
}

#[test]
fn a_prefix_grant_is_a_literal_string_prefix() {
    let reader = matcher(&["cache.read:cache:t1/ns/rooms/room1/*"]);
    assert!(may_read(&reader, "ns", "rooms", CacheKeys::Key("room1/")));
    assert!(may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Key("room1/x/y")
    ));
    assert!(!may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Key("room10/x")
    ));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Key("room1")));
    assert!(may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Prefix("room1/")
    ));
    assert!(may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Prefix("room1/a")
    ));
    assert!(!may_read(
        &reader,
        "ns",
        "rooms",
        CacheKeys::Prefix("room1")
    ));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Prefix("")));

    // Without a separator the prefix reaches longer ids too.
    let loose = matcher(&["cache.read:cache:t1/ns/rooms/user:1*"]);
    assert!(may_read(&loose, "ns", "rooms", CacheKeys::Key("user:10")));
}

#[test]
fn a_key_grant_needs_its_action() {
    let reader = matcher(&["cache.read:cache:t1/ns/rooms/a"]);
    let may_write = reader.allows_cache_keys(
        Action::CacheWrite,
        &crate::TenantId::new("t1"),
        &crate::Namespace::new("ns"),
        &crate::CacheScope::new("rooms"),
        CacheKeys::Key("a"),
    );
    assert!(!may_write);
    let writer = matcher(&["cache.write:cache:t1/ns/rooms/a"]);
    assert!(!may_read(&writer, "ns", "rooms", CacheKeys::Key("a")));
}

/// A `*` in a key grant only ever means "this prefix". Anywhere else it
/// would turn the key into a pattern, so the grant matches nothing.
#[test]
fn a_star_inside_a_key_grant_matches_nothing() {
    for grant in [
        "cache.read:cache:t1/ns/rooms/a*b",
        "cache.read:cache:t1/ns/rooms/*a",
        "cache.read:cache:t1/ns/rooms/a**",
        "cache.read:cache:t1/ns/rooms/",
    ] {
        let reader = matcher(&[grant]);
        for key in ["a*b", "ab", "axb", "a", "*a", "a**", ""] {
            assert!(
                !may_read(&reader, "ns", "rooms", CacheKeys::Key(key)),
                "{grant} {key}"
            );
        }
    }
}

/// Matched per segment, a wildcard in a cache-wide grant cannot slide across
/// `/` and turn the cache name into a key.
#[test]
fn a_cache_wide_wildcard_does_not_reach_keys_of_other_caches() {
    let reader = matcher(&["cache.read:cache:t1/*/secret"]);
    assert!(may_read(&reader, "ns", "secret", CacheKeys::Key("a")));
    assert!(!may_read(&reader, "ns", "rooms", CacheKeys::Key("secret")));

    let keyed = matcher(&["cache.read:cache:t1/*/rooms/a*"]);
    assert!(may_read(&keyed, "ns", "rooms", CacheKeys::Key("a1")));
    assert!(!may_read(&keyed, "ns", "other", CacheKeys::Key("rooms/a1")));
}

/// Only `node.view:cluster:*` itself reaches the cluster: no wildcard that
/// happens to match the string, no other action, and no tenant object.
#[test]
fn only_an_exact_cluster_grant_allows_cluster_actions() {
    assert!(matcher(&["node.view:cluster:*"]).allows_cluster(Action::NodeView));
    assert!(
        matcher(&["stream.subscribe:stream:t1/*/*", "node.view:cluster:*"])
            .allows_cluster(Action::NodeView)
    );

    for refused in [
        "node.view:*",
        "node.view:cluster*",
        "node.view:tenant:t1",
        "node.manage:cluster:*",
        "stream.manage:cluster:*",
    ] {
        assert!(
            !matcher(&[refused]).allows_cluster(Action::NodeView),
            "{refused}"
        );
    }
    assert!(!matcher(&[]).allows_cluster(Action::NodeView));
}

/// A token an operator also uses against the control plane carries
/// `node.manage`; a broker must still accept it.
#[test]
fn node_actions_parse() {
    let parsed = PermissionMatcher::from_strings(&[
        "node.view:cluster:*".to_string(),
        "node.manage:node:broker-1".to_string(),
    ])
    .expect("node actions parse");
    assert_eq!(parsed.patterns().len(), 2);
}
