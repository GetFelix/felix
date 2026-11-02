use super::*;
use crate::{PermissionMatcher, stream_resource};

fn ids() -> (TenantId, Namespace, CacheScope) {
    (
        TenantId::new("t1"),
        Namespace::new("prod"),
        CacheScope::new("leases"),
    )
}

fn grants(perms: &[&str], key: &str) -> AuthzResult<Vec<String>> {
    let perms: Vec<String> = perms.iter().map(|perm| (*perm).to_string()).collect();
    let (tenant, namespace, cache) = ids();
    PermissionMatcher::from_strings(&perms)
        .expect("perms parse")
        .lease_grants(&tenant, &namespace, &cache, key)
}

#[test]
fn well_formed_templates_parse() {
    for template in [
        "stream:t1/prod/wl.{leases}.out",
        "stream:t1/prod/{leases}",
        "stream:t1/prod/{leases}-out",
        "cache:t1/prod/snaps/wl.{leases}.snap",
    ] {
        LeaseTemplate::parse(template).unwrap_or_else(|err| panic!("{template}: {err}"));
    }
}

#[test]
fn malformed_templates_are_refused() {
    for template in [
        "stream:t1/prod/wl.{leases}.{other}",
        "stream:t1/prod/wl.{leases",
        "stream:t1/prod/wl.leases}",
        "stream:t1/prod/wl.}leases{",
        "stream:t1/prod/wl.{}.out",
        "stream:t1/prod/wl.{lea:ses}.out",
        "stream:t1/prod/wl.{leases}*",
        "stream:t1/*/wl.{leases}.out",
        "stream:t1/{leases}/out",
        "stream:t1/prod/a/wl.{leases}",
        "stream:t1/prod/w l.{leases}",
        "cache:t1/prod/{leases}",
        "cache:t1/prod/snaps/x/{leases}",
        "namespace:t1/{leases}",
        "group:t1/prod/s/{leases}",
        "tenant:{leases}",
    ] {
        assert!(
            LeaseTemplate::parse(template).is_err(),
            "{template} should be refused"
        );
    }
}

#[test]
fn a_template_takes_only_data_actions_on_its_kind() {
    let stream = LeaseTemplate::parse("stream:t1/prod/wl.{leases}.out").unwrap();
    assert!(stream.takes(Action::StreamPublish));
    assert!(stream.takes(Action::StreamSubscribe));
    assert!(!stream.takes(Action::StreamManage));
    assert!(!stream.takes(Action::CacheWrite));
    let cache = LeaseTemplate::parse("cache:t1/prod/snaps/wl.{leases}").unwrap();
    assert!(cache.takes(Action::CacheRead));
    assert!(cache.takes(Action::CacheWrite));
    assert!(!cache.takes(Action::CacheManage));
    assert!(!cache.takes(Action::StreamPublish));
}

#[test]
fn templates_expand_for_the_named_cache_only() {
    let expanded = grants(
        &[
            "stream.publish:stream:t1/prod/wl.{leases}.out",
            "cache.write:cache:t1/prod/snaps/wl.{leases}.snap",
            "stream.publish:stream:t1/prod/wl.{other}.out",
            "stream.publish:stream:t1/dev/wl.{leases}.out",
            "stream.publish:stream:t1/prod/plain",
        ],
        "w7",
    )
    .unwrap();
    assert_eq!(
        expanded,
        vec![
            "stream.publish:stream:t1/prod/wl.w7.out".to_string(),
            "cache.write:cache:t1/prod/snaps/wl.w7.snap".to_string(),
        ]
    );
}

#[test]
fn a_template_with_the_wrong_action_expands_to_nothing() {
    assert!(
        grants(&["stream.manage:stream:t1/prod/wl.{leases}.out"], "w7")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_key_that_cannot_name_a_resource_refuses_the_whole_mint() {
    let perms = [
        "stream.publish:stream:t1/prod/wl.{leases}.out",
        "stream.subscribe:stream:t1/prod/wl.{leases}.in",
    ];
    let too_long = "k".repeat(crate::MAX_IDENTIFIER_LEN);
    for key in [
        "", "*", "w*", "a/b", "a:b", "{x}", "a{", "}", ".hidden", "-x", "a b", "a\nb", "naïve",
        &too_long,
    ] {
        assert!(
            matches!(grants(&perms, key), Err(AuthzError::InvalidLeaseKey(_))),
            "{key:?} should refuse"
        );
    }
}

#[test]
fn templates_never_match_as_grants() {
    let matcher = PermissionMatcher::from_strings(&[
        "stream.publish:stream:t1/prod/wl.{leases}.out".to_string(),
        "cache.write:cache:t1/prod/snaps/wl.{leases}.snap".to_string(),
        "cache.write:cache:t1/prod/{leases}".to_string(),
    ])
    .unwrap();
    let (tenant, namespace, _) = ids();
    for stream in ["wl.{leases}.out", "wl.w7.out", "wl..out"] {
        assert!(!matcher.allows(
            Action::StreamPublish,
            &stream_resource(&tenant, &namespace, &crate::StreamName::new(stream))
        ));
    }
    for (cache, key) in [
        ("snaps", "wl.{leases}.snap"),
        ("snaps", "wl.w7.snap"),
        ("{leases}", "x"),
    ] {
        assert!(!matcher.allows_cache_keys(
            Action::CacheWrite,
            &tenant,
            &namespace,
            &CacheScope::new(cache),
            crate::CacheKeys::Key(key),
        ));
    }
}
