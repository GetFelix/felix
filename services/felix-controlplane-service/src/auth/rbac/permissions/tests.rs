use felix_common::random::HashSet;

use super::*;
use crate::auth::rbac::enforcer::build_enforcer;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};

#[tokio::test]
async fn ns_manage_implies_stream_and_cache() {
    let policies = vec![PolicyRule {
        subject: "role:ns-admin".to_string(),
        object: "namespace:tenant-a/payments".to_string(),
        action: "ns.manage".to_string(),
    }];
    let groupings = vec![GroupingRule {
        user: "p:user".to_string(),
        role: "role:ns-admin".to_string(),
    }];
    let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
        .await
        .expect("enforcer");

    let perms = effective_permissions(&enforcer, "p:user", "tenant-a");
    assert!(perms.contains(&"ns.manage:namespace:tenant-a/payments".to_string()));
    assert!(perms.contains(&"stream.publish:stream:tenant-a/payments/*".to_string()));
    assert!(perms.contains(&"cache.read:cache:tenant-a/payments/*".to_string()));
}

#[tokio::test]
async fn tenant_admin_implies_wildcards() {
    let policies = vec![PolicyRule {
        subject: "role:admin".to_string(),
        object: "tenant:tenant-a".to_string(),
        action: "tenant.manage".to_string(),
    }];
    let groupings = vec![GroupingRule {
        user: "p:admin".to_string(),
        role: "role:admin".to_string(),
    }];
    let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
        .await
        .expect("enforcer");
    let perms = effective_permissions(&enforcer, "p:admin", "tenant-a");
    assert!(perms.contains(&"ns.manage:namespace:tenant-a/*".to_string()));
    assert!(perms.contains(&"stream.publish:stream:tenant-a/*/*".to_string()));
    assert!(perms.contains(&"cache.write:cache:tenant-a/*/*".to_string()));
}

#[tokio::test]
async fn effective_permissions_skips_unknown_actions() {
    let policies = vec![PolicyRule {
        subject: "role:custom".to_string(),
        object: "tenant:tenant-a".to_string(),
        action: "totally.unknown".to_string(),
    }];
    let groupings = vec![GroupingRule {
        user: "p:user".to_string(),
        role: "role:custom".to_string(),
    }];
    let enforcer = build_enforcer(&policies, &groupings, "tenant-a")
        .await
        .expect("enforcer");

    let perms = effective_permissions(&enforcer, "p:user", "tenant-a");
    assert!(perms.is_empty());
}

#[test]
fn expand_inheritance_handles_wildcards_and_malformed_permissions() {
    let mut perms: HashSet<String> = vec![
        "ns.manage:namespace:tenant-a/*".to_string(),
        "malformed".to_string(),
        "stream.publish:stream:tenant-a/default/orders".to_string(),
    ]
    .into_iter()
    .collect();

    expand_inheritance(&mut perms);

    assert!(perms.contains("ns.manage:namespace:tenant-a/*"));
    assert!(perms.contains("stream.manage:stream:tenant-a/*/*"));
    assert!(perms.contains("cache.write:cache:tenant-a/*/*"));
    assert!(perms.contains("malformed"));
}

#[test]
fn tenant_id_from_object_supports_all_resource_prefixes() {
    assert_eq!(tenant_id_from_object("tenant:tenant-a"), Some("tenant-a"));
    assert_eq!(
        tenant_id_from_object("namespace:tenant-a/default"),
        Some("tenant-a")
    );
    assert_eq!(
        tenant_id_from_object("stream:tenant-a/default/orders"),
        Some("tenant-a")
    );
    assert_eq!(
        tenant_id_from_object("cache:tenant-a/default/primary"),
        Some("tenant-a")
    );
}

#[test]
fn tenant_id_from_object_rejects_invalid_shapes() {
    assert_eq!(tenant_id_from_object("tenant:"), None);
    assert_eq!(tenant_id_from_object("namespace:tenant-a"), None);
    assert_eq!(tenant_id_from_object("stream:tenant-a"), None);
    assert_eq!(tenant_id_from_object("cache:tenant-a"), None);
    assert_eq!(tenant_id_from_object("unknown:tenant-a"), None);
}

#[test]
fn namespace_from_object_parses_namespace_and_rejects_non_namespace() {
    assert_eq!(
        namespace_from_object("namespace:tenant-a/default"),
        Some(("tenant-a", "default"))
    );
    assert_eq!(namespace_from_object("namespace:tenant-a"), None);
    assert_eq!(namespace_from_object("tenant:tenant-a"), None);
}
