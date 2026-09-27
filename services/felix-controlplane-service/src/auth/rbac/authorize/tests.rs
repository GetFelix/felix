use super::*;

/// The boundary the whole node credential rests on: a scope for one node
/// contains that node and nothing else.
#[test]
fn a_node_scope_covers_exactly_one_node() {
    let a = parse_object("node:broker-a", "t1").expect("parse");
    let b = parse_object("node:broker-b", "t1").expect("parse");

    assert!(object_within_scope(&a, &a));
    assert!(
        !object_within_scope(&a, &b),
        "a broker must not be able to act for another broker",
    );
    assert!(!object_within_scope(&b, &a));
}

/// An operator managing the fleet holds cluster scope, which covers every
/// node -- but a node scope never widens back out to the cluster.
#[test]
fn cluster_scope_covers_every_node_but_not_the_reverse() {
    let cluster = ParsedObject::Cluster;
    let node = parse_object("node:broker-a", "t1").expect("parse");

    assert!(object_within_scope(&cluster, &node));
    assert!(
        !object_within_scope(&node, &cluster),
        "one node's credential must not confer fleet-wide access",
    );
}

/// Two spellings for one scope is how a policy review misses one.
#[test]
fn a_node_wildcard_is_rejected() {
    assert!(parse_object("node:*", "t1").is_err());
    assert!(parse_object("node:", "t1").is_err());
}

#[test]
fn a_node_object_ignores_the_request_tenant() {
    assert_eq!(
        parse_object("node:broker-a", "t1"),
        parse_object("node:broker-a", "t2"),
    );
}

#[test]
fn node_manage_is_a_recognised_action() {
    let parsed = parse_permission("node.manage:node:broker-a", "t1").expect("parse");
    assert_eq!(parsed.action, ACTION_NODE_MANAGE);
    assert_eq!(
        parsed.object,
        ParsedObject::Node {
            node_id: "broker-a".to_string()
        }
    );
}

/// A tenant admin must not be able to write themselves a node permission,
/// for the same reason they cannot write a cluster one.
#[test]
fn a_tenant_scope_never_reaches_a_node() {
    let tenant_scopes = [
        ParsedObject::Tenant {
            tenant_id: "t1".to_string(),
        },
        parse_object("namespace:t1/*", "t1").expect("namespace"),
    ];
    let node = parse_object("node:broker-a", "t1").expect("parse");

    for scope in &tenant_scopes {
        assert!(!object_within_scope(scope, &node), "{scope:?}");
    }

    let rule = PolicyRule {
        subject: "role:tenant-admin".to_string(),
        object: "node:broker-a".to_string(),
        action: ACTION_NODE_MANAGE.to_string(),
    };
    assert!(
        validate_new_rule_allowed(&tenant_scopes, "t1", &rule).is_err(),
        "a tenant admin must not be able to grant node access",
    );
}

/// And a node credential confers nothing inside a tenant.
#[test]
fn a_node_scope_confers_nothing_in_a_tenant() {
    let node = [parse_object("node:broker-a", "t1").expect("parse")];
    for target in ["tenant:t1", "namespace:t1/payments", "stream:t1/ns/orders"] {
        let parsed = parse_object(target, "t1").expect("parse");
        assert!(!object_within_scope(&node[0], &parsed), "{target}");
    }
}

#[test]
fn the_cluster_object_parses_independently_of_any_tenant() {
    // Same object, whatever tenant the token belongs to.
    assert_eq!(parse_object("cluster:*", "t1"), Ok(ParsedObject::Cluster));
    assert_eq!(parse_object("cluster:*", "t2"), Ok(ParsedObject::Cluster));
    // One spelling only, so a typo is rejected rather than silently scoped.
    assert!(parse_object("cluster:nodes", "t1").is_err());
    assert!(parse_object("cluster:", "t1").is_err());
}

#[test]
fn node_view_is_a_recognised_action() {
    let parsed = parse_permission("node.view:cluster:*", "t1").expect("parse");
    assert_eq!(parsed.action, ACTION_NODE_VIEW);
    assert_eq!(parsed.object, ParsedObject::Cluster);
}

/// The property the whole cluster scope rests on. `validate_new_rule_allowed`
/// admits a rule only if its object is inside the caller's existing scope,
/// so if a tenant scope never contains the cluster, no tenant admin can
/// write themselves a cluster permission.
#[test]
fn a_tenant_scope_never_reaches_the_cluster() {
    let tenant_scopes = [
        ParsedObject::Tenant {
            tenant_id: "t1".to_string(),
        },
        parse_object("namespace:t1/*", "t1").expect("namespace"),
        parse_object("stream:t1/payments/*", "t1").expect("stream"),
    ];

    for scope in &tenant_scopes {
        assert!(
            !object_within_scope(scope, &ParsedObject::Cluster),
            "{scope:?} must not contain the cluster",
        );
    }

    let rule = PolicyRule {
        subject: "role:tenant-admin".to_string(),
        object: "cluster:*".to_string(),
        action: ACTION_NODE_VIEW.to_string(),
    };
    assert!(
        validate_new_rule_allowed(&tenant_scopes, "t1", &rule).is_err(),
        "a tenant admin must not be able to grant cluster access",
    );
}

/// And the converse: cluster scope is not a backdoor into tenant data.
#[test]
fn cluster_scope_confers_nothing_inside_a_tenant() {
    let cluster = [ParsedObject::Cluster];
    for target in [
        "tenant:t1",
        "namespace:t1/payments",
        "stream:t1/payments/orders",
        "cache:t1/payments/sessions",
    ] {
        let parsed = parse_object(target, "t1").expect("parse");
        assert!(
            !object_within_scope(&ParsedObject::Cluster, &parsed),
            "cluster scope must not reach {target}",
        );
    }

    let rule = PolicyRule {
        subject: "role:ops".to_string(),
        object: "stream:t1/payments/orders".to_string(),
        action: ACTION_STREAM_MANAGE.to_string(),
    };
    assert!(validate_new_rule_allowed(&cluster, "t1", &rule).is_err());
}

#[test]
fn cluster_scope_contains_the_cluster() {
    assert!(object_within_scope(
        &ParsedObject::Cluster,
        &ParsedObject::Cluster
    ));
}

#[test]
fn strict_object_validation_accepts_canonical_forms() {
    let tenant = "t1";
    assert!(parse_object("tenant:t1", tenant).is_ok());
    assert!(parse_object("namespace:t1/payments", tenant).is_ok());
    assert!(parse_object("namespace:t1/*", tenant).is_ok());
    assert!(parse_object("stream:t1/payments/orders", tenant).is_ok());
    assert!(parse_object("stream:t1/payments/*", tenant).is_ok());
    assert!(parse_object("cache:t1/payments/sessions", tenant).is_ok());
    assert!(parse_object("cache:t1/payments/*", tenant).is_ok());
}

#[test]
fn strict_object_validation_rejects_broad_wildcards() {
    let tenant = "t1";
    assert!(parse_object("tenant:*", tenant).is_err());
    assert!(parse_object("stream:*/*", tenant).is_err());
    assert!(parse_object("cache:*/*", tenant).is_err());
}

/// The form token exchange expands `tenant.manage` and `ns.manage` to.
/// A token carrying it has to be readable by the control plane's own
/// resource API, or a tenant admin could not manage their streams.
#[test]
fn a_tenant_wide_stream_or_cache_object_parses_and_sits_under_the_tenant() {
    let tenant = parse_object("tenant:t1", "t1").expect("tenant");
    for raw in ["stream:t1/*/*", "cache:t1/*/*"] {
        let object = parse_object(raw, "t1").expect(raw);
        assert!(object_within_scope(&tenant, &object), "{raw}");
        let named = parse_object(&raw.replace("*/*", "payments/orders"), "t1").expect(raw);
        assert!(object_within_scope(&object, &named), "{raw} covers a name");
    }
    // But not the other way round: a namespace scope does not reach it.
    let namespace = parse_object("namespace:t1/payments", "t1").expect("namespace");
    let all = parse_object("stream:t1/*/*", "t1").expect("all");
    assert!(!object_within_scope(&namespace, &all));
}

#[test]
fn scope_contains_expected_targets() {
    let scope = parse_object("namespace:t1/payments", "t1").expect("scope");
    let in_scope = parse_object("stream:t1/payments/orders", "t1").expect("target");
    let out_of_scope = parse_object("stream:t1/orders/events", "t1").expect("target");
    assert!(object_within_scope(&scope, &in_scope));
    assert!(!object_within_scope(&scope, &out_of_scope));
}

/// **A scope cannot reach into another tenant.** Every object form is
/// checked, because the mismatch is caught per form: one that forgot the
/// check would hand a tenant admin objects belonging to someone else.
#[test]
fn an_object_naming_another_tenant_is_refused() {
    for object in [
        "tenant:t2",
        "namespace:t2/ns",
        "stream:t2/ns/orders",
        "cache:t2/ns/sessions",
    ] {
        assert!(
            parse_object(object, "t1").is_err(),
            "{object} was accepted while acting for t1",
        );
    }
}

/// The object grammar has exactly the shapes documented on `parse_object`.
/// Anything else is refused rather than being read as the nearest match.
#[test]
fn an_object_outside_the_grammar_is_refused() {
    for object in [
        "cluster:everything",
        "node:",
        "node:*",
        "tenant:*",
        "namespace:t1",
        "namespace:t1/ns/extra",
        "stream:t1/ns",
        "stream:t1/ns/orders/extra",
        "cache:t1/ns",
        "orders",
        "",
    ] {
        assert!(
            parse_object(object, "t1").is_err(),
            "{object:?} was accepted",
        );
    }
}

/// A wildcard is allowed only in the last position. `stream:t1/*/orders`
/// would be a grant across namespaces wearing the shape of a single-stream
/// grant, which is the kind of rule a policy review reads past.
#[test]
fn a_wildcard_is_only_allowed_in_the_last_position() {
    assert!(parse_object("stream:t1/ns/*", "t1").is_ok());
    assert!(parse_object("cache:t1/ns/*", "t1").is_ok());
    assert!(parse_object("namespace:t1/*", "t1").is_ok());

    assert!(parse_object("stream:t1/*/orders", "t1").is_err());
    assert!(parse_object("cache:t1/*/sessions", "t1").is_err());
}

/// A segment carrying a separator would re-split differently somewhere
/// else, so it is refused where it is first seen.
#[test]
fn a_segment_containing_a_separator_is_refused() {
    assert!(parse_object("namespace:t1/ns:extra", "t1").is_err());
    assert!(parse_object("node:broker-a", "t1").is_ok());
}

/// Scope containment down the hierarchy: a tenant scope covers everything
/// inside that tenant, and a namespace scope covers the streams and caches
/// inside it.
#[test]
fn a_scope_covers_what_sits_underneath_it() {
    let tenant = parse_object("tenant:t1", "t1").expect("parse");
    for target in [
        "tenant:t1",
        "namespace:t1/ns",
        "stream:t1/ns/orders",
        "cache:t1/ns/sessions",
    ] {
        let target = parse_object(target, "t1").expect("parse");
        assert!(object_within_scope(&tenant, &target), "{target:?}");
    }

    let namespace = parse_object("namespace:t1/ns", "t1").expect("parse");
    for target in [
        "namespace:t1/ns",
        "stream:t1/ns/orders",
        "cache:t1/ns/sessions",
    ] {
        let target = parse_object(target, "t1").expect("parse");
        assert!(object_within_scope(&namespace, &target), "{target:?}");
    }
}

/// **Containment does not run upward.** A stream scope reaching its
/// namespace would turn a single-stream grant into a namespace-wide one.
#[test]
fn a_narrow_scope_does_not_widen_back_out() {
    let stream = parse_object("stream:t1/ns/orders", "t1").expect("parse");
    let namespace = parse_object("namespace:t1/ns", "t1").expect("parse");
    let tenant = parse_object("tenant:t1", "t1").expect("parse");

    assert!(!object_within_scope(&stream, &namespace));
    assert!(!object_within_scope(&stream, &tenant));
    assert!(!object_within_scope(&namespace, &tenant));
}

/// A wildcard scope covers any name in that position, but an exact scope is
/// not covered by a wildcard target: `stream:t1/ns/orders` does not confer
/// `stream:t1/ns/*`.
#[test]
fn a_wildcard_scope_covers_names_but_a_name_does_not_cover_the_wildcard() {
    let any = parse_object("stream:t1/ns/*", "t1").expect("parse");
    let one = parse_object("stream:t1/ns/orders", "t1").expect("parse");

    assert!(object_within_scope(&any, &one));
    assert!(!object_within_scope(&one, &any));
}

/// A stream scope and a cache scope are different objects even when they
/// are spelled alike.
#[test]
fn a_stream_scope_does_not_cover_a_cache_of_the_same_name() {
    let stream = parse_object("stream:t1/ns/orders", "t1").expect("parse");
    let cache = parse_object("cache:t1/ns/orders", "t1").expect("parse");

    assert!(!object_within_scope(&stream, &cache));
    assert!(!object_within_scope(&cache, &stream));
}

/// A node scope neither covers nor is covered by a tenant object.
#[test]
fn a_node_scope_and_a_tenant_object_do_not_meet() {
    let node = parse_object("node:broker-a", "t1").expect("parse");
    let stream = parse_object("stream:t1/ns/orders", "t1").expect("parse");

    assert!(!object_within_scope(&node, &stream));
    assert!(!object_within_scope(&stream, &node));
}

/// Delegation: **a caller cannot write a rule wider than what it holds.**
#[test]
fn a_rule_outside_the_callers_scope_is_refused() {
    let caller = vec![parse_object("namespace:t1/ns", "t1").expect("parse")];

    let inside = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/ns/orders".to_string(),
        action: ACTION_STREAM_PUBLISH.to_string(),
    };
    assert!(validate_new_rule_allowed(&caller, "t1", &inside).is_ok());

    let outside = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/other/orders".to_string(),
        action: ACTION_STREAM_PUBLISH.to_string(),
    };
    assert!(validate_new_rule_allowed(&caller, "t1", &outside).is_err());
}

/// An action the enforcer does not know is refused before anything else is
/// considered — a rule that grants an unrecognised verb is a rule nobody
/// can reason about.
#[test]
fn a_rule_with_an_unknown_action_is_refused() {
    let caller = vec![parse_object("tenant:t1", "t1").expect("parse")];
    let rule = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/ns/orders".to_string(),
        action: "teleport".to_string(),
    };
    assert!(validate_new_rule_allowed(&caller, "t1", &rule).is_err());
}

/// Assigning a role hands over every policy that role carries, so **each
/// one** has to be inside the caller's scope — checking only the first
/// would let one wide policy ride along behind a narrow one.
#[test]
fn assigning_a_role_checks_every_policy_it_carries() {
    let caller = vec![parse_object("namespace:t1/ns", "t1").expect("parse")];
    let assignment = GroupingRule {
        user: "someone".to_string(),
        role: "reader".to_string(),
    };
    let inside = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/ns/orders".to_string(),
        action: ACTION_STREAM_PUBLISH.to_string(),
    };
    let outside = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/other/orders".to_string(),
        action: ACTION_STREAM_PUBLISH.to_string(),
    };

    assert!(
        validate_assignment_allowed(&caller, "t1", &assignment, std::slice::from_ref(&inside))
            .is_ok()
    );
    assert!(
        validate_assignment_allowed(&caller, "t1", &assignment, &[inside, outside]).is_err(),
        "a policy outside the caller's scope rode along behind one inside it",
    );
}

/// A role with no policies grants nothing, and an assignment naming no user
/// or no role is not a request that can be honoured.
#[test]
fn an_empty_assignment_is_refused() {
    let caller = vec![parse_object("tenant:t1", "t1").expect("parse")];
    let policy = PolicyRule {
        subject: "reader".to_string(),
        object: "stream:t1/ns/orders".to_string(),
        action: ACTION_STREAM_PUBLISH.to_string(),
    };

    for (user, role) in [("", "reader"), ("someone", ""), ("  ", "reader")] {
        let assignment = GroupingRule {
            user: user.to_string(),
            role: role.to_string(),
        };
        assert!(
            validate_assignment_allowed(&caller, "t1", &assignment, std::slice::from_ref(&policy))
                .is_err(),
            "user={user:?} role={role:?}",
        );
    }

    let assignment = GroupingRule {
        user: "someone".to_string(),
        role: "reader".to_string(),
    };
    assert!(validate_assignment_allowed(&caller, "t1", &assignment, &[]).is_err());
}

/// A group object names one consumer group of one stream, with a wildcard
/// allowed only under wildcards, as for a stream.
#[test]
fn a_group_object_parses_with_the_stream_wildcard_rule() {
    for object in [
        "group:t1/ns/orders/workers",
        "group:t1/ns/orders/*",
        "group:t1/ns/*/*",
        "group:t1/*/*/*",
    ] {
        assert!(parse_object(object, "t1").is_ok(), "{object:?} was refused");
    }
    for object in [
        "group:t1/ns/*/workers",
        "group:t1/*/orders/*",
        "group:t1/ns/orders",
        "group:t1/ns/orders/workers/extra",
        "group:t2/ns/orders/workers",
        "group:t1/ns/orders/a:b",
    ] {
        assert!(
            parse_object(object, "t1").is_err(),
            "{object:?} was accepted"
        );
    }
}

/// A delegated admin can grant a group inside whatever scope they hold over
/// its stream, and a group scope reaches only its own groups.
#[test]
fn a_group_sits_under_its_stream_namespace_and_tenant() {
    let group = parse_object("group:t1/ns/orders/workers", "t1").expect("parse");
    for scope in [
        "tenant:t1",
        "namespace:t1/ns",
        "stream:t1/ns/orders",
        "stream:t1/ns/*",
        "group:t1/ns/orders/*",
        "group:t1/ns/orders/workers",
    ] {
        let scope = parse_object(scope, "t1").expect("parse");
        assert!(object_within_scope(&scope, &group), "{scope:?}");
    }
    for scope in [
        "stream:t1/ns/refunds",
        "group:t1/ns/orders/billing",
        "cache:t1/ns/*",
    ] {
        let scope = parse_object(scope, "t1").expect("parse");
        assert!(!object_within_scope(&scope, &group), "{scope:?}");
    }
    let stream = parse_object("stream:t1/ns/orders", "t1").expect("parse");
    let all_groups = parse_object("group:t1/ns/orders/*", "t1").expect("parse");
    assert!(!object_within_scope(&all_groups, &stream));
}
