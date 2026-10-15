use clap::Parser;
use serde_json::json;

use super::*;
use crate::cli::{Cli, Command};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("felixctl").chain(args.iter().copied())).expect("parses")
}

fn policy(object: &str, action: &str) -> PolicyArgs {
    PolicyArgs {
        subject: "role:r".to_string(),
        object: object.to_string(),
        action: action.to_string(),
    }
}

#[test]
fn policy_commands_parse() {
    let cli = parse(&[
        "rbac",
        "policy",
        "add",
        "role:reader",
        "cache:t1/ns/c/user:*",
        "cache.read",
    ]);
    let Some(Command::Rbac(RbacCommand::Policy(PolicyCommand::Add(args)))) = cli.command else {
        panic!("not policy add: {:?}", cli.command);
    };
    assert_eq!(args.subject, "role:reader");
    assert_eq!(args.object, "cache:t1/ns/c/user:*");
    assert_eq!(args.action, "cache.read");

    let cli = parse(&[
        "rbac",
        "policy",
        "rm",
        "role:r",
        "tenant:t1",
        "ns.manage",
        "-y",
    ]);
    let Some(Command::Rbac(RbacCommand::Policy(PolicyCommand::Rm { policy, confirm }))) =
        cli.command
    else {
        panic!("not policy rm: {:?}", cli.command);
    };
    assert!(confirm.yes);
    assert_eq!(policy.object, "tenant:t1");

    let cli = parse(&["rbac", "policy", "ls", "--subject", "role:r", "--json"]);
    assert!(cli.json);
    let Some(Command::Rbac(RbacCommand::Policy(PolicyCommand::Ls { subject }))) = cli.command
    else {
        panic!("not policy ls: {:?}", cli.command);
    };
    assert_eq!(subject.as_deref(), Some("role:r"));
}

#[test]
fn grouping_commands_parse() {
    let cli = parse(&["rbac", "grouping", "rm", "p:alice", "role:reader"]);
    let Some(Command::Rbac(RbacCommand::Grouping(GroupingCommand::Rm { grouping, confirm }))) =
        cli.command
    else {
        panic!("not grouping rm: {:?}", cli.command);
    };
    assert!(!confirm.yes);
    assert_eq!(grouping.user, "p:alice");
    assert_eq!(grouping.role, "role:reader");

    let cli = parse(&["rbac", "grouping", "ls", "--role", "role:reader"]);
    let Some(Command::Rbac(RbacCommand::Grouping(GroupingCommand::Ls { user, role }))) =
        cli.command
    else {
        panic!("not grouping ls: {:?}", cli.command);
    };
    assert_eq!(user, None);
    assert_eq!(role.as_deref(), Some("role:reader"));
}

#[test]
fn a_policy_without_an_action_is_a_usage_error() {
    let err = Cli::try_parse_from(["felixctl", "rbac", "policy", "add", "role:r", "tenant:t1"])
        .unwrap_err();
    assert_eq!(err.exit_code(), 2);
}

#[test]
fn request_bodies_carry_the_rule_as_given() {
    assert_eq!(
        policy_body(&policy("stream:t1/ns/orders", "stream.publish")),
        json!({"subject": "role:r", "object": "stream:t1/ns/orders", "action": "stream.publish"})
    );
    let grouping = GroupingArgs {
        user: "p:alice".to_string(),
        role: "role:r".to_string(),
    };
    assert_eq!(
        grouping_body(&grouping),
        json!({"user": "p:alice", "role": "role:r"})
    );
}

#[test]
fn objects_the_control_plane_accepts_pass() {
    for (object, kind) in [
        ("cluster:*", ObjectKind::Cluster),
        ("node:broker-1", ObjectKind::Node),
        ("tenant:t1", ObjectKind::Tenant),
        ("namespace:t1/*", ObjectKind::Namespace),
        ("namespace:t1/payments", ObjectKind::Namespace),
        ("stream:t1/payments/orders", ObjectKind::Stream),
        ("stream:t1/payments/*", ObjectKind::Stream),
        ("stream:t1/*/*", ObjectKind::Stream),
        ("cache:t1/ns/sessions", ObjectKind::Cache),
        ("cache:t1/*/*", ObjectKind::Cache),
        ("cache:t1/ns/sessions/user-1", ObjectKind::CacheKey),
        ("cache:t1/ns/sessions/user:*", ObjectKind::CacheKey),
        // A key may hold `/` and `:`; only the cache part may not.
        ("cache:t1/ns/sessions/a/b:c", ObjectKind::CacheKey),
        ("group:t1/ns/orders/billing", ObjectKind::Group),
        ("group:t1/ns/orders/*", ObjectKind::Group),
        ("group:t1/*/*/*", ObjectKind::Group),
        ("future:anything", ObjectKind::Other),
    ] {
        assert_eq!(check_object(object, "t1"), Ok(kind), "{object}");
    }
}

#[test]
fn objects_the_control_plane_refuses_fail() {
    for object in [
        "orders",
        "cluster:t1",
        "node:",
        "node:*",
        "tenant:*",
        "tenant:t2",
        "namespace:t1",
        "namespace:t1/a/b",
        "namespace:t1/",
        "stream:t2/ns/orders",
        "stream:t1/*/orders",
        "stream:t1/ns",
        "stream:t1/ns/a:b",
        "cache:t1/ns/sessions/",
        "cache:t1/ns/sessions/*",
        "cache:t1/ns/sessions/a*b",
        "cache:t1/ns/sessions/a**",
        "cache:t1/ns/*/user-1",
        "cache:t1/*/*/user-1",
        "cache:t2/ns/sessions/user-1",
        "group:t1/ns/*/billing",
        "group:t1/*/orders/*",
        "group:t1/ns/orders",
    ] {
        assert!(check_object(object, "t1").is_err(), "{object} passed");
    }
}

#[test]
fn a_key_object_takes_only_data_actions() {
    let object = "cache:t1/ns/sessions/user:*";
    assert_eq!(
        check_policy(&policy(object, "cache.read"), "t1"),
        Ok(ObjectKind::CacheKey)
    );
    assert!(check_policy(&policy(object, "cache.write"), "t1").is_ok());
    let err = check_policy(&policy(object, "cache.manage"), "t1").unwrap_err();
    assert!(err.contains("cache.read or cache.write"), "{err}");
    // The whole cache takes any action; the control plane judges the name.
    assert!(check_policy(&policy("cache:t1/ns/sessions", "cache.manage"), "t1").is_ok());
}

#[test]
fn a_refusal_names_the_object() {
    let err = check_policy(&policy("stream:t2/ns/orders", "stream.publish"), "t1").unwrap_err();
    assert!(err.contains("stream:t2/ns/orders"), "{err}");
    assert!(err.contains("t1"), "{err}");
}
