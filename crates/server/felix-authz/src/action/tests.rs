use super::Action;

#[test]
fn action_string_roundtrip() {
    let actions = [
        Action::RbacView,
        Action::RbacPolicyManage,
        Action::RbacAssignmentManage,
        Action::TenantManage,
        Action::NamespaceManage,
        Action::StreamManage,
        Action::CacheManage,
        Action::StreamPublish,
        Action::StreamSubscribe,
        Action::CacheRead,
        Action::CacheWrite,
        Action::GroupConsume,
        Action::GroupManage,
    ];

    for action in actions {
        let as_str = action.as_str();
        assert_eq!(
            <Action as std::str::FromStr>::from_str(as_str).ok(),
            Some(action)
        );
        assert_eq!(action.to_string(), as_str);
    }
}

#[test]
fn action_from_str_invalid() {
    assert!(<Action as std::str::FromStr>::from_str("tenant.write").is_err());
}

/// Reading a stream lets a principal work its groups, as it always has, but
/// operating a group's dead letters takes managing the stream.
#[test]
fn group_actions_follow_the_stream_grants_that_covered_them() {
    assert!(Action::GroupConsume.is_granted_by(Action::GroupConsume));
    assert!(Action::GroupConsume.is_granted_by(Action::StreamSubscribe));
    assert!(Action::GroupManage.is_granted_by(Action::GroupManage));
    assert!(Action::GroupManage.is_granted_by(Action::StreamManage));

    assert!(!Action::GroupManage.is_granted_by(Action::StreamSubscribe));
    assert!(!Action::GroupManage.is_granted_by(Action::GroupConsume));
    assert!(!Action::GroupConsume.is_granted_by(Action::StreamPublish));
    assert!(!Action::StreamSubscribe.is_granted_by(Action::GroupConsume));
}
