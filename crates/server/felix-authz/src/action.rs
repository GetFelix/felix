//! The action vocabulary. These strings are persisted in policies, so they
//! are frozen: renaming one breaks every stored rule that uses it, and a new
//! variant is invisible until `FromStr` learns it too.
use serde::{Deserialize, Serialize};

/// Authorization actions used in policy and permission checks. The serialized
/// form is snake_case and stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    RbacView,
    RbacPolicyManage,
    RbacAssignmentManage,
    TenantManage,
    NamespaceManage,
    StreamManage,
    CacheManage,
    StreamPublish,
    StreamSubscribe,
    CacheRead,
    CacheWrite,
    /// Work a stream's consumer groups: poll, acknowledge, hand back, and list
    /// dead letters. Granted by `stream.subscribe` too; see
    /// [`Action::is_granted_by`].
    GroupConsume,
    /// Operate a stream's consumer groups: redrive or discard a dead letter.
    /// Granted by `stream.manage` too.
    GroupManage,
    /// Read the cluster's state: membership, assignments, and a broker's view
    /// of a shard. Only ever granted over `cluster:*`; see
    /// [`crate::PermissionMatcher::allows_cluster`].
    NodeView,
    /// Change a node's membership. Granted over `node:{id}` to a broker and
    /// over `cluster:*` to an operator. Brokers enforce nothing with it; it is
    /// here so a token that carries it still parses.
    NodeManage,
    /// Exchange a user's token for one that names the holder as its actor
    /// (`act`), to present on the user's behalf. Granted over
    /// `tenant:{tenant_id}`; only the control plane enforces it.
    TokenDelegate,
}

impl Action {
    /// The persisted identifier, e.g. `"stream.publish"`. Must stay in
    /// lockstep with `FromStr`.
    pub fn as_str(self) -> &'static str {
        match self {
            Action::RbacView => "rbac.view",
            Action::RbacPolicyManage => "rbac.policy.manage",
            Action::RbacAssignmentManage => "rbac.assignment.manage",
            Action::TenantManage => "tenant.manage",
            Action::NamespaceManage => "ns.manage",
            Action::StreamManage => "stream.manage",
            Action::CacheManage => "cache.manage",
            Action::StreamPublish => "stream.publish",
            Action::StreamSubscribe => "stream.subscribe",
            Action::CacheRead => "cache.read",
            Action::CacheWrite => "cache.write",
            Action::GroupConsume => "group.consume",
            Action::GroupManage => "group.manage",
            Action::NodeView => "node.view",
            Action::NodeManage => "node.manage",
            Action::TokenDelegate => "token.delegate",
        }
    }

    /// Whether a grant of `granted` allows `self`.
    ///
    /// Group actions arrived after policies granting `stream.subscribe` and
    /// `stream.manage` were already deployed, and those grants covered group
    /// work. Honouring them here keeps every existing consumer working; the
    /// new actions let a policy grant group work without the stream action.
    pub fn is_granted_by(self, granted: Action) -> bool {
        self == granted
            || matches!(
                (self, granted),
                (Action::GroupConsume, Action::StreamSubscribe)
                    | (Action::GroupManage, Action::StreamManage)
            )
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Action {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rbac.view" => Ok(Action::RbacView),
            "rbac.policy.manage" => Ok(Action::RbacPolicyManage),
            "rbac.assignment.manage" => Ok(Action::RbacAssignmentManage),
            "tenant.manage" => Ok(Action::TenantManage),
            "ns.manage" => Ok(Action::NamespaceManage),
            "stream.manage" => Ok(Action::StreamManage),
            "cache.manage" => Ok(Action::CacheManage),
            "stream.publish" => Ok(Action::StreamPublish),
            "stream.subscribe" => Ok(Action::StreamSubscribe),
            "cache.read" => Ok(Action::CacheRead),
            "cache.write" => Ok(Action::CacheWrite),
            "group.consume" => Ok(Action::GroupConsume),
            "group.manage" => Ok(Action::GroupManage),
            "node.view" => Ok(Action::NodeView),
            "node.manage" => Ok(Action::NodeManage),
            "token.delegate" => Ok(Action::TokenDelegate),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests;
