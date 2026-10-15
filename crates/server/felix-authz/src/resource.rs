//! Canonical resource strings for policies. Always build them through these
//! helpers — a hand-rolled string that drops the `/` separator or the kind
//! prefix silently stops matching wildcards.
use crate::{CacheScope, GroupName, Namespace, StreamName, TenantId};

/// `cluster:*`, the cluster itself. Outside every tenant, and the only
/// cluster object there is: a grant on it is spelled exactly this way.
pub const CLUSTER_RESOURCE: &str = "cluster:*";

/// `tenant:{id}`
pub fn tenant_resource(tenant_id: &TenantId) -> String {
    format!("tenant:{}", tenant_id.as_str())
}

/// `namespace:{tenant}/{namespace}`
pub fn namespace_resource(tenant_id: &TenantId, namespace: &Namespace) -> String {
    format!("namespace:{}/{}", tenant_id.as_str(), namespace.as_str())
}

/// `stream:{tenant}/{namespace}/{stream}`
pub fn stream_resource(tenant_id: &TenantId, namespace: &Namespace, stream: &StreamName) -> String {
    format!(
        "stream:{}/{}/{}",
        tenant_id.as_str(),
        namespace.as_str(),
        stream.as_str()
    )
}

/// `group:{tenant}/{namespace}/{stream}/{group}`: one consumer group of one
/// stream, across all its shards. See [`crate::PermissionMatcher::allows_group`]
/// for how stream grants reach it.
pub fn group_resource(
    tenant_id: &TenantId,
    namespace: &Namespace,
    stream: &StreamName,
    group: &GroupName,
) -> String {
    format!(
        "{}{}",
        group_prefix(tenant_id, namespace, stream),
        group.as_str()
    )
}

/// `group:{tenant}/{namespace}/{stream}/`, what every group of the stream
/// starts with.
pub(crate) fn group_prefix(
    tenant_id: &TenantId,
    namespace: &Namespace,
    stream: &StreamName,
) -> String {
    format!(
        "group:{}/{}/{}/",
        tenant_id.as_str(),
        namespace.as_str(),
        stream.as_str()
    )
}

/// `cache:{tenant}/{namespace}/{cache}`
pub fn cache_resource(tenant_id: &TenantId, namespace: &Namespace, cache: &CacheScope) -> String {
    format!(
        "cache:{}/{}/{}",
        tenant_id.as_str(),
        namespace.as_str(),
        cache.as_str()
    )
}

#[cfg(test)]
mod tests;
