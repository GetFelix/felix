//! RBAC authorization helpers for strict object parsing and delegated admin checks.
//!
//! This module centralizes the policy grammar and scope math used by admin APIs.
//! Keeping these checks in one place avoids privilege-escalation drift across
//! endpoints and makes future deny/simulation work additive.
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};

pub const ACTION_RBAC_VIEW: &str = "rbac.view";
pub const ACTION_RBAC_POLICY_MANAGE: &str = "rbac.policy.manage";
pub const ACTION_RBAC_ASSIGNMENT_MANAGE: &str = "rbac.assignment.manage";
pub const ACTION_TENANT_MANAGE: &str = "tenant.manage";
pub const ACTION_NS_MANAGE: &str = "ns.manage";
pub const ACTION_STREAM_MANAGE: &str = "stream.manage";
pub const ACTION_CACHE_MANAGE: &str = "cache.manage";
pub const ACTION_STREAM_PUBLISH: &str = "stream.publish";
pub const ACTION_STREAM_SUBSCRIBE: &str = "stream.subscribe";
pub const ACTION_CACHE_READ: &str = "cache.read";
pub const ACTION_CACHE_WRITE: &str = "cache.write";
/// Work a stream's consumer groups. `stream.subscribe` also grants it.
pub const ACTION_GROUP_CONSUME: &str = "group.consume";
/// Redrive or discard a stream's dead letters. `stream.manage` also grants it.
pub const ACTION_GROUP_MANAGE: &str = "group.manage";
/// Read cluster membership. Cluster-scoped, so it is never reachable from a
/// tenant scope -- see [`ParsedObject::Cluster`].
pub const ACTION_NODE_VIEW: &str = "node.view";
/// Claim or change a node's membership: register, report health, drain, leave.
///
/// Granted over `node:{node_id}` for a broker, which is what stops one broker
/// speaking for another, or over `cluster:*` for an operator that manages the
/// whole fleet.
pub const ACTION_NODE_MANAGE: &str = "node.manage";

/// Validate and normalize RBAC action names.
pub fn canonical_action(action: &str) -> Option<&'static str> {
    match action {
        ACTION_RBAC_VIEW => Some(ACTION_RBAC_VIEW),
        ACTION_RBAC_POLICY_MANAGE => Some(ACTION_RBAC_POLICY_MANAGE),
        ACTION_RBAC_ASSIGNMENT_MANAGE => Some(ACTION_RBAC_ASSIGNMENT_MANAGE),
        ACTION_TENANT_MANAGE => Some(ACTION_TENANT_MANAGE),
        ACTION_NS_MANAGE => Some(ACTION_NS_MANAGE),
        ACTION_STREAM_MANAGE => Some(ACTION_STREAM_MANAGE),
        ACTION_CACHE_MANAGE => Some(ACTION_CACHE_MANAGE),
        ACTION_STREAM_PUBLISH => Some(ACTION_STREAM_PUBLISH),
        ACTION_STREAM_SUBSCRIBE => Some(ACTION_STREAM_SUBSCRIBE),
        ACTION_CACHE_READ => Some(ACTION_CACHE_READ),
        ACTION_CACHE_WRITE => Some(ACTION_CACHE_WRITE),
        ACTION_GROUP_CONSUME => Some(ACTION_GROUP_CONSUME),
        ACTION_GROUP_MANAGE => Some(ACTION_GROUP_MANAGE),
        ACTION_NODE_VIEW => Some(ACTION_NODE_VIEW),
        ACTION_NODE_MANAGE => Some(ACTION_NODE_MANAGE),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPermission {
    pub action: String,
    pub object: ParsedObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedObject {
    /// One named node.
    ///
    /// The scope a broker's own credential carries. It contains only itself, so
    /// a broker holding `node.manage:node:broker-a` cannot register, drain, or
    /// report health for `broker-b` -- which is the whole reason node identity
    /// is an object rather than a field the caller asserts.
    Node {
        node_id: String,
    },
    /// The cluster itself: brokers, their liveness, their placement standing.
    ///
    /// Deliberately outside the tenant hierarchy. No tenant scope contains it,
    /// so a tenant admin cannot grant it to themselves through
    /// [`validate_new_rule_allowed`], which only admits rules already inside the
    /// caller's scope. It reaches a token only when an operator who already has
    /// cluster scope writes the rule.
    Cluster,
    Tenant {
        tenant_id: String,
    },
    Namespace {
        tenant_id: String,
        namespace: Segment,
    },
    Stream {
        tenant_id: String,
        namespace: Segment,
        stream: Segment,
    },
    Cache {
        tenant_id: String,
        namespace: Segment,
        cache: Segment,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Exact(String),
    Any,
}

pub fn parse_permission(raw: &str, tenant_id: &str) -> Result<ParsedPermission, String> {
    let (action, object) = raw
        .split_once(':')
        .ok_or_else(|| "invalid permission format".to_string())?;
    let action = canonical_action(action)
        .ok_or_else(|| format!("unknown action: {action}"))?
        .to_string();
    let object = parse_object(object, tenant_id)?;
    Ok(ParsedPermission { action, object })
}

/// Parse an RBAC object string into a typed structure.
///
/// Canonical grammar:
/// - `cluster:*`
/// - `node:{node_id}`
/// - `tenant:{tenant_id}`
/// - `namespace:{tenant_id}/{namespace}`
/// - `stream:{tenant_id}/{namespace}/{stream}`
/// - `cache:{tenant_id}/{namespace}/{cache}`
pub fn parse_object(raw: &str, tenant_id: &str) -> Result<ParsedObject, String> {
    if raw == "tenant:*" {
        return Err("tenant:* is not allowed".to_string());
    }

    // Checked before the tenant-scoped forms, and without consulting
    // `tenant_id`: the cluster belongs to no tenant.
    if raw == "cluster:*" {
        return Ok(ParsedObject::Cluster);
    }
    if raw.starts_with("cluster:") {
        return Err("the only cluster object is cluster:*".to_string());
    }

    // Also tenant-independent: a node belongs to the cluster, not to a tenant.
    if let Some(node_id) = raw.strip_prefix("node:") {
        if node_id.is_empty() || node_id == "*" {
            // `node:*` would be `cluster:*` by another name, and having two
            // spellings for one scope is how a policy review misses one.
            return Err("node objects must name one node; use cluster:* for all".to_string());
        }
        return Ok(ParsedObject::Node {
            node_id: node_id.to_string(),
        });
    }

    if let Some(rest) = raw.strip_prefix("tenant:") {
        if rest != tenant_id {
            return Err("tenant object must match request tenant".to_string());
        }
        return Ok(ParsedObject::Tenant {
            tenant_id: tenant_id.to_string(),
        });
    }
    if let Some(rest) = raw.strip_prefix("namespace:") {
        let (tid, ns) = split2(rest)?;
        if tid != tenant_id {
            return Err("namespace object tenant mismatch".to_string());
        }
        return Ok(ParsedObject::Namespace {
            tenant_id: tid.to_string(),
            namespace: parse_segment(ns, true)?,
        });
    }
    if let Some(rest) = raw.strip_prefix("stream:") {
        let (tid, ns, stream) = split3(rest)?;
        if tid != tenant_id {
            return Err("stream object tenant mismatch".to_string());
        }
        let (namespace, stream) = parse_leaf_segments(ns, stream)?;
        return Ok(ParsedObject::Stream {
            tenant_id: tid.to_string(),
            namespace,
            stream,
        });
    }
    if let Some(rest) = raw.strip_prefix("cache:") {
        let (tid, ns, cache) = split3(rest)?;
        if tid != tenant_id {
            return Err("cache object tenant mismatch".to_string());
        }
        let (namespace, cache) = parse_leaf_segments(ns, cache)?;
        return Ok(ParsedObject::Cache {
            tenant_id: tid.to_string(),
            namespace,
            cache,
        });
    }

    Err("object does not match RBAC grammar".to_string())
}

pub fn object_within_scope(scope: &ParsedObject, target: &ParsedObject) -> bool {
    match (scope, target) {
        // Cluster scope is its own island in both directions. A tenant scope
        // does not reach it, which is what stops a tenant admin granting
        // themselves cluster access; and cluster scope confers nothing inside a
        // tenant, so it cannot be used to read tenant data either.
        (ParsedObject::Cluster, ParsedObject::Cluster) => true,
        // Cluster scope covers every node, which is what an operator managing
        // the fleet holds.
        (ParsedObject::Cluster, ParsedObject::Node { .. }) => true,
        (ParsedObject::Cluster, _) | (_, ParsedObject::Cluster) => false,
        // A node scope is exactly one node. No wildcard, no hierarchy: this is
        // the boundary that stops one broker acting as another.
        (ParsedObject::Node { node_id: scope }, ParsedObject::Node { node_id: target }) => {
            scope == target
        }
        (ParsedObject::Node { .. }, _) | (_, ParsedObject::Node { .. }) => false,
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Tenant { tenant_id: t }) => s == t,
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Namespace { tenant_id: t, .. }) => {
            s == t
        }
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Stream { tenant_id: t, .. }) => {
            s == t
        }
        (ParsedObject::Tenant { tenant_id: s }, ParsedObject::Cache { tenant_id: t, .. }) => s == t,
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Namespace {
                tenant_id: tt,
                namespace: tns,
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Stream {
                tenant_id: tt,
                namespace: tns,
                ..
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Namespace {
                tenant_id: st,
                namespace: sns,
            },
            ParsedObject::Cache {
                tenant_id: tt,
                namespace: tns,
                ..
            },
        ) => st == tt && segment_contains(sns, tns),
        (
            ParsedObject::Stream {
                tenant_id: st,
                namespace: sns,
                stream: sstream,
            },
            ParsedObject::Stream {
                tenant_id: tt,
                namespace: tns,
                stream: tstream,
            },
        ) => st == tt && segment_contains(sns, tns) && segment_contains(sstream, tstream),
        (
            ParsedObject::Cache {
                tenant_id: st,
                namespace: sns,
                cache: scache,
            },
            ParsedObject::Cache {
                tenant_id: tt,
                namespace: tns,
                cache: tcache,
            },
        ) => st == tt && segment_contains(sns, tns) && segment_contains(scache, tcache),
        _ => false,
    }
}

/// The part of `granted` that `requested` also covers, as the same kind of
/// object as `granted`; `None` when they do not overlap.
///
/// The kind is kept so a narrowed permission still names what its action
/// acts on: `tenant.manage` narrowed to a stream would be a permission no
/// check ever matches.
pub fn narrow_object(granted: &ParsedObject, requested: &ParsedObject) -> Option<ParsedObject> {
    if object_within_scope(requested, granted) {
        return Some(granted.clone());
    }
    let narrowed = if object_within_scope(granted, requested) {
        requested.clone()
    } else {
        // Not nested either way, but a namespace can still cut across a
        // tenant-wide leaf grant: `stream:t1/*/*` within `namespace:t1/ns`
        // is `stream:t1/ns/*`.
        match (granted, requested) {
            (
                ParsedObject::Stream {
                    tenant_id,
                    namespace: Segment::Any,
                    stream,
                },
                ParsedObject::Namespace {
                    tenant_id: requested_tenant,
                    namespace: namespace @ Segment::Exact(_),
                },
            ) if tenant_id == requested_tenant => ParsedObject::Stream {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                stream: stream.clone(),
            },
            (
                ParsedObject::Cache {
                    tenant_id,
                    namespace: Segment::Any,
                    cache,
                },
                ParsedObject::Namespace {
                    tenant_id: requested_tenant,
                    namespace: namespace @ Segment::Exact(_),
                },
            ) if tenant_id == requested_tenant => ParsedObject::Cache {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
            },
            _ => return None,
        }
    };
    (std::mem::discriminant(&narrowed) == std::mem::discriminant(granted)).then_some(narrowed)
}

/// The canonical string for an object; the inverse of [`parse_object`].
pub fn format_object(object: &ParsedObject) -> String {
    fn segment(value: &Segment) -> &str {
        match value {
            Segment::Exact(value) => value,
            Segment::Any => "*",
        }
    }
    match object {
        ParsedObject::Cluster => "cluster:*".to_string(),
        ParsedObject::Node { node_id } => format!("node:{node_id}"),
        ParsedObject::Tenant { tenant_id } => format!("tenant:{tenant_id}"),
        ParsedObject::Namespace {
            tenant_id,
            namespace,
        } => format!("namespace:{tenant_id}/{}", segment(namespace)),
        ParsedObject::Stream {
            tenant_id,
            namespace,
            stream,
        } => format!(
            "stream:{tenant_id}/{}/{}",
            segment(namespace),
            segment(stream)
        ),
        ParsedObject::Cache {
            tenant_id,
            namespace,
            cache,
        } => format!(
            "cache:{tenant_id}/{}/{}",
            segment(namespace),
            segment(cache)
        ),
    }
}

/// Validate that a new/updated policy rule stays inside caller delegation scope.
pub fn validate_new_rule_allowed(
    caller_scopes: &[ParsedObject],
    tenant_id: &str,
    rule: &PolicyRule,
) -> Result<ParsedObject, String> {
    canonical_action(&rule.action).ok_or_else(|| "unknown action".to_string())?;
    let parsed = parse_object(&rule.object, tenant_id)?;
    if caller_scopes
        .iter()
        .any(|scope| object_within_scope(scope, &parsed))
    {
        Ok(parsed)
    } else {
        Err("scope does not allow policy object".to_string())
    }
}

/// Validate that role assignment cannot grant privileges beyond caller scope.
pub fn validate_assignment_allowed(
    caller_scopes: &[ParsedObject],
    tenant_id: &str,
    assignment: &GroupingRule,
    role_policies: &[PolicyRule],
) -> Result<(), String> {
    if assignment.user.trim().is_empty() || assignment.role.trim().is_empty() {
        return Err("invalid grouping payload".to_string());
    }
    if role_policies.is_empty() {
        return Err("role has no policies".to_string());
    }
    for policy in role_policies {
        let parsed = parse_object(&policy.object, tenant_id)?;
        if !caller_scopes
            .iter()
            .any(|scope| object_within_scope(scope, &parsed))
        {
            return Err("role policy exceeds assignment scope".to_string());
        }
    }
    Ok(())
}

/// The namespace and leaf of a stream or cache object.
///
/// A wildcard namespace is allowed only under a wildcard leaf: `t1/*/*` is
/// "every stream in the tenant", which is what token exchange expands a
/// tenant-wide grant to. `t1/*/orders` is refused -- a grant across
/// namespaces wearing the shape of a single-stream grant is the kind of rule
/// a policy review reads past.
fn parse_leaf_segments(ns: &str, leaf: &str) -> Result<(Segment, Segment), String> {
    let leaf = parse_segment(leaf, true)?;
    let namespace = parse_segment(ns, leaf == Segment::Any)?;
    Ok((namespace, leaf))
}

fn parse_segment(raw: &str, allow_star: bool) -> Result<Segment, String> {
    if raw == "*" {
        if allow_star {
            return Ok(Segment::Any);
        }
        return Err("wildcard not allowed in this object position".to_string());
    }
    if raw.is_empty() {
        return Err("empty object segment".to_string());
    }
    if raw.contains(':') {
        return Err("invalid object segment".to_string());
    }
    Ok(Segment::Exact(raw.to_string()))
}

fn segment_contains(scope: &Segment, target: &Segment) -> bool {
    match (scope, target) {
        (Segment::Any, _) => true,
        (Segment::Exact(left), Segment::Exact(right)) => left == right,
        (Segment::Exact(_), Segment::Any) => false,
    }
}

fn split2(input: &str) -> Result<(&str, &str), String> {
    let (a, b) = input
        .split_once('/')
        .ok_or_else(|| "invalid object shape".to_string())?;
    if b.contains('/') {
        return Err("invalid object shape".to_string());
    }
    Ok((a, b))
}

fn split3(input: &str) -> Result<(&str, &str, &str), String> {
    let mut parts = input.split('/');
    let a = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    let b = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    let c = parts
        .next()
        .ok_or_else(|| "invalid object shape".to_string())?;
    if parts.next().is_some() {
        return Err("invalid object shape".to_string());
    }
    Ok((a, b, c))
}

#[cfg(test)]
mod tests;
