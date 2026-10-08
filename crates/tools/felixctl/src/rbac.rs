//! `rbac policy` and `rbac grouping`: list, add and remove the tenant's RBAC
//! rules over the control plane's `/rbac/policies` and `/rbac/groupings`.
//!
//! Policy objects are checked here against the control plane's grammar before
//! anything is sent, so a typo fails with a reason instead of a bare 400. An
//! object of a kind this build does not know is sent as is: a newer control
//! plane may accept it, and it checks every object again either way. Action
//! names are left to the control plane for the same reason.

use serde_json::{Value, json};

use crate::cli::{GroupingArgs, GroupingCommand, PolicyArgs, PolicyCommand, RbacCommand};
use crate::context::Settings;
use crate::controlplane::{Api, Column, rows, segment};
use crate::error::{Exit, fail};
use crate::manage::ask;
use crate::output::{Output, table};

const POLICY_COLUMNS: &[Column] = &[
    ("SUBJECT", "/subject"),
    ("OBJECT", "/object"),
    ("ACTION", "/action"),
];
const GROUPING_COLUMNS: &[Column] = &[("USER", "/user"), ("ROLE", "/role")];

/// The actions a cache key or key prefix object can carry. Anything else on
/// a key is refused by the control plane, since no check would consult it.
const KEY_ACTIONS: &[&str] = &["cache.read", "cache.write"];

pub(crate) async fn run(
    command: &RbacCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let tenant = settings.tenant()?;
    match command {
        RbacCommand::Policy(command) => policy(command, tenant, settings, out).await,
        RbacCommand::Grouping(command) => grouping(command, tenant, settings, out).await,
    }
}

async fn policy(
    command: &PolicyCommand,
    tenant: &str,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let path = format!("/v1/tenants/{}/rbac/policies", segment(tenant));
    match command {
        PolicyCommand::Ls { subject } => {
            let api = Api::new(settings)?;
            let policies = api
                .list(&path, &[])
                .await?
                .into_iter()
                .filter(|item| subject.as_deref().is_none_or(|s| item["subject"] == s))
                .collect();
            print_list(out, "policies", POLICY_COLUMNS, policies)
        }
        PolicyCommand::Add(policy) => {
            check_policy(policy, tenant).map_err(|reason| fail(Exit::Usage, reason))?;
            let body = policy_body(policy);
            Api::new(settings)?
                .send(reqwest::Method::POST, &path, &[], Some(&body))
                .await?;
            out.done(
                &format!("added policy {}", describe_policy(policy)),
                json!({ "added": body }),
            )
        }
        // Not checked here: a rule written before a grammar change must stay
        // removable, and the control plane says why when it is not.
        PolicyCommand::Rm { policy, confirm } => {
            ask(
                &format!(
                    "Remove policy {} in tenant {tenant}?",
                    describe_policy(policy)
                ),
                *confirm,
            )?;
            let body = policy_body(policy);
            Api::new(settings)?
                .send(reqwest::Method::DELETE, &path, &[], Some(&body))
                .await?;
            out.done(
                &format!("removed policy {}", describe_policy(policy)),
                json!({ "removed": body }),
            )
        }
    }
}

async fn grouping(
    command: &GroupingCommand,
    tenant: &str,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let path = format!("/v1/tenants/{}/rbac/groupings", segment(tenant));
    match command {
        GroupingCommand::Ls { user, role } => {
            let api = Api::new(settings)?;
            let groupings = api
                .list(&path, &[])
                .await?
                .into_iter()
                .filter(|item| user.as_deref().is_none_or(|u| item["user"] == u))
                .filter(|item| role.as_deref().is_none_or(|r| item["role"] == r))
                .collect();
            print_list(out, "groupings", GROUPING_COLUMNS, groupings)
        }
        GroupingCommand::Add(grouping) => {
            let body = grouping_body(grouping);
            Api::new(settings)?
                .send(reqwest::Method::POST, &path, &[], Some(&body))
                .await?;
            out.done(
                &format!("assigned {} to {}", grouping.role, grouping.user),
                json!({ "added": body }),
            )
        }
        GroupingCommand::Rm { grouping, confirm } => {
            ask(
                &format!(
                    "Take {} away from {} in tenant {tenant}?",
                    grouping.role, grouping.user
                ),
                *confirm,
            )?;
            let body = grouping_body(grouping);
            Api::new(settings)?
                .send(reqwest::Method::DELETE, &path, &[], Some(&body))
                .await?;
            out.done(
                &format!("took {} away from {}", grouping.role, grouping.user),
                json!({ "removed": body }),
            )
        }
    }
}

/// The body of a policy add or remove.
pub(crate) fn policy_body(policy: &PolicyArgs) -> Value {
    json!({
        "subject": policy.subject,
        "object": policy.object,
        "action": policy.action,
    })
}

/// The body of a grouping add or remove.
pub(crate) fn grouping_body(grouping: &GroupingArgs) -> Value {
    json!({ "user": grouping.user, "role": grouping.role })
}

fn describe_policy(policy: &PolicyArgs) -> String {
    format!("{} {} {}", policy.subject, policy.object, policy.action)
}

fn print_list(
    out: &Output,
    kind: &str,
    columns: &[Column],
    items: Vec<Value>,
) -> anyhow::Result<()> {
    if out.json {
        return out.json_value(&json!({ kind: items }));
    }
    let headers: Vec<&str> = columns.iter().map(|(header, _)| *header).collect();
    out.text(&table(&headers, rows(columns, &items)))
}

/// What kind of thing a policy object names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Cluster,
    Node,
    Tenant,
    Namespace,
    Stream,
    Cache,
    /// A cache object limited to one key or a key prefix.
    CacheKey,
    Group,
    /// A kind this build does not know; the control plane decides.
    Other,
}

/// Check a policy before it is sent: its object, and that a key object
/// carries a data action.
pub(crate) fn check_policy(policy: &PolicyArgs, tenant: &str) -> Result<ObjectKind, String> {
    let kind = check_object(&policy.object, tenant)
        .map_err(|why| format!("{:?}: {why}", policy.object))?;
    if kind == ObjectKind::CacheKey && !KEY_ACTIONS.contains(&policy.action.as_str()) {
        return Err(format!(
            "{:?}: a cache key object only takes cache.read or cache.write, not {}",
            policy.object, policy.action
        ));
    }
    Ok(kind)
}

/// Check `raw` against the control plane's object grammar, for objects in
/// `tenant`. The same rules as the control plane's `parse_object`.
pub(crate) fn check_object(raw: &str, tenant: &str) -> Result<ObjectKind, String> {
    let Some((kind, rest)) = raw.split_once(':') else {
        return Err("an object is kind:name, such as stream:t1/payments/orders".to_string());
    };
    match kind {
        "cluster" if rest == "*" => Ok(ObjectKind::Cluster),
        "cluster" => Err("the only cluster object is cluster:*".to_string()),
        "node" if rest.is_empty() || rest == "*" => {
            Err("a node object names one node; use cluster:* for all".to_string())
        }
        "node" => Ok(ObjectKind::Node),
        "tenant" if rest == "*" => Err("tenant:* is not allowed".to_string()),
        "tenant" => {
            same_tenant(rest, tenant)?;
            Ok(ObjectKind::Tenant)
        }
        "namespace" => {
            let [tid, ns] = split_exact(rest)?;
            same_tenant(tid, tenant)?;
            check_segment(ns, true)?;
            Ok(ObjectKind::Namespace)
        }
        "stream" => {
            let [tid, ns, stream] = split_exact(rest)?;
            same_tenant(tid, tenant)?;
            check_leaf(ns, stream)?;
            Ok(ObjectKind::Stream)
        }
        "cache" => {
            // Cache names cannot hold `/`, so everything after the third one
            // is the key, which can.
            let (cache_part, key) = match rest.match_indices('/').nth(2) {
                Some((at, _)) => (&rest[..at], Some(&rest[at + 1..])),
                None => (rest, None),
            };
            let [tid, ns, cache] = split_exact(cache_part)?;
            same_tenant(tid, tenant)?;
            check_leaf(ns, cache)?;
            let Some(key) = key else {
                return Ok(ObjectKind::Cache);
            };
            check_key(key)?;
            if ns == "*" || cache == "*" {
                return Err("a cache key object must name its namespace and cache".to_string());
            }
            Ok(ObjectKind::CacheKey)
        }
        "group" => {
            let [tid, ns, stream, group] = split_exact(rest)?;
            same_tenant(tid, tenant)?;
            check_segment(group, true)?;
            check_segment(stream, group == "*")?;
            check_segment(ns, stream == "*")?;
            Ok(ObjectKind::Group)
        }
        _ => Ok(ObjectKind::Other),
    }
}

fn same_tenant(named: &str, tenant: &str) -> Result<(), String> {
    if named == tenant {
        Ok(())
    } else {
        Err(format!(
            "names tenant {named:?}, but the request is for tenant {tenant:?}"
        ))
    }
}

/// `rest` split on `/` into exactly `N` parts.
fn split_exact<const N: usize>(rest: &str) -> Result<[&str; N], String> {
    let parts: Vec<&str> = rest.split('/').collect();
    parts
        .try_into()
        .map_err(|_| format!("expected {N} parts separated by /"))
}

/// A stream or cache and its namespace: the namespace may be `*` only when
/// the leaf is, so a wildcard never sits over a named stream.
fn check_leaf(ns: &str, leaf: &str) -> Result<(), String> {
    check_segment(leaf, true)?;
    check_segment(ns, leaf == "*")
}

fn check_segment(raw: &str, allow_star: bool) -> Result<(), String> {
    if raw == "*" && !allow_star {
        return Err("* fits here only when every part after it is * too".to_string());
    }
    if raw.is_empty() {
        return Err("a part is empty".to_string());
    }
    if raw.contains(':') {
        return Err(format!("{raw:?} holds a ':'"));
    }
    Ok(())
}

/// A key is a literal, or a literal prefix ending in the one `*`.
fn check_key(key: &str) -> Result<(), String> {
    let literal = key.strip_suffix('*').unwrap_or(key);
    if literal.is_empty() {
        return Err("a cache key object names a key or a non-empty prefix; \
                    grant the whole cache without a key part"
            .to_string());
    }
    if literal.contains('*') {
        return Err("a cache key may only end in *, which makes it a prefix".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
