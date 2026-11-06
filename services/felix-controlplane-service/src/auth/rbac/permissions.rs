//! Effective-permission computation for token exchange: ask Casbin what a
//! principal holds, then expand the implied hierarchy (tenant admin implies
//! namespace/stream/cache rights). Expansion must stay conservative — it only
//! ever adds what the hierarchy implies, never more.
use felix_common::random::HashSet;

use casbin::{Enforcer, RbacApi};

use crate::auth::rbac::authorize::{
    ACTION_CACHE_READ, ACTION_CACHE_WRITE, ACTION_NS_MANAGE, ACTION_TENANT_MANAGE, canonical_action,
};

/// Flatten a principal's permissions in a domain into deduplicated
/// `action:object` strings, with the implied hierarchy expanded — the shape
/// brokers evaluate cheaply at request time.
pub fn effective_permissions(enforcer: &Enforcer, principal: &str, domain: &str) -> Vec<String> {
    let rules: Vec<Vec<String>> =
        enforcer.get_implicit_permissions_for_user(principal, Some(domain));

    let mut perms: HashSet<String> = HashSet::default();
    for rule in rules {
        if rule.len() < 4 {
            continue;
        }
        let obj = &rule[2];
        let Some(act) = canonical_action(&rule[3]) else {
            continue;
        };
        perms.insert(format!("{act}:{obj}"));
    }

    expand_inheritance(&mut perms);
    perms.into_iter().collect()
}

fn expand_inheritance(perms: &mut HashSet<String>) {
    // Collect implied permissions to avoid mutating the set while iterating.
    let mut implied = Vec::new();
    for perm in perms.iter() {
        let Some((action, object)) = perm.split_once(':') else {
            continue;
        };
        match action {
            ACTION_TENANT_MANAGE => {
                if let Some(tenant_id) = tenant_id_from_object(object) {
                    implied.push(format!("{ACTION_NS_MANAGE}:namespace:{tenant_id}/*"));
                    implied.push(format!("stream.manage:stream:{tenant_id}/*/*"));
                    implied.push(format!("stream.publish:stream:{tenant_id}/*/*"));
                    implied.push(format!("stream.subscribe:stream:{tenant_id}/*/*"));
                    implied.push(format!("cache.manage:cache:{tenant_id}/*/*"));
                    implied.push(format!("{ACTION_CACHE_READ}:cache:{tenant_id}/*/*"));
                    implied.push(format!("{ACTION_CACHE_WRITE}:cache:{tenant_id}/*/*"));
                }
            }
            ACTION_NS_MANAGE => {
                if let Some((tenant_id, namespace)) = namespace_from_object(object) {
                    if namespace == "*" {
                        implied.push(format!("stream.manage:stream:{tenant_id}/*/*"));
                        implied.push(format!("stream.publish:stream:{tenant_id}/*/*"));
                        implied.push(format!("stream.subscribe:stream:{tenant_id}/*/*"));
                        implied.push(format!("cache.manage:cache:{tenant_id}/*/*"));
                        implied.push(format!("{ACTION_CACHE_READ}:cache:{tenant_id}/*/*"));
                        implied.push(format!("{ACTION_CACHE_WRITE}:cache:{tenant_id}/*/*"));
                    } else {
                        implied.push(format!("stream.manage:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("stream.publish:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("stream.subscribe:stream:{tenant_id}/{namespace}/*"));
                        implied.push(format!("cache.manage:cache:{tenant_id}/{namespace}/*"));
                        implied.push(format!(
                            "{ACTION_CACHE_READ}:cache:{tenant_id}/{namespace}/*"
                        ));
                        implied.push(format!(
                            "{ACTION_CACHE_WRITE}:cache:{tenant_id}/{namespace}/*"
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    for perm in implied {
        perms.insert(perm);
    }
}

fn tenant_id_from_object(object: &str) -> Option<&str> {
    object
        .strip_prefix("tenant:")
        .filter(|value| !value.is_empty())
        .or_else(|| {
            object
                .strip_prefix("namespace:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
        .or_else(|| {
            object
                .strip_prefix("stream:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
        .or_else(|| {
            object
                .strip_prefix("cache:")
                .and_then(|rest| rest.split_once('/').map(|(tid, _)| tid))
        })
}

fn namespace_from_object(object: &str) -> Option<(&str, &str)> {
    if let Some(rest) = object.strip_prefix("namespace:") {
        return rest.split_once('/');
    }
    None
}

#[cfg(test)]
mod tests;
