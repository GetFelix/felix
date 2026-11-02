//! Lease templates: grants that name a stream or cache key after a key of a
//! lease cache.
//!
//! `stream.publish:stream:t1/prod/wl.{leases}.out` grants nothing by itself.
//! It says that whoever holds key `K` of cache `t1/prod/leases` may get a
//! lease token for `stream.publish:stream:t1/prod/wl.K.out`. A broker expands
//! it when the holder wins a conditional write to that key; see
//! `docs/auth.md` ("Lease tokens").
//!
//! A template is never matched as a grant. Names cannot hold `{` or `}`, so
//! a broker that predates templates reads one as a pattern that matches
//! nothing, and this crate's matcher skips them outright.
use crate::{
    Action, AuthzError, AuthzResult, CacheScope, Namespace, PermissionPattern, TenantId,
    validate_identifier,
};

/// A parsed lease template object: `stream:{t}/{ns}/{prefix}{cache}{suffix}`
/// or `cache:{t}/{ns}/{cache}/{prefix}{cache}{suffix}`, with exactly one
/// `{cache}` slot, in the last segment. The slot's cache is in the template's
/// own tenant and namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseTemplate {
    object: TemplateObject,
    tenant: String,
    namespace: String,
    slot: String,
    prefix: String,
    suffix: String,
}

impl LeaseTemplate {
    /// Parse a template object.
    ///
    /// # Errors
    /// [`AuthzError::InvalidLeaseTemplate`] for anything that is not exactly
    /// the grammar above: a `*` anywhere, a second slot, a slot outside the
    /// last segment, or a literal part that is not a valid name.
    pub fn parse(resource: &str) -> AuthzResult<Self> {
        let invalid = |why: &str| AuthzError::InvalidLeaseTemplate(format!("{resource}: {why}"));
        if resource.contains('*') {
            return Err(invalid("a template cannot hold `*`"));
        }
        let (kind, rest) = if let Some(rest) = resource.strip_prefix("stream:") {
            ("stream", rest)
        } else if let Some(rest) = resource.strip_prefix("cache:") {
            ("cache", rest)
        } else {
            return Err(invalid("only stream and cache key objects take a slot"));
        };
        let segments: Vec<&str> = rest.split('/').collect();
        let (tenant, namespace, cache, leaf) = match (kind, segments.as_slice()) {
            ("stream", [tenant, namespace, leaf]) => (*tenant, *namespace, None, *leaf),
            ("cache", [tenant, namespace, cache, leaf]) => {
                (*tenant, *namespace, Some(*cache), *leaf)
            }
            _ => return Err(invalid("the slot must be in the last segment")),
        };
        for (what, literal) in [("tenant", tenant), ("namespace", namespace)]
            .into_iter()
            .chain(cache.map(|cache| ("cache", cache)))
        {
            validate_identifier(what, literal).map_err(|err| invalid(&err))?;
        }
        let (Some(open), Some(close)) = (leaf.find('{'), leaf.find('}')) else {
            return Err(invalid("the last segment needs one `{cache}` slot"));
        };
        if close < open || leaf.matches('{').count() != 1 || leaf.matches('}').count() != 1 {
            return Err(invalid("the last segment needs exactly one `{cache}` slot"));
        }
        let (prefix, slot, suffix) = (&leaf[..open], &leaf[open + 1..close], &leaf[close + 1..]);
        validate_identifier("lease cache", slot).map_err(|err| invalid(&err))?;
        if !prefix.bytes().chain(suffix.bytes()).all(is_name_byte) {
            return Err(invalid(
                "the text around the slot may contain only ASCII letters, digits, '-', '_' and '.'",
            ));
        }
        Ok(Self {
            object: match cache {
                Some(cache) => TemplateObject::CacheKey {
                    cache: cache.to_string(),
                },
                None => TemplateObject::Stream,
            },
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            slot: slot.to_string(),
            prefix: prefix.to_string(),
            suffix: suffix.to_string(),
        })
    }

    /// Whether `action` may be granted through this template: publish or
    /// subscribe on a stream, read or write on a cache key.
    pub fn takes(&self, action: Action) -> bool {
        match self.object {
            TemplateObject::Stream => {
                matches!(action, Action::StreamPublish | Action::StreamSubscribe)
            }
            TemplateObject::CacheKey { .. } => {
                matches!(action, Action::CacheRead | Action::CacheWrite)
            }
        }
    }

    /// Whether the slot names this lease cache.
    pub fn names_cache(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        cache: &CacheScope,
    ) -> bool {
        self.tenant == tenant_id.as_str()
            && self.namespace == namespace.as_str()
            && self.slot == cache.as_str()
    }

    /// The object this template names for lease key `key`.
    ///
    /// # Errors
    /// [`AuthzError::InvalidLeaseKey`] when `key`, or the name it expands to,
    /// is not a valid name. A key with `*`, `/` or `:` would otherwise widen
    /// or reshape the object.
    pub fn expand(&self, key: &str) -> AuthzResult<String> {
        validate_identifier("lease key", key).map_err(AuthzError::InvalidLeaseKey)?;
        let leaf = format!("{}{key}{}", self.prefix, self.suffix);
        validate_identifier("expanded name", &leaf).map_err(AuthzError::InvalidLeaseKey)?;
        Ok(match &self.object {
            TemplateObject::Stream => format!("stream:{}/{}/{leaf}", self.tenant, self.namespace),
            TemplateObject::CacheKey { cache } => {
                format!("cache:{}/{}/{cache}/{leaf}", self.tenant, self.namespace)
            }
        })
    }
}

/// What a template names once its slot is filled.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplateObject {
    Stream,
    CacheKey { cache: String },
}

/// Whether `resource` is a lease template rather than a grant. Names cannot
/// hold braces, so any brace makes it one, well formed or not.
pub fn is_lease_template(resource: &str) -> bool {
    resource.contains(['{', '}'])
}

/// The `action:object` permissions every template in `patterns` that names
/// lease cache `cache` grants the holder of `key`. Empty when none names it.
///
/// A malformed template, or one whose action it cannot take, is skipped as
/// the matcher skips it: it grants nothing.
///
/// # Errors
/// [`AuthzError::InvalidLeaseKey`] when `key` cannot expand one of them. The
/// mint is refused whole then, never granted in part.
pub fn expand_lease_templates(
    patterns: &[PermissionPattern],
    tenant_id: &TenantId,
    namespace: &Namespace,
    cache: &CacheScope,
    key: &str,
) -> AuthzResult<Vec<String>> {
    let mut expanded = Vec::new();
    for pattern in patterns {
        if !is_lease_template(&pattern.resource_pattern) {
            continue;
        }
        let Ok(template) = LeaseTemplate::parse(&pattern.resource_pattern) else {
            continue;
        };
        if !template.takes(pattern.action) || !template.names_cache(tenant_id, namespace, cache) {
            continue;
        }
        let permission = format!("{}:{}", pattern.action.as_str(), template.expand(key)?);
        if !expanded.contains(&permission) {
            expanded.push(permission);
        }
    }
    Ok(expanded)
}

fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
}

#[cfg(test)]
mod tests;
