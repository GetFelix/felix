//! Wildcard permission matching. `*` matches zero or more bytes; there is no
//! `?` or character-class syntax. Matching is byte-based and case-sensitive.
use crate::resource::group_prefix;
use crate::{
    Action, AuthzResult, CacheScope, GroupName, Namespace, PermissionPattern, StreamName, TenantId,
    cache_resource, group_resource, stream_resource,
};

/// A set of permission patterns checked against action/resource requests.
/// Grants only — there is no deny rule, so any match allows.
#[derive(Debug, Clone)]
pub struct PermissionMatcher {
    patterns: Vec<PermissionPattern>,
}

impl PermissionMatcher {
    pub fn new(patterns: Vec<PermissionPattern>) -> Self {
        Self { patterns }
    }

    /// Parse raw `action:resource` strings into a matcher.
    ///
    /// # Errors
    /// Returns the first pattern's parse error.
    pub fn from_strings(patterns: &[String]) -> AuthzResult<Self> {
        let mut parsed = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            parsed.push(pattern.parse()?);
        }
        Ok(Self::new(parsed))
    }

    /// Whether any pattern allows `action` on `resource`, directly or through
    /// an action that implies it ([`Action::is_granted_by`]).
    pub fn allows(&self, action: Action, resource: &str) -> bool {
        self.patterns.iter().any(|pattern| {
            action.is_granted_by(pattern.action)
                && wildcard_match(&pattern.resource_pattern, resource)
        })
    }

    /// Whether `action` is allowed on one consumer group of a stream.
    ///
    /// A grant on the group's own object (`group:{tenant}/{ns}/{stream}/{group}`)
    /// allows it. Failing that, a grant on the stream does, as it always has --
    /// unless this matcher holds a group-object grant for `action` that could
    /// name some group of the same stream. Then the principal has been scoped
    /// to particular groups there, and the stream grant stops speaking for the
    /// others. Without that, a policy granting one group would be silently
    /// widened by the `stream.subscribe` a consumer holds anyway.
    pub fn allows_group(
        &self,
        action: Action,
        tenant_id: &TenantId,
        namespace: &Namespace,
        stream: &StreamName,
        group: &GroupName,
    ) -> bool {
        if self.allows(action, &group_resource(tenant_id, namespace, stream, group)) {
            return true;
        }
        let prefix = group_prefix(tenant_id, namespace, stream);
        let narrowed = self.patterns.iter().any(|pattern| {
            action.is_granted_by(pattern.action)
                && pattern.resource_pattern.starts_with("group:")
                && matches_some_extension(&pattern.resource_pattern, &prefix)
        });
        !narrowed && self.allows(action, &stream_resource(tenant_id, namespace, stream))
    }

    /// Whether `action` is allowed on the keys `keys` names in one cache.
    ///
    /// A grant on the whole cache (`cache:{tenant}/{ns}/{cache}`) allows it, as
    /// it always has. So does a key grant, `cache:{tenant}/{ns}/{cache}/{key}`,
    /// whose key part is either an exact key or a literal prefix followed by
    /// one `*`. A prefix is a plain string prefix: `user:1*` covers `user:10`.
    ///
    /// Key grants are matched segment by segment, never by [`wildcard_match`]
    /// over the whole string. A `*` there crosses `/`, so `cache:t/*/c` would
    /// otherwise reach key `c` of every cache in the tenant.
    pub fn allows_cache_keys(
        &self,
        action: Action,
        tenant_id: &TenantId,
        namespace: &Namespace,
        cache: &CacheScope,
        keys: CacheKeys<'_>,
    ) -> bool {
        if self.allows(action, &cache_resource(tenant_id, namespace, cache)) {
            return true;
        }
        self.patterns.iter().any(|pattern| {
            action.is_granted_by(pattern.action)
                && KeyGrant::parse(&pattern.resource_pattern).is_some_and(|grant| {
                    wildcard_match(grant.tenant, tenant_id.as_str())
                        && wildcard_match(grant.namespace, namespace.as_str())
                        && wildcard_match(grant.cache, cache.as_str())
                        && grant.keys.covers(keys)
                })
        })
    }

    /// The parsed patterns, for inspection and tests.
    pub fn patterns(&self) -> &[PermissionPattern] {
        &self.patterns
    }
}

/// The keys a cache request touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheKeys<'a> {
    /// One key: a get, put, delete, conditional write, counter op, or key watch.
    Key(&'a str),
    /// Every key starting with this string: a prefix watch.
    Prefix(&'a str),
}

impl CacheKeys<'_> {
    /// Whether every key in `requested` is also in `self`.
    fn covers(self, requested: CacheKeys<'_>) -> bool {
        match (self, requested) {
            (CacheKeys::Key(granted), CacheKeys::Key(key)) => granted == key,
            (CacheKeys::Prefix(granted), CacheKeys::Key(key) | CacheKeys::Prefix(key)) => {
                key.starts_with(granted)
            }
            // A prefix watch reads keys past any one exact key.
            (CacheKeys::Key(_), CacheKeys::Prefix(_)) => false,
        }
    }
}

/// A `cache:{tenant}/{ns}/{cache}/{key}` pattern split into its parts.
struct KeyGrant<'a> {
    tenant: &'a str,
    namespace: &'a str,
    cache: &'a str,
    keys: CacheKeys<'a>,
}

impl<'a> KeyGrant<'a> {
    /// `None` for anything that is not a key grant, including one whose key
    /// part has a `*` anywhere but the end: that would make the key a
    /// pattern, so it matches nothing instead.
    fn parse(pattern: &'a str) -> Option<Self> {
        let rest = pattern.strip_prefix("cache:")?;
        // Tenant, namespace, and cache names cannot hold `/`; a key can.
        let mut parts = rest.splitn(4, '/');
        let (tenant, namespace, cache, key) =
            (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
        let keys = match key.strip_suffix('*') {
            Some(prefix) if !prefix.contains('*') => CacheKeys::Prefix(prefix),
            None if !key.is_empty() && !key.contains('*') => CacheKeys::Key(key),
            _ => return None,
        };
        Some(Self {
            tenant,
            namespace,
            cache,
            keys,
        })
    }
}

/// Glob-match `value` against `pattern`, where `*` matches any run of bytes.
///
/// Greedy scan with backtracking, so worst case is O(pattern × value) — fine
/// for permission-sized strings.
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let (mut p_idx, mut v_idx) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);
    let pattern_bytes = pattern.as_bytes();
    let value_bytes = value.as_bytes();

    while v_idx < value_bytes.len() {
        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
            star_idx = Some(p_idx);
            match_idx = v_idx;
            p_idx += 1;
            continue;
        }

        if p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == value_bytes[v_idx] {
            p_idx += 1;
            v_idx += 1;
            continue;
        }

        if let Some(star) = star_idx {
            // Mismatch after a `*`: let the star swallow one more byte and retry.
            p_idx = star + 1;
            match_idx += 1;
            v_idx = match_idx;
            continue;
        }

        return false;
    }

    // Trailing `*`s match the empty tail.
    while p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern_bytes.len()
}

/// Whether `pattern` matches `prefix` followed by something: some value
/// starting with `prefix` that [`wildcard_match`] would accept.
///
/// Runs the pattern as a set of positions over the prefix. Any position still
/// live at the end can finish on the pattern's remaining literal bytes, with
/// every `*` left empty.
fn matches_some_extension(pattern: &str, prefix: &str) -> bool {
    let pattern = pattern.as_bytes();
    let mut live = vec![false; pattern.len() + 1];
    live[0] = true;
    close_over_stars(pattern, &mut live);
    for &byte in prefix.as_bytes() {
        let mut next = vec![false; pattern.len() + 1];
        for (at, _) in live.iter().enumerate().filter(|(_, live)| **live) {
            match pattern.get(at) {
                Some(b'*') => next[at] = true,
                Some(&literal) if literal == byte => next[at + 1] = true,
                _ => {}
            }
        }
        close_over_stars(pattern, &mut next);
        if !next.contains(&true) {
            return false;
        }
        live = next;
    }
    true
}

/// A `*` can match nothing, so a live position at one is live past it too.
fn close_over_stars(pattern: &[u8], live: &mut [bool]) {
    for at in 0..pattern.len() {
        if live[at] && pattern[at] == b'*' {
            live[at + 1] = true;
        }
    }
}

#[cfg(test)]
mod tests;
