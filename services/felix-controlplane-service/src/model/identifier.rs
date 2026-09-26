//! The one grammar for tenant, namespace, stream and cache names.
//!
//! Names end up inside RBAC objects (`stream:{tenant}/{namespace}/{stream}`),
//! URL paths and broker storage paths, so each of those syntaxes constrains
//! them: a stream named `*` would read as a wildcard in every grant written
//! for it, `/` or `:` would shift an object's segments, and `..` is a path.
//! Checked when a resource is created; names that exist already are left
//! alone.

/// Longest name accepted.
pub(crate) const MAX_IDENTIFIER_LEN: usize = 128;

/// Check a new name: 1 to [`MAX_IDENTIFIER_LEN`] ASCII letters, digits, `-`,
/// `_` or `.`, starting with a letter or digit.
///
/// # Errors
/// A message naming `what` and the rule it broke, fit to return to the caller.
pub(crate) fn validate_identifier(what: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_LEN {
        return Err(format!(
            "{what} must be 1 to {MAX_IDENTIFIER_LEN} characters"
        ));
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(format!("{what} must start with a letter or digit"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(format!(
            "{what} may contain only ASCII letters, digits, '-', '_' and '.'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
