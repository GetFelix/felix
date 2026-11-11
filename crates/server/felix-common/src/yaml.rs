//! The YAML dialect the service config files are read in.
//!
//! The rules are pinned here rather than left to the parser's defaults, so a
//! config file means the same thing to both services and to every parser
//! version. The rules an operator sees are in
//! `docs-site/src/content/docs/reference/configuration.md`.

use serde::de::DeserializeOwned;

/// A config file that could not be read as the type asked for.
pub type Error = serde_saphyr::Error;

/// Parse one YAML document into `T` with the config-file rules:
///
/// - Only `true` and `false` (in any case) are booleans. `yes`, `no`, `on`
///   and `off` are strings, so a boolean setting spelled that way is an error
///   rather than a guess.
/// - `<<` is an ordinary key, not a merge. With `deny_unknown_fields` it is
///   refused like any other unknown key.
/// - A duplicate key is an error.
/// - A tag the parser does not know (`!custom`) is an error. `!!binary` on a
///   string is kept as written, not decoded.
/// - Errors carry a line and column. A wrong-typed value is not quoted, since
///   the file may hold a credential; an unknown enum value is.
pub fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    serde_saphyr::from_str_with_options(if has_content(text) { text } else { "{}" }, options())
}

/// False for an empty, comment-only or `---`-only file, which means "no
/// settings". serde-saphyr 2.1 reads those as a null document and refuses a
/// struct. Mirrored in felix-client; change both.
fn has_content(text: &str) -> bool {
    let mut markers = 0;
    for line in text.lines().map(str::trim) {
        if line == "---" {
            markers += 1;
        }
        if markers > 1 || !(line.is_empty() || line.starts_with('#') || line == "---") {
            return true;
        }
    }
    false
}

// Mirrored in felix-client's `ClientConfigOverride::parse`; change both.
fn options() -> serde_saphyr::Options {
    serde_saphyr::options! {
        strict_booleans: true,
        merge_keys: serde_saphyr::options::MergeKeyPolicy::AsOrdinary,
        duplicate_keys: serde_saphyr::options::DuplicateKeyPolicy::Error,
        with_snippet: false,
        reject_unsupported_tags: true,
        // The old parser kept `!!binary` text as written; decoding it would
        // silently change the value.
        ignore_binary_tag_for_string: true,
    }
}

#[cfg(test)]
mod tests;
