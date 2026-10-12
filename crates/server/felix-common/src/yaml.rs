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
/// - Errors carry a line and column but never quote the file, which may
///   hold a credential.
pub fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    serde_saphyr::from_str_with_options(text, options())
}

fn options() -> serde_saphyr::Options {
    serde_saphyr::options! {
        strict_booleans: true,
        merge_keys: serde_saphyr::options::MergeKeyPolicy::AsOrdinary,
        duplicate_keys: serde_saphyr::options::DuplicateKeyPolicy::Error,
        with_snippet: false,
    }
}

#[cfg(test)]
mod tests;
