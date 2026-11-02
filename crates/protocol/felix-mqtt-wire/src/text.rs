//! MQTT strings: UTF-8 that can be held without copying.

use std::fmt;

use bytes::Bytes;

use crate::limits::MAX_FIELD_BYTES;

/// Why a byte sequence is not an MQTT string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StringError {
    #[error("not well-formed UTF-8")]
    InvalidUtf8,
    #[error("contains U+0000")]
    ContainsNull,
    #[error("longer than 65535 bytes")]
    TooLong,
}

/// A string that is well-formed UTF-8, has no U+0000 and fits the two byte
/// length prefix, as the specification requires of every MQTT string. It shares
/// the buffer it was read from.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Utf8(Bytes);

impl Utf8 {
    /// Checks `bytes` and wraps them.
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self, StringError> {
        let bytes = bytes.into();
        validate(&bytes)?;
        Ok(Utf8(bytes))
    }

    /// For bytes `validate` has already accepted.
    pub(crate) fn from_validated(bytes: Bytes) -> Self {
        Utf8(bytes)
    }

    pub fn as_str(&self) -> &str {
        // Checked in `new`; the fallback is unreachable.
        std::str::from_utf8(&self.0).unwrap_or_default()
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<&str> for Utf8 {
    type Error = StringError;

    fn try_from(s: &str) -> Result<Self, StringError> {
        Utf8::new(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl TryFrom<String> for Utf8 {
    type Error = StringError;

    fn try_from(s: String) -> Result<Self, StringError> {
        Utf8::new(Bytes::from(s))
    }
}

impl AsRef<str> for Utf8 {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for Utf8 {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other.as_bytes()
    }
}

impl PartialEq<&str> for Utf8 {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == other.as_bytes()
    }
}

impl fmt::Debug for Utf8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Utf8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `std`'s UTF-8 check already refuses surrogates and overlong forms, which are
/// the other two things the specification forbids.
pub(crate) fn validate(bytes: &[u8]) -> Result<&str, StringError> {
    if bytes.len() > MAX_FIELD_BYTES {
        return Err(StringError::TooLong);
    }
    let s = std::str::from_utf8(bytes).map_err(|_| StringError::InvalidUtf8)?;
    if s.contains('\0') {
        return Err(StringError::ContainsNull);
    }
    Ok(s)
}

#[cfg(test)]
mod tests;
