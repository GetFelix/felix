//! Conditional writes: the condition JavaScript passes in and the typed
//! answers it gets back. A refusal is an answer, not an error; only a call the
//! broker could not take rejects. See `docs/atomic-commit.md` and the cache
//! page's conditional writes section.

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::u64_of;
use crate::errors::invalid;
use crate::types::or_null;

/// What a conditional cache put requires of the key's current entry:
/// `{ kind: "absent" }` or `{ kind: "version", version }`.
#[napi(object)]
pub struct CacheCondition {
    pub kind: String,
    pub version: Option<BigInt>,
}

impl CacheCondition {
    pub(crate) fn into_client(self) -> Result<felix_client::CacheCondition> {
        match self.kind.as_str() {
            "absent" => Ok(felix_client::CacheCondition::Absent),
            "version" => {
                let version = self
                    .version
                    .ok_or_else(|| invalid("a `version` condition needs `version`"))?;
                Ok(felix_client::CacheCondition::Version(u64_of(version)?))
            }
            other => Err(invalid(format!("unknown cache condition `{other}`"))),
        }
    }
}

/// The answer to a conditional cache write.
#[napi(object)]
pub struct CacheConditionResult {
    /// The condition held and the write was made.
    pub applied: bool,
    /// After an applied put, the version it wrote. Otherwise the key's current
    /// version, null when it has no live entry.
    pub version: Either<BigInt, Null>,
}

impl From<felix_client::CacheConditionResult> for CacheConditionResult {
    fn from(result: felix_client::CacheConditionResult) -> Self {
        Self {
            applied: result.applied,
            version: or_null(result.version.map(BigInt::from)),
        }
    }
}

/// A cache value and the version a conditional write compares against.
#[napi(object)]
pub struct VersionedValue {
    pub value: Buffer,
    pub version: BigInt,
}

impl From<felix_client::VersionedValue> for VersionedValue {
    fn from(entry: felix_client::VersionedValue) -> Self {
        Self {
            value: entry.value.to_vec().into(),
            version: BigInt::from(entry.version),
        }
    }
}

/// The answer to a conditional publish or commit.
#[napi(object)]
pub struct ConditionalWrite {
    /// The write was made at the expected offset.
    pub written: bool,
    /// Where it was written; null when refused.
    pub offset: Either<BigInt, Null>,
    /// The shard's next offset when refused; null when written. Nothing was
    /// written and no offset was consumed.
    pub tail: Either<BigInt, Null>,
}

impl From<felix_client::ConditionalWrite> for ConditionalWrite {
    fn from(answer: felix_client::ConditionalWrite) -> Self {
        match answer {
            felix_client::ConditionalWrite::Written { offset } => Self {
                written: true,
                offset: or_null(Some(BigInt::from(offset))),
                tail: or_null(None),
            },
            felix_client::ConditionalWrite::Refused { tail } => Self {
                written: false,
                offset: or_null(None),
                tail: or_null(Some(BigInt::from(tail))),
            },
        }
    }
}

/// The answer to `trimBefore`. A stale tail is an answer, not an error.
#[napi(object)]
pub struct ConditionalTrim {
    /// The trim is durable and reads of the shard below `oldest` are refused.
    pub trimmed: bool,
    /// Where the trim record landed. Null when refused, and when the floor
    /// already stood at or past `before` so nothing was written.
    pub offset: Either<BigInt, Null>,
    /// The oldest offset reads of the shard start at now; null when refused.
    pub oldest: Either<BigInt, Null>,
    /// The shard's next offset when refused; null when trimmed.
    pub tail: Either<BigInt, Null>,
}

impl From<felix_client::ConditionalTrim> for ConditionalTrim {
    fn from(answer: felix_client::ConditionalTrim) -> Self {
        match answer {
            felix_client::ConditionalTrim::Trimmed { offset, oldest } => Self {
                trimmed: true,
                offset: or_null(offset.map(BigInt::from)),
                oldest: or_null(Some(BigInt::from(oldest))),
                tail: or_null(None),
            },
            felix_client::ConditionalTrim::Refused { tail } => Self {
                trimmed: false,
                offset: or_null(None),
                oldest: or_null(None),
                tail: or_null(Some(BigInt::from(tail))),
            },
        }
    }
}

#[cfg(test)]
mod tests;
