//! A region id, typed so it cannot be confused with any other UUID.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

/// Identifies a region.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RegionId(Uuid);

impl RegionId {
    /// A new random id.
    pub fn new() -> Self {
        Self(crate::random::uuid_v4())
    }

    /// Wrap an existing UUID, e.g. one decoded from storage.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for RegionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RegionId {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self> {
        // Preserve the original input for clearer error messages.
        let uuid = Uuid::parse_str(input).map_err(|_| Error::InvalidId(input.into()))?;
        Ok(Self(uuid))
    }
}

#[cfg(test)]
mod tests;
