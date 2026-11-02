//! Record headers on a decoded publish batch, checked before they are queued.

use bytes::Bytes;
use felix_wire::{RESERVED_HEADER_PREFIX, RecordHeaders};

use crate::serving::quic::client_error::ClientError;

/// The headers to publish a decoded batch with: `None` when no record has a
/// block, so a frame that set the extension for nothing is the plain publish.
///
/// Names under [`RESERVED_HEADER_PREFIX`] belong to Felix's own listeners, so a
/// client that sends one is refused rather than left to pass as one.
pub(crate) fn publishable(
    headers: Option<Vec<Option<Bytes>>>,
) -> Result<Option<Vec<Option<Bytes>>>, ClientError> {
    let Some(headers) = headers.filter(|headers| headers.iter().any(Option::is_some)) else {
        return Ok(None);
    };
    for block in headers.iter().flatten() {
        // The decoder validated the block, so a failure here is not expected;
        // it is refused all the same.
        match RecordHeaders::any_name_starts_with(block, RESERVED_HEADER_PREFIX) {
            Ok(false) => {}
            Ok(true) => {
                return Err(ClientError::invalid(format!(
                    "header names starting with \"{RESERVED_HEADER_PREFIX}\" are reserved"
                )));
            }
            Err(_) => return Err(ClientError::invalid("malformed record headers")),
        }
    }
    Ok(Some(headers))
}

#[cfg(test)]
mod tests;
