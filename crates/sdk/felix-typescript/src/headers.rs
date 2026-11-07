//! Record headers: a key and named byte values that travel with a record
//! without being part of its payload.

use bytes::Bytes;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types::or_null;

/// One named header value. A name may repeat, and order is kept.
#[napi(object)]
pub struct HeaderField {
    pub name: String,
    pub value: Buffer,
}

/// The key and headers a record carried, as `Event.headers` hands them back.
#[napi(object)]
pub struct RecordHeaders {
    /// The record's key. It belongs to the record and does not route it.
    pub key: Either<Buffer, Null>,
    pub headers: Vec<HeaderField>,
}

impl From<felix_wire::RecordHeaders> for RecordHeaders {
    fn from(block: felix_wire::RecordHeaders) -> Self {
        Self {
            key: or_null(block.key.map(|key| key.to_vec().into())),
            headers: block
                .headers
                .into_iter()
                .map(|(name, value)| HeaderField {
                    name,
                    value: value.to_vec().into(),
                })
                .collect(),
        }
    }
}

/// The key and headers to publish a record with.
#[napi(object)]
pub struct RecordHeadersInput {
    pub key: Option<Buffer>,
    pub headers: Option<Vec<HeaderField>>,
}

impl RecordHeadersInput {
    pub(crate) fn into_client(self) -> felix_wire::RecordHeaders {
        felix_wire::RecordHeaders {
            key: self.key.map(|key| Bytes::copy_from_slice(&key)),
            headers: self
                .headers
                .unwrap_or_default()
                .into_iter()
                .map(|field| (field.name, Bytes::copy_from_slice(&field.value)))
                .collect(),
        }
    }
}

/// One record of `publishBatchWithHeaders`.
#[napi(object)]
pub struct HeaderedRecord {
    pub headers: RecordHeadersInput,
    pub payload: Buffer,
}

#[cfg(test)]
mod tests;
