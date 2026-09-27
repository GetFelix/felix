//! `limit` and `cursor` on the list endpoints; `docs/control-plane.md` has
//! the contract.
//!
//! A cursor is the key of the last entry a page returned, as base64 JSON:
//! opaque to callers, and never the key of an entry they could not see, so it
//! reveals nothing the page itself did not.
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use serde::de::DeserializeOwned;
use utoipa::IntoParams;

use crate::api::error::{ApiError, api_validation_error};
use crate::store::{Page, PageRequest, StoreResult};

/// Entries per page when the caller names no `limit`.
pub(crate) const DEFAULT_PAGE_LIMIT: usize = 1_000;
/// The most a caller may ask for in one page.
pub(crate) const MAX_PAGE_LIMIT: usize = 10_000;

/// The paging half of a list request's query string.
#[derive(Debug, Default, serde::Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PageParams {
    /// Most entries to return: 1 to 10000, default 1000.
    #[param(value_type = Option<u32>)]
    limit: Option<String>,
    /// The `next_cursor` of the previous page.
    cursor: Option<String>,
}

impl PageParams {
    /// Whether the caller asked for paging at all. The listings that predate
    /// paging and answer with a bare array keep doing so when it did not.
    pub(crate) fn is_absent(&self) -> bool {
        self.limit.is_none() && self.cursor.is_none()
    }

    /// The store request these parameters describe.
    ///
    /// # Errors
    /// 400 for a limit outside 1..=10000 or a cursor this listing did not
    /// issue.
    pub(crate) fn request<K: DeserializeOwned>(&self) -> Result<PageRequest<K>, ApiError> {
        let limit = match &self.limit {
            None => DEFAULT_PAGE_LIMIT,
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|limit| (1..=MAX_PAGE_LIMIT).contains(limit))
                .ok_or_else(|| {
                    api_validation_error(&format!(
                        "limit must be an integer from 1 to {MAX_PAGE_LIMIT}"
                    ))
                })?,
        };
        let after = self
            .cursor
            .as_deref()
            .map(|cursor| {
                URL_SAFE_NO_PAD
                    .decode(cursor)
                    .ok()
                    .and_then(|json| serde_json::from_slice(&json).ok())
                    .ok_or_else(|| api_validation_error("cursor is not one this listing issued"))
            })
            .transpose()?;
        Ok(PageRequest { after, limit })
    }
}

/// One page of a listing as the caller sees it.
#[derive(Debug)]
pub(crate) struct Listed<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next_cursor: Option<String>,
}

/// Fill a page with up to `request.limit` entries the caller may see, reading
/// the store a page at a time.
///
/// Filtering happens here rather than leaving short pages: a cursor has to
/// name an entry, and naming one the caller was not shown would leak it. The
/// cost is that a caller who can see little of a large listing makes this read
/// on past what it returns, which is no more than the unpaged listing read.
///
/// Every page but the last holds exactly `limit` entries. The last can be
/// empty, when everything after the previous page was filtered out.
pub(crate) async fn list_visible<T, K, Fetch, Fut>(
    request: PageRequest<K>,
    mut fetch: Fetch,
    visible: impl Fn(&T) -> bool,
    key: impl Fn(&T) -> K,
) -> StoreResult<Listed<T>>
where
    K: Serialize,
    Fetch: FnMut(PageRequest<K>) -> Fut,
    Fut: Future<Output = StoreResult<Page<T>>>,
{
    let limit = request.limit;
    let mut after = request.after;
    let mut items: Vec<T> = Vec::new();
    loop {
        let page = fetch(PageRequest { after, limit }).await?;
        let more = page.more;
        after = page.items.last().map(&key);
        for item in page.items.into_iter().filter(|item| visible(item)) {
            if items.len() == limit {
                // A visible entry beyond this page, so there is a next one.
                return Ok(Listed {
                    next_cursor: items.last().map(|last| encode(&key(last))),
                    items,
                });
            }
            items.push(item);
        }
        if !more {
            return Ok(Listed {
                items,
                next_cursor: None,
            });
        }
        if items.len() == limit {
            // The store has more, visible or not; the next page will say.
            return Ok(Listed {
                next_cursor: items.last().map(|last| encode(&key(last))),
                items,
            });
        }
    }
}

fn encode<K: Serialize>(key: &K) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(key).expect("a cursor key serializes"))
}

#[cfg(test)]
mod tests;
