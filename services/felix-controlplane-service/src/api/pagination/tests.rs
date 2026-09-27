use super::*;

/// A store listing of `0..n`, paged the way every backend pages.
async fn numbers(n: u32, page: PageRequest<u32>) -> StoreResult<Page<u32>> {
    Ok(Page::from_unordered(
        (0..n).rev(),
        |value| *value,
        page.after.as_ref(),
        page.limit,
    ))
}

fn params(limit: Option<&str>, cursor: Option<&str>) -> PageParams {
    PageParams {
        limit: limit.map(str::to_string),
        cursor: cursor.map(str::to_string),
    }
}

/// Walk every page and return what each held.
async fn walk(n: u32, limit: &str, visible: impl Fn(&u32) -> bool + Copy) -> Vec<Vec<u32>> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let request = params(Some(limit), cursor.as_deref())
            .request::<u32>()
            .expect("valid params");
        let listed = list_visible(request, |page| numbers(n, page), visible, |v| *v)
            .await
            .expect("list");
        pages.push(listed.items);
        match listed.next_cursor {
            Some(next) => cursor = Some(next),
            None => return pages,
        }
    }
}

#[tokio::test]
async fn following_cursors_returns_everything_once_in_order() {
    let pages = walk(10, "3", |_| true).await;
    assert_eq!(
        pages,
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9]]
    );
}

#[tokio::test]
async fn an_exact_multiple_ends_without_a_cursor_to_nowhere() {
    // The last full page knows the store is exhausted, so it offers no
    // cursor to an empty page.
    let pages = walk(6, "3", |_| true).await;
    assert_eq!(pages, vec![vec![0, 1, 2], vec![3, 4, 5]]);
}

/// Filtering fills the page from further on rather than returning it short,
/// and the cursor names only what was returned.
#[tokio::test]
async fn hidden_entries_neither_shorten_pages_nor_reach_the_cursor() {
    let visible = |v: &u32| v.is_multiple_of(3);
    let pages = walk(20, "2", visible).await;
    assert_eq!(pages, vec![vec![0, 3], vec![6, 9], vec![12, 15], vec![18]]);

    let first = list_visible(
        params(Some("2"), None).request::<u32>().unwrap(),
        |page| numbers(20, page),
        visible,
        |v| *v,
    )
    .await
    .unwrap();
    let cursor = first.next_cursor.expect("more pages");
    let named: u32 = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(cursor).unwrap()).unwrap();
    assert_eq!(named, 3, "the cursor is the last entry returned");
}

#[tokio::test]
async fn nothing_visible_is_an_empty_last_page() {
    let pages = walk(50, "10", |_| false).await;
    assert_eq!(pages, vec![Vec::<u32>::new()]);
}

#[test]
fn the_default_limit_applies_when_none_is_named() {
    let request = PageParams::default().request::<String>().unwrap();
    assert_eq!(request.limit, DEFAULT_PAGE_LIMIT);
    assert!(request.after.is_none());
    assert!(PageParams::default().is_absent());
    assert!(!params(Some("5"), None).is_absent());
}

#[test]
fn a_limit_outside_the_range_is_refused() {
    for bad in ["0", "10001", "-1", "ten", ""] {
        assert!(
            params(Some(bad), None).request::<String>().is_err(),
            "limit={bad} was accepted",
        );
    }
    assert_eq!(
        params(Some("10000"), None)
            .request::<String>()
            .unwrap()
            .limit,
        MAX_PAGE_LIMIT
    );
}

#[test]
fn a_cursor_that_was_not_issued_is_refused() {
    for bad in ["not base64!", "bm90IGpzb24", ""] {
        assert!(
            params(None, Some(bad)).request::<String>().is_err(),
            "cursor={bad} was accepted",
        );
    }
    // A cursor from a listing keyed differently does not decode either.
    let string_cursor = encode(&"tenant-a".to_string());
    assert!(
        params(None, Some(&string_cursor))
            .request::<crate::model::ShardKey>()
            .is_err()
    );
}
