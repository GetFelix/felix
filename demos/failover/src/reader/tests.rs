use std::sync::Mutex;

use super::*;

fn page(keys: &[(u16, u64)], next_offset: u64) -> Page {
    Page {
        payloads: keys
            .iter()
            .map(|key| payload::encode("run", *key, 16))
            .collect(),
        next_offset,
    }
}

/// A fake stream: `pages[i]` answers the i-th call.
async fn collect_from(pages: Vec<Page>, read_to: Option<u64>) -> Result<(HashMap<Key, u32>, u64)> {
    let pages = Mutex::new(pages.into_iter());
    collect("run", read_to, Duration::from_millis(50), |from| {
        let next = pages.lock().expect("pages").next();
        async move {
            Ok(match next {
                Some(page) => page,
                None => Page {
                    payloads: Vec::new(),
                    next_offset: from,
                },
            })
        }
    })
    .await
}

#[tokio::test]
async fn an_empty_page_that_moves_the_offset_does_not_end_the_read() {
    // The middle page holds no record of ours but skips ahead.
    let pages = vec![page(&[(0, 0)], 1), page(&[], 5), page(&[(0, 1)], 6)];
    let (counts, records) = collect_from(pages, None).await.expect("read");
    assert_eq!(records, 2);
    assert_eq!(counts.len(), 2);
}

#[tokio::test]
async fn the_read_waits_out_an_empty_answer_short_of_an_acknowledged_offset() {
    // Empty and not moving, as just after a failover, while an acknowledged
    // write sits at offset 1.
    let pages = vec![page(&[(0, 0)], 1), page(&[], 1), page(&[(0, 1)], 2)];
    let (counts, _) = collect_from(pages, Some(2)).await.expect("read");
    assert_eq!(counts.len(), 2);
}

#[tokio::test]
async fn a_stream_that_never_reaches_an_acknowledged_offset_is_an_error() {
    let pages = vec![page(&[(0, 0)], 1)];
    assert!(collect_from(pages, Some(9)).await.is_err());
}

#[tokio::test]
async fn the_read_ends_when_the_offset_stops_moving_past_the_last_acknowledgement() {
    let pages = vec![page(&[(0, 0)], 1), page(&[], 1)];
    let (counts, records) = collect_from(pages, Some(1)).await.expect("read");
    assert_eq!((counts.len(), records), (1, 1));
}
