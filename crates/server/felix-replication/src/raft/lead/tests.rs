use std::sync::{Arc, Mutex};

use super::Leadership;
use crate::{ShardKey, ShardKind};

struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// **A member logs the open of its term in the hybrid driver's words.** The
/// history checker's one-leader-per-generation rule reads this line from
/// the broker logs, with the term as the generation, and logs it once.
#[test]
fn an_opened_term_is_logged_like_a_fenced_open() {
    let leadership = Leadership::new(
        Arc::new(ShardKey {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
            shard: 2,
            kind: ShardKind::Stream,
        }),
        None,
    );
    let text = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&text);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || Logs(Arc::clone(&sink)))
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        // Leading but nothing of the term committed: not open yet.
        leadership.observe(Some(4), Some(3), false);
        leadership.observe(Some(4), Some(4), false);
        leadership.observe(Some(4), Some(4), false);
    });
    assert_eq!(leadership.lead().term, Some(4));
    assert!(leadership.lead().open);
    let logged = String::from_utf8(text.lock().expect("lock").clone()).expect("utf8");
    let opens: Vec<&str> = logged
        .lines()
        .filter(|line| line.contains("a majority took the fence; opening the shard for writes"))
        .collect();
    assert_eq!(opens.len(), 1, "{logged}");
    assert!(
        opens[0].ends_with("stream=orders shard=2 generation=4"),
        "{}",
        opens[0]
    );
}
