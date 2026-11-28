use std::sync::Arc;

use parking_lot::Mutex;

use super::{EngineHost, Host, WriteFence};
use crate::ShardKey;

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

/// Records which call came in, with its generation, so a crossed delegation
/// shows.
#[derive(Default)]
struct Fence {
    calls: Mutex<Vec<(&'static str, u64)>>,
}

impl WriteFence for Fence {
    fn quiesced(&self, _key: &ShardKey) -> bool {
        self.calls.lock().push(("quiesced", 0));
        false
    }

    fn opened(&self, _key: &ShardKey, generation: u64) {
        self.calls.lock().push(("opened", generation));
    }

    fn deposed(&self, _key: &ShardKey, generation: u64) {
        self.calls.lock().push(("deposed", generation));
    }
}

#[test]
fn opened_and_deposed_go_to_the_fence() {
    let fence = Arc::new(Fence::default());
    let host = Host::new(Arc::clone(&fence));

    host.opened(&key(), 5);
    host.deposed(&key(), 6);

    assert_eq!(*fence.calls.lock(), vec![("opened", 5), ("deposed", 6)]);
}
