use super::*;
use crate::legacy_swap::fixture::{Stop, assert_no_siblings, old_swap};

/// **A shard an older build left mid-swap opens with its data.** Upgrading
/// must not turn a crash between the old renames into an empty store.
#[tokio::test]
async fn a_shard_left_mid_swap_by_an_older_build_opens_whole() {
    for stop in Stop::ALL {
        let root = tempfile::tempdir().expect("tempdir");
        let expected = {
            let store = cache(root.path()).await;
            let expected = overwritten(&store, 40).await;
            store.shutdown().await.expect("shutdown");
            expected
        };
        let dir = layout::shard_dir(
            root.path(),
            &ShardKey {
                tenant: T.into(),
                namespace: NS.into(),
                stream: C.into(),
                shard: 0,
            },
        );
        let live = expected
            .iter()
            .map(|(key, value)| {
                CacheOp::Put {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at_millis: 0,
                }
                .encode()
            })
            .collect();
        old_swap(&dir, config(), live, stop).await;

        let when = format!("after an old swap stopped at {stop:?}");
        let store = cache(root.path()).await;
        assert_reads(&store, &expected, &when).await;
        assert_no_siblings(&dir, &when);
        store
            .put_checked(T, NS, C, 0, "after", Bytes::from_static(b"new"), None)
            .await
            .expect("put");
        store.shutdown().await.expect("shutdown");

        let store = cache(root.path()).await;
        assert_reads(&store, &expected, &format!("{when}, restarted")).await;
        assert_eq!(
            store.get_checked(T, NS, C, 0, "after").await.expect("get"),
            Some(Bytes::from_static(b"new")),
            "a write after the recovery was lost {when}",
        );
        store.shutdown().await.expect("shutdown");
    }
}
