use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::{SubscriptionCursor, SubscriptionFilter};

use crate::{Broker, DurableStorage, StreamMetadata, SubscriberOwner};

async fn broker_with(streams: &[&str], durable: bool) -> (Broker, Option<tempfile::TempDir>) {
    let mut broker = Broker::new(EphemeralCache::new().into());
    let mut dir = None;
    if durable {
        let tmp = tempfile::tempdir().expect("dir");
        let storage = DurableStorage::open(
            tmp.path(),
            LogConfig {
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage");
        broker = broker.with_durable_storage(storage);
        dir = Some(tmp);
    }
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    for stream in streams {
        broker
            .register_stream(
                "t1",
                "ns",
                *stream,
                StreamMetadata {
                    durable,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
    }
    (broker, dir)
}

/// A subscriber that never reads loses what does not fit its queue. The count
/// it reports is exactly the hole in the offsets it then receives.
#[tokio::test]
async fn reported_drops_match_the_gap_the_subscriber_sees() {
    let (broker, _dir) = broker_with(&["orders"], true).await;
    let subscription = broker
        .subscribe_sized("t1", "ns", "orders", 0, Some(2))
        .await
        .expect("subscribe");
    for value in ["a", "b", "c", "d", "e"] {
        broker
            .publish("t1", "ns", "orders", Bytes::from(value))
            .await
            .expect("publish");
    }

    let page = broker
        .list_subscriptions(&SubscriptionFilter::default(), None, 10)
        .await;
    let [listed] = page.subscriptions.as_slice() else {
        panic!("one subscriber: {page:?}");
    };
    assert_eq!((listed.depth, listed.capacity), (2, 2));
    assert_eq!(listed.dropped, 3);
    assert_eq!(listed.policy, "drop_new");
    assert_eq!((listed.position, listed.tail), (None, 5));
    assert!(page.next_cursor.is_none());

    let (mut receiver, _guard) = subscription.into_parts();
    let mut offsets = Vec::new();
    for _ in 0..2 {
        offsets.push(receiver.recv().await.expect("queued").base_offset());
    }
    broker
        .publish("t1", "ns", "orders", Bytes::from("f"))
        .await
        .expect("publish");
    offsets.push(receiver.recv().await.expect("live").base_offset());
    assert_eq!(offsets, vec![Some(0), Some(1), Some(5)]);
    let gap = 5 - 1 - 1;

    let page = broker
        .list_subscriptions(&SubscriptionFilter::default(), None, 10)
        .await;
    let listed = &page.subscriptions[0];
    assert_eq!(listed.dropped, gap);
    assert_eq!(
        (listed.position, listed.tail, listed.depth),
        (Some(6), 6, 0)
    );
}

/// Pages run in shard order and then by subscriber id. A subscriber that
/// leaves or joins between pages neither repeats nor shifts what follows.
#[tokio::test]
async fn paging_survives_subscribers_coming_and_going() {
    let (broker, _dir) = broker_with(&["a", "b"], false).await;
    let mut held = Vec::new();
    for stream in ["b", "a", "a", "b"] {
        held.push(
            broker
                .subscribe("t1", "ns", stream, 0)
                .await
                .expect("subscribe"),
        );
    }
    let all = SubscriptionFilter::default();
    let first = broker.list_subscriptions(&all, None, 2).await;
    let names: Vec<_> = first
        .subscriptions
        .iter()
        .map(|s| (s.stream.as_str(), s.subscriber_id))
        .collect();
    assert_eq!(names, vec![("a", 0), ("a", 1)]);
    let cursor = first.next_cursor.expect("more to come");
    assert_eq!(
        cursor,
        SubscriptionCursor {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "a".to_string(),
            shard: 0,
            subscriber_id: 1,
        }
    );

    // The last one listed leaves, and newcomers join both streams.
    held.remove(2);
    held.push(broker.subscribe("t1", "ns", "a", 0).await.expect("join a"));
    held.push(broker.subscribe("t1", "ns", "b", 0).await.expect("join b"));

    let rest = broker.list_subscriptions(&all, Some(&cursor), 10).await;
    let names: Vec<_> = rest
        .subscriptions
        .iter()
        .map(|s| (s.stream.as_str(), s.subscriber_id))
        .collect();
    assert_eq!(names, vec![("a", 2), ("b", 0), ("b", 1), ("b", 2)]);
    assert!(rest.next_cursor.is_none());
}

/// The filter narrows by shard, by principal and to subscribers that have
/// dropped something, and the owner the serving layer set is reported.
#[tokio::test]
async fn filters_narrow_the_list() {
    let (broker, _dir) = broker_with(&["a", "b"], false).await;
    let billing = broker.subscribe("t1", "ns", "a", 0).await.expect("sub");
    billing.set_owner(SubscriberOwner {
        subscription_id: 77,
        connection_id: 3,
        peer: "10.0.0.7:5000".to_string(),
        principal: Some("p:billing".to_string()),
    });
    let _other = broker.subscribe("t1", "ns", "b", 0).await.expect("sub");
    let _slow = broker
        .subscribe_sized("t1", "ns", "b", 0, Some(1))
        .await
        .expect("sub");
    for value in ["x", "y"] {
        broker
            .publish("t1", "ns", "b", Bytes::from(value))
            .await
            .expect("publish");
    }

    let only = |filter: SubscriptionFilter| {
        let broker = &broker;
        async move {
            broker
                .list_subscriptions(&filter, None, 10)
                .await
                .subscriptions
                .into_iter()
                .map(|s| (s.stream, s.subscriber_id))
                .collect::<Vec<_>>()
        }
    };
    let by_principal = broker
        .list_subscriptions(
            &SubscriptionFilter {
                principal: Some("p:billing".to_string()),
                ..Default::default()
            },
            None,
            10,
        )
        .await;
    let listed = &by_principal.subscriptions[..];
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].subscription_id, Some(77));
    assert_eq!(listed[0].connection, Some(3));
    assert_eq!(listed[0].peer.as_deref(), Some("10.0.0.7:5000"));

    assert_eq!(
        only(SubscriptionFilter {
            dropping: true,
            ..Default::default()
        })
        .await,
        vec![("b".to_string(), 1)]
    );
    assert_eq!(
        only(SubscriptionFilter {
            stream: Some("b".to_string()),
            ..Default::default()
        })
        .await,
        vec![("b".to_string(), 0), ("b".to_string(), 1)]
    );
    assert!(
        only(SubscriptionFilter {
            tenant_id: Some("other".to_string()),
            ..Default::default()
        })
        .await
        .is_empty()
    );
}
