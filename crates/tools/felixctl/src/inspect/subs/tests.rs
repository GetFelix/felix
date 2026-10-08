use felix_client::{InspectedSubscription, SubscriptionCursor, SubscriptionsPage};

use super::*;

fn slow_reader() -> InspectedSubscription {
    InspectedSubscription {
        tenant_id: "acme".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        subscriber_id: 17,
        subscription_id: Some(9001),
        connection: Some(42),
        peer: Some("10.0.0.7:51234".to_string()),
        principal: Some("p:billing".to_string()),
        policy: "drop_new".to_string(),
        depth: 1024,
        capacity: 1024,
        dropped: 3812,
        position: Some(1_040_000),
        tail: 1_048_576,
        age_ms: 61_000,
    }
}

fn cursor() -> SubscriptionCursor {
    SubscriptionCursor {
        tenant_id: "acme".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        subscriber_id: 17,
    }
}

#[test]
fn a_printed_cursor_reads_back() {
    let printed = encode_cursor(&cursor());
    assert!(
        !printed.contains('/') && !printed.contains(' '),
        "{printed}"
    );
    assert_eq!(decode_cursor(&printed).expect("decodes"), cursor());
    let err = decode_cursor("not-a-cursor").expect_err("refused");
    assert_eq!(crate::error::exit_for(&err), Exit::Usage);
}

#[test]
fn the_json_adds_how_far_behind_and_an_opaque_cursor() {
    let page = NodePage::Answered(SubscriptionsPage {
        node_id: "broker-a".to_string(),
        subscriptions: vec![
            slow_reader(),
            InspectedSubscription {
                position: None,
                ..slow_reader()
            },
        ],
        next_cursor: Some(cursor()),
    });
    let json = page_json(&page);
    assert_eq!(json["node_id"], "broker-a");
    assert_eq!(json["subscriptions"][0]["behind"], 8576);
    assert_eq!(json["subscriptions"][0]["principal"], "p:billing");
    assert!(json["subscriptions"][1].get("behind").is_none());
    assert_eq!(json["next_cursor"], encode_cursor(&cursor()));

    let failed = page_json(&NodePage::Failed {
        node_id: "broker-b".to_string(),
        error: "broker broker-b does not support inspect".to_string(),
    });
    assert_eq!(failed["error"], "broker broker-b does not support inspect");
}

#[test]
fn the_table_lists_every_broker_and_what_it_left_out() {
    let pages = [
        NodePage::Answered(SubscriptionsPage {
            node_id: "broker-a".to_string(),
            subscriptions: vec![slow_reader()],
            next_cursor: Some(cursor()),
        }),
        NodePage::Failed {
            node_id: "broker-b".to_string(),
            error: "unreachable".to_string(),
        },
    ];
    let text = render(&pages);
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines[0].starts_with("NODE"), "{text}");
    assert!(lines[1].contains("acme/default/orders/3"), "{text}");
    assert!(lines[1].contains("42 10.0.0.7:51234"), "{text}");
    assert!(lines[1].contains("1024/1024"), "{text}");
    assert!(lines[1].ends_with("8576"), "{text}");
    assert!(
        text.contains(&format!(
            "more on broker-a: --node broker-a --cursor {}",
            encode_cursor(&cursor())
        )),
        "{text}"
    );
    assert!(text.contains("broker-b: unreachable"), "{text}");

    assert_eq!(render(&[]), "no subscriptions");
}
