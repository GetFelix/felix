use super::*;

/// The specification's rules, written the direct way.
fn direct(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    let mut i = 0;
    loop {
        match (f.get(i), t.get(i)) {
            (Some(&"#"), _) => return true,
            (Some(&"+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
        i += 1;
    }
}

fn trie(filters: &[&str]) -> FilterTrie {
    let mut trie = FilterTrie::default();
    for filter in filters {
        trie.insert(filter, 1);
    }
    trie
}

#[test]
fn the_specifications_examples() {
    let player = trie(&["sport/tennis/player1/#"]);
    assert!(player.matches("sport/tennis/player1").is_some());
    assert!(player.matches("sport/tennis/player1/ranking").is_some());
    assert!(
        player
            .matches("sport/tennis/player1/score/wimbledon")
            .is_some()
    );
    assert!(player.matches("sport/tennis/player2").is_none());

    assert!(trie(&["sport/#"]).matches("sport").is_some());

    let plus = trie(&["sport/tennis/+"]);
    assert!(plus.matches("sport/tennis/player1").is_some());
    assert!(plus.matches("sport/tennis/player1/ranking").is_none());

    let sport = trie(&["sport/+"]);
    assert!(sport.matches("sport/").is_some());
    assert!(sport.matches("sport").is_none());

    assert!(trie(&["+/+"]).matches("/finance").is_some());
    assert!(trie(&["/+"]).matches("/finance").is_some());
    assert!(trie(&["+"]).matches("/finance").is_none());
}

#[test]
fn a_dollar_topic_is_not_matched_by_a_leading_wildcard() {
    for filter in ["#", "+/monitor/Clients", "+/#"] {
        assert!(trie(&[filter]).matches("$SYS/monitor/Clients").is_none());
    }
    assert!(trie(&["$SYS/#"]).matches("$SYS/monitor/Clients").is_some());
    assert!(trie(&["a/$b"]).matches("a/$b").is_some());
}

#[test]
fn matches_agree_with_the_rules_over_every_small_filter_and_topic() {
    let levels = ["a", "b", "", "+", "#"];
    let mut filters = vec![];
    let mut topics = vec![];
    let mut combos: Vec<Vec<&str>> = vec![vec![]];
    for _ in 0..3 {
        combos = combos
            .into_iter()
            .flat_map(|c| {
                levels.iter().map(move |l| {
                    let mut c = c.clone();
                    c.push(*l);
                    c
                })
            })
            .collect();
        for combo in &combos {
            // `#` is only the last level.
            if combo
                .iter()
                .enumerate()
                .all(|(i, l)| *l != "#" || i == combo.len() - 1)
            {
                filters.push(combo.join("/"));
            }
            if combo.iter().all(|l| *l != "+" && *l != "#") {
                topics.push(combo.join("/"));
            }
        }
    }
    topics.push("$s/a".into());
    for filter in &filters {
        let single = trie(&[filter]);
        for topic in &topics {
            assert_eq!(
                single.matches(topic).is_some(),
                direct(filter, topic),
                "filter {filter:?} topic {topic:?}"
            );
        }
    }
}

#[test]
fn the_highest_granted_qos_wins_and_remove_prunes() {
    let mut trie = FilterTrie::default();
    trie.insert("a/+", 0);
    trie.insert("a/b", 1);
    assert_eq!(trie.matches("a/b"), Some(1));
    assert_eq!(trie.matches("a/c"), Some(0));
    assert!(trie.remove("a/b"));
    assert_eq!(trie.matches("a/b"), Some(0));
    assert!(!trie.remove("a/b"));
    assert!(trie.remove("a/+"));
    assert_eq!(trie.len(), 0);
    assert!(trie.root.children.is_empty());
}

#[test]
fn a_literal_prefix_names_the_mount() {
    assert_eq!(literal_prefix("plant/+/temp"), Some("plant/"));
    assert_eq!(literal_prefix("plant/#"), Some("plant/"));
    assert_eq!(literal_prefix("#"), Some(""));
    assert_eq!(literal_prefix("plant/1/temp"), None);
}
