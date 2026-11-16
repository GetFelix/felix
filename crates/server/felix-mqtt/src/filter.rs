//! Topic filters: the trie a session matches records against.
//!
//! Matching follows MQTT 4.7: `+` is one level, `#` is the rest including the
//! parent, and a topic that starts with `$` is not matched by a filter whose
//! first level is a wildcard.

use std::collections::HashMap;

/// A filter's levels that come before its first wildcard, with the trailing
/// `/`. Every topic the filter matches starts with it, so it names the mount.
/// `None` when the filter has no wildcard.
pub(crate) fn literal_prefix(filter: &str) -> Option<&str> {
    let at = filter.find(['+', '#'])?;
    // A wildcard is a whole level, so what precedes it ends in `/` or is empty.
    Some(&filter[..at])
}

/// Filters mapped to the highest QoS granted, matched by topic.
#[derive(Default)]
pub(crate) struct FilterTrie {
    root: Node,
    len: usize,
    next_id: u64,
}

#[derive(Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    /// The QoS granted to a filter that ends here.
    granted: Option<u8>,
    /// That filter's id, kept while it is replaced.
    id: u64,
}

impl FilterTrie {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn contains(&self, filter: &str) -> bool {
        let mut node = &self.root;
        for level in filter.split('/') {
            match node.children.get(level) {
                Some(next) => node = next,
                None => return false,
            }
        }
        node.granted.is_some()
    }

    /// The id of `filter`, if it is here.
    pub(crate) fn id_of(&self, filter: &str) -> Option<u64> {
        let mut node = &self.root;
        for level in filter.split('/') {
            node = node.children.get(level)?;
        }
        node.granted.map(|_| node.id)
    }

    /// Add `filter`, or change its QoS. Returns the filter's id, which a
    /// replaced filter keeps.
    pub(crate) fn insert(&mut self, filter: &str, granted: u8) -> u64 {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.into()).or_default();
        }
        if node.granted.replace(granted).is_none() {
            self.len += 1;
            self.next_id += 1;
            node.id = self.next_id;
        }
        node.id
    }

    /// Remove `filter`; whether it was there.
    pub(crate) fn remove(&mut self, filter: &str) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        let removed = remove(&mut self.root, &levels);
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// The highest QoS among the filters `topic` matches, or `None`.
    #[cfg(test)]
    pub(crate) fn matches(&self, topic: &str) -> Option<u8> {
        self.matches_where(topic, |_| true)
    }

    /// [`Self::matches`], counting only the filters whose id `keep` accepts.
    pub(crate) fn matches_where(&self, topic: &str, keep: impl Fn(u64) -> bool) -> Option<u8> {
        let levels: Vec<&str> = topic.split('/').collect();
        let dollar = topic.starts_with('$');
        let mut best = None;
        walk(&self.root, &levels, 0, dollar, &keep, &mut best);
        best
    }
}

fn remove(node: &mut Node, levels: &[&str]) -> bool {
    let Some((first, rest)) = levels.split_first() else {
        return node.granted.take().is_some();
    };
    let Some(child) = node.children.get_mut(*first) else {
        return false;
    };
    let removed = remove(child, rest);
    if child.granted.is_none() && child.children.is_empty() {
        node.children.remove(*first);
    }
    removed
}

fn walk(
    node: &Node,
    levels: &[&str],
    at: usize,
    dollar: bool,
    keep: &dyn Fn(u64) -> bool,
    best: &mut Option<u8>,
) {
    let mut take = |node: &Node| {
        if let Some(granted) = node.granted
            && keep(node.id)
        {
            *best = Some(best.map_or(granted, |b: u8| b.max(granted)));
        }
    };
    let wild_ok = !(at == 0 && dollar);
    if at == levels.len() {
        take(node);
        // `a/#` matches `a`.
        if let Some(hash) = node.children.get("#")
            && wild_ok
        {
            take(hash);
        }
        return;
    }
    if wild_ok {
        if let Some(hash) = node.children.get("#") {
            take(hash);
        }
        if let Some(plus) = node.children.get("+") {
            walk(plus, levels, at + 1, dollar, keep, best);
        }
    }
    if let Some(next) = node.children.get(levels[at]) {
        walk(next, levels, at + 1, dollar, keep, best);
    }
}

#[cfg(test)]
mod tests;
