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
}

#[derive(Default)]
struct Node {
    children: HashMap<Box<str>, Node>,
    /// The QoS granted to a filter that ends here.
    granted: Option<u8>,
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

    /// Add `filter`, or change its QoS.
    pub(crate) fn insert(&mut self, filter: &str, granted: u8) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.into()).or_default();
        }
        if node.granted.replace(granted).is_none() {
            self.len += 1;
        }
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
    pub(crate) fn matches(&self, topic: &str) -> Option<u8> {
        let levels: Vec<&str> = topic.split('/').collect();
        let dollar = topic.starts_with('$');
        let mut best = None;
        walk(&self.root, &levels, 0, dollar, &mut best);
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

fn walk(node: &Node, levels: &[&str], at: usize, dollar: bool, best: &mut Option<u8>) {
    let mut take = |granted: Option<u8>| {
        if let Some(granted) = granted {
            *best = Some(best.map_or(granted, |b: u8| b.max(granted)));
        }
    };
    let wild_ok = !(at == 0 && dollar);
    if at == levels.len() {
        take(node.granted);
        // `a/#` matches `a`.
        if let Some(hash) = node.children.get("#")
            && wild_ok
        {
            take(hash.granted);
        }
        return;
    }
    if wild_ok {
        if let Some(hash) = node.children.get("#") {
            take(hash.granted);
        }
        if let Some(plus) = node.children.get("+") {
            walk(plus, levels, at + 1, dollar, best);
        }
    }
    if let Some(next) = node.children.get(levels[at]) {
        walk(next, levels, at + 1, dollar, best);
    }
}

#[cfg(test)]
mod tests;
