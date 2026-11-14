//! A cache group's maps while a snapshot is built from them.
//!
//! The snapshot cut moves the live maps into a frozen base that nothing
//! changes, so the build can encode it off apply. Apply then writes to a
//! delta laid over the base, and reads look in the delta first. Once the
//! build lets go of the base, the delta is folded back into it a chunk at
//! a time. Steady state has no delta, so reads and apply cost what plain
//! maps cost.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::Peekable;
use std::ops::Bound;

use super::state::{Answer, Entry, Producer};

/// The maps that make up a cache group's state, apart from its time.
// Ordered maps throughout: a snapshot walks them, and two members must
// write the same bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Tables {
    pub(super) entries: BTreeMap<String, Entry>,
    /// Every entry that expires, by when.
    pub(super) expiring: BTreeSet<(u64, String)>,
    pub(super) counters: BTreeMap<String, i64>,
    pub(super) producers: BTreeMap<u64, Producer>,
}

/// What apply changed over a frozen base.
#[derive(Debug, Clone, Default)]
pub(super) struct Delta {
    /// `None` hides the base's value.
    pub(super) entries: BTreeMap<String, Option<Entry>>,
    /// `false` hides the base's member.
    pub(super) expiring: BTreeMap<(u64, String), bool>,
    /// Counters are never removed, so a sum here simply replaces the base's.
    pub(super) counters: BTreeMap<String, i64>,
    /// Each producer touched since the cut: its floor, and the answers
    /// kept since. The base's answers below that floor are dropped.
    pub(super) producers: BTreeMap<u64, Producer>,
    /// Every base member of `expiring` up to here is hidden, so a walk of
    /// the set starts after it. Expiry removes members from the front, and
    /// without this each walk during a build would step over all of them.
    pub(super) hidden_through: Option<(u64, String)>,
}

impl Delta {
    /// Add `member` to the expiring set.
    pub(super) fn show_expiring(&mut self, base: &Tables, member: (u64, String)) {
        if base.expiring.contains(&member) {
            self.expiring.remove(&member);
            self.unhide(base, &member);
        } else {
            self.expiring.insert(member, true);
        }
    }

    /// Remove `member` from the expiring set.
    pub(super) fn hide_expiring(&mut self, base: &Tables, member: (u64, String)) {
        if !base.expiring.contains(&member) {
            self.expiring.remove(&member);
            return;
        }
        self.expiring.insert(member, false);
        let from = match &self.hidden_through {
            Some(through) => Bound::Excluded(through),
            None => Bound::Unbounded,
        };
        let mut through = None;
        for member in base
            .expiring
            .range::<(u64, String), _>((from, Bound::Unbounded))
        {
            if self.expiring.get(member) != Some(&false) {
                break;
            }
            through = Some(member);
        }
        if let Some(through) = through {
            self.hidden_through = Some(through.clone());
        }
    }

    /// `member` of the base is no longer hidden, or is about to join it.
    fn unhide(&mut self, base: &Tables, member: &(u64, String)) {
        if self
            .hidden_through
            .as_ref()
            .is_some_and(|through| member <= through)
        {
            self.hidden_through = base
                .expiring
                .range::<(u64, String), _>((Bound::Unbounded, Bound::Excluded(member)))
                .next_back()
                .cloned();
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.expiring.is_empty()
            && self.counters.is_empty()
            && self.producers.is_empty()
    }

    /// Move up to about `budget` changes into `base`. Returns whether any
    /// are left.
    pub(super) fn fold_into(&mut self, base: &mut Tables, budget: usize) -> bool {
        let mut left = budget;
        while left > 0 {
            let cost = if let Some((key, entry)) = self.entries.pop_first() {
                match entry {
                    Some(entry) => base.entries.insert(key, entry),
                    None => base.entries.remove(&key),
                };
                1
            } else if let Some((member, present)) = self.expiring.pop_first() {
                if present {
                    self.unhide(base, &member);
                    base.expiring.insert(member);
                } else {
                    base.expiring.remove(&member);
                }
                1
            } else if let Some((key, sum)) = self.counters.pop_first() {
                base.counters.insert(key, sum);
                1
            } else if let Some(mut delta) = self.producers.first_entry() {
                // A producer's answers move a chunk at a time too: one that
                // never raises its floor can have many.
                let known = base.producers.entry(*delta.key()).or_default();
                known.raise(delta.get().floor);
                let mut moved = 1;
                while moved < left
                    && let Some((sequence, answer)) = delta.get_mut().answers.pop_first()
                {
                    known.answers.insert(sequence, answer);
                    moved += 1;
                }
                if delta.get().answers.is_empty() {
                    delta.remove();
                }
                moved
            } else {
                return false;
            };
            left = left.saturating_sub(cost);
        }
        !self.is_empty()
    }
}

/// The state's maps as reads see them: the base with the delta, if any,
/// laid over it.
#[derive(Clone, Copy)]
pub(super) struct Layers<'a> {
    pub(super) base: &'a Tables,
    pub(super) delta: Option<&'a Delta>,
}

/// One producer as reads see it.
#[derive(Clone, Copy)]
pub(super) struct ProducerView<'a> {
    pub(super) floor: u64,
    base: Option<&'a BTreeMap<u64, Answer>>,
    delta: Option<&'a BTreeMap<u64, Answer>>,
}

impl<'a> ProducerView<'a> {
    pub(super) fn answer(&self, sequence: u64) -> Option<Answer> {
        if let Some(answer) = self.delta.and_then(|delta| delta.get(&sequence)) {
            return Some(*answer);
        }
        if sequence < self.floor {
            return None;
        }
        self.base?.get(&sequence).copied()
    }

    /// The kept answers, by sequence.
    pub(super) fn answers(&self) -> impl Iterator<Item = (&'a u64, &'a Answer)> + use<'a> {
        let floor = self.floor;
        let base = self
            .base
            .into_iter()
            .flat_map(move |base| base.range(floor..));
        let delta = self.delta.into_iter().flatten().map(|(k, v)| (k, Some(v)));
        overlay(base, delta)
    }
}

impl<'a> Layers<'a> {
    pub(super) fn entry(&self, key: &str) -> Option<&'a Entry> {
        if let Some(delta) = self.delta
            && let Some(entry) = delta.entries.get(key)
        {
            return entry.as_ref();
        }
        self.base.entries.get(key)
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&'a String, &'a Entry)> + use<'a> {
        let delta = self.delta.into_iter().flat_map(|delta| &delta.entries);
        overlay(
            self.base.entries.iter(),
            delta.map(|(k, v)| (k, v.as_ref())),
        )
    }

    /// Every entry that expires, by when.
    pub(super) fn expiring(&self) -> impl Iterator<Item = &'a (u64, String)> + use<'a> {
        let from = match self.delta.and_then(|delta| delta.hidden_through.as_ref()) {
            Some(through) => Bound::Excluded(through),
            None => Bound::Unbounded,
        };
        let base = self
            .base
            .expiring
            .range::<(u64, String), _>((from, Bound::Unbounded));
        let delta = self.delta.into_iter().flat_map(|delta| &delta.expiring);
        overlay(
            base.map(|member| (member, &())),
            delta.map(|(member, present)| (member, present.then_some(&()))),
        )
        .map(|(member, ())| member)
    }

    pub(super) fn counter(&self, key: &str) -> Option<i64> {
        self.delta
            .and_then(|delta| delta.counters.get(key))
            .or_else(|| self.base.counters.get(key))
            .copied()
    }

    pub(super) fn counters(&self) -> impl Iterator<Item = (&'a String, &'a i64)> + use<'a> {
        let delta = self.delta.into_iter().flat_map(|delta| &delta.counters);
        overlay(self.base.counters.iter(), delta.map(|(k, v)| (k, Some(v))))
    }

    pub(super) fn producer(&self, producer: u64) -> Option<ProducerView<'a>> {
        let base = self.base.producers.get(&producer);
        let delta = self.delta.and_then(|delta| delta.producers.get(&producer));
        view(base, delta)
    }

    pub(super) fn producers(&self) -> impl Iterator<Item = (&'a u64, ProducerView<'a>)> + use<'a> {
        let delta = self.delta.into_iter().flat_map(|delta| &delta.producers);
        zip(self.base.producers.iter(), delta)
            .filter_map(|(producer, base, delta)| Some((producer, view(base, delta)?)))
    }
}

fn view<'a>(base: Option<&'a Producer>, delta: Option<&'a Producer>) -> Option<ProducerView<'a>> {
    Some(ProducerView {
        floor: delta.or(base)?.floor,
        base: base.map(|base| &base.answers),
        delta: delta.map(|delta| &delta.answers),
    })
}

/// `base` with `top` laid over it, in key order: a key in `top` replaces
/// the base's, and `None` there hides it.
fn overlay<'a, K: Ord + 'a, V: 'a>(
    base: impl Iterator<Item = (&'a K, &'a V)>,
    top: impl Iterator<Item = (&'a K, Option<&'a V>)>,
) -> impl Iterator<Item = (&'a K, &'a V)> {
    zip(base, top).filter_map(|(key, below, above)| match above {
        Some(above) => above.map(|value| (key, value)),
        None => below.map(|value| (key, value)),
    })
}

/// Two iterators in key order, walked together: each key once, with what
/// each side has for it.
fn zip<'a, K: Ord + 'a, A, B>(
    a: impl Iterator<Item = (&'a K, A)>,
    b: impl Iterator<Item = (&'a K, B)>,
) -> impl Iterator<Item = (&'a K, Option<A>, Option<B>)> {
    Zip {
        a: a.peekable(),
        b: b.peekable(),
    }
}

struct Zip<I: Iterator, J: Iterator> {
    a: Peekable<I>,
    b: Peekable<J>,
}

impl<'a, K: Ord + 'a, A, B, I, J> Iterator for Zip<I, J>
where
    I: Iterator<Item = (&'a K, A)>,
    J: Iterator<Item = (&'a K, B)>,
{
    type Item = (&'a K, Option<A>, Option<B>);

    fn next(&mut self) -> Option<Self::Item> {
        let order = match (self.a.peek(), self.b.peek()) {
            (None, None) => return None,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a, _)), Some((b, _))) => a.cmp(b),
        };
        Some(match order {
            Ordering::Less => {
                let (key, a) = self.a.next()?;
                (key, Some(a), None)
            }
            Ordering::Greater => {
                let (key, b) = self.b.next()?;
                (key, None, Some(b))
            }
            Ordering::Equal => {
                let (key, a) = self.a.next()?;
                let (_, b) = self.b.next()?;
                (key, Some(a), Some(b))
            }
        })
    }
}
