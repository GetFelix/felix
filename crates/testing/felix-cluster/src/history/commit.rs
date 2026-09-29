//! Atomic commits: each writes a value as a list's event and as its state,
//! in one record. The rule is that no reader ever sees one without the
//! other.
//!
//! A commit read takes three looks, in order: the list, the state, the list
//! again. The first read bounds the state from below: every commit event it
//! saw was in the log before the state was read, so the state must be at
//! least that commit. The state bounds the second read: its version is an
//! offset that read must hold, with the state's value in it. A commit's
//! append is recorded as an ordinary append too, so the list rules cover its
//! event as well.

use std::collections::BTreeSet;
use std::fmt;

use super::checker::{Rule, Violation};
use super::model::{Element, millis};

/// The key every commit writes, in its list's state.
pub const STATE_KEY: &str = "state";

/// One commit read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRead {
    pub process: usize,
    pub invoke: u64,
    pub complete: u64,
    pub list: String,
    /// The list, read to its tail before the state was read.
    pub before: Vec<Element>,
    /// The state: the value it held and the offset of the commit that wrote
    /// it. `None` when no commit had written it.
    pub state: Option<StateSeen>,
    /// The list read from the state's version after the state was read, to
    /// a tail past it; `None` when there was no state to look for, or the
    /// read fell short of it.
    pub after: Option<Vec<Element>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateSeen {
    pub value: u64,
    pub version: u64,
}

impl fmt::Display for CommitRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span = format!("{}..{}", millis(self.invoke), millis(self.complete));
        let state = match self.state {
            Some(state) => format!("{} at {}", state.value, state.version),
            None => "nothing".to_string(),
        };
        write!(
            f,
            "client {} read {} and its state -> {state} [{span}]",
            self.process, self.list
        )
    }
}

/// Check every commit read against the values commits wrote.
pub(crate) fn check(reads: &[CommitRead], commits: &BTreeSet<u64>, out: &mut Vec<Violation>) {
    for (index, read) in reads.iter().enumerate() {
        let mut push = |explanation: String| {
            out.push(Violation {
                rule: Rule::PartialCommit,
                list: read.list.clone(),
                ops: vec![index],
                explanation,
            });
        };
        let newest = read
            .before
            .iter()
            .filter(|element| commits.contains(&element.value))
            .max_by_key(|element| element.offset);
        if let Some(event) = newest
            && read.state.is_none_or(|state| state.version < event.offset)
        {
            push(format!(
                "commit read #{index} {read} saw the event of commit {} at offset {} before \
                 it read the state, and the state lacks it",
                event.value, event.offset
            ));
        }
        let Some(state) = read.state else {
            continue;
        };
        if !commits.contains(&state.value) {
            push(format!(
                "commit read #{index} {read} saw state {} that no commit wrote",
                state.value
            ));
            continue;
        }
        let Some(after) = &read.after else {
            continue;
        };
        match after.iter().find(|element| element.offset == state.version) {
            Some(event) if event.value == state.value => {}
            Some(event) => push(format!(
                "commit read #{index} {read} saw state {} at version {}, but offset {} holds \
                 value {}",
                state.value, state.version, state.version, event.value
            )),
            None => push(format!(
                "commit read #{index} {read} saw state {} at version {}, and a later read of \
                 the list has no event there",
                state.value, state.version
            )),
        }
    }
}

#[cfg(test)]
mod tests;
