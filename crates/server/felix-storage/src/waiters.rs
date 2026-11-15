//! Tasks waiting for a watched value to reach a key of their own, woken in
//! key order.
//!
//! `watch::Receiver::wait_for` wakes every waiter on every send, and each one
//! looks and sleeps again. With hundreds of writes waiting on one log's
//! position, every advance wakes all of them. A [`Queue`] sits beside the
//! watch and wakes only the waiters an advance reaches. The watch still
//! holds the value, and a woken waiter checks it there, so the queue decides
//! only when to look, never what is true.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
use tokio::sync::watch;

/// The sending side. Call [`Queue::advance`] after every send on the watch
/// it stands beside. Dropping it wakes every waiter, which then reports
/// [`Closed`] unless the value already satisfies it.
pub struct Queue<K: Ord + Copy> {
    shared: Arc<Mutex<State<K>>>,
}

/// The waiting side, cheap to clone.
pub struct Waiters<K: Ord + Copy> {
    shared: Arc<Mutex<State<K>>>,
}

/// The queue was dropped, or the watch's sender was, before the value got
/// where a waiter needed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

struct State<K> {
    /// The key of the value the last advance saw.
    reached: Option<K>,
    next: u64,
    /// Waiting for the key to reach theirs. The `u64` keeps equal keys apart.
    keyed: BTreeMap<(K, u64), Option<Waker>>,
    /// Waiting for any advance: their key was reached and the value still
    /// did not satisfy them.
    any: BTreeMap<u64, Option<Waker>>,
    closed: bool,
}

#[derive(Clone, Copy)]
enum Entry<K> {
    Keyed(K, u64),
    Any(u64),
}

/// One registration. Ready once an advance or the close took it off the
/// queue.
struct Wake<K: Ord + Copy> {
    shared: Arc<Mutex<State<K>>>,
    entry: Option<Entry<K>>,
    closed: bool,
}

impl<K: Ord + Copy> Queue<K> {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(State {
                reached: None,
                next: 0,
                keyed: BTreeMap::new(),
                any: BTreeMap::new(),
                closed: false,
            })),
        }
    }

    pub fn waiters(&self) -> Waiters<K> {
        Waiters {
            shared: Arc::clone(&self.shared),
        }
    }

    /// The watched value's key is now `now`. Wakes every waiter whose key is
    /// at or below it, and every waiter waiting for any advance. `now` may
    /// go down, as a truncated log's position does. Call it after the send,
    /// so a woken waiter finds the new value.
    pub fn advance(&self, now: Option<K>) {
        let mut due = Vec::new();
        {
            let mut state = self.shared.lock();
            state.reached = now;
            if let Some(now) = now {
                while let Some(entry) = state.keyed.first_entry() {
                    if entry.key().0 > now {
                        break;
                    }
                    due.extend(entry.remove());
                }
            }
            due.extend(std::mem::take(&mut state.any).into_values().flatten());
        }
        for waker in due {
            waker.wake();
        }
    }
}

impl<K: Ord + Copy> Default for Queue<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Copy> Drop for Queue<K> {
    fn drop(&mut self) {
        let due: Vec<Waker> = {
            let mut state = self.shared.lock();
            state.closed = true;
            let keyed = std::mem::take(&mut state.keyed).into_values();
            let any = std::mem::take(&mut state.any).into_values();
            keyed.chain(any).flatten().collect()
        };
        for waker in due {
            waker.wake();
        }
    }
}

impl<K: Ord + Copy> Clone for Waiters<K> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<K: Ord + Copy> Waiters<K> {
    /// Wait until `ready` returns something for the value `watched` holds,
    /// as `watch::Receiver::wait_for` does. `ready` must return `None`
    /// while the value's key is below `key`; it may return `None` above it
    /// too, and is then asked again on every advance.
    ///
    /// The waiter is queued before it looks, so an advance that lands
    /// between the look and the sleep still wakes it.
    pub async fn wait_for<V, T>(
        &self,
        watched: &watch::Receiver<V>,
        key: K,
        mut ready: impl FnMut(&V) -> Option<T>,
    ) -> Result<T, Closed> {
        loop {
            let wake = self.register(key);
            if let Some(found) = ready(&watched.borrow()) {
                return Ok(found);
            }
            if wake.closed || watched.has_changed().is_err() {
                return Err(Closed);
            }
            wake.await;
        }
    }

    fn register(&self, key: K) -> Wake<K> {
        let mut state = self.shared.lock();
        let closed = state.closed;
        let entry = (!closed).then(|| {
            let id = state.next;
            state.next += 1;
            // A key already reached would be woken by nothing below it, so
            // it waits for the next advance of any kind.
            if state.reached.is_some_and(|reached| reached >= key) {
                state.any.insert(id, None);
                Entry::Any(id)
            } else {
                state.keyed.insert((key, id), None);
                Entry::Keyed(key, id)
            }
        });
        Wake {
            shared: Arc::clone(&self.shared),
            entry,
            closed,
        }
    }

    /// How many waits are queued, for tests and metrics.
    pub fn queued(&self) -> usize {
        let state = self.shared.lock();
        state.keyed.len() + state.any.len()
    }
}

impl<K: Ord + Copy> Future for Wake<K> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(entry) = self.entry else {
            return Poll::Ready(());
        };
        let mut state = self.shared.lock();
        let slot = match entry {
            Entry::Keyed(key, id) => state.keyed.get_mut(&(key, id)),
            Entry::Any(id) => state.any.get_mut(&id),
        };
        match slot {
            Some(slot) => {
                if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *slot = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

impl<K: Ord + Copy> Drop for Wake<K> {
    fn drop(&mut self) {
        let Some(entry) = self.entry else {
            return;
        };
        let mut state = self.shared.lock();
        match entry {
            Entry::Keyed(key, id) => state.keyed.remove(&(key, id)),
            Entry::Any(id) => state.any.remove(&id),
        };
    }
}

#[cfg(test)]
mod tests;
