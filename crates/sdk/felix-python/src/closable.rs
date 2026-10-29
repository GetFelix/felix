//! A handle's inner value behind a lock that `close` never waits on.
//!
//! A reader parked on an idle stream holds the lock for as long as it waits.
//! `close` raises a flag instead of taking the lock, which wakes the reader,
//! and the reader drops the value on its way out.

use std::future::Future;
use std::ops::{Deref, DerefMut};

use tokio::sync::{Mutex, MutexGuard, watch};

pub(crate) struct Closable<T> {
    inner: Mutex<Option<T>>,
    closed: watch::Sender<bool>,
}

/// The inner value while a reader uses it. If the handle was closed in the
/// meantime the value is dropped on release, whoever got there first.
pub(crate) struct Locked<'a, T> {
    owner: &'a Closable<T>,
    guard: MutexGuard<'a, Option<T>>,
}

impl<T> Closable<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(Some(value)),
            closed: watch::Sender::new(false),
        }
    }

    /// Wake any reader and release the value, without waiting on a reader.
    pub(crate) fn close(&self) {
        self.closed.send_replace(true);
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// The value, or `None` if the handle is closed, including when it closes
    /// while this waits behind another reader.
    pub(crate) async fn lock(&self) -> Option<Locked<'_, T>> {
        let mut closed = self.closed.subscribe();
        let guard = tokio::select! {
            guard = self.inner.lock() => guard,
            _ = closed.wait_for(|closed| *closed) => return None,
        };
        let locked = Locked { owner: self, guard };
        (!self.is_closed() && locked.guard.is_some()).then_some(locked)
    }

    /// Run `future`, or `None` if the handle closes first.
    pub(crate) async fn until_closed<F: Future>(&self, future: F) -> Option<F::Output> {
        let mut closed = self.closed.subscribe();
        tokio::select! {
            output = future => Some(output),
            _ = closed.wait_for(|closed| *closed) => None,
        }
    }
}

impl<T> Locked<'_, T> {
    /// The stream ended: release the value and read as closed from now on.
    pub(crate) fn end(&mut self) {
        self.guard.take();
        self.owner.closed.send_replace(true);
    }
}

impl<T> Deref for Locked<'_, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for Locked<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<T> Drop for Locked<'_, T> {
    fn drop(&mut self) {
        if self.owner.is_closed() {
            self.guard.take();
        }
    }
}

#[cfg(test)]
mod tests;
