//! Threads that run Raft groups' tasks, a few groups to a thread.
//!
//! openraft spawns its tasks on the runtime it is started from. A group
//! started on one of these threads keeps its core, state machine worker,
//! IO forwarder and replication streams there, so a wake between them is
//! a local queue push instead of a cross-thread unpark, and one unpark
//! serves every ready group on the thread. See docs/per-shard-raft.md,
//! "Group threads".

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// A fixed set of group threads. Stops them when dropped.
pub(crate) struct GroupThreads {
    threads: Vec<GroupThread>,
    stop: CancellationToken,
}

struct GroupThread {
    handle: Handle,
    placed: Arc<AtomicUsize>,
}

/// A group's place on a thread. Dropping it frees the place.
pub(crate) struct Placed {
    handle: Handle,
    placed: Arc<AtomicUsize>,
}

impl GroupThreads {
    /// Start `count` threads. `count` must be at least one.
    pub(crate) fn start(count: usize) -> std::io::Result<Self> {
        let stop = CancellationToken::new();
        let mut threads = Vec::with_capacity(count);
        for at in 0..count {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let handle = runtime.handle().clone();
            let stopped = stop.clone();
            std::thread::Builder::new()
                .name(format!("felix-raft-group-{at}"))
                // The runtime is dropped here, off any async context, once
                // the stop comes. Tasks still on it are dropped with it.
                .spawn(move || runtime.block_on(stopped.cancelled()))?;
            threads.push(GroupThread {
                handle,
                placed: Arc::default(),
            });
        }
        Ok(Self { threads, stop })
    }

    /// A place on the thread with the fewest groups. Groups never move,
    /// since tasks cannot, so placing new ones is the only balancing.
    pub(crate) fn place(&self) -> Placed {
        let thread = self
            .threads
            .iter()
            .min_by_key(|thread| thread.placed.load(Ordering::Acquire))
            .expect("at least one group thread");
        thread.placed.fetch_add(1, Ordering::AcqRel);
        Placed {
            handle: thread.handle.clone(),
            placed: Arc::clone(&thread.placed),
        }
    }

    /// Groups placed on each thread.
    pub(crate) fn placed(&self) -> Vec<usize> {
        self.threads
            .iter()
            .map(|thread| thread.placed.load(Ordering::Acquire))
            .collect()
    }
}

impl Drop for GroupThreads {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Placed {
    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for Placed {
    fn drop(&mut self) {
        self.placed.fetch_sub(1, Ordering::AcqRel);
    }
}
