//! Running work that its caller must not cut short.

use std::future::Future;

use crate::{Result, StorageError};

/// Run `work` to the end on a task of its own, and wait for its answer.
///
/// For an append followed by work that has to happen once the record is in
/// the log, such as applying it and telling watchers. The append thread
/// writes a batch whatever happens to the future that asked for it, so a
/// caller cancelled while it waits would otherwise leave the record in the log
/// and nowhere else. Here a cancelled caller only stops waiting.
pub(crate) async fn run_to_end<T, F>(work: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::spawn(work).await {
        Ok(answer) => answer,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        Err(_) => Err(StorageError::Closed("the runtime is shutting down".into())),
    }
}
