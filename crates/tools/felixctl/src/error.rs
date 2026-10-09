//! Exit statuses, and which one an error ends the process with.
//!
//! Scripts branch on these, so each one means one thing and they do not change.
//! Code returns plain `anyhow` errors; [`exit_for`] reads the status from what
//! is in the chain: a [`Marked`] context added where the failure was
//! classified, or a typed error from `felix-client` or `reqwest`.

use std::fmt;

use felix_client::{
    BrokerError, CommitError, NotLeaderError, PublishRefused, SubscribeCursorError,
};

/// Why the process stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Exit {
    /// Anything not covered below.
    Failure = 1,
    /// Bad arguments, or settings missing or unreadable. clap uses 2 as well.
    Usage = 2,
    /// No broker or control plane could be reached.
    Connection = 3,
    /// A broker or the control plane refused the request.
    Server = 4,
    /// The key, resource or context does not exist.
    NotFound = 5,
    /// `inspect segments`: startup would repair a shard (a torn tail, or what
    /// an interrupted rollover left).
    WouldRepair = 6,
    /// `inspect segments`: startup would refuse a shard, or a record fails its
    /// checksum.
    Damaged = 7,
}

impl Exit {
    pub(crate) fn code(self) -> u8 {
        self as u8
    }
}

/// An error context carrying the exit status the error should produce.
#[derive(Debug)]
pub(crate) struct Marked {
    exit: Exit,
    message: String,
}

impl Marked {
    pub(crate) fn new(exit: Exit, message: impl Into<String>) -> Self {
        Self {
            exit,
            message: message.into(),
        }
    }
}

impl fmt::Display for Marked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Marked {}

/// A new error with an exit status.
pub(crate) fn fail(exit: Exit, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(Marked::new(exit, message))
}

/// Attach an exit status to a fallible result.
pub(crate) trait MarkExit<T> {
    fn mark(self, exit: Exit, message: impl Into<String>) -> anyhow::Result<T>;
}

impl<T, E> MarkExit<T> for Result<T, E>
where
    E: Into<anyhow::Error>,
{
    fn mark(self, exit: Exit, message: impl Into<String>) -> anyhow::Result<T> {
        self.map_err(|err| err.into().context(Marked::new(exit, message)))
    }
}

/// The exit status for `err`. The outermost [`Marked`] wins; without one, a
/// broker's typed refusal is a server error and an HTTP connect failure a
/// connection error.
pub(crate) fn exit_for(err: &anyhow::Error) -> Exit {
    if let Some(marked) = err.downcast_ref::<Marked>() {
        return marked.exit;
    }
    if err.downcast_ref::<BrokerError>().is_some()
        || err.downcast_ref::<PublishRefused>().is_some()
        || err.downcast_ref::<NotLeaderError>().is_some()
        || err.downcast_ref::<SubscribeCursorError>().is_some()
        || err.downcast_ref::<CommitError>().is_some()
    {
        return Exit::Server;
    }
    for cause in err.chain() {
        if let Some(http) = cause.downcast_ref::<reqwest::Error>()
            && (http.is_connect() || http.is_timeout())
        {
            return Exit::Connection;
        }
    }
    Exit::Failure
}

#[cfg(test)]
mod tests;
