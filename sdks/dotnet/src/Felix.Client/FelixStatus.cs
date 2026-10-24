namespace Felix;

/// <summary>The status codes of the C ABI, which are the Rust client's error classes.</summary>
public enum FelixStatus
{
    /// <summary>The call did what it was asked.</summary>
    Ok = 0,
    /// <summary>A bounded wait ended with nothing.</summary>
    Timeout = 1,
    /// <summary>The broker ended the subscription.</summary>
    Closed = 2,
    /// <summary>The call was cancelled.</summary>
    Cancelled = 3,
    /// <summary>A bad argument; nothing was sent.</summary>
    InvalidArgument = 10,
    /// <summary>The native library panicked; a bug worth reporting.</summary>
    Panic = 11,
    /// <summary>A failure with no more specific class.</summary>
    Error = 20,
    /// <summary>The broker was unreachable or the connection was lost.</summary>
    Connection = 21,
    /// <summary>The token was rejected or lacks the permission.</summary>
    Auth = 22,
    /// <summary>No such tenant, namespace, stream or cache.</summary>
    NotFound = 23,
    /// <summary>The start offset is no longer retained.</summary>
    Cursor = 24,
    /// <summary>No broker can serve the shard, usually while it moves.</summary>
    ShardUnavailable = 25,
    /// <summary>The broker is shedding load.</summary>
    Overloaded = 26,
    /// <summary>A write may or may not have been applied.</summary>
    OutcomeUnknown = 27,
    /// <summary>The broker does not support the operation.</summary>
    Unsupported = 28,
}
