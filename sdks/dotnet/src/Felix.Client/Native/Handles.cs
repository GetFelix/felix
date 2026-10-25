using System.Runtime.InteropServices;

namespace Felix.Native;

// `*_free` accepts a handle from any thread, including a callback, but never
// while another thread is calling the handle. SafeHandle's reference count
// keeps a free waiting until a call that is starting has returned.

/// <summary>
/// A native handle that shares the client's runtime and so its token source: the provider
/// stays uncancelled until the client and every such handle are disposed.
/// </summary>
internal abstract class TokenHeldHandle : SafeHandle
{
    private TokenSource? tokens;

    protected TokenHeldHandle(nint invalid, bool ownsHandle)
        : base(invalid, ownsHandle)
    {
    }

    /// <summary>Takes a reference on <paramref name="source"/> until this handle is released.</summary>
    internal void Track(TokenSource? source)
    {
        if (source is null || Volatile.Read(ref tokens) is not null || !source.Retain())
        {
            return;
        }

        if (Interlocked.CompareExchange(ref tokens, source, null) is not null)
        {
            source.ReleaseHolder();
            return;
        }

        if (IsClosed)
        {
            Untrack();
        }
    }

    protected void Untrack() => Interlocked.Exchange(ref tokens, null)?.ReleaseHolder();
}

internal sealed class FelixClientHandle : SafeHandle
{
    internal FelixClientHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_client_free(handle);
        return true;
    }
}

internal sealed class FelixSubscriptionHandle : TokenHeldHandle
{
    internal FelixSubscriptionHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_subscription_free(handle);
        Untrack();
        return true;
    }
}

internal sealed class FelixShardedSubscriptionHandle : TokenHeldHandle
{
    internal FelixShardedSubscriptionHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_sharded_subscription_free(handle);
        Untrack();
        return true;
    }
}

internal sealed class FelixEventHandle : SafeHandle
{
    internal FelixEventHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_event_free(handle);
        return true;
    }
}

internal sealed class FelixShardedGroupHandle : TokenHeldHandle
{
    internal FelixShardedGroupHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_sharded_group_free(handle);
        Untrack();
        return true;
    }
}

internal sealed class FelixCacheWatchHandle : TokenHeldHandle
{
    internal FelixCacheWatchHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_cache_watch_free(handle);
        Untrack();
        return true;
    }
}

internal sealed class FelixProducerHandle : TokenHeldHandle
{
    internal FelixProducerHandle(nint handle)
        : base(0, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.felix_producer_free(handle);
        Untrack();
        return true;
    }
}

/// <summary>Holds a SafeHandle's reference for the span of one native call.</summary>
internal readonly struct HandleLease : IDisposable
{
    private readonly SafeHandle handle;

    internal HandleLease(SafeHandle handle)
    {
        this.handle = handle;
        var added = false;
        handle.DangerousAddRef(ref added);
        Pointer = handle.DangerousGetHandle();
    }

    internal nint Pointer { get; }

    public void Dispose() => handle.DangerousRelease();
}
