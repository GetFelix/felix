using System.Runtime.InteropServices;

namespace Felix.Native;

// `*_free` accepts a handle from any thread, including a callback, but never
// while another thread is calling the handle. SafeHandle's reference count
// keeps a free waiting until a call that is starting has returned.

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

internal sealed class FelixSubscriptionHandle : SafeHandle
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

internal sealed class FelixShardedGroupHandle : SafeHandle
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
        return true;
    }
}

internal sealed class FelixCacheWatchHandle : SafeHandle
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
        return true;
    }
}

internal sealed class FelixProducerHandle : SafeHandle
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
