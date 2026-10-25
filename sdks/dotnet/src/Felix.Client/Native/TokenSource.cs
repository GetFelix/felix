using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

/// <summary>How a fetch is answered; a seam so tests can run the provider without a native reply.</summary>
internal interface ITokenReplies
{
    /// <summary>Completes a fetch with a token. Non-zero means the reply is still open.</summary>
    int Ok(nint reply, string token);

    /// <summary>Completes a fetch with a failure.</summary>
    int Err(nint reply, string message);
}

internal sealed class NativeTokenReplies : ITokenReplies
{
    internal static readonly NativeTokenReplies Instance = new();

    public int Ok(nint reply, string token) => NativeMethods.felix_token_reply_ok(reply, token);

    public int Err(nint reply, string message) => NativeMethods.felix_token_reply_err(reply, message);
}

/// <summary>
/// The app's <see cref="FelixClientOptions.TokenProvider"/> behind the library's fetch callback.
/// The library decides when to fetch (laziness, the two-thirds refresh, one retry after a
/// refusal, one call at a time, the 30 s wait); this only runs the provider and answers each
/// reply exactly once. Its GCHandle is the callback's <c>user_data</c> and lives until
/// <c>token_release</c>, which runs after the client is freed and the last reply is answered.
/// </summary>
internal sealed class TokenSource
{
    /// <summary>How long a fetch may run before the provider's token is cancelled.</summary>
    internal static readonly TimeSpan FetchTimeout = TimeSpan.FromSeconds(30);

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static int live;
    private static int released;

    private readonly Func<CancellationToken, ValueTask<string>> provider;
    private readonly ITokenReplies replies;
    private readonly TimeSpan timeout;
    private readonly object gate = new();
    private readonly List<CancellationTokenSource> inFlight = [];
    private GCHandle self;
    private int allocated;

    internal TokenSource(
        Func<CancellationToken, ValueTask<string>> provider,
        ITokenReplies? replies = null,
        TimeSpan? timeout = null)
    {
        this.provider = provider;
        this.replies = replies ?? NativeTokenReplies.Instance;
        this.timeout = timeout ?? FetchTimeout;
        Interlocked.Increment(ref live);
    }

    /// <summary>Sources whose <c>token_release</c> has not run (or that were not abandoned); a test hook for leaks.</summary>
    internal static int Live => Volatile.Read(ref live);

    /// <summary>Sources released by the library so far; a test hook.</summary>
    internal static int Released => Volatile.Read(ref released);

    /// <summary>The <c>token_user_data</c> to pass to the library.</summary>
    internal nint Pin()
    {
        self = GCHandle.Alloc(this);
        Volatile.Write(ref allocated, 1);
        return GCHandle.ToIntPtr(self);
    }

    /// <summary>The connect call was refused, so <c>token_release</c> will never run.</summary>
    internal void Abandon() => Free();

    internal static TokenSource? FromHandle(nint userData) =>
        userData == 0 ? null : GCHandle.FromIntPtr(userData).Target as TokenSource;

    /// <summary>
    /// Cancels the fetches running now, so a provider stuck on the network gives up when the
    /// client is disposed. Later fetches (a subscription outliving the client may still need
    /// one) get a fresh token.
    /// </summary>
    internal void Cancel()
    {
        CancellationTokenSource[] running;
        lock (gate)
        {
            running = [.. inFlight];
        }

        foreach (var source in running)
        {
            try
            {
                source.Cancel();
            }
            catch
            {
                // Disposed by its fetch finishing, or a registered callback threw.
            }
        }
    }

    /// <summary>The library is done with the source: runs once, from <c>token_release</c>.</summary>
    internal void Release() => Free();

    /// <summary>
    /// Starts the provider and returns. The library's thread must not wait on the app, so
    /// the call runs on the thread pool. Never throws.
    /// </summary>
    internal void Fetch(nint reply)
    {
        try
        {
            _ = Task.Run(() => RunAsync(reply));
        }
        catch (Exception e)
        {
            Answer(reply, null, Message(e));
        }
    }

    private void Free()
    {
        if (Interlocked.Exchange(ref allocated, 0) == 1)
        {
            self.Free();
            Interlocked.Decrement(ref live);
            Interlocked.Increment(ref released);
        }
    }

    private async Task RunAsync(nint reply)
    {
        string? token = null;
        string? failure = null;
        CancellationTokenSource? fetch = null;
        try
        {
            fetch = new CancellationTokenSource(timeout);
            lock (gate)
            {
                inFlight.Add(fetch);
            }

            token = await provider(fetch.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (fetch is { IsCancellationRequested: true })
        {
            failure = "the token provider was cancelled: it did not finish within 30 seconds, or the client was disposed";
        }
        catch (Exception e)
        {
            failure = Message(e);
        }
        finally
        {
            if (fetch is not null)
            {
                lock (gate)
                {
                    inFlight.Remove(fetch);
                }

                fetch.Dispose();
            }
        }

        Answer(reply, token, failure ?? Invalid(token));
    }

    /// <summary>Why <paramref name="token"/> cannot be handed to the library, or null if it can.</summary>
    private static string? Invalid(string? token)
    {
        if (token is null)
        {
            return "the token provider returned null";
        }

        if (token.Length == 0)
        {
            return "the token provider returned an empty token";
        }

        if (token.Contains('\0'))
        {
            return "the token provider returned a token containing a NUL character";
        }

        try
        {
            StrictUtf8.GetByteCount(token);
        }
        catch (EncoderFallbackException)
        {
            return "the token provider returned a token that is not valid UTF-8";
        }

        return null;
    }

    private static string Message(Exception e) => string.IsNullOrEmpty(e.Message) ? e.GetType().Name : e.Message;

    /// <summary>Completes the reply, once. Nothing may escape into native code.</summary>
    private void Answer(nint reply, string? token, string? failure)
    {
        try
        {
            if (failure is null && token is not null)
            {
                var status = replies.Ok(reply, token);
                if (status == 0)
                {
                    return;
                }

                // Refused for a bad argument, which leaves the reply open.
                failure = $"the token library refused the token (status {status})";
            }

            replies.Err(reply, failure ?? "the token fetch failed");
        }
        catch
        {
            try
            {
                replies.Err(reply, failure ?? "the token fetch failed");
            }
            catch
            {
                // Nothing more can be done, and nothing may escape.
            }
        }
    }
}
