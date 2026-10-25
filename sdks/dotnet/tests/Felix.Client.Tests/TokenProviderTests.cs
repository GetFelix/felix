using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// <see cref="FelixClientOptions.TokenProvider"/>. The library owns the timing rules (lazy fetch,
/// two-thirds refresh, one retry after a refusal, one call at a time, the 30 s wait) and tests them in
/// Rust. These cover what the binding adds: how the provider is started and cancelled, that every
/// reply is answered once, and that failures reach the caller. The fixture mints only hour-long
/// tokens, so none of them waits for an expiry. One class, so the counters below see one test at a time.
/// </summary>
public class TokenProviderTests
{
    /// <summary>Records how each fetch was answered instead of calling the library.</summary>
    private sealed class Replies : ITokenReplies
    {
        private readonly object gate = new();
        private readonly List<string> answers = [];
        private readonly TaskCompletionSource first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int okStatus;

        internal Replies(int okStatus = 0) => this.okStatus = okStatus;

        internal IReadOnlyList<string> Answers
        {
            get
            {
                lock (gate)
                {
                    return [.. answers];
                }
            }
        }

        internal Task Answered => first.Task;

        public int Ok(nint reply, string token)
        {
            Record($"ok:{token}");
            return okStatus;
        }

        public int Err(nint reply, string message)
        {
            Record($"err:{message}");
            return 0;
        }

        private void Record(string answer)
        {
            lock (gate)
            {
                answers.Add(answer);
            }

            first.TrySetResult();
        }
    }

    private static async Task<IReadOnlyList<string>> Run(
        Func<CancellationToken, ValueTask<string>> provider,
        int okStatus = 0,
        TimeSpan? timeout = null)
    {
        var replies = new Replies(okStatus);
        new TokenSource(provider, replies, timeout).Fetch(1);
        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(50);
        return replies.Answers;
    }

    // --- Argument checks and replies, no broker ---------------------------------

    [Fact]
    public async Task ATokenOrAProviderIsRequired()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t" }));
        await Assert.ThrowsAsync<ArgumentException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t", Token = "" }));
    }

    [Fact]
    public async Task AReturnedTokenAnswersOnce()
    {
        Assert.Equal(["ok:tok"], await Run(_ => new ValueTask<string>("tok")));
    }

    [Fact]
    public async Task TheProviderRunsOffTheCallingThread()
    {
        var caller = Environment.CurrentManagedThreadId;
        var release = new TaskCompletionSource();
        var ran = new TaskCompletionSource<int>();
        var replies = new Replies();
        new TokenSource(async _ =>
        {
            ran.SetResult(Environment.CurrentManagedThreadId);
            await release.Task;
            return "tok";
        }, replies).Fetch(1);

        // Fetch returned while the provider is still waiting, on another thread.
        Assert.NotEqual(caller, await ran.Task.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Empty(replies.Answers);
        release.SetResult();
        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task AnExceptionsMessageIsTheFailure()
    {
        var answers = await Run(_ => throw new InvalidOperationException("idp said: tenant été suspended"));
        Assert.Equal(["err:idp said: tenant été suspended"], answers);
    }

    [Fact]
    public async Task AnExceptionFromAnAsyncProviderIsTheFailure()
    {
        var answers = await Run(async _ =>
        {
            await Task.Yield();
            throw new KeyNotFoundException("no such refresh token");
        });
        Assert.Equal(["err:no such refresh token"], answers);
    }

    [Fact]
    public async Task AnExceptionWithoutAMessageIsNamed()
    {
        var answers = await Run(_ => throw new MessageLess());
        Assert.Equal(["err:MessageLess"], answers);
    }

    private sealed class MessageLess : Exception
    {
        public override string Message => "";
    }

    [Theory]
    [InlineData(null, "null")]
    [InlineData("", "empty")]
    [InlineData("to\0ken", "NUL")]
    public async Task ATokenTheLibraryCannotTakeIsAFailure(string? result, string says)
    {
        var answers = await Run(_ => new ValueTask<string>(result!));
        var answer = Assert.Single(answers);
        Assert.StartsWith("err:", answer);
        Assert.Contains(says, answer);
    }

    [Fact]
    public async Task ATokenThatIsNotUtf8IsAFailure()
    {
        // A lone surrogate would reach the library as U+FFFD, a different token.
        var answer = Assert.Single(await Run(_ => new ValueTask<string>("tok" + '\ud800' + "en")));
        Assert.StartsWith("err:", answer);
        Assert.Contains("UTF-8", answer);
    }

    [Fact]
    public async Task ADefaultValueTaskIsAFailure()
    {
        var answer = Assert.Single(await Run(_ => default));
        Assert.StartsWith("err:", answer);
    }

    [Fact]
    public async Task AReplyTheLibraryRefusesIsAnsweredWithAFailure()
    {
        // A refusal for a bad argument leaves the reply open, so it still needs an answer.
        var answers = await Run(_ => new ValueTask<string>("tok"), okStatus: 10);
        Assert.Equal(2, answers.Count);
        Assert.Equal("ok:tok", answers[0]);
        Assert.StartsWith("err:", answers[1]);
    }

    [Fact]
    public async Task ThePerFetchTokenFiresAfterTheTimeoutAndTheFetchIsAnswered()
    {
        var seen = new TaskCompletionSource();
        var answers = await Run(async ct =>
        {
            ct.Register(() => seen.SetResult());
            await Task.Delay(Timeout.Infinite, ct);
            return "unreachable";
        }, timeout: TimeSpan.FromMilliseconds(200));
        await seen.Task.WaitAsync(TimeSpan.FromSeconds(30));
        var answer = Assert.Single(answers);
        Assert.StartsWith("err:", answer);
        Assert.Contains("30 seconds", answer);
    }

    [Fact]
    public async Task DisposingCancelsAFetchInFlightAndStillAnswersIt()
    {
        var started = new TaskCompletionSource<CancellationToken>();
        var replies = new Replies();
        var source = new TokenSource(async ct =>
        {
            started.SetResult(ct);
            await Task.Delay(Timeout.Infinite, ct);
            return "unreachable";
        }, replies);
        source.Fetch(1);
        var ct = await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.False(ct.IsCancellationRequested);

        source.Cancel();

        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(ct.IsCancellationRequested);
        Assert.StartsWith("err:", Assert.Single(replies.Answers));
    }

    [Fact]
    public async Task AProviderThatIgnoresCancellationIsStillAnsweredOnce()
    {
        var started = new TaskCompletionSource();
        var release = new TaskCompletionSource<string>();
        var replies = new Replies();
        var source = new TokenSource(_ =>
        {
            started.SetResult();
            return new ValueTask<string>(release.Task);
        }, replies);
        source.Fetch(1);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        source.Cancel();
        Assert.Empty(replies.Answers);

        release.SetResult("late");

        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(50);
        Assert.Equal(["ok:late"], replies.Answers);
    }

    [Fact]
    public async Task AFetchAfterDisposeGetsAFreshToken()
    {
        // A subscription can outlive its client and still need a token.
        var replies = new Replies();
        var source = new TokenSource(ct => new ValueTask<string>(ct.IsCancellationRequested ? "cancelled" : "fresh"), replies);
        source.Cancel();
        source.Fetch(1);
        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(["ok:fresh"], replies.Answers);
    }

    [Fact]
    public async Task AFetchIsNotCancelledWhileAHandleMadeFromTheClientIsOpen()
    {
        var started = new TaskCompletionSource<CancellationToken>();
        var replies = new Replies();
        var source = new TokenSource(async ct =>
        {
            started.SetResult(ct);
            await Task.Delay(Timeout.Infinite, ct);
            return "unreachable";
        }, replies);
        // The client's reference, and a subscription's.
        Assert.Equal(1, source.Holders);
        var subscription = new FelixSubscriptionHandle(0);
        subscription.Track(source);
        Assert.Equal(2, source.Holders);
        source.Fetch(1);
        var ct = await started.Task.WaitAsync(TimeSpan.FromSeconds(30));

        source.ReleaseHolder(); // the client is disposed
        await Task.Delay(200);
        Assert.False(ct.IsCancellationRequested);
        Assert.Empty(replies.Answers);

        source.ReleaseHolder(); // the last handle is disposed
        await replies.Answered.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(ct.IsCancellationRequested);
        Assert.Single(replies.Answers);
    }

    [Fact]
    public void AHandleTakesOneReferenceHoweverOftenItIsTracked()
    {
        var source = new TokenSource(_ => new ValueTask<string>("tok"), new Replies());
        var handle = new FelixProducerHandle(0);
        handle.Track(source);
        handle.Track(source);
        Assert.Equal(2, source.Holders);
    }

    [Fact]
    public void ASourceWhoseLastReferenceIsGoneTakesNoMore()
    {
        var source = new TokenSource(_ => new ValueTask<string>("tok"), new Replies());
        source.ReleaseHolder();
        Assert.False(source.Retain());
    }

    [Fact]
    public async Task AFailedConnectFreesTheSource()
    {
        // Nothing listens on port 1: the connect fails through its callback, and the
        // library releases the source after the client it never handed out is freed.
        var live = TokenSource.Live;
        var released = TokenSource.Released;
        await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = ["127.0.0.1:1"],
            TenantId = "t",
            TokenProvider = _ => new ValueTask<string>("tok"),
        }));

        await Eventually(() => TokenSource.Live == live, "the token source was never released");
        Assert.Equal(released + 1, TokenSource.Released);
    }

    [Fact]
    public async Task ARefusedConnectCallFreesTheSource()
    {
        // The library refuses a malformed address list before it starts, so token_release never
        // runs and the SDK must free the source itself.
        var live = TokenSource.Live;
        var released = TokenSource.Released;
        var error = await Record.ExceptionAsync(() => FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = [","],
            TenantId = "t",
            TokenProvider = _ => new ValueTask<string>("tok"),
        }));

        Assert.NotNull(error);
        Assert.Equal(live, TokenSource.Live);
        Assert.Equal(released + 1, TokenSource.Released);
    }

    private static async Task Eventually(Func<bool> condition, string failure)
    {
        for (var i = 0; i < 200 && !condition(); i++)
        {
            await Task.Delay(50);
        }

        Assert.True(condition(), failure);
    }

    // --- Against the fixture ----------------------------------------------------

    private static FelixClientOptions Options(Fixture f, string? token, Func<CancellationToken, ValueTask<string>>? provider, bool oneBroker = false) =>
        new()
        {
            Addrs = oneBroker ? f.Addrs[..1] : f.Addrs,
            TenantId = f.Tenant,
            Token = token,
            TokenProvider = provider,
            CaFile = f.CaFile,
        };

    private sealed class Provider
    {
        private int calls;
        private int running;
        private int maxRunning;
        private readonly Func<CancellationToken, Task<string>> fetch;

        internal Provider(Func<CancellationToken, Task<string>> fetch) => this.fetch = fetch;

        internal int Calls => Volatile.Read(ref calls);

        internal int MaxRunning => Volatile.Read(ref maxRunning);

        internal Func<CancellationToken, ValueTask<string>> Fetch => async ct =>
        {
            Interlocked.Increment(ref calls);
            var now = Interlocked.Increment(ref running);
            int seen;
            while (now > (seen = Volatile.Read(ref maxRunning)) && Interlocked.CompareExchange(ref maxRunning, now, seen) != seen)
            {
            }

            try
            {
                return await fetch(ct).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        };
    }

    [FixtureFact]
    public async Task WithoutAnInitialTokenTheFirstConnectCallsTheProviderOnceWhateverOpensTogether()
    {
        var f = Fixture.Load();
        // Holds long enough that anything started during the connect would overlap it.
        var provider = new Provider(async _ =>
        {
            await Task.Delay(500);
            return f.Token;
        });
        await using var client = await FelixClient.ConnectAsync(Options(f, null, provider.Fetch));
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);

        // Publishers, a subscriber and a cache stream open together.
        var publishes = Enumerable.Range(0, 4)
            .Select(i => client.PublishAsync(f.Tenant, f.Namespace, f.Stream, new[] { (byte)i }));
        var subscribe = client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        var read = cache.GetAsync("token-provider-single-flight");
        await Task.WhenAll(publishes.Cast<Task>().Append(subscribe).Append(read));
        await (await subscribe).DisposeAsync();

        // The token it returned is held, so none of those asked again.
        Assert.Equal(1, provider.Calls);
        Assert.Equal(1, provider.MaxRunning);
    }

    [FixtureFact]
    public async Task AnEmptyInitialTokenCountsAsNone()
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult(f.Token));
        await using var client = await FelixClient.ConnectAsync(Options(f, "", provider.Fetch));
        Assert.Equal(1, provider.Calls);
    }

    [FixtureFact]
    public async Task AnIdleClientWithAValidTokenMakesNoCalls()
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult(f.Token));
        await using (var client = await FelixClient.ConnectAsync(Options(f, f.Token, provider.Fetch)))
        {
            await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "a"u8.ToArray());
            await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
            await Task.Delay(300);
        }

        Assert.Equal(0, provider.Calls);
    }

    [FixtureFact]
    public async Task AProviderFailureReachesTheCallerUnchanged()
    {
        var f = Fixture.Load();
        const string message = "idp said: tenant été suspended (code=7)";
        var provider = new Provider(_ => throw new InvalidOperationException(message));
        var error = await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(Options(f, null, provider.Fetch)));
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.Contains(message, error.Message);
    }

    [FixtureTheory]
    [InlineData(null, "null")]
    [InlineData("", "empty")]
    public async Task ANullOrEmptyTokenIsAnAuthFailureThatSaysSo(string? result, string says)
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult(result!));
        var error = await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(Options(f, null, provider.Fetch)));
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.Contains(says, error.Message);
    }

    [FixtureFact]
    public async Task ARefusedTokenCostsOneFetchAndOneRetry()
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult(f.Token));
        // One broker: each broker's refusal is its own authentication, so a cluster
        // client fetches once per broker that refuses.
        await using var client = await FelixClient.ConnectAsync(Options(f, "not-a-token", provider.Fetch, oneBroker: true));
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "a"u8.ToArray());
        Assert.Equal(1, provider.Calls);
    }

    [FixtureFact]
    public async Task AProviderReturningTheRefusedTokenIsNotRetried()
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult("not-a-token"));
        var error = await Assert.ThrowsAsync<FelixException>(
            () => FelixClient.ConnectAsync(Options(f, "not-a-token", provider.Fetch, oneBroker: true)));
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.Equal(1, provider.Calls);
    }

    [FixtureFact]
    public async Task AProviderThatNeverAnswersFailsTheConnectAfterThirtySeconds()
    {
        var f = Fixture.Load();
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new Provider(async ct =>
        {
            ct.Register(() => fired.TrySetResult());
            await Task.Delay(Timeout.Infinite, ct);
            return f.Token;
        });
        var started = System.Diagnostics.Stopwatch.StartNew();

        var error = await Assert.ThrowsAsync<FelixException>(
            () => FelixClient.ConnectAsync(Options(f, null, provider.Fetch, oneBroker: true)));

        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.InRange(started.Elapsed, TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(60));
        // The provider's token fires at 30 s too, and can trail the connect's error.
        await fired.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, provider.Calls);
    }

    [FixtureFact]
    public async Task CancellingTheConnectCancelsTheProviderInFlight()
    {
        var f = Fixture.Load();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new Provider(async ct =>
        {
            ct.Register(() => fired.TrySetResult());
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return f.Token;
        });
        using var cts = new CancellationTokenSource();
        var connect = FelixClient.ConnectAsync(Options(f, null, provider.Fetch), cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        await fired.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [FixtureFact]
    public async Task AProviderAnsweringAfterTheClientIsGoneIsAnsweredAndReleasedOnce()
    {
        var f = Fixture.Load();
        var released = TokenSource.Released;
        var live = TokenSource.Live;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var answer = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        // Ignores its cancellation token, as a provider that cannot be interrupted would.
        var provider = new Provider(_ =>
        {
            entered.TrySetResult();
            return answer.Task;
        });
        using var cts = new CancellationTokenSource();
        var connect = FelixClient.ConnectAsync(Options(f, null, provider.Fetch), cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        Assert.Equal(released, TokenSource.Released);

        answer.SetResult(f.Token);

        await Eventually(() => TokenSource.Released == released + 1, "token_release did not run after the late answer");
        Assert.Equal(live, TokenSource.Live);
        await Task.Delay(300);
        Assert.Equal(released + 1, TokenSource.Released);
    }

    [FixtureFact]
    public async Task EveryHandleMadeFromTheClientHoldsTheTokenSourceUntilItIsDisposed()
    {
        var f = Fixture.Load();
        var provider = new Provider(_ => Task.FromResult(f.Token));
        var client = await FelixClient.ConnectAsync(Options(f, null, provider.Fetch));
        var source = client.Tokens!;
        Assert.Equal(1, source.Holders);

        var subscription = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        Assert.Equal(2, source.Holders);
        var producer = await client.CreateProducerAsync();
        Assert.Equal(3, source.Holders);
        var sharded = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest);
        Assert.Equal(4, source.Holders);
        var watch = await client.Cache(f.Tenant, f.Namespace, f.Cache).WatchKeyAsync("token-provider-holders");
        Assert.Equal(5, source.Holders);

        // A call that fails hands its reference back.
        await Assert.ThrowsAsync<FelixException>(
            () => client.SubscribeAsync(f.Tenant, f.Namespace, f.MissingStream, 0, StartPosition.Latest));
        Assert.Equal(5, source.Holders);

        // The client is not the last user: the handles keep the provider's token alive.
        await client.DisposeAsync();
        await client.DisposeAsync();
        Assert.Equal(4, source.Holders);

        await subscription.DisposeAsync();
        Assert.Equal(3, source.Holders);
        await producer.DisposeAsync();
        Assert.Equal(2, source.Holders);
        await sharded.DisposeAsync();
        Assert.Equal(1, source.Holders);
        await watch.DisposeAsync();
        Assert.Equal(0, source.Holders);
    }

    [FixtureFact]
    public async Task DisposingAClientCancelsNothingWhenNoFetchIsRunning()
    {
        var f = Fixture.Load();
        var live = TokenSource.Live;
        var provider = new Provider(_ => Task.FromResult(f.Token));
        var client = await FelixClient.ConnectAsync(Options(f, null, provider.Fetch));
        await client.DisposeAsync();
        await client.DisposeAsync();
        await Eventually(() => TokenSource.Live == live, "the token source outlived the client");
        Assert.Equal(1, provider.Calls);
    }
}

/// <summary>
/// The example on the C# page, compiled with the tests so it cannot drift. It is never run: it
/// needs a control plane.
/// </summary>
internal static class TokenDocsExample
{
    internal static async Task<FelixClient> ConnectAsync(string oidcToken)
    {
        var http = new HttpClient { BaseAddress = new Uri("https://controlplane:8443/v1/tenants/t1/") };

        // Exchange the OIDC token once. The refresh token works once: every refresh returns a
        // replacement, so the provider stores it before it returns.
        var exchange = new HttpRequestMessage(HttpMethod.Post, "token/exchange")
        {
            Content = JsonContent.Create(new
            {
                requested = new[] { "stream.publish", "stream.subscribe" },
                resources = new[] { "stream:t1/default/*" },
            }),
        };
        exchange.Headers.Authorization = new("Bearer", oidcToken);
        using var response = await http.SendAsync(exchange);
        var minted = await response.EnsureSuccessStatusCode().Content.ReadFromJsonAsync<Minted>()
            ?? throw new InvalidOperationException("the control plane returned no token");
        var refreshToken = minted.RefreshToken;

        return await FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = ["127.0.0.1:5000"],
            TenantId = "t1",
            Token = minted.FelixToken,
            TokenProvider = async ct =>
            {
                var refreshed = await http.PostAsJsonAsync("token/refresh", new { refresh_token = refreshToken }, ct);
                var next = await refreshed.EnsureSuccessStatusCode().Content.ReadFromJsonAsync<Minted>(ct)
                    ?? throw new InvalidOperationException("the control plane returned no token");
                refreshToken = next.RefreshToken;
                return next.FelixToken;
            },
        });
    }

    internal sealed record Minted(
        [property: JsonPropertyName("felix_token")] string FelixToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn,
        [property: JsonPropertyName("refresh_token")] string RefreshToken);
}
