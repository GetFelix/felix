using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Skips locally when libfelix is not built. CI sets FELIX_REQUIRE_NATIVE, so a
/// library that fails to load there is a failure, not a skip.
/// </summary>
public sealed class NativeFactAttribute : FactAttribute
{
    public NativeFactAttribute()
    {
        var path = Environment.GetEnvironmentVariable("FELIX_NATIVE_LIB");
        var required = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FELIX_REQUIRE_NATIVE"));
        if (string.IsNullOrEmpty(path) && !required)
        {
            Skip = "FELIX_NATIVE_LIB is not set; build libfelix with `cargo build --release -p felix-capi`";
        }
    }
}

public class NativeLoadTests
{
    [NativeFact]
    public void TheNativeLibraryLoadsAndAnExportRuns()
    {
        var token = NativeMethods.felix_cancel_token_new();
        Assert.NotEqual(0, token);
        NativeMethods.felix_cancel_token_cancel(token);
        NativeMethods.felix_cancel_token_free(token);
    }

    [NativeFact]
    public async Task ConnectToNothingFailsWithATypedError()
    {
        // Port 1 is never a broker. A name that does not resolve is refused up front;
        // an address nothing listens on is a connection failure.
        var error = await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t", Token = "x" }));
        Assert.True(error.Status is FelixStatus.Connection or FelixStatus.Error or FelixStatus.InvalidArgument, error.Status.ToString());
    }
}
