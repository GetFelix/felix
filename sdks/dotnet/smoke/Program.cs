using System.Text;
using System.Text.Json;
using Felix;

// Usage: Smoke [fixture.json]
// With a fixture: connect, publish and read the record back.
// Without: connect to a closed port and expect FelixException(Connection), which can only
// happen once the native library has loaded and run.
if (args.Length == 0)
{
    return await NativeLoadOnly();
}

return await RoundTrip(args[0]);

static async Task<int> NativeLoadOnly()
{
    try
    {
        await using var client = await FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "smoke", Token = "x" });
    }
    catch (FelixException e) when (e.Status == FelixStatus.Connection)
    {
        Console.WriteLine($"native library loaded: {e.Message}");
        return 0;
    }

    Console.Error.WriteLine("connecting to a closed port succeeded");
    return 1;
}

static async Task<int> RoundTrip(string fixturePath)
{
    using var doc = JsonDocument.Parse(File.ReadAllText(fixturePath));
    var root = doc.RootElement;
    string Text(string name) => root.GetProperty(name).GetString()!;
    var tenant = Text("tenant_id");
    var ns = Text("namespace");
    var stream = Text("durable_stream");

    await using var client = await FelixClient.ConnectAsync(new FelixClientOptions
    {
        Addrs = root.GetProperty("addrs").EnumerateArray().Select(a => a.GetString()!).ToArray(),
        TenantId = tenant,
        Token = Text("token"),
        CaFile = Text("ca_file"),
    });
    await using var subscription = await client.SubscribeAsync(tenant, ns, stream, 0, StartPosition.Latest);

    var offset = await client.PublishAsync(tenant, ns, stream, Encoding.UTF8.GetBytes("smoke"));
    var received = await subscription.ReadAsync(TimeSpan.FromSeconds(30));
    if (received is null || Encoding.UTF8.GetString(received.Payload.Span) != "smoke")
    {
        Console.Error.WriteLine("the published record did not come back");
        return 1;
    }

    Console.WriteLine($"round trip ok at offset {offset}");
    return 0;
}
