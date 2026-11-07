using System.Text.Json;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Mutual TLS against the fixture's broker that serves only a client presenting its certificate,
/// the way a broker started with <c>FELIX_TLS_CLIENT_CA</c> does.
/// </summary>
public class ClientCertificateTests
{
    private sealed record MutualTls(string Addr, string Tenant, string CaFile, string CertFile, string KeyFile)
    {
        internal static MutualTls Load()
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(Environment.GetEnvironmentVariable("FELIX_FIXTURE")!));
            var root = doc.RootElement;
            string Text(string name) => root.GetProperty(name).GetString()!;
            return new MutualTls(Text("mtls_addr"), Text("tenant_id"), Text("ca_file"), Text("client_cert_file"), Text("client_key_file"));
        }

        internal FelixClientOptions Options(string? cert, string? key) => new()
        {
            Addrs = [Addr],
            TenantId = Tenant,
            Token = "token",
            CaFile = CaFile,
            ClientCertFile = cert,
            ClientKeyFile = key,
            ReconnectAttempts = 1,
        };
    }

    [FixtureFact]
    public async Task AClientWithACertificateConnectsToABrokerThatRequiresOne()
    {
        var f = MutualTls.Load();
        await using var client = await FelixClient.ConnectAsync(f.Options(f.CertFile, f.KeyFile));
    }

    [FixtureFact]
    public async Task AClientWithoutACertificateIsRefusedByIt()
    {
        var f = MutualTls.Load();
        await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(f.Options(null, null)));
    }

    [FixtureFact]
    public async Task AnUnusableCertificateFileIsAnInvalidArgumentNamingTheFile()
    {
        var f = MutualTls.Load();
        var missing = Path.Combine(Path.GetTempPath(), "felix-missing-client.pem");
        var error = await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(f.Options(missing, f.KeyFile)));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
        Assert.Contains("felix-missing-client.pem", error.Message);

        // A certificate file is not a key file.
        error = await Assert.ThrowsAsync<FelixException>(() => FelixClient.ConnectAsync(f.Options(f.CertFile, f.CertFile)));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
    }
}
