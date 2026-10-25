using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Felix.Client.Tests;

/// <summary>
/// Thrown from a test body to record it as skipped, for conditions only known mid-run (a control
/// endpoint refusing a fault, a broker process that cannot be found). The C harness records the same
/// cases as skips.
/// </summary>
internal sealed class RuntimeSkipException : Exception
{
    internal RuntimeSkipException(string reason)
        : base(reason)
    {
    }

    internal static RuntimeSkipException Because(string reason) => new(reason);
}

/// <summary>Discovers <see cref="FactAttribute"/> tests that may skip themselves at run time.</summary>
public sealed class RuntimeSkipDiscoverer : FactDiscoverer
{
    public RuntimeSkipDiscoverer(IMessageSink diagnosticMessageSink)
        : base(diagnosticMessageSink)
    {
    }

    protected override IXunitTestCase CreateTestCase(
        ITestFrameworkDiscoveryOptions discoveryOptions,
        ITestMethod testMethod,
        IAttributeInfo factAttribute) =>
        new RuntimeSkipTestCase(
            DiagnosticMessageSink,
            discoveryOptions.MethodDisplayOrDefault(),
            discoveryOptions.MethodDisplayOptionsOrDefault(),
            testMethod);
}

/// <summary>A test case that reports a <see cref="RuntimeSkipException"/> as a skip instead of a failure.</summary>
public sealed class RuntimeSkipTestCase : XunitTestCase
{
    [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
    public RuntimeSkipTestCase()
    {
    }

    public RuntimeSkipTestCase(
        IMessageSink diagnosticMessageSink,
        TestMethodDisplay defaultMethodDisplay,
        TestMethodDisplayOptions defaultMethodDisplayOptions,
        ITestMethod testMethod)
        : base(diagnosticMessageSink, defaultMethodDisplay, defaultMethodDisplayOptions, testMethod)
    {
    }

    public override async Task<RunSummary> RunAsync(
        IMessageSink diagnosticMessageSink,
        IMessageBus messageBus,
        object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
    {
        var bus = new SkipBus(messageBus);
        var summary = await base.RunAsync(diagnosticMessageSink, bus, constructorArguments, aggregator, cancellationTokenSource);
        if (bus.Skipped == 0)
        {
            return summary;
        }

        return new RunSummary
        {
            Total = summary.Total,
            Failed = summary.Failed - bus.Skipped,
            Skipped = summary.Skipped + bus.Skipped,
            Time = summary.Time,
        };
    }

    private sealed class SkipBus : IMessageBus
    {
        private readonly IMessageBus inner;

        internal SkipBus(IMessageBus inner) => this.inner = inner;

        internal int Skipped { get; private set; }

        public bool QueueMessage(IMessageSinkMessage message)
        {
            if (message is ITestFailed failed
                && failed.ExceptionTypes.FirstOrDefault() == typeof(RuntimeSkipException).FullName)
            {
                Skipped++;
                return inner.QueueMessage(new TestSkipped(failed.Test, failed.Messages.FirstOrDefault() ?? "skipped"));
            }

            return inner.QueueMessage(message);
        }

        public void Dispose()
        {
        }
    }
}
