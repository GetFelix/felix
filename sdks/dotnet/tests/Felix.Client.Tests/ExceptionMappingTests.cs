using Xunit;

namespace Felix.Client.Tests;

public class ExceptionMappingTests
{
    [Theory]
    [InlineData(FelixStatus.Connection, true)]
    [InlineData(FelixStatus.ShardUnavailable, true)]
    [InlineData(FelixStatus.Overloaded, true)]
    [InlineData(FelixStatus.Auth, false)]
    [InlineData(FelixStatus.NotFound, false)]
    [InlineData(FelixStatus.OutcomeUnknown, false)]
    [InlineData(FelixStatus.InvalidArgument, false)]
    public void RetryClassMatchesTheCAbi(FelixStatus status, bool retryable)
    {
        var error = Assert.IsType<FelixException>(FelixException.FromStatus((int)status, "m"));
        Assert.Equal(status, error.Status);
        Assert.Equal(retryable, error.IsRetryable);
    }

    [Fact]
    public void OutcomeUnknownIsFlagged()
    {
        var error = Assert.IsType<FelixException>(FelixException.FromStatus(27, "may have landed"));
        Assert.True(error.OutcomeUnknown);
        Assert.False(error.IsRetryable);
        Assert.Equal("may have landed", error.Message);
    }

    [Fact]
    public void CancelledBecomesOperationCanceled()
    {
        using var source = new CancellationTokenSource();
        var error = Assert.IsAssignableFrom<OperationCanceledException>(FelixException.FromStatus(3, null, source.Token));
        Assert.Equal(source.Token, error.CancellationToken);
    }

    [Fact]
    public void AMissingMessageStillSaysWhat()
    {
        var error = Assert.IsType<FelixException>(FelixException.FromStatus(21, null));
        Assert.Contains("Connection", error.Message);
    }
}
