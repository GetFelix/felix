"""Error classification and lifecycle.

The scenarios here are about what an application can *decide* from a failure.
A client that reports everything as one error type forces callers to match on
message text, which breaks the first time a message is reworded — so these
assert on exception identity, never on wording.
"""

from __future__ import annotations

import concurrent.futures
import json
import time
import urllib.error
import urllib.request

import pytest

import felix


@pytest.mark.scenario("error.unknown_stream_is_typed")
def test_publishing_to_an_unknown_stream_is_distinguishable(client, fixture):
    with pytest.raises(felix.FelixError) as caught:
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["missing_stream"],
            b"nowhere",
        )

    # Distinguishable from a transport failure, which is the decision this
    # supports: an unknown stream will not start existing because you retried.
    assert not isinstance(caught.value, felix.ConnectionError), (
        "an unknown stream was reported as a connection failure, which tells "
        "an application to retry something that cannot succeed"
    )
    # Typed either way. A broker with no assignment for the shard cannot tell
    # an unregistered stream from one not placed yet, and says the latter.
    assert caught.value.code in ("not_found", "shard_unavailable")
    if caught.value.code == "not_found":
        assert isinstance(caught.value, felix.NotFoundError)


@pytest.mark.scenario(
    "error.unauthorized_is_typed",
    "retry.terminal_failures_are_not_retried",
)
def test_an_unauthorized_publish_is_distinguishable_and_not_retried(fixture):
    """A token without publish permission fails as auth, promptly.

    Promptly is half the assertion: a client that retried this would take its
    full backoff budget to arrive at the same answer, so the elapsed time is
    evidence that it did not.
    """
    import time

    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["unauthorized_token"],
        ca_file=fixture["ca_file"],
    ) as restricted:
        started = time.monotonic()
        with pytest.raises(felix.FelixError) as caught:
            restricted.publish(
                fixture["tenant_id"],
                fixture["namespace"],
                fixture["durable_stream"],
                b"not allowed",
            )
        elapsed = time.monotonic() - started

    assert isinstance(caught.value, felix.AuthError), (
        f"an authorization failure surfaced as {type(caught.value).__name__}; "
        "an application cannot tell it from a retryable fault"
    )
    assert caught.value.code == "forbidden"
    assert caught.value.retry == "fatal"
    assert elapsed < 5.0, (
        f"the refusal took {elapsed:.2f}s — it was retried, and no amount of "
        "retrying grants a permission"
    )


@pytest.mark.scenario("error.shard_unavailable_is_retryable")
def test_a_publish_refused_during_a_move_is_retryable(disposable_fixture):
    """A fenced shard refuses, and says the refusal is safe to retry.

    Published straight to the fenced owner: through another broker the answer
    would be about the forward rather than the shard.
    """
    fixture = disposable_fixture
    owner = _control(fixture, "/fence")
    stream = fixture["movable_stream"]

    with _client_on(fixture, owner["addr"]) as client:
        with pytest.raises(felix.FelixError) as caught:
            client.publish(
                fixture["tenant_id"], fixture["namespace"], stream, b"mid-move"
            )
    error = caught.value
    assert isinstance(error, felix.ShardUnavailableError), (
        f"a refusal during a move surfaced as {type(error).__name__} "
        f"({error.code}/{error.retry}): {error}"
    )
    assert error.code in ("shard_unavailable", "not_leader")
    assert error.retry in ("retry", "redirect")

    # Retryable means the same publish lands once the move finishes.
    _control(fixture, "/heal")
    deadline = time.monotonic() + 60
    with _client_on(fixture, owner["addr"]) as client:
        while True:
            try:
                client.publish(
                    fixture["tenant_id"], fixture["namespace"], stream, b"mid-move"
                )
                break
            except felix.FelixError as later:
                if time.monotonic() > deadline:
                    raise AssertionError(
                        f"the move never finished; last refusal: {later}"
                    ) from later
                time.sleep(0.25)


@pytest.mark.scenario("error.quorum_timeout_is_outcome_unknown")
def test_a_write_no_majority_confirmed_is_outcome_unknown(disposable_fixture):
    """The leader wrote it and cannot say whether it will survive."""
    fixture = disposable_fixture
    leader = _control(fixture, "/partition")

    with _client_on(fixture, leader["addr"]) as client:
        with pytest.raises(felix.FelixError) as caught:
            client.publish(
                fixture["tenant_id"],
                fixture["namespace"],
                fixture["quorum_stream"],
                b"no majority",
            )
    error = caught.value
    assert isinstance(error, felix.OutcomeUnknownError), (
        f"a write no majority confirmed surfaced as {type(error).__name__} "
        f"({error.code}/{error.retry}): {error}"
    )
    # A Raft leader that steps down while the appended batch waits reports
    # leadership_lost.
    assert error.code in ("quorum_timeout", "leadership_lost")
    assert error.retry == "outcome_unknown"


def _control(fixture, path):
    """Ask the fixture for a fault, or skip if it cannot produce one."""
    url = fixture.get("control_url")
    if not url:
        pytest.skip("this fixture has no control endpoint")
    request = urllib.request.Request(url + path, data=b"", method="POST")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as refused:
        if refused.code == 409:
            pytest.skip(refused.read().decode())
        raise


def _client_on(fixture, addr):
    return felix.Client(
        addr,
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    )


@pytest.mark.scenario("client.close_is_idempotent")
def test_closing_twice_is_safe(client, fixture):
    """Cleanup runs on paths that may already have cleaned up."""
    subscription = client.subscribe(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    )
    subscription.close()
    assert subscription.closed
    subscription.close()  # must not raise
    assert subscription.closed

    # And through the context manager, over an already-closed subscription.
    with subscription:
        pass

    # A client's own exit is idempotent for the same reason.
    with client:
        pass


@pytest.mark.scenario("client.concurrent_calls")
def test_one_client_serves_concurrent_callers(client, fixture, key):
    """Threads share a client; every publish must land, none may corrupt.

    The binding blocks with the GIL released, so this is genuine concurrency
    rather than interleaved bytecode — which is exactly the condition under
    which shared mutable state would show.
    """
    stream = fixture["durable_stream"]
    payloads = [f"{key}-{index}".encode() for index in range(24)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        errors = list(
            pool.map(
                lambda payload: _publish_capturing(client, fixture, stream, payload),
                payloads,
            )
        )
    assert [error for error in errors if error] == []

    with client.subscribe(
        fixture["tenant_id"], fixture["namespace"], stream, start="earliest"
    ) as events:
        seen = set()
        while True:
            event = events.next_event(timeout=5.0)
            if event is None:
                break
            seen.add(event.payload)

    missing = [payload for payload in payloads if payload not in seen]
    assert not missing, f"{len(missing)} concurrent publishes were lost"


def _publish_capturing(client, fixture, stream, payload):
    try:
        client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        return None
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"{payload!r}: {error}"


@pytest.mark.scenario("client.subscription_ends_cleanly")
def test_a_closed_subscription_reports_the_end_rather_than_hanging(client, fixture):
    """Iteration must end, not block forever.

    A consumer whose loop never returns after a shutdown is a hung process, and
    the cause is invisible: nothing errors, nothing logs, the loop is simply
    still waiting. So a closed subscription reports the end immediately.
    """
    import time

    subscription = client.subscribe(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    )
    subscription.close()

    started = time.monotonic()
    assert subscription.next_event(timeout=5.0) is None
    elapsed = time.monotonic() - started
    assert elapsed < 2.0, (
        f"a closed subscription took {elapsed:.2f}s to report the end; a "
        "consumer loop would sit there rather than shutting down"
    )

    # And as an iterator, which is how most consumers actually read it.
    assert list(subscription) == []
