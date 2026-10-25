"""`token_provider=`: the app's function behind the Rust client's `RefreshingToken`.

The timing rules (refresh at two thirds of a token's life, keep a still-valid
token when a fetch fails, 30 s timeout) belong to the Rust client and are
tested there. These tests cover what the binding adds: when the Python callable
is called, on which thread and loop, and how its failures reach the caller.
Tests tagged `token.*` are catalogue scenarios. The ones about expiry take a
short-lived token from the fixture's `POST /tokens` and wait for it.
"""

from __future__ import annotations

import asyncio
import io
import json
import pathlib
import re
import threading
import time
import urllib.error
import urllib.request

import pytest

import felix

DOCS_PAGE = (
    pathlib.Path(__file__).resolve().parents[4]
    / "docs-site/src/content/docs/clients/python.md"
)


class Provider:
    """A provider that counts its calls and never runs twice at once."""

    def __init__(self, token, delay=0.0):
        self.token = token
        self.delay = delay
        self.calls = 0
        self.threads = set()
        self.running = 0
        self.max_running = 0
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            self.calls += 1
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            self.threads.add(threading.current_thread())
        try:
            time.sleep(self.delay)
            return self.token
        finally:
            with self._lock:
                self.running -= 1


def mint(fixture, ttl_secs, forged=False):
    """A token for the fixture tenant that lives `ttl_secs`, from the fixture."""
    if not fixture.get("control_url"):
        pytest.skip("the fixture file has no control_url to mint tokens from")
    request = urllib.request.Request(
        fixture["control_url"] + "/tokens",
        data=json.dumps({"ttl_secs": ttl_secs, "forged": forged}).encode(),
        method="POST",
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())["token"]


def connect(fixture, addrs=None, **kwargs):
    return felix.Client(
        fixture["addrs"] if addrs is None else addrs,
        tenant_id=fixture["tenant_id"],
        ca_file=fixture["ca_file"],
        **kwargs,
    )


# --- Argument checks, which need no broker -----------------------------------


def test_a_client_needs_a_token_or_a_provider():
    with pytest.raises(TypeError):
        felix.Client("127.0.0.1:1", tenant_id="t")


def test_a_provider_must_be_callable():
    with pytest.raises(TypeError):
        felix.Client("127.0.0.1:1", tenant_id="t", token_provider="not callable")


def test_async_client_rejects_a_non_callable():
    async def main():
        with pytest.raises(TypeError, match="callable"):
            await felix.AsyncClient.connect(
                "127.0.0.1:1", tenant_id="t", token_provider="not callable"
            )

    asyncio.run(main())


# --- Contract points, against the fixture ------------------------------------


@pytest.mark.scenario("token.lazy")
def test_an_idle_client_makes_no_calls(fixture, key):
    provider = Provider(fixture["token"])
    with connect(fixture, token=fixture["token"], token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
        time.sleep(0.3)
    assert provider.calls == 0


@pytest.mark.scenario("token.lazy")
@pytest.mark.parametrize("initial", [None, ""])
def test_without_an_initial_token_the_first_connect_calls_the_provider(fixture, initial):
    provider = Provider(fixture["token"])
    kwargs = {} if initial is None else {"token": initial}
    with connect(fixture, token_provider=provider, **kwargs) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"b")
    # The hour-long token it returned is held, so later streams do not ask.
    assert provider.calls == 1


@pytest.mark.scenario("token.single_flight")
def test_the_provider_is_never_called_concurrently(fixture):
    """A publisher and a subscriber opened together both need the token, so
    without single-flight the provider would run twice at once."""
    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["durable_stream"],
    )
    with connect(fixture, token=fixture["token"]) as client:
        provider = Provider(fixture["token"], delay=0.5)
        with client.with_identity(tenant, token_provider=provider) as identity:
            barrier = threading.Barrier(2)
            errors = []

            def publish():
                try:
                    barrier.wait()
                    identity.publish(tenant, namespace, stream, b"a")
                except Exception as err:  # noqa: BLE001
                    errors.append(err)

            def subscribe():
                try:
                    barrier.wait()
                    with identity.subscribe(tenant, namespace, stream):
                        pass
                except Exception as err:  # noqa: BLE001
                    errors.append(err)

            threads = [threading.Thread(target=f) for f in (publish, subscribe)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(60)
    assert not errors, errors
    assert provider.calls == 1
    assert provider.max_running == 1


@pytest.mark.scenario("token.refused_retry_once")
def test_a_refused_token_costs_one_fetch_and_one_retry(fixture):
    provider = Provider(fixture["token"])
    with connect(fixture, token="not-a-token", token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
    assert provider.calls == 1


@pytest.mark.scenario("token.refused_retry_once")
def test_a_live_looking_token_the_broker_refuses_is_replaced_once(fixture):
    # Signed with a key no broker trusts but not expired, so only the broker's
    # refusal can tell the client to fetch.
    provider = Provider(fixture["token"])
    forged = mint(fixture, 60, forged=True)
    with connect(fixture, fixture["addrs"][:1], token=forged, token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
    assert provider.calls == 1


@pytest.mark.scenario("token.refused_retry_once")
def test_a_provider_returning_the_refused_token_is_not_retried(fixture):
    # One broker: each broker's refusal is its own authentication, so a
    # cluster client would fetch once per broker it tries.
    provider = Provider("not-a-token")
    with pytest.raises(felix.AuthError):
        connect(
            fixture,
            fixture["addrs"][:1],
            token="not-a-token",
            token_provider=provider,
        )
    assert provider.calls == 1


@pytest.mark.scenario("token.failure_message")
def test_a_failure_with_no_token_held_reaches_the_caller(fixture):
    def provider():
        raise RuntimeError("the refresh token was revoked")

    with pytest.raises(felix.AuthError, match="the refresh token was revoked"):
        connect(fixture, token_provider=provider)


def test_an_exception_without_a_message_is_named(fixture):
    def provider():
        raise KeyError()

    with pytest.raises(felix.AuthError, match="KeyError"):
        connect(fixture, token_provider=provider)


def test_the_provider_is_called_with_no_arguments(fixture):
    seen = []

    def provider(*args, **kwargs):
        seen.append((args, kwargs))
        return fixture["token"]

    with connect(fixture, token_provider=provider):
        pass
    assert seen == [((), {})]


@pytest.mark.scenario("token.failure_message")
def test_a_custom_exception_message_passes_through_unchanged(fixture):
    class IdpError(Exception):
        pass

    message = "idp said: tenant \u00e9t\u00e9 suspended (code=7)"

    def provider():
        raise IdpError(message)

    with pytest.raises(felix.AuthError) as raised:
        connect(fixture, token_provider=provider)
    assert message in str(raised.value)


def test_a_str_that_is_not_valid_utf8_is_named(fixture):
    with pytest.raises(felix.AuthError, match="not valid UTF-8"):
        connect(fixture, token_provider=lambda: "tok\ud800en")


@pytest.mark.parametrize("result", [42, None, b"bytes", ""])
def test_a_result_that_is_not_a_token_is_a_failure(fixture, result):
    with pytest.raises(felix.AuthError, match="str|empty"):
        connect(fixture, token_provider=lambda: result)


def test_a_sync_provider_runs_on_a_worker_thread_not_the_caller(fixture):
    provider = Provider(fixture["token"])
    with connect(fixture, token_provider=provider):
        pass
    assert provider.threads
    assert threading.current_thread() not in provider.threads


def test_an_async_def_provider_works_on_the_blocking_client(fixture):
    calls = []

    async def provider():
        await asyncio.sleep(0.01)
        calls.append(threading.current_thread())
        return fixture["token"]

    with connect(fixture, token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
    assert len(calls) == 1
    assert calls[0] is not threading.main_thread()


def test_the_provider_runs_while_the_callers_event_loop_is_blocked(fixture):
    """A synchronous call from a coroutine blocks the loop; the provider must
    not need that loop."""
    outcome = {}

    async def fetch():
        await asyncio.sleep(0.01)
        return fixture["token"]

    async def main():
        connect(fixture, token_provider=fetch)  # blocks this loop
        outcome["done"] = True

    runner = threading.Thread(target=lambda: asyncio.run(main()), daemon=True)
    runner.start()
    runner.join(60)
    assert not runner.is_alive(), "the provider deadlocked against the blocked loop"
    assert outcome == {"done": True}


# --- AsyncClient -------------------------------------------------------------


def test_async_client_awaits_the_provider_on_its_own_loop(fixture):
    seen = []

    async def main():
        loop = asyncio.get_running_loop()
        # Bound to this loop: a Lock made here fails on any other loop's thread.
        lock = asyncio.Lock()

        async def provider():
            async with lock:
                seen.append(asyncio.get_running_loop() is loop)
                return fixture["token"]

        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token_provider=provider,
            ca_file=fixture["ca_file"],
        )
        async with client:
            await client.publish(
                fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a"
            )
            identity = await client.with_identity(
                fixture["tenant_id"], token_provider=provider
            )
            del identity

    asyncio.run(main())
    assert seen and all(seen)


def test_async_client_accepts_a_callable_returning_a_coroutine(fixture):
    async def fetch():
        return fixture["token"]

    def wrapped():  # what a sync decorator or lambda around `async def` looks like
        return fetch()

    async def main():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token_provider=wrapped,
            ca_file=fixture["ca_file"],
        )
        async with client:
            await client.publish(
                fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a"
            )

    asyncio.run(main())


def test_async_client_rejects_a_provider_returning_a_str(fixture):
    async def main():
        with pytest.raises(felix.AuthError, match="async def"):
            await felix.AsyncClient.connect(
                fixture["addrs"],
                tenant_id=fixture["tenant_id"],
                token_provider=lambda: fixture["token"],
                ca_file=fixture["ca_file"],
            )

    asyncio.run(main())


def test_the_provider_runs_while_another_task_is_busy(fixture):
    """The coroutine runs on the client's loop, which stays free for other
    tasks while the provider awaits."""

    async def main():
        ticks = 0
        stop = False

        async def busy():
            nonlocal ticks
            while not stop:
                ticks += 1
                await asyncio.sleep(0.005)

        async def provider():
            before = ticks
            await asyncio.sleep(0.2)
            # The other task ran while this one slept on the same loop.
            assert ticks > before
            return fixture["token"]

        task = asyncio.create_task(busy())
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token_provider=provider,
            ca_file=fixture["ca_file"],
        )
        stop = True
        await task
        async with client:
            pass

    asyncio.run(main())


@pytest.mark.scenario("token.failure_message")
def test_async_client_failure_reaches_the_caller(fixture):
    async def provider():
        raise RuntimeError("idp is down")

    async def main():
        with pytest.raises(felix.AuthError, match="idp is down"):
            await felix.AsyncClient.connect(
                fixture["addrs"],
                tenant_id=fixture["tenant_id"],
                token_provider=provider,
                ca_file=fixture["ca_file"],
            )

    asyncio.run(main())


# --- Expiry, against short-lived tokens from the fixture -----------------------


@pytest.mark.scenario("token.expiry_refresh")
def test_a_stream_opened_after_two_thirds_of_a_tokens_life_gets_a_fresh_one(fixture, key):
    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["durable_stream"],
    )
    provider = Provider(fixture["token"])
    payload = f"{key}-after-refresh".encode()
    with connect(fixture, token=mint(fixture, 6), token_provider=provider) as client:
        with client.subscribe(tenant, namespace, stream):
            pass
        assert provider.calls == 0, "the token still had most of its life"
        time.sleep(4.5)
        with client.subscribe(tenant, namespace, stream) as events:
            client.publish(tenant, namespace, stream, payload)
            received = []
            for _ in range(200):
                event = events.next_event(timeout=15.0)
                if event is None or event.payload == payload:
                    received.append(event)
                    break
    assert provider.calls == 1
    assert received and received[0] is not None, "the refreshed stream got nothing"


@pytest.mark.scenario("token.keep_valid_on_failure")
def test_a_failed_refresh_keeps_the_token_and_is_retried_later(fixture):
    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["durable_stream"],
    )
    calls = []
    healthy = threading.Event()

    def provider():
        calls.append(time.monotonic())
        if not healthy.is_set():
            raise RuntimeError("idp is down")
        return fixture["token"]

    # Refresh is due at 6 s; the token expires at 9 s.
    with connect(fixture, token=mint(fixture, 9), token_provider=provider) as client:
        time.sleep(6.5)
        with client.subscribe(tenant, namespace, stream):
            pass  # the held token is still valid, so this works
        assert len(calls) == 1
        with client.subscribe(tenant, namespace, stream):
            pass
        assert len(calls) == 1, "a failed refresh must not be retried on every open"

        # Past the retry delay and past the token's expiry: the provider is
        # asked again, now answers, and its token is used.
        healthy.set()
        time.sleep(max(0.0, calls[0] + 9.5 - time.monotonic()))
        with client.subscribe(tenant, namespace, stream):
            pass
    assert len(calls) == 2


# --- Timeout and cancellation -------------------------------------------------
#
# The 30 s fetch timeout is the Rust client's constant and is not exposed, so
# these two wait it out for real.


def test_async_provider_is_cancelled_at_the_timeout(fixture):
    cancelled = []

    async def provider():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def main():
        started = time.monotonic()
        with pytest.raises(felix.AuthError, match="took longer than"):
            await felix.AsyncClient.connect(
                fixture["addrs"],
                tenant_id=fixture["tenant_id"],
                token_provider=provider,
                ca_file=fixture["ca_file"],
            )
        assert 25 < time.monotonic() - started < 45
        # The cancel is delivered on the provider's loop, just after the error.
        for _ in range(100):
            if cancelled:
                break
            await asyncio.sleep(0.05)
        assert cancelled, "the provider's task was not cancelled at the timeout"

    asyncio.run(main())


def test_async_provider_is_cancelled_when_the_client_is_gone(fixture):
    import gc

    started = threading.Event()
    cancelled = threading.Event()

    async def provider():
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def main():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        # with_identity authenticates its streams, so it calls the provider.
        pending = asyncio.ensure_future(
            client.with_identity(fixture["tenant_id"], token_provider=provider)
        )
        while not started.is_set():
            await asyncio.sleep(0.05)
        # Abandon the call, then the client: nothing uses the token source.
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        del pending
        await client.__aexit__(None, None, None)
        del client
        gc.collect()
        for _ in range(200):
            if cancelled.is_set():
                return
            await asyncio.sleep(0.05)
        raise AssertionError("the provider's task outlived the client")

    asyncio.run(main())


def test_with_identity_cancels_an_async_provider_at_the_timeout(fixture):
    cancelled = threading.Event()

    async def provider():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def main():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        async with client:
            with pytest.raises(felix.AuthError, match="longer than 30s"):
                await client.with_identity(fixture["tenant_id"], token_provider=provider)
            # Poll without blocking: the provider runs on this loop.
            for _ in range(200):
                if cancelled.is_set():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("the provider's task was not cancelled")

    asyncio.run(main())


@pytest.mark.scenario("token.timeout_joins")
def test_a_sync_provider_past_the_timeout_is_joined_by_the_retry(fixture):
    # The held token expires at 6 s, so there is nothing to fall back on. The
    # provider answers 33 s after it is called. The wait on it fails at 30 s,
    # the cluster client retries the open (a fetch timeout carries no broker
    # code, so it is retried), and the retry joins the call still running and
    # gets its late answer. Sync providers cannot be cancelled, so joining is
    # the only way the provider stays single-flight.
    provider = Provider(fixture["token"], delay=33.0)
    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["durable_stream"],
    )
    with connect(
        fixture, fixture["addrs"][:1], token=mint(fixture, 6), token_provider=provider
    ) as client:
        time.sleep(6.5)
        started = time.monotonic()
        with client.subscribe(tenant, namespace, stream):
            pass
        # Loose upper bound: a loaded runner is slow.
        assert 30 < time.monotonic() - started < 60
        assert provider.calls == 1, "the retry started another call"
        with client.subscribe(tenant, namespace, stream):
            pass
        assert provider.calls == 1, "the late answer was not held"
        assert provider.max_running == 1


# --- The documentation example -----------------------------------------------


def _doc_snippets():
    page = DOCS_PAGE.read_text()
    section = page.split("### Getting and refreshing a token", 1)[1].split("\n## ", 1)[0]
    return re.findall(r"```python\n(.*?)```", section, re.S)


class FakeControlPlane:
    """Refresh tokens work once, like the real control plane's."""

    def __init__(self):
        self.live = {"rt0"}
        self.serial = 0
        self.refreshes = 0

    def urlopen(self, request, timeout=None):
        body = json.loads(request.data)
        if request.full_url.endswith("/token/exchange"):
            assert request.get_header("Authorization") == "Bearer oidc"
            used = None
        else:
            used = body["refresh_token"]
            if used not in self.live:
                raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)
            self.live.discard(used)
            self.refreshes += 1
        self.serial += 1
        self.live.add(f"rt{self.serial}")
        reply = {"felix_token": f"ft{self.serial}", "refresh_token": f"rt{self.serial}"}

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return Response(json.dumps(reply).encode())


def test_the_documented_example_rotates_the_refresh_token(monkeypatch):
    import urllib.request

    plane = FakeControlPlane()
    monkeypatch.setattr(urllib.request, "urlopen", plane.urlopen)
    built = {}

    class FakeClient:
        def __init__(self, addrs, **kwargs):
            built["client"] = kwargs

    class FakeAsyncClient:
        @staticmethod
        async def connect(addrs, **kwargs):
            built["async"] = kwargs

    monkeypatch.setattr(felix, "Client", FakeClient)
    monkeypatch.setattr(felix, "AsyncClient", FakeAsyncClient)

    snippets = _doc_snippets()
    assert len(snippets) == 2
    namespace = {}
    for snippet in snippets:
        exec(compile(snippet, "python.md", "exec"), namespace)  # noqa: S102

    namespace["connect"]("oidc")
    asyncio.run(namespace["connect_async"]("oidc"))

    sync = built["client"]
    assert sync["token"] == "ft1"
    assert [sync["token_provider"]() for _ in range(3)] == ["ft3", "ft4", "ft5"]

    fetch = built["async"]["token_provider"]
    assert asyncio.iscoroutinefunction(fetch)
    # Each refresh spent the newest token; a stale one would have raised 403.
    assert asyncio.run(fetch()) == "ft6"
    assert asyncio.run(fetch()) == "ft7"
    assert plane.refreshes == 5
