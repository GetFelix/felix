"""`token_provider=`: the app's function behind the Rust client's `RefreshingToken`.

The timing rules (refresh at two thirds of a token's life, keep a still-valid
token when a fetch fails, 30 s timeout) belong to the Rust client and are
tested there. These tests cover what the binding adds: when the Python callable
is called, on which thread and loop, and how its failures reach the caller.
The fixture mints only hour-long tokens, so none of these wait for an expiry.
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


def connect(fixture, **kwargs):
    return felix.Client(
        fixture["addrs"],
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


def test_async_client_rejects_a_plain_callable():
    async def main():
        with pytest.raises(TypeError, match="async def"):
            await felix.AsyncClient.connect(
                "127.0.0.1:1", tenant_id="t", token_provider=lambda: "x"
            )

    asyncio.run(main())


# --- Contract points, against the fixture ------------------------------------


def test_an_idle_client_makes_no_calls(fixture, key):
    provider = Provider(fixture["token"])
    with connect(fixture, token=fixture["token"], token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
        time.sleep(0.3)
    assert provider.calls == 0


@pytest.mark.parametrize("initial", [None, ""])
def test_without_an_initial_token_the_first_connect_calls_the_provider(fixture, initial):
    provider = Provider(fixture["token"])
    kwargs = {} if initial is None else {"token": initial}
    with connect(fixture, token_provider=provider, **kwargs) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"b")
    # The hour-long token it returned is held, so later streams do not ask.
    assert provider.calls == 1


def test_the_provider_is_never_called_concurrently(fixture):
    """A handle's publish and cache streams both need the token at once."""
    with connect(fixture, token=fixture["token"]) as client:
        provider = Provider(fixture["token"], delay=0.5)
        with client.with_identity(fixture["tenant_id"], token_provider=provider):
            pass
    assert provider.calls == 1
    assert provider.max_running == 1


def test_a_refused_token_costs_one_fetch_and_one_retry(fixture):
    provider = Provider(fixture["token"])
    with connect(fixture, token="not-a-token", token_provider=provider) as client:
        client.publish(fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], b"a")
    assert provider.calls == 1


def test_a_provider_returning_the_refused_token_is_not_retried(fixture):
    provider = Provider("not-a-token")
    with pytest.raises(felix.AuthError):
        connect(fixture, token="not-a-token", token_provider=provider)
    assert provider.calls == 1


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
        client = connect(fixture, token_provider=fetch)  # blocks this loop
        client.close()
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
