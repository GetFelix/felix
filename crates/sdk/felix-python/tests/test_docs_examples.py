"""The examples on docs-site's Python page, tutorials and resilience guide compile and match the binding.

Every block is compiled, and every call on a client, a handle or the `felix`
module is bound against the signatures in `_felix.pyi`, so a renamed method or
keyword fails here instead of on a reader's machine. The blocks with control
flow worth getting right (resuming a subscription, mirroring a cache, resuming
a sharded stream, a thread pool) are also run against a fake client. The token
section's examples are run in test_token_provider.py.

Nothing here needs a broker or the native module, except the check that the
stubs name what the module has.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import pathlib
import re
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCS_PAGE = ROOT.parents[2] / "docs-site/src/content/docs/clients/python.md"
STUBS = ROOT / "python/felix/_felix.pyi"
RESILIENT_PAGE = ROOT.parents[2] / "docs-site/src/content/docs/clients/resilient-clients.mdx"
TUTORIALS = ROOT.parents[2] / "docs-site/src/content/docs/tutorials"
TUTORIAL_PAGES = ["first-stream", "work-queue", "cache-leases-watches", "build-an-app"]

# Names the examples give to objects, and the stub class each one is.
RECEIVERS = {
    "client": "Client",
    "alice": "Client",
    "events": "SubscriptionHandle",
    "event": "Event",
    "record": "GroupRecord",
    "watch": "CacheWatchHandle",
    "stream": "ShardedSubscriptionHandle",
    "receipt": "CommitReceipt",
    "producer": "Producer",
    "state": "StateValue",
}


def blocks_of(path):
    """Every ```python block on a page, with the line it starts on."""
    page = path.read_text()
    found = []
    for match in re.finditer(r"```python\n(.*?)```", page, re.S):
        found.append((page.count("\n", 0, match.start()) + 2, match.group(1)))
    return found


def blocks():
    """Every ```python block on the Python page."""
    return blocks_of(DOCS_PAGE)


def tutorial_blocks():
    """Every ```python block of every tutorial, as (page, line, source)."""
    return [
        (slug, line, source)
        for slug in TUTORIAL_PAGES
        for line, source in blocks_of(TUTORIALS / f"{slug}.mdx")
    ]


def block_containing(text):
    (source,) = [source for _, source in blocks() if text in source]
    return source


def _signature(function: ast.FunctionDef | ast.AsyncFunctionDef, drop_self: bool):
    args = function.args
    params = []
    positional = args.posonlyargs + args.args
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    for arg, default in zip(positional, defaults):
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=inspect.Parameter.empty if default is None else None,
            )
        )
    if args.vararg:
        params.append(inspect.Parameter(args.vararg.arg, inspect.Parameter.VAR_POSITIONAL))
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                default=inspect.Parameter.empty if default is None else None,
            )
        )
    if args.kwarg:
        params.append(inspect.Parameter(args.kwarg.arg, inspect.Parameter.VAR_KEYWORD))
    if drop_self and params and params[0].name in ("self", "cls"):
        params = params[1:]
    return inspect.Signature(params)


# The same names, for a block that uses the asyncio client.
ASYNC_RECEIVERS = {
    **RECEIVERS,
    "client": "AsyncClient",
    "events": "AsyncSubscription",
    "changes": "AsyncCacheWatch",
    "watch": "AsyncCacheWatch",
    "producer": "AsyncProducer",
}


def receivers_for(source):
    return ASYNC_RECEIVERS if "AsyncClient" in source else RECEIVERS


def stubs():
    """{class: {member: Signature or None for an attribute}} from `_felix.pyi`."""
    classes = {}
    for node in ast.parse(STUBS.read_text()).body:
        if not isinstance(node, ast.ClassDef):
            continue
        members = {}
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                static = any(
                    isinstance(d, ast.Name) and d.id == "staticmethod"
                    for d in item.decorator_list
                )
                members[item.name] = _signature(item, drop_self=not static)
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                members[item.target.id] = None
        classes[node.name] = members
    return classes


def _bind(signature, call: ast.Call, where: str):
    if any(isinstance(arg, ast.Starred) for arg in call.args) or any(
        keyword.arg is None for keyword in call.keywords
    ):
        return
    try:
        signature.bind(*call.args, **{keyword.arg: keyword.value for keyword in call.keywords})
    except TypeError as err:
        pytest.fail(f"{where}: {ast.unparse(call)}: {err}")


def test_the_page_has_examples():
    assert len(blocks()) > 20


@pytest.mark.parametrize("line,source", blocks(), ids=lambda value: str(value)[:12])
def test_every_example_compiles(line, source):
    compile(source, f"python.md:{line}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


def check_against_stubs(where, source):
    classes = stubs()
    receivers = receivers_for(source)
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Attribute):
            continue
        receiver = node.value
        # felix.Name and felix.Name.member
        if isinstance(receiver, ast.Name) and receiver.id == "felix":
            assert node.attr in classes, f"{where}: felix.{node.attr} is not in the stubs"
            continue
        if (
            isinstance(receiver, ast.Attribute)
            and isinstance(receiver.value, ast.Name)
            and receiver.value.id == "felix"
        ):
            members = classes.get(receiver.attr, {})
            assert node.attr in members, f"{where}: felix.{receiver.attr}.{node.attr} is unknown"
            continue
        if isinstance(receiver, ast.Name) and receiver.id in receivers:
            cls = receivers[receiver.id]
            members = classes[cls]
            assert node.attr in members, f"{where}: {cls} has no {node.attr}"

    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        func = node.func
        if isinstance(func.value, ast.Name) and func.value.id == "felix":
            init = classes[func.attr].get("__init__")
            if init is not None:
                _bind(init, node, where)
        elif (
            isinstance(func.value, ast.Attribute)
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "felix"
        ):
            _bind(classes[func.value.attr][func.attr], node, where)
        elif isinstance(func.value, ast.Name) and func.value.id in receivers:
            signature = classes[receivers[func.value.id]][func.attr]
            if signature is not None:
                _bind(signature, node, where)


@pytest.mark.parametrize("line,source", blocks(), ids=lambda value: str(value)[:12])
def test_every_example_matches_the_stubs(line, source):
    check_against_stubs(f"python.md:{line}", source)


def test_the_tutorials_have_examples():
    assert {slug for slug, _, _ in tutorial_blocks()} == set(TUTORIAL_PAGES)


@pytest.mark.parametrize(
    "page,line,source", tutorial_blocks(), ids=lambda value: str(value)[:12]
)
def test_every_tutorial_example_compiles(page, line, source):
    compile(source, f"{page}.mdx:{line}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


@pytest.mark.parametrize(
    "page,line,source", tutorial_blocks(), ids=lambda value: str(value)[:12]
)
def test_every_tutorial_example_matches_the_stubs(page, line, source):
    check_against_stubs(f"{page}.mdx:{line}", source)


def test_the_resilience_guide_has_examples():
    assert len(blocks_of(RESILIENT_PAGE)) >= 10


@pytest.mark.parametrize(
    "line,source", blocks_of(RESILIENT_PAGE), ids=lambda value: str(value)[:12]
)
def test_every_resilience_example_compiles(line, source):
    compile(source, f"resilient-clients.mdx:{line}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


@pytest.mark.parametrize(
    "line,source", blocks_of(RESILIENT_PAGE), ids=lambda value: str(value)[:12]
)
def test_every_resilience_example_matches_the_stubs(line, source):
    check_against_stubs(f"resilient-clients.mdx:{line}", source)


def test_the_stubs_name_what_the_module_has():
    felix = pytest.importorskip("felix")
    for cls, members in stubs().items():
        assert hasattr(felix, cls), cls
        # Methods only: exception attributes are set on instances.
        for member, signature in members.items():
            if signature is None or member.startswith("__"):
                continue
            assert hasattr(getattr(felix, cls), member), f"{cls}.{member}"


# --- Running examples against a fake client ----------------------------------


def fake_felix():
    class FelixError(Exception):
        pass

    module = types.SimpleNamespace(
        FelixError=FelixError,
        ConnectionError=type("ConnectionError", (FelixError,), {}),
        CursorError=type("CursorError", (FelixError,), {}),
        CacheWatchLagged=type("CacheWatchLagged", (), {}),
        CacheWatchShardMoved=type("CacheWatchShardMoved", (), {}),
        ShardRecord=type("ShardRecord", (), {}),
        CacheWatchFilter=types.SimpleNamespace(prefix=lambda value: ("prefix", value)),
    )
    return module


class Feed:
    """A context-managed iterator that can end with an exception."""

    def __init__(self, items, error=None, **attrs):
        self.items = items
        self.error = error
        self.closed = False
        self.__dict__.update(attrs)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False

    def __iter__(self):
        yield from self.items
        if self.error is not None:
            raise self.error

    def recv(self):
        return self.items.pop(0)


def run_block(source, monkeypatch, **names):
    module = fake_felix()
    monkeypatch.setitem(sys.modules, "felix", module)
    namespace = {"felix": module, **names}
    exec(compile(source, "python.md", "exec"), namespace)  # noqa: S102
    return module, namespace


def test_the_restartable_consumer_resumes_after_what_it_handled(monkeypatch):
    import time

    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    felix, namespace = run_block(block_containing("def run("), monkeypatch)
    event = lambda offset: types.SimpleNamespace(offset=offset, payload=b"p%d" % offset)  # noqa: E731
    feeds = [
        Feed([event(5), event(6)], felix.ConnectionError("lost")),
        Feed([event(7)], felix.CursorError("trimmed")),
        Feed([event(0)]),
    ]
    starts, saved, handled = [], [], []

    class Client:
        def subscribe(self, tenant_id, namespace, stream, *, start=None):
            starts.append(start)
            return feeds[len(starts) - 1]

    checkpoint = types.SimpleNamespace(load=lambda: 4, save=saved.append)
    namespace["run"](Client(), checkpoint, handled.append)

    assert starts == [5, 7, "earliest"]
    assert saved == [5, 6, 7, 0]
    assert handled == [b"p5", b"p6", b"p7", b"p0"]
    assert all(feed.closed for feed in feeds)


def test_the_cache_mirror_resumes_and_starts_over_after_a_resnapshot(monkeypatch):
    felix, namespace = run_block(block_containing("def mirror("), monkeypatch)
    change = lambda key, value: types.SimpleNamespace(key=key, value=value)  # noqa: E731

    def lagged(offset):
        item = felix.CacheWatchLagged()
        item.resume_from = offset
        return item

    watches = [
        Feed(
            [change("a", b"1"), change("b", b"2"), change("c", b"3"), change("a", None), lagged(10)],
            retained_count=2,
            resnapshot=False,
        ),
        Feed([change("b", None), felix.CacheWatchShardMoved(), lagged(20)], resnapshot=False),
        Feed([change("stale", b"x")], resnapshot=True),
        Feed([change("x", b"9")], retained_count=1, resnapshot=False),
    ]
    calls = []
    seen = []

    class Client:
        def watch_cache(self, tenant_id, namespace, cache, filter, *, start=None, retained=False):
            calls.append((start, retained))
            return watches[len(calls) - 1]

    roster = {"left over": b""}
    original = Feed.__iter__

    def tracking(feed):
        for item in original(feed):
            seen.append(dict(roster))
            yield item

    monkeypatch.setattr(Feed, "__iter__", tracking)
    namespace["mirror"](Client(), roster)

    assert calls == [(None, True), (10, False), (20, False), (None, True)]
    # Before the resnapshot the resumed watch applied the delete of "b".
    assert {"c": b"3"} in seen
    assert roster == {"x": b"9"}
    assert all(watch.closed for watch in watches)


def test_the_sharded_resume_saves_positions_while_open(monkeypatch):
    felix = fake_felix()
    record = felix.ShardRecord()
    record.event = types.SimpleNamespace(payload=b"x")
    feed = Feed([record, object()])
    feed.positions = lambda: {} if feed.closed else {0: 4}
    opened, saved, handled = [], [], []

    class Client:
        def subscribe_sharded(self, tenant_id, namespace, stream, *, start=None, resume=None):
            opened.append(resume)
            return feed

    monkeypatch.setitem(sys.modules, "felix", felix)
    exec(  # noqa: S102
        compile(block_containing("resume=resume"), "python.md", "exec"),
        {
            "felix": felix,
            "client": Client(),
            "checkpoint": types.SimpleNamespace(load=lambda: {0: 3}, save=saved.append),
            "handle": handled.append,
        },
    )
    assert opened == [{0: 3}]
    assert handled == [b"x"]
    assert saved == [{0: 4}]


def test_the_thread_pool_example_raises_a_failed_publish(monkeypatch):
    class Client:
        def publish(self, tenant_id, namespace, stream, payload):
            if payload == b"bad":
                raise RuntimeError("publish failed")
            return len(payload)

    source = block_containing("ThreadPoolExecutor")
    _, namespace = run_block(source, monkeypatch, client=Client(), payloads=[b"a", b"bb"])
    assert namespace["offsets"] == [1, 2]
    with pytest.raises(RuntimeError, match="publish failed"):
        run_block(source, monkeypatch, client=Client(), payloads=[b"a", b"bad"])


# Names a fragment may use without defining: the client, credentials and
# application callbacks the surrounding prose supplies. Anything else, an
# import or a logger included, has to be in the block.
CONTEXT_NAMES = {
    "client", "alice", "tok", "addrs", "alice_token", "oidc_token",
    "payload", "payloads", "orders", "data", "customer_id", "shard", "records",
    "checkpoint", "handle", "alert", "charge", "quarantine", "retry",
    "reconcile", "give_up", "create_stream", "fixed_the_cause",
    "finished", "work_a_while",
    "RetryableError",
}


def undefined_names(source):
    tree = ast.parse(source)
    defined = set(dir(builtins)) | {"__name__"} | CONTEXT_NAMES
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.arg):
            defined.add(node.arg)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                defined.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            defined.add(node.name)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            defined.update(node.names)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            defined.add(node.id)
    return sorted(
        {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id not in defined}
    )


@pytest.mark.parametrize("line,source", blocks())
def test_blocks_use_only_defined_names(line, source):
    assert undefined_names(source) == [], f"python.md line {line}"
