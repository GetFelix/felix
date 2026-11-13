"""A real cluster for the tests to talk to, and the conformance ledger.

Deliberately not a mock. The claim this binding makes is that Python gets the
*same* client behaviour Rust does — reconnection, redirect-following, offset
accounting — and a mock would only validate the mock. So these tests start the
real thing: `felix-cluster client-fixture` runs a control plane and brokers,
registers what the scenarios need, and writes a JSON file saying how to reach
them.

Every test that covers a catalogued scenario tags itself with
`@pytest.mark.scenario("<id>")`. What each one did is recorded and written out
as a results document at the end of the session, which
`felix-conformance verify` then checks against the catalogue. A test that
fails, errors, or never runs becomes a non-passing outcome — so the claim
"Python is conformant" is a thing the suite earns rather than asserts.
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import subprocess
import time

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
FIXTURE_WAIT_SECONDS = 120
RESULTS_ENV = "FELIX_CONFORMANCE_RESULTS"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "scenario(*ids): the conformance scenario(s) this test covers",
    )


def _require_binaries() -> None:
    for binary in ("felix-broker", "felix-cluster"):
        if not (REPO_ROOT / "target" / "debug" / binary).exists():
            pytest.skip(
                f"no {binary} binary — build with "
                "`cargo build -p felix-broker-service --bin felix-broker && "
                "cargo build -p felix-cluster --bin felix-cluster`"
            )


@contextlib.contextmanager
def _cluster(work: pathlib.Path):
    """Run a conformance cluster over `work`, yielding its fixture document."""
    fixture_path = work / "fixture.json"
    ca_file = work / "brokers.pem"

    # To a file, not a pipe: nothing here reads a pipe while the suite runs, and
    # a full one blocks the fixture's control plane mid-write, which looks like
    # a broker that stopped answering.
    log_path = work / "fixture.log"
    log = log_path.open("w")
    process = subprocess.Popen(
        [
            str(REPO_ROOT / "target" / "debug" / "felix-cluster"),
            "client-fixture",
            "--out",
            str(fixture_path),
            "--ca-file",
            str(ca_file),
        ],
        env={**os.environ, "RUST_LOG": "warn"},
        cwd=REPO_ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log.close()

    deadline = time.monotonic() + FIXTURE_WAIT_SECONDS
    body = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = log_path.read_text()
            pytest.fail(f"the fixture exited during startup:\n{output}")
        if fixture_path.exists() and ca_file.exists():
            try:
                body = json.loads(fixture_path.read_text())
            except json.JSONDecodeError:
                time.sleep(0.2)  # being written right now
                continue
            break
        time.sleep(0.2)

    if body is None:
        process.terminate()
        pytest.fail(
            f"the fixture was not ready within {FIXTURE_WAIT_SECONDS}s:\n"
            f"{log_path.read_text()}"
        )

    try:
        yield body
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.fixture(scope="session")
def fixture(tmp_path_factory) -> dict:
    """The cluster every non-destructive test shares."""
    _require_binaries()
    with _cluster(tmp_path_factory.mktemp("felix-fixture")) as body:
        yield body


@pytest.fixture
def disposable_fixture(tmp_path_factory) -> dict:
    """A cluster of this test's own, for tests that break one deliberately.

    Killing a broker is a legitimate thing to assert against and a ruinous
    thing to do to a shared fixture: every test after it would fail against a
    cluster someone else damaged, and the failures would look like client
    bugs. So the destructive tests pay for their own cluster.
    """
    _require_binaries()
    with _cluster(tmp_path_factory.mktemp("felix-disposable")) as body:
        yield body


@pytest.fixture
def client(fixture):
    """A connected synchronous client, closed after the test."""
    import felix

    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    ) as connected:
        yield connected


@pytest.fixture
def key(request) -> str:
    """A key or stream suffix unique to the test, so tests cannot collide."""
    return request.node.name.replace("[", "-").replace("]", "").replace(" ", "-")


# ---------------------------------------------------------------------------
# The conformance ledger
# ---------------------------------------------------------------------------
#
# Recorded per scenario id rather than per test: several tests may cover one
# scenario, and the scenario passes only when all of them do. A scenario whose
# test errored during setup is not a pass either — the point of the ledger is
# that only a demonstrated semantic counts.

_OUTCOMES: dict[str, dict] = {}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    report = (yield).get_result()
    marker = item.get_closest_marker("scenario")
    if marker is None or not marker.args:
        return

    # One test may demonstrate several scenarios at once — a resume proves both
    # the resume semantic and the reconnect one — and each is recorded.
    for scenario_id in marker.args:
        if report.when == "call" and report.passed:
            _OUTCOMES.setdefault(scenario_id, {"id": scenario_id, "status": "pass"})
        elif report.failed:
            # A failure overwrites a pass: a scenario covered by several tests
            # holds only if every one of them holds.
            _OUTCOMES[scenario_id] = {
                "id": scenario_id,
                "status": "fail",
                "detail": f"{item.nodeid}: {report.longreprtext.splitlines()[-1][:300]}"
                if report.longreprtext
                else item.nodeid,
            }
        elif report.skipped and scenario_id not in _OUTCOMES:
            reason = ""
            if isinstance(report.longrepr, tuple) and len(report.longrepr) == 3:
                reason = report.longrepr[2]
            _OUTCOMES[scenario_id] = {
                "id": scenario_id,
                "status": "skip",
                "detail": reason or item.nodeid,
            }


def pytest_sessionfinish(session, exitstatus):
    """Write the results document `felix-conformance verify` reads."""
    destination = os.environ.get(RESULTS_ENV)
    if not destination:
        return
    try:
        import felix

        version = felix.__version__
    except Exception:  # noqa: BLE001 - reporting must not depend on the import
        version = "unknown"

    document = {
        "client": "felix-client (Python)",
        "version": version,
        "outcomes": sorted(_OUTCOMES.values(), key=lambda outcome: outcome["id"]),
    }
    path = pathlib.Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n")
    print(f"\nconformance results written to {path}")
