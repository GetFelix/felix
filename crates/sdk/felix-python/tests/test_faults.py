"""Connection faults: the link to a broker drops, resets or stalls mid-publish
and mid-subscribe.

The steps come from the fixture, which copies them out of the catalogue, and
the fault comes from its `/link` endpoint: an interposer between this client
and the broker that owns `link_stream`. See the "Connection faults" section of
`scenarios.toml` for what each scenario must show.
"""

from __future__ import annotations

import concurrent.futures
import json
import time
import urllib.request

import pytest

# Room for the idle timeout to fire and a reconnect to land, on top of the
# fault's own hold.
SETTLE = 45.0

# Short on purpose: a read with a timeout is how Python code polls, and each
# expiry cancels the read in flight. The client has to survive that mid-resume.
POLL = 0.25

FAULT_IDS = [
    "fault.publish_through_a_dropped_link",
    "fault.publish_through_a_reset_link",
    "fault.publish_through_a_stalled_link",
    "fault.subscription_through_a_dropped_link",
    "fault.subscription_through_a_reset_link",
    "fault.subscription_through_a_stalled_link",
]


def _step(fixture, scenario_id):
    for fault in fixture.get("faults", []):
        if fault["id"] == scenario_id:
            return fault
    pytest.fail(f"the fixture has no fault step for {scenario_id}")


def _post(fixture, path, body=None):
    request = urllib.request.Request(
        fixture["control_url"] + path,
        data=json.dumps(body or {}).encode(),
        method="POST",
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def _inject(fixture, step):
    _post(fixture, "/link", {"fault": step["fault"], "hold_ms": step["hold_ms"]})


def _linked_client(fixture):
    import felix

    return felix.Client(
        [fixture["link_addr"]],
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    )


@pytest.fixture
def linked(fixture):
    if not fixture.get("link_addr") or not fixture.get("control_url"):
        pytest.skip("this fixture has no link interposer")
    try:
        with _linked_client(fixture) as client:
            yield client
    finally:
        # A client that resumed elsewhere can finish inside a drop's hold, and
        # the next test's client would connect into it.
        _post(fixture, "/heal")


@pytest.mark.parametrize(
    "scenario_id",
    [pytest.param(sid, marks=pytest.mark.scenario(sid), id=sid) for sid in FAULT_IDS],
)
def test_a_connection_fault(scenario_id, fixture, client, linked):
    step = _step(fixture, scenario_id)
    if step["during"] == "publish":
        _publish_through_a_fault(scenario_id, step, fixture, client, linked)
    else:
        _subscribe_through_a_fault(scenario_id, step, fixture, client, linked)


def _publish_through_a_fault(scenario_id, step, fixture, direct, linked):
    import felix

    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["link_stream"],
    )
    hold = step["hold_ms"] / 1000
    stamp = f"{scenario_id}-{time.monotonic_ns()}"
    acked = []
    healed_at = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for index in range(step["records"]):
            if index == step["after_records"]:
                _inject(fixture, step)
                healed_at = time.monotonic() + hold
            payload = f"{stamp}-{index}".encode()
            call = pool.submit(linked.publish, tenant, namespace, stream, payload)
            try:
                call.result(timeout=hold + SETTLE)
                acked.append(payload)
            except concurrent.futures.TimeoutError:
                pytest.fail(f"publish {index} neither returned nor failed")
            except felix.FelixError as err:
                if step["fault"] == "stall":
                    pytest.fail(f"publish {index} failed during a stall: {err!r}")

    # Once the fault is over, the client has to be able to publish again.
    if healed_at is not None:
        time.sleep(max(0.0, healed_at - time.monotonic()))
    deadline = time.monotonic() + SETTLE
    while True:
        payload = f"{stamp}-after".encode()
        try:
            linked.publish(tenant, namespace, stream, payload)
            acked.append(payload)
            break
        except felix.FelixError as err:
            if time.monotonic() >= deadline:
                pytest.fail(f"the client never published again after the fault: {err!r}")
            time.sleep(POLL)

    # Read back on an untouched connection: an acknowledged record that is not
    # there was lost.
    missing = set(acked)
    with direct.subscribe(tenant, namespace, stream, start="earliest") as check:
        deadline = time.monotonic() + SETTLE
        while missing and time.monotonic() < deadline:
            event = check.next_event(timeout=1.0)
            if event is not None:
                missing.discard(event.payload)
    assert not missing, f"{len(missing)} acknowledged records were not in the stream"


def _subscribe_through_a_fault(scenario_id, step, fixture, direct, linked):
    import felix

    tenant, namespace, stream = (
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["link_stream"],
    )
    hold = step["hold_ms"] / 1000
    stamp = f"{scenario_id}-{time.monotonic_ns()}"
    payloads = [f"{stamp}-{index}".encode() for index in range(step["records"])]
    before, rest = payloads[: step["after_records"]], payloads[step["after_records"] :]

    received = []
    with linked.subscribe(tenant, namespace, stream) as events:
        for payload in before:
            direct.publish(tenant, namespace, stream, payload)

        injected = False
        deadline = time.monotonic() + SETTLE
        while len(received) < len(payloads):
            if not injected and len(received) == len(before):
                _inject(fixture, step)
                injected = True
                deadline = time.monotonic() + hold + SETTLE
                for payload in rest:
                    direct.publish(tenant, namespace, stream, payload)
            if time.monotonic() >= deadline:
                pytest.fail(
                    "neither resumed nor reported the loss: "
                    f"{len(received)} of {len(payloads)} records, then nothing"
                )
            try:
                event = events.next_event(timeout=POLL)
            except felix.FelixError as err:
                if not injected or step["fault"] == "stall":
                    pytest.fail(f"the subscription failed with no loss to report: {err!r}")
                # Reported the loss, which is one of the two right answers.
                _check_delivered(received, payloads)
                return
            if event is None:
                assert not events.closed, (
                    f"the subscription ended cleanly after {len(received)} of "
                    f"{len(payloads)} records; a dead connection must be resumed "
                    "or reported, not read as the end of the stream"
                )
                continue
            received.append(event)

    _check_delivered(received, payloads)


def _check_delivered(received, payloads):
    """What arrived is a prefix of what was published, at contiguous offsets."""
    got = [event.payload for event in received]
    assert got == payloads[: len(got)], "a gap, a duplicate or a reordering"
    offsets = [event.offset for event in received]
    assert all(offset is not None for offset in offsets)
    for earlier, later in zip(offsets, offsets[1:]):
        assert later == earlier + 1, f"offsets jumped: {earlier} then {later}"
