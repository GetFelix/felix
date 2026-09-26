# The local cluster harness

A three-node Felix cluster on one machine, for integration and failure tests.

```bash
task cluster:status   # start, print membership and ownership, tear down
task cluster:smoke    # publish through a non-owner, receive from the owner
task cluster:up       # start and hold until Ctrl-C
task cluster:test     # the cross-broker integration tests
```

`-- --nodes 5` sets the size. Nothing else is required: no compose file, no
images, no ports to reserve, and no state left behind.

## Peer certificates

Every cluster the harness starts runs under broker-to-broker mTLS. It issues
one CA per cluster under the data root (`pki/ca.pem`, `pki/ca.key.pem`) and one
certificate per broker, issued to the broker's node id, and passes the three
paths as `FELIX_INTERNAL_TLS_CERT`, `FELIX_INTERNAL_TLS_KEY` and
`FELIX_INTERNAL_TLS_CA`. A broker spawned again keeps its identity: its
certificate is re-issued from the same CA. So what the failover, reconnect and
partition tests prove, they prove of the authenticated transport, not of a
mode no deployment should run.

## Driving it by hand

`up` writes a session file — the broker addresses and a credential — so a second
terminal has something to attach to. The other commands find it themselves.

```bash
# window 1
task cluster:up

# window 2
task -s cluster:subscribe -- orders

# window 3
task -s cluster:publish -- orders hello
```

```text
# window 2
subscribing to orders on broker-2 (owner)
[broker-2] offset      1  hello

# window 3
published "hello" to orders via broker-0 → forwarded to broker-2 → acknowledged
```

The stream and the payload are named on both sides deliberately. Two terminals
side by side have nothing else linking what was published to what arrived, and a
demo that does not show that linkage is not showing anything.

`-s` keeps Task from echoing its own `cargo run` line above every result.

A subscriber can sit idle indefinitely. That is worth stating because it was not
always true: QUIC closes a connection that has been idle for `max_idle_timeout`,
and a subscription to a quiet stream sends nothing in either direction, so
without a keep-alive the connection died at the idle timeout and took the
subscription with it. The transport now sends keep-alives by default.

That second line is the whole point of a cluster, and it is why the commands say
which broker they went through. A single broker produces the same records; only
the routing differs, so the routing is what the output shows.

`publish` defaults to a broker that does **not** own the shard, because that is
the path a single-node deployment cannot demonstrate. `--via broker-2` (the
owner) prints `written locally, no hop` instead.

The stream is required, not defaulted: a demo where the stream is implicit does
not show that the publisher and the subscriber are talking about the same one.

`subscribe` defaults to the owner, because the owner is the only broker that
serves a subscription today. `--on` a different broker is allowed and says
plainly that nothing will arrive; [subscribe routing](subscribe-routing.md)
covers how a subscribe reaches the owner.

A burst, for filling a subscriber's panel:

```bash
task cluster:burst                      # 30 messages, in order
COUNT=60 GAP=0.2 task cluster:burst     # slower, readable on camera
PARALLEL=10 task cluster:burst          # concurrent, and visibly reordered
```

It spreads messages across every broker, asking the cluster which ones exist so
it keeps working with `--nodes 5`.

**Ordering is worth understanding before demonstrating anything with it.** At
`PARALLEL=1` records arrive in the order they were sent. Above that they do not,
and that is correct rather than a defect: concurrent publishes through different
brokers have no defined relative order, and the owner assigns offsets in the
order it commits them. Felix orders a publisher's own sequence, not a race
between three of them. Measured at `PARALLEL=10`, all 30 records arrive and the
order interleaves.

`task cluster:owners` prints who leads what, which is worth having on screen
before publishing: the owner is chosen by rendezvous hash, so it differs between
runs.

Two clusters share one session file, so a second `up` takes it over and warns
that it has. Whichever stops first leaves the file alone unless it still
describes that cluster — otherwise stopping the second would leave the first
running and unreachable, holding its ports with no way to address it.

The session file holds a bearer token. It is written owner-only into the temp
directory and removed on teardown; the cluster it opens is loopback-only with dev
certificates, and disappears with the process.

## What is real, and what is not

**Brokers are real processes.** Each gets its own client-facing QUIC port,
internal peer port, metrics port, node identity, credential, and data directory.
They register with the control plane, forward to each other over the internal
transport, and stopping one is a real process exit.

**The control plane runs in the harness process.** Not for convenience: a broker
needs a credential, and the only way to obtain one today is an OIDC token
exchange against a real identity provider. Holding the store in process lets the
harness mint node and client tokens against the tenant's signing keys, which is
what `services/felix-broker-service/tests/membership_lifecycle.rs` already does for the same
reason.

The consequence, stated plainly: the control plane's router, store, placement,
and HTTP contract are all exercised; its `main`, its own configuration, and its
shutdown are not. A harness that ran it as a process would need a fake IdP, or a
way to issue a first credential without one — the latter is worth having on its
own, and would let this become a fully out-of-process cluster.

## Waiting

Nothing here sleeps for a fixed duration and hopes. Start-up returns only once:

1. every broker answers `/ready`,
2. the control plane considers every broker placeable,
3. every shard has a leader — placement is *stepped*, not waited for, so it does
   not depend on a reconcile timer, and
4. **a publish succeeds**.

The last one is the only honest check for the gap between "assigned" and
"servable": a broker can hold an assignment it has not finished opening, no
control-plane state distinguishes the two, and a publish in that window is
refused. The probe deliberately publishes through an arbitrary broker rather
than the owner, so the routing path is covered by start-up itself.

A wait that times out says what it was still waiting for.

### Budgets, and where they are wrong

The deadlines are wall-clock constants chosen against a developer machine, and
what they wait for is almost always setup — a leader elected, a replica caught
up — rather than the thing under test. On a shared CI runner, and especially
under coverage instrumentation, that setup takes longer for reasons that say
nothing about the code, and the failure then reports as the semantic having
broken. That has cost real investigation more than once.

`FELIX_TEST_TIMEOUT_SCALE` multiplies every deadline in `wait::until`. Unset
means 1, so a developer's run is unchanged and still fails fast on a genuine
hang; CI sets `3` and the coverage job `5`. Raising the constants instead would
have bought the same green at the cost of never noticing a hang on the machines
that are fast enough to.

It does not apply to a wait that *is* the subject. `losing_quorum_fails_writes_loudly_not_silently`
waits on a leader noticing it has not heard a quorum, and widening that would
only make the test slower at noticing nothing; it is serialised instead.

## Two tokens

The harness mints two credentials, and they are not interchangeable:

- A **client token** carrying only `stream.publish` and `stream.subscribe`,
  presented to brokers over QUIC.
- An **admin token** carrying tenant, namespace, and `node.view:cluster:*`,
  presented to the control plane's HTTP API.

A broker validates every action in a token it is given and rejects the whole
token if one is not a client-facing action, so a single credential carrying
`node.view` cannot publish at all.

## Faults

`stop_node` kills a broker and waits until the control plane no longer considers
it placeable, which is the primitive a failure test needs — without it, every
such test races the expiry sweep. Liveness windows are tuned short (a 1s expiry
timeout) because every process is local, so a stopped broker is observable in
about a second.

`restart_control_plane` takes the control plane down for a given time and
brings it back on the same address over the same store, so brokers see a
restart: cut connections, failed requests, and a fresh expiry sweep facing
heartbeat stamps as old as the outage. `stop_control_plane` stops it for good.

`kill_node` kills without waiting for the control plane to react. A test
measuring how long failover takes has to start its clock at the kill, not after
the cluster has already responded to it.

`pause_node` and `resume_node` suspend and resume a broker with `SIGSTOP` and
`SIGCONT`. This is the fault a kill cannot produce: the process stays alive,
keeps every lease and connection it holds, and answers nothing. It is what the
commit-boundary lease check exists for — a broker suspended past its lease
expiry has to refuse the write it was in the middle of when it wakes, rather
than committing to a shard someone else now leads. Unix only; there is no
equivalent elsewhere that leaves the process holding its state, and a test that
quietly did something weaker would be worse than one that does not run.

`crates/testing/felix-cluster/tests/failures/faults.rs` asserts each fault is the fault it
claims — a paused broker stops answering *and* stays alive, a resumed one comes
back, a kill returns immediately — because a scenario built on a fault that is
really something else passes for the wrong reason. It also pins that teardown
reclaims a suspended broker, so a test panicking mid-fault fails on its own
rather than wedging the suite.

`drain_node` marks a broker draining through the same endpoint an operator
uses, and `drain_until_empty` steps placement until it leads nothing — a move
is three assignment writes with a catch-up between the first two, so a single
`place_shards` does not finish one. `undrain_node` puts it back. `add_node`
starts one more broker against the running control plane and waits until it
is placeable, which is the join half of a rebalance. `shard_successors` reads
each shard's staged destination, for a test that wants to kill it mid-move.

Skewing a broker's clock is not supported. Lease expiry is read from the system
clock, so testing expiry against a skewed one needs either an injectable clock
in the broker or `libfaketime` around the process, and neither is in place.

Blocking a peer link without stopping the process is not supported yet; it needs
either a proxy in front of the internal listener or platform firewall rules, and
nothing so far has required it.

## The failover demo

```bash
task cluster:failover              # or -- --pace 0.5 to slow it down
```

Starts three brokers with a shard replicated three ways, publishes under
`Quorum`, kills the broker that acknowledged those records, and reads the whole
stream back from the broker that took over. It also publishes *through* the
failover, with a client configured with exactly one broker address — it asks
that broker who else is there, and reconnects to one of them on its own.

It fails loudly rather than narrating past a problem: a record acknowledged
before the kill that is not readable afterwards ends the demo with an error.

Two things in its output look like defects and are not, and it says so:
several `harness-probe` records (one per broker, published at startup to prove
the cluster can serve) and, sometimes, one duplicated record — that is
`publish_at_least_once`, which resends after a failure it cannot prove was not
applied.

## Watching it

A recording of the three-pane demo is on the docs site:
[Demo: Cross-broker Publishing](https://gabloe.github.io/felix/demos/cross-broker-cluster/).
Embedded as video rather than an animated GIF — the same 45 seconds of terminal
output would be tens of megabytes as a GIF, and could not be paused on the line
that matters.

## Running it hands-free

Two ways, depending on whether you want the three-panel view.

```bash
task cluster:demo         # ONE pane, drives itself, narrated headings
task cluster:demo:tmux    # THREE panes: cluster, subscriber, publisher
```

`cluster:demo` runs the whole sequence in a single process: the cluster comes
up, a subscriber attaches to the owner, a record is published through a broker
that does *not* own the shard, then through the one that does, then a burst
across all of them. Records arriving at the subscriber are indented with an
arrow so they read as a separate voice from the publisher's lines. `--pace 0`
runs it flat out, which is what a test wants; the default leaves room to narrate.

`cluster:demo:tmux` does the same thing across three real panes, which reads
better on camera. It needs `tmux`, and `PACE=6 task cluster:demo:tmux` slows it
down to narrate over.

The tmux version ends with two bursts rather than one: six records sent one at a
time, which arrive in order, then twenty-four sent twelve at a time, which do
not. Both prefixes are distinct (`seq-`, `par-`) so the subscriber's panel shows
which is which. The second burst is the interesting one — it makes visible that
concurrent publishes through different brokers have no relative order, which is
a property worth showing rather than a defect worth hiding. Both wait on `owners` rather than `nodes` to decide
the cluster is up: `nodes` only reads the session file, so a file left by a
previous cluster satisfies it immediately and the pane then talks to a control
plane that is gone.

## Running the tests

They are `#[serial]`. Each starts three broker processes, and a two-core CI
runner asked to start nine at once starves them all — the first symptom is a
readiness timeout that looks like a bug in the broker rather than in the test
setup. Serial runs also narrow the window in which two clusters can be handed
the same ephemeral port.

A broker that exits during start-up is started again, up to three times, with
fresh ports. Port selection is inherently racy — a port is probed, released, and
only then handed to the child — so losing it is a retry rather than a cluster
failure. A broker that fails every time is genuinely misconfigured and is
reported with its exit status and the tail of its own log, which is the
difference between "lost a port" and "the control plane refused this identity".

Broker output goes to `broker.log` in each node's data directory rather than
being discarded, so that reason exists to be quoted.

## The conformance suite

`crates/testing/felix-cluster/tests/conformance.rs` runs one set of assertions against
**both** a single broker and a three-node cluster. That equivalence is the
claim being tested: a client must not be able to tell how many brokers there
are, or which one it connected to.

Scenarios live in `scenarios.rs` and are parameterised by which broker the
publish goes through — the owner, or one that is not. A scenario that a
deployment cannot express (a non-owner, on a single node) reports `Skipped` and
says so in the log; it is never quietly run against the owner, which would look
like coverage while asserting nothing.

Everything goes through the client-facing API. A test that reached into broker
internals could not distinguish a correctly routed publish from one the wrong
broker handled locally, which is the failure the suite exists to catch. The
`delivery` scenario checks the forward counter on the ingress broker before
waiting for the record, because a broker that served the publish itself delivers
to its own subscribers and looks correct from any single vantage point.

Failures name the ingress broker, the owner, the shard, and the generation:

```
a non-owner handled the publish locally instead of forwarding it
  (ingress=broker-0 owner=broker-2 shard=t1/ns/orders/0 generation=0)
```

## The stale-ownership window, and what closes it

Ownership reaches a broker through its watch, so between the control plane
writing an assignment and the old owner reading it, that broker still believes
it owns the shard. Two mechanisms keep that from becoming acknowledged-write
loss, one per way a shard can change hands.

An **unplanned** change — the leader is gone — is fenced by the lease: the
control plane grants the next generation only after the old one's lease has
lapsed with a margin, and the old leader stopped serving before that by its
own clock.

A **planned** change — the leader is alive and the shard is moved — is fenced
by the assignment. The old owner is told to stop (`state: draining`) at a new
generation, stops serving the shard the moment its watch delivers that, and
reports once its log has stopped growing; the successor is named only after
that report. In between, writes to the shard are held until the cut-over and
then sent to the new owner. `move_shard`
drives exactly this, and `a_moved_shard_converges_on_the_new_owner` asserts the
old owner ends up forwarding to the new one; `tests/routing/rebalance.rs` asserts that
nothing acknowledged across the move is lost.

## Using it from a test

```rust
let cluster = Cluster::start(ClusterConfig::default()).await?;
let (owner, non_owner) = cluster.owner_and_non_owner("orders").await?;
cluster.publish_via(&non_owner, "orders", payload).await?;
```

`Cluster::metric` reads a counter or gauge from one broker's `/metrics`, and
returns `None` when it was never recorded — which for a counter is the
difference between "zero so far" and "this code path never ran". That
distinction is usually what a cross-broker assertion is making: a publish served
locally by the wrong broker looks exactly like one whose delivery is slow.

Dropping a `Cluster` kills its brokers, so a panicking test leaves nothing
running.
