# Failover on your laptop

A three-broker Felix cluster you can kill brokers in, and an independent
checker that proves what was acknowledged is still there afterwards. One
command starts it:

```bash
./run.sh up
```

It needs Docker, or Podman with the `docker-compose` provider (2.20 or later).
The first run builds the checker image from this repository's source (there is no
released image of it), which takes a few minutes. The brokers and control plane
are the released images from `ghcr.io/getfelix`. The tag is `FELIX_VERSION` in
`.env`, which the release check keeps on the newest tagged release.
The cluster uses the private network 172.30.8.0/24; set `FAILOVER_NET=172.30.9`
(the first three octets) if that is taken on your machine.

Then, from a second terminal:

```bash
./run.sh kill-leader                # SIGKILL the broker leading the stream
./run.sh restart broker-2           # bring it back (use the name kill-leader printed)
./run.sh kill-controlplane          # replicas elect a leader on their own
./run.sh restart-controlplane
./run.sh receipt                    # check everything acknowledged so far
./run.sh scenario                   # all of the above, hands off
./run.sh down                       # final receipt, then remove everything
```

`receipt` and `down` exit non-zero unless the checker's verdict is a pass, so
they can gate a script.

`up` follows the checker's output, one line a second. Ctrl-C stops following and
leaves the cluster running; `./run.sh watch` follows it again.
[The docs page](../../docs-site/src/content/docs/getting-started/failover-on-your-laptop.md)
has the commands with real output.

## What runs

| Service | What it is |
|---|---|
| `controlplane` | One control plane member with its metadata on a volume, so it can be killed and restarted. |
| `broker-1`, `broker-2`, `broker-3` | The brokers, on fixed addresses on a private network. |
| `provision` | Runs once: turns on election among replicas and creates the stream. |
| `checker` | The independent checker, built from `src/`. |

The stream is `default/orders`: one shard, **replication factor 3**,
**`Quorum`** consistency, durable. A publish is acknowledged once a majority of
the three copies hold it. Brokers run with `FELIX_DURABLE_FSYNC_MODE=on_commit`,
so "hold it" means flushed to disk; the broker's own default is `periodic`,
which acknowledges before the flush. Set `FELIX_DURABLE_FSYNC_MODE=periodic`
before `./run.sh up` to try that.

Two settings are demo tuning, not defaults, and the numbers depend on them:
the control plane declares a broker dead after 3 s instead of about 20 s, and
brokers elect among themselves (`FELIX_INTERNAL_ELECTION`), which is off by
default. [Failover without the control plane](../../docs-site/src/content/docs/deployment/failover-without-control-plane.md)
says what that costs and what it needs.

## What the checker checks

`failover-check` runs `PRODUCERS` (default 4) producers at `RATE` (default 25)
writes a second each, through the SDK's `IdempotentProducer`, and a reader on
the same stream.

- Each producer numbers its own writes `0, 1, 2, ...` and puts
  `(run, producer, sequence)` in the payload. That numbering is the checker's,
  not the broker's, so the check does not rest on the mechanism under test.
- A write is **acknowledged** when the broker answers `Ok` to its publish. Under
  `Quorum` that means a majority of the shard's copies hold it.
- A publish that errors has an unknown outcome. The producer sends the same
  bytes again under the same sequence until it is acknowledged, which the
  idempotent producer makes safe. While it waits, the write is counted as
  **indeterminate**, never as lost. One that is still unanswered when the
  checker stops stays indeterminate, and the receipt says whether it turned up
  in the stream. Neither outcome is a violation.
  **Indeterminate is usually 0 anyway.** The SDK retries a publish inside the
  call while it looks for the new leader, and it does not report how often, so
  a failover shows up as a slow publish and an error never reaches the checker.
  A 0 there does not show that the re-send path ran; the unit tests do that.
- The reader follows the stream from its first record and counts what it
  delivers. The live `gaps` and `duplicates` columns come from it.
- **The receipt does not trust the reader.** It takes the set of writes
  acknowledged so far, then reads the whole stream back, page by page from its
  oldest record, and checks that every acknowledged `(producer, sequence)`
  appears exactly once, that nothing appears twice, and that nothing this run
  never sent is there.

A run **fails**, rather than passes with zero problems, if nothing was
acknowledged, if a producer could not start, or if a producer stopped early
(for example the broker refused it). The receipt says how many producers ran and
why any stopped. The read-back keeps going past empty pages and stops only once
it has read past the highest offset an acknowledgement reported.

The status line:

```
[  42s] acknowledged   4180  in-flight  3  indeterminate  0  delivered   4177  gaps 0  duplicates 0  producers 4/4
```

`delivered` trails `acknowledged` by what is in transit. `gaps` is writes
acknowledged more than 5 s ago that the reader has not delivered. When publishes
stop completing, the checker prints how long they were blocked, from the oldest
publish left waiting to the first acknowledgement after it.

### What it does not check

- It is one stream with one shard. It does not look at consumer groups,
  caches or multi-shard ordering.
- Order is not checked, only presence and multiplicity.
- A `SIGKILL` of a broker process does not test power loss: the kernel keeps the
  page cache. `on_commit` is what covers a flush that never happened, and this
  demo does not pull a plug.
- Killing a container is not a network partition or a paused process. A leader
  that comes back still believing it leads is a different test.
- One broker down at a time among three. Losing two stops writes by design.
- The checker is a single process on one network. It cannot see a fault in its
  own bookkeeping, which is why the logic is small and has unit tests
  (`cargo test --manifest-path demos/failover/Cargo.toml`).

## Why a demo crate and not a felixctl command

The checker is an application that uses the public client, the same way yours
would, and it stays out of the tool people run in production. The repository's
demos live outside the workspace for the same reason: `task demo:check` builds
and tests it.

## Safety

This is for development only: the credentials are fixed and the certificate is
self-signed.

`run.sh` runs everything through `docker compose -p felix-failover`, so it only
starts, kills and removes containers and volumes of that one project. The
project name is fixed in the script, not read from your environment, so two
copies of this demo cannot run side by side. No host port is published, so
nothing here can collide with, or be reached from, anything else. The credentials are fixed development tokens; do not reuse this
file anywhere that matters.
