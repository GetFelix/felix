---
title: "Demo: Queue Semantics"
description: "Work distribution, redelivery after a crash, dead letters, and the duplicate cost of at-least-once."
---

## What this shows

A stream and a queue are the same log read two ways. `subscribe` pushes every
record to every subscriber and forgets it. A **consumer group** hands each record
to *one* consumer, waits to be told it was handled, and hands it to somebody else
if it is not.

The demo walks that difference through four acts and prints a ledger at the end,
so every claim it makes is a number rather than a story.

```bash
task demo:queues
```

## The four acts

**1. Work is distributed.** Two workers poll the same group. No offset is handed
to both: a claimed record is withheld from every other consumer while the claim
holds. This is the whole difference from `subscribe`, which would have given
every job to both workers.

**2. A worker dies holding work.** It claims two jobs and vanishes without
acknowledging them. Polling again returns *nothing*: the claims are still live.
Once the visibility timeout lapses, another worker gets them, at
`attempts = 2`.

Nothing was lost. But those jobs were done by the first worker and done **again**
by the second, and that is the honest half of this act:

> At-least-once is a promise about **loss**, not about duplicates. A crash
> between finishing the work and acknowledging it is indistinguishable from a
> crash before starting it, so the broker has to assume the worse of the two.

A consumer must be idempotent. `attempts > 1` is the signal it can key off.

**3. A job that always fails.** One job fails every time it is handled. It is
retried up to `max_attempts`, then dead-lettered. The point of the whole
mechanism is what happens next: **the two jobs queued behind it run anyway**.

Without an attempt bound a poison record would be redelivered for ever, and
nothing behind it would run.

The dead-lettered record is untouched: it is still in the stream's log at its
offset, readable by an ordinary replay. A dead letter is a **pointer, not a
copy**, so nothing is duplicated and nothing is thrown away.

**4. The ledger.** The demo asserts `published = completed + dead-lettered`.
Every job is accounted for: finished, or explicitly given up on. Nothing is in
limbo.

## Why it has no sleeps

`GroupReader::poll` takes the current time as an argument rather than reading a
clock, so the demo drives the visibility timeout forward itself. The output is
identical on every run instead of depending on how loaded the machine is.

That matters more than tidiness: a demo that sometimes fails to show its own
point is worse than no demo. It also means this doubles as a behavioural test,
which is why `task demo:check` runs it. Every guarantee narrated above is an
assertion, and the demo exits non-zero if one breaks.

## Notes

- In-process broker over a temp directory. No QUIC, no network.
- Durable storage is **required**: a group's cursor is a projection over the
  stream's log, so a non-durable stream serves no groups. A broker started
  without `FELIX_DURABLE_STORAGE_DIR` answers a poll with "this broker has no
  durable storage, so it serves no consumer groups".
- `max_attempts` is 3 here so the whole retry sequence fits on screen. The
  broker default is `FELIX_GROUP_MAX_ATTEMPTS`.

## What this does not show

- **One shard.** A group is bound to the shard the caller names, so consuming a
  multi-shard stream means polling each shard's group separately.
- **One broker.** A group's position and its dead-letter list replicate with the
  shard and survive a failover (see
  [Queues and Consumer Groups](/features/queues/#what-a-queue-does-not-promise)),
  but this demo runs a single in-process broker and does not show it.
- **No competing brokers.** Only the shard's leader serves its group. That is
  what stops two brokers handing out the same record, and it is why a poll is
  refused rather than forwarded when it reaches the wrong broker.

## See also

- [Queues and Consumer Groups](/features/queues/): the API and the rules
- [Projections](/architecture/projections/): how a cursor is a log
- [Semantics](/architecture/semantics/): where at-least-once sits
