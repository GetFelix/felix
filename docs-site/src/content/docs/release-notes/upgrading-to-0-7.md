---
title: Upgrading to 0.7
description: What an operator moving a cluster from 0.6.2 to the 0.7 line needs to know. 0.7 is not released, so this page is a working draft.
---

:::caution[0.7 is not released]
The 0.7 line is in development on `main` as `0.7.0-preview.1`. This page lists
what has changed since 0.6.2 and is reviewed and finalized when 0.7.0 is
tagged. More will be added before then. Do not plan a production upgrade from
it yet. The released versions are in the [release notes](/release-notes/).
:::

Read [Upgrades and compatibility](/deployment/upgrades/) for the rules these
notes rely on: the five compatibility surfaces, the upgrade order, fleet
features and rollback.

## Summary for an upgrade from 0.6.2

Nothing so far changes the client protocol, the internal protocol or the
storage format. No new fleet feature has been added. Broker and control-plane
settings are unchanged. The ordinary upgrade order applies: control plane,
then brokers one at a time, then clients.

One change needs attention during the upgrade, on the control plane.

## Control plane

A leader promoted by a failover now reports, for each follower it has not
reached yet, the storage identity that its own promotion named the follower by.
The next failover names the follower by that identity, so a second failover
soon after the first no longer leaves the shard closed (#1574).

- With the Postgres backend, the control plane runs migration `0037` when it
  starts. Nothing else is needed.
- With the Raft backend, the new field is kept only once every member is at
  metadata level 20. Until the last member is upgraded, a follower named this
  way does not have its fence answer counted, as in 0.6.2, and a shard that
  needs that answer waits. Upgrade every control-plane member before the
  brokers, and see [Rollback](/deployment/upgrades/#rollback) for the limit on
  going back once level 20 entries are written.

## Clients

Both changes below are additive. Existing code keeps working without edits.

- Every client binding takes four reconnect options: an attempt count, a
  deadline for one retried call, and the backoff and max backoff between
  attempts. When unset they keep the Rust defaults. The C struct
  `FelixConnectOptions` gained four fields, `reconnect_attempts`,
  `reconnect_deadline_ms`, `reconnect_backoff_ms` and
  `reconnect_max_backoff_ms`, after `token_release`. A program built against
  the older `felix.h` keeps the defaults, since zero means default for all.
- Cache and consumer-group calls on `ClusterClient` now follow
  `ReconnectPolicy`, as publishes already did. The attempt limit, backoff and
  deadline bound the whole call, re-routes included. A write is sent again only
  after a refusal that says nothing was applied.

## Packaging

Each GitHub release now carries prebuilt `libfelix` C libraries, shared and
static, with `felix.h` and a `LINKING.txt`, for Linux, macOS and Windows on x64
and arm64. See [the C client](/clients/c/).

## Still to check at release

- Whether 0.7 adds a fleet feature, and so needs a finalize step.
- Whether it raises `INTERNAL_VERSION` or `FORMAT_VERSION`.
- Any removed or renamed setting.
