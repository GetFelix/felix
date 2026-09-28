# Server

The libraries the two services in [`../../services`](../../services) are built
from. None of them opens a socket or reads the environment on its own; the
services do the wiring, which is what lets these be tested without a network.
The one exception is `felix-replication`'s peer transport, which binds its
listener and reads its `FELIX_INTERNAL_*` settings when the broker service
asks it to.

- [`felix-broker`](felix-broker) is the broker's semantics: streams, caches and
  consumer groups over one log, the publish path, and fanout. Start at
  `Broker`.
- [`felix-storage`](felix-storage) is where records are kept: the segment
  format, the durable log with recovery and group commit, and the cache stores
  built on it.
- [`felix-kafka`](felix-kafka) is the Kafka wire protocol, read-only: it
  answers a Kafka consumer's requests from the broker's shard logs. The broker
  service owns its socket and TLS and hands it each connection.
- [`felix-router`](felix-router) answers which node serves a shard, from the
  assignments the control plane publishes.
- [`felix-replication`](felix-replication) is how brokers talk to each other:
  the broker-internal transport, and the log replication that runs over it.
  The broker service plugs in through three small traits.
- [`felix-authz`](felix-authz) is tokens and permissions: the broker uses it
  to verify a client's token and match its permissions, and the test tools and
  demos use it to mint tokens.
- [`felix-common`](felix-common) is the small set of things the broker and the
  control plane must agree on exactly: the membership JSON shapes, the
  `FELIX_*` variable registry, and process lifecycle helpers.

`felix-common` is Apache-2.0; the rest are AGPL-3.0. See `LICENSING.md`.
