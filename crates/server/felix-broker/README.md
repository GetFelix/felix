# felix-broker

The broker core of [Felix](https://github.com/GetFelix/felix): streams, caches and
consumer groups over one log, the publish path with its commit ordering, and
fanout to per-subscriber queues. Start at `Broker`.

It has no sockets and no control-plane client; `services/felix-broker-service`
wires those around it. That split is what lets the semantics be tested without
a network.

Not published; it is built into the broker service. AGPL-3.0-only. See
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md).
