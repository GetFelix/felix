# felix-transport

QUIC transport primitives shared by [Felix](https://github.com/GetFelix/felix)
clients and brokers: endpoint construction, connection and stream lifetime, and
the datagram sizing that decides whether a connection is fast or stalled. TLS is
the caller's: an endpoint is built from the quinn server or client config it is
given.

## What is not obvious here

Linux's UDP GSO batches segments into one IP datagram, so `MTU × segments` must
stay under 65,535. quinn batches up to 10, which puts the ceiling at 6,553
bytes — and a kernel that refuses an oversized batch returns `EMSGSIZE`, which
is not recognised as a GSO failure and stalls delivery rather than degrading it.
MTU discovery is therefore bounded below that ceiling by default.

Socket buffer sizes are read back from the socket rather than trusted, because
Linux silently clamps `SO_RCVBUF`/`SO_SNDBUF` to `net.core.rmem_max`/`wmem_max`.
A clamped host gets a warning naming those sysctls, and does not get the
loopback MTU pin, which needs the headroom to absorb bursts.

## Documentation

- [QUIC transport](https://docs.getfelix.dev/features/quic-transport/)
- [Environment variables](https://docs.getfelix.dev/reference/environment-variables/)

Apache-2.0.
