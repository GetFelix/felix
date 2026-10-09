# felix-storage

The log-structured segment store behind [Felix](https://github.com/GetFelix/felix)
durable streams: segments, sparse indexes, torn-tail repair, group commit, and
the cache and counter stores built on the log.

Records are never rewritten, so recovery can trust that valid bytes end at EOF.
A torn tail is repaired; interior corruption is fatal, because refusing to start
beats silently losing acknowledged records. Indexes are derived and never
trusted — a missing, short or stale one is rebuilt from the segment it
describes.

Built into the broker service, and published so that `felixctl inspect
segments` can read a data directory offline with the same recovery plan the
broker runs at startup (`felix_storage::inspect`). AGPL-3.0-only. See
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md).
