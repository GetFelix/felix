- A commit can require a key to hold a version (#1051). `CommitOp::put_if` and
  `CommitOp::delete_if` take `StateCondition::Version(n)` or `Absent`; if any key
  differs the commit writes nothing and is refused with `publish_refused` and
  `version_mismatch`, naming the key and the version it holds. Of commits racing on
  one version exactly one lands, and writers of different keys do not refuse each
  other, unlike `commit_if`. Negotiated by `FEATURE_COMMIT_IF_VERSION` (extended bit
  `0x80`); a client does not send the field to a broker that lacks it.
