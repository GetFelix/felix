- A failed `publish_if` or `commit` now says whether it may have written. The
  Rust client wraps any failure after the send, other than a coded refusal, in
  `AckLost`, and
  `felix_client::outcome_unknown` is true for that and for a broker error
  classed `outcome_unknown`, false for a refusal, validation error or
  `forbidden`. A multi-record `publish_if` on a `Quorum` stream is documented as
  not all or nothing: after an unknown outcome any prefix may exist, so re-read
  from the expected offset. Use `commit` for a batch that must be all or
  nothing. The Python, TypeScript and C bindings already map `AckLost` to their
  outcome-unknown error, so they inherit this. (#1630)
