- The C ABI test (`crates/sdk/felix-capi/tests/c/pubsub.c`) is now a conformance harness (#1493): it tags the
  `felix-conformance` catalogue scenarios it demonstrates, records a reason for each one it skips, and `run.sh fixture`
  merges the shared and static builds into a results document at `$FELIX_CONFORMANCE_RESULTS` that
  `felix-conformance verify` reads. CI reports the verdict without gating on it until the fault scenarios land.
