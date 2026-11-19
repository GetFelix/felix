- Each release now signs the server `SHA256SUMS` with Sigstore and publishes
  `SHA256SUMS.sigstore.json` next to it, so the archive can be verified with
  `cosign verify-blob` (#2796).
