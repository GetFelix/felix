- The control plane can serve an API reference page, off by default (#1361).
  Set `FELIX_CONTROLPLANE_API_UI=scalar` to get it at `/api`, or move it with
  `FELIX_CONTROLPLANE_API_UI_PATH`. The page loads a pinned, integrity-checked
  Scalar script from a CDN and reads `/v1/openapi.json` from the same origin.
