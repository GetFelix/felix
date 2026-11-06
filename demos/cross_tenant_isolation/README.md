# Cross-Tenant Isolation Demo

This demo proves that Felix enforces tenant boundaries end-to-end. It boots a
real control plane backed by Postgres, a real broker, a fake ES256 OIDC IdP, and
uses real Felix token exchange. It then shows that a principal with RBAC in
`t1` cannot access resources in `t2`.

## Run

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

## Expected output (excerpt)

```
STEP 15 t1 publish allowed: PASS
STEP 18 t1 token on t2 publish denied: PASS
STEP 21 t2 token publish denied: PASS

Summary
- STEP 15 t1 publish allowed: PASS
- STEP 18 t1 token on t2 publish denied: PASS
- STEP 21 t2 token publish denied: PASS
```
