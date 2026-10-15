-- Principal revocations: one row per principal, at its latest revocation.
--
-- Brokers refuse a principal's tokens issued at or before `revoked_at` and
-- end the streams they opened. They read this table as a feed ordered by
-- `seq`, which is rewritten each time the principal is revoked again.
-- `principal_id` '*' stands for every principal of the tenant.
CREATE TABLE IF NOT EXISTS principal_revocations (
    tenant_id    TEXT   NOT NULL,
    principal_id TEXT   NOT NULL,
    revoked_at   BIGINT NOT NULL,
    seq          BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, principal_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_principal_revocations_seq
    ON principal_revocations (seq);
