-- The copies of each shard its leader has stopped shipping to, keyed by node:
-- the reason, the generation it was reported at, and when the store first saw
-- it. Placement keeps new copies off these nodes and replaces a copy that
-- stays halted. Empty is what every report held before this said.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS halted JSONB NOT NULL DEFAULT '{}'::jsonb;
