-- The failure domain a broker reports within its region (an availability
-- zone, a rack). Placement spreads a shard's copies across zones.
--
-- Nullable with no default: a broker registered before this column existed
-- reports none, and a node without a zone is placed as it always was.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS zone TEXT;
