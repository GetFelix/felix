-- The fleet features a broker reported at its last registration.
--
-- An empty array, not NULL, for a broker registered before this column
-- existed: it reported none, and the supported set reads it that way.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS features JSONB NOT NULL DEFAULT '[]'::jsonb;

-- Fleet features an operator finalized. Rows are only ever added: an enabled
-- feature is never disabled.
CREATE TABLE IF NOT EXISTS fleet_features (
    feature TEXT PRIMARY KEY,
    enabled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
