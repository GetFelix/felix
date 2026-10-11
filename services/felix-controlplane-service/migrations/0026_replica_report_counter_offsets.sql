-- How far each replica of a cache shard had got with its counter log. Placement
-- reads a move here as progress for a copy being restored, since a cache used
-- only for counters never moves its own log. Empty is what every report held
-- before this said.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS counter_offsets JSONB NOT NULL DEFAULT '{}'::jsonb;
