-- Whether the leader that made the report serves nothing on its lease.
-- Placement replaces a leader on its followers' word only when it does. False
-- is what every report held before this said, and the safe reading of it.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS by_round BOOLEAN NOT NULL DEFAULT FALSE;
