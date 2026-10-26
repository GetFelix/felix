-- For a follower its leader had not reached yet, the storage identity (32
-- lowercase hex) the assignment that promoted the leader named for it, as a
-- JSON object of node id to identity. A failover from the report names the
-- follower by it instead of by none (#1574). NULL is what every row held
-- before this said, and is read as none.
--
-- The marker is as follower_storage_at is for follower_storage (0030). A
-- member that predates the column updates the row and leaves the map behind,
-- so it is read only while the marker matches reported_at_millis.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS inherited_storage JSONB,
    ADD COLUMN IF NOT EXISTS inherited_storage_at BIGINT;
