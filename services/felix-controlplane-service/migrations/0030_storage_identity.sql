-- The storage identity each node registered with (32 lowercase hex), and the
-- storage identity the leader saw for each follower when it reported (a JSON
-- object of node id to identity). Placement counts a follower from a report
-- only while the two match, and never once they differ. NULL is what every
-- row held before this said, and is read as unknown.
--
-- Each has a marker, as by_round_at is for by_round (0029). A member that
-- predates these columns writes the row without them, so what it leaves
-- behind belongs to an earlier write. storage_id is read only while
-- storage_id_incarnation matches the row's incarnation, which an older
-- member's registration bumps; follower_storage only while
-- follower_storage_at matches reported_at_millis.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS storage_id TEXT,
    ADD COLUMN IF NOT EXISTS storage_id_incarnation BIGINT;
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS follower_storage JSONB,
    ADD COLUMN IF NOT EXISTS follower_storage_at BIGINT;
