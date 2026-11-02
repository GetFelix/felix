-- The generation a report's leader refused to open because it had promised it
-- to another node, and that node (#1730). Placement then names the leader
-- again at a generation above the refused one. NULL is what every row held
-- before this said, and is read as none.
--
-- refused_at is the marker, as given_up_at is for given_up (0033): the refusal
-- is read only while it matches reported_at_millis, so what a member that
-- predates the columns leaves behind is never read as a refusal.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS refused_generation BIGINT,
    ADD COLUMN IF NOT EXISTS refused_to TEXT,
    ADD COLUMN IF NOT EXISTS refused_at BIGINT;
