-- The narrowing an exchange applied (requested actions, resource hints,
-- audience), re-applied on every refresh so a refresh cannot widen it.
--
-- Nullable with no default: a row written before this column existed recorded
-- no narrowing, and refreshes to full RBAC rights as it always did.
ALTER TABLE refresh_tokens
    ADD COLUMN IF NOT EXISTS narrowing JSONB;
