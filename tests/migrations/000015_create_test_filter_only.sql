-- Test table for an entity with filters but no list (counted, never paged).
-- `label` is nullable so NE / NIN / IS_NULL meet SQL three-valued logic.

CREATE TABLE IF NOT EXISTS relais_test_filter_only (
    id       BIGSERIAL PRIMARY KEY,
    category TEXT    NOT NULL DEFAULT '',
    owner_id BIGINT  NOT NULL DEFAULT 0,
    score    INTEGER NOT NULL DEFAULT 0,
    label    TEXT
);
