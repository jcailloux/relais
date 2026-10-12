-- Test table for list filters on nullable columns: `rank` and `label` meet
-- SQL three-valued logic under every operator.

CREATE TABLE IF NOT EXISTS relais_test_nullable_list (
    id       BIGSERIAL PRIMARY KEY,
    owner_id BIGINT  NOT NULL DEFAULT 0,
    rank     INTEGER,
    label    TEXT
);
