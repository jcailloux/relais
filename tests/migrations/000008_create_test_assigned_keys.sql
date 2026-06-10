-- Test table for relais simple-but-assigned primary key.
-- The PK is set by the caller (no DEFAULT, not db_managed), unlike the
-- BIGSERIAL-style db_managed keys used by the other single-key fixtures.
-- Regression coverage for the UPDATE param off-by-one on non-db_managed PKs.

CREATE TABLE IF NOT EXISTS relais_test_assigned_keys (
    key_id  BIGINT NOT NULL,
    payload BIGINT NOT NULL DEFAULT 0,
    note    TEXT   NOT NULL DEFAULT '',
    PRIMARY KEY (key_id)
);