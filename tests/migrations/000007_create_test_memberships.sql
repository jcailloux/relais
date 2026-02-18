-- Test table for relais composite key integration tests
-- Simple composite PK (user_id, group_id) without partitioning

CREATE TABLE IF NOT EXISTS relais_test_memberships (
    user_id BIGINT NOT NULL,
    group_id BIGINT NOT NULL,
    role TEXT NOT NULL DEFAULT '',
    joined_at BIGINT NOT NULL DEFAULT extract(epoch from now())::bigint,
    PRIMARY KEY (user_id, group_id)
);
