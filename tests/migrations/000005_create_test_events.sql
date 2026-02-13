-- Test table for smartrepo PartialKey integration tests
-- Partitioned by region with composite PK (id, region)
-- Uses a shared sequence so 'id' is globally unique across partitions

CREATE SEQUENCE IF NOT EXISTS smartrepo_test_events_id_seq;

CREATE TABLE IF NOT EXISTS smartrepo_test_events (
    id BIGINT NOT NULL DEFAULT nextval('smartrepo_test_events_id_seq'),
    region VARCHAR(20) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES smartrepo_test_users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE IF NOT EXISTS smartrepo_test_events_eu
    PARTITION OF smartrepo_test_events FOR VALUES IN ('eu');
CREATE TABLE IF NOT EXISTS smartrepo_test_events_us
    PARTITION OF smartrepo_test_events FOR VALUES IN ('us');

CREATE INDEX IF NOT EXISTS idx_smartrepo_test_events_user_id
    ON smartrepo_test_events(user_id);
CREATE INDEX IF NOT EXISTS idx_smartrepo_test_events_id
    ON smartrepo_test_events(id);
