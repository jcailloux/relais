-- Test table for relais partition key integration tests
-- Partitioned by region with composite PK (id, region)
-- Uses a shared sequence so 'id' is globally unique across partitions

CREATE SEQUENCE IF NOT EXISTS relais_test_events_id_seq;

CREATE TABLE IF NOT EXISTS relais_test_events (
    id BIGINT NOT NULL DEFAULT nextval('relais_test_events_id_seq'),
    region VARCHAR(20) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES relais_test_users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE IF NOT EXISTS relais_test_events_eu
    PARTITION OF relais_test_events FOR VALUES IN ('eu');
CREATE TABLE IF NOT EXISTS relais_test_events_us
    PARTITION OF relais_test_events FOR VALUES IN ('us');

CREATE INDEX IF NOT EXISTS idx_relais_test_events_user_id
    ON relais_test_events(user_id);
CREATE INDEX IF NOT EXISTS idx_relais_test_events_id
    ON relais_test_events(id);
