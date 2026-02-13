-- Test table for smartrepo integration tests
-- Contains various field types to test different scenarios

CREATE TABLE IF NOT EXISTS smartrepo_test_items (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_smartrepo_test_items_name ON smartrepo_test_items(name);
CREATE INDEX IF NOT EXISTS idx_smartrepo_test_items_is_active ON smartrepo_test_items(is_active);