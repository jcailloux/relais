-- Test table for relais list caching tests
-- Contains fields for filtering, sorting, and pagination scenarios

CREATE TABLE IF NOT EXISTS relais_test_articles (
    id BIGSERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    author_id BIGINT NOT NULL,
    title VARCHAR(200) NOT NULL,
    view_count INTEGER NOT NULL DEFAULT 0,
    is_published BOOLEAN NOT NULL DEFAULT false,
    published_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_relais_test_articles_category ON relais_test_articles(category);
CREATE INDEX IF NOT EXISTS idx_relais_test_articles_author_id ON relais_test_articles(author_id);
CREATE INDEX IF NOT EXISTS idx_relais_test_articles_created_at ON relais_test_articles(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_relais_test_articles_published
    ON relais_test_articles(published_at DESC) WHERE is_published = true;
