-- Differential tests of the prudent guard evaluator: one column per kind it
-- tells apart, including text under a nondeterministic collation and char(n),
-- whose equality differs from C++'s.
CREATE COLLATION IF NOT EXISTS relais_test_ci
    (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

CREATE TABLE IF NOT EXISTS relais_test_guard_rows (
    id    BIGSERIAL PRIMARY KEY,
    num   INTEGER          NOT NULL DEFAULT 0,
    big   BIGINT,
    flag  BOOLEAN          NOT NULL DEFAULT false,
    ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    name  TEXT             NOT NULL DEFAULT '',
    label VARCHAR(20)      NOT NULL DEFAULT '',
    code  CHAR(4)          NOT NULL DEFAULT '',
    tag   TEXT COLLATE relais_test_ci NOT NULL DEFAULT '',
    tone  TEXT             NOT NULL DEFAULT 'low',
    at    TIMESTAMPTZ
);
