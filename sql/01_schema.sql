-- Runs once, on first container start, via docker-entrypoint-initdb.d.

CREATE TABLE IF NOT EXISTS customers (
    id          BIGSERIAL PRIMARY KEY,
    full_name   TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    country     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT      NOT NULL REFERENCES customers (id),
    amount       NUMERIC(12, 2) NOT NULL,
    currency     CHAR(3)     NOT NULL DEFAULT 'USD',
    channel      TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transactions_customer_idx ON transactions (customer_id);

-- REPLICA IDENTITY decides what Postgres writes to the WAL for UPDATE and
-- DELETE. The default only logs the primary key, so Debezium's "before" image
-- comes through almost entirely null. FULL logs every column, which is what
-- makes before/after diffs usable downstream. It costs WAL volume — that is
-- the trade.
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE customers    REPLICA IDENTITY FULL;

-- Debezium can create its own publication, but doing it here keeps the set of
-- replicated tables explicit and reviewable rather than implicit in connector
-- config.
DROP PUBLICATION IF EXISTS cdc_publication;
CREATE PUBLICATION cdc_publication FOR TABLE customers, transactions;
