-- sql/schema.sql
-- Medallion architecture DDL for financial_pipeline.
-- Run once against the target database to initialise the DWH schema.

-- ── Bronze – raw ingest ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_transactions (
    tx_id        TEXT            PRIMARY KEY,
    user_id      INTEGER         NOT NULL,
    amount       NUMERIC(12, 2)  NOT NULL CHECK (amount > 0),
    currency     TEXT            NOT NULL DEFAULT 'PLN',
    merchant     TEXT,
    category     TEXT,
    tx_date      DATE            NOT NULL,
    ingested_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_tx_date   ON raw_transactions (tx_date);
CREATE INDEX IF NOT EXISTS idx_raw_user_date ON raw_transactions (user_id, tx_date);
CREATE INDEX IF NOT EXISTS idx_raw_category  ON raw_transactions (category, tx_date);

-- ── Silver – anomaly quarantine ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS anomalies (
    id           BIGSERIAL       PRIMARY KEY,
    tx_id        TEXT            NOT NULL REFERENCES raw_transactions (tx_id),
    user_id      INTEGER         NOT NULL,
    amount       NUMERIC(12, 2)  NOT NULL,
    reason       TEXT,                          -- z-score and category context
    detected_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_date ON anomalies (detected_at::date);

-- ── Gold – aggregated summary (BI-ready) ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS daily_summary (
    summary_date  DATE            NOT NULL,
    category      TEXT            NOT NULL,
    total_amount  NUMERIC(14, 2)  NOT NULL,
    tx_count      INTEGER         NOT NULL,
    avg_amount    NUMERIC(12, 2),
    max_amount    NUMERIC(12, 2),
    processed_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (summary_date, category)
);

-- ── Data-quality audit log ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dq_run_log (
    id            BIGSERIAL       PRIMARY KEY,
    run_date      DATE            NOT NULL UNIQUE,
    total_count   INTEGER         NOT NULL,
    reject_count  INTEGER         NOT NULL,
    reject_rate   NUMERIC(6, 4),
    max_amount    NUMERIC(12, 2),
    avg_amount    NUMERIC(12, 2),
    logged_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
