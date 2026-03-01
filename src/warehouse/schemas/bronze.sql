-- =============================================================================
-- BRONZE LAYER  ── raw ingest, all columns TEXT, zero transformations
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- ---------------------------------------------------------------------------
-- bronze.web_events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.web_events (
    raw_id       BIGSERIAL    PRIMARY KEY,
    user_id      TEXT,
    session_id   TEXT,
    event_type   TEXT,
    page_url     TEXT,
    referrer     TEXT,
    device_type  TEXT,
    timestamp    TEXT,
    country      TEXT,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- bronze.crm_users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.crm_users (
    raw_id       BIGSERIAL    PRIMARY KEY,
    user_id      TEXT,
    first_name   TEXT,
    last_name    TEXT,
    email        TEXT,
    phone        TEXT,
    state        TEXT,
    city         TEXT,
    signup_date  TEXT,
    plan_tier    TEXT,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- bronze.sales_transactions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.sales_transactions (
    raw_id           BIGSERIAL    PRIMARY KEY,
    transaction_id   TEXT,
    user_id          TEXT,
    product_id       TEXT,
    product_name     TEXT,
    category         TEXT,
    quantity         TEXT,
    unit_price       TEXT,
    total_amount     TEXT,
    region           TEXT,
    transaction_date TEXT,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
