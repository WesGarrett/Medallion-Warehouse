-- =============================================================================
-- SILVER LAYER  ── typed, deduplicated, cleaned
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ---------------------------------------------------------------------------
-- silver.web_events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.web_events (
    session_id   TEXT         PRIMARY KEY,
    user_id      TEXT         NOT NULL,
    event_type   TEXT         NOT NULL,
    page_url     TEXT,
    referrer     TEXT,
    device_type  TEXT,
    event_ts     TIMESTAMPTZ,
    country      TEXT,
    loaded_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- silver.crm_users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.crm_users (
    user_id      TEXT         PRIMARY KEY,
    first_name   TEXT,
    last_name    TEXT,
    email        TEXT         UNIQUE NOT NULL,
    phone        TEXT,
    state        CHAR(2),
    city         TEXT,
    signup_date  DATE,
    plan_tier    TEXT,
    loaded_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- silver.sales_transactions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.sales_transactions (
    transaction_id   TEXT           PRIMARY KEY,
    user_id          TEXT,
    product_id       TEXT,
    product_name     TEXT,
    category         TEXT,
    quantity         INTEGER,
    unit_price       NUMERIC(14,4),
    total_amount     NUMERIC(14,4),
    is_refund        BOOLEAN        NOT NULL DEFAULT FALSE,
    region           TEXT,
    transaction_date DATE,
    loaded_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
