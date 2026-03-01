-- =============================================================================
-- GOLD LAYER  ── star schema, fully indexed, query-optimised
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- gold.dim_date  (date spine 2019-01-01 → 2026-12-31, populated by modeler)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INTEGER      PRIMARY KEY,   -- YYYYMMDD
    full_date    DATE         NOT NULL UNIQUE,
    day          SMALLINT     NOT NULL,
    month        SMALLINT     NOT NULL,
    quarter      SMALLINT     NOT NULL,
    year         SMALLINT     NOT NULL,
    day_of_week  SMALLINT     NOT NULL,      -- 0=Sunday … 6=Saturday
    is_weekend   BOOLEAN      NOT NULL
);

-- ---------------------------------------------------------------------------
-- gold.dim_users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_users (
    user_sk      SERIAL       PRIMARY KEY,
    user_id      TEXT         UNIQUE NOT NULL,
    full_name    TEXT,
    region       TEXT,
    plan_tier    TEXT,
    signup_date  DATE,
    loaded_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- gold.dim_products
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_sk     SERIAL       PRIMARY KEY,
    product_id     TEXT         UNIQUE NOT NULL,
    product_name   TEXT,
    category       TEXT,
    avg_unit_price NUMERIC(14,4),
    loaded_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- gold.fact_sales
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_id          BIGSERIAL    PRIMARY KEY,
    transaction_id   TEXT         UNIQUE NOT NULL,
    user_sk          INTEGER      REFERENCES gold.dim_users(user_sk),
    product_sk       INTEGER      NOT NULL REFERENCES gold.dim_products(product_sk),
    date_key         INTEGER      REFERENCES gold.dim_date(date_key),
    quantity         INTEGER,
    unit_price       NUMERIC(14,4),
    total_amount     NUMERIC(14,4),
    region           TEXT,
    loaded_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Indexes for fast analytical joins
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_sales_user_sk    ON gold.fact_sales(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_sk ON gold.fact_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key   ON gold.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_region     ON gold.fact_sales(region);
