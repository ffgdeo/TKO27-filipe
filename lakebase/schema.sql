-- Lakebase Schema: Loyalty Engine State Store
-- Provides sub-second reads for the customer portal app

CREATE TABLE IF NOT EXISTS personalized_offers (
    offer_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    customer_id     TEXT NOT NULL,
    offer_code      TEXT NOT NULL,
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    relevance_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    offer_type      TEXT NOT NULL DEFAULT 'recommendation',
    discount_pct    DOUBLE PRECISION DEFAULT 0.0,
    expires_at      TIMESTAMP WITH TIME ZONE,
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_offers_customer ON personalized_offers (customer_id);
CREATE INDEX IF NOT EXISTS idx_offers_status ON personalized_offers (status, expires_at);

CREATE TABLE IF NOT EXISTS active_sessions (
    session_id              TEXT PRIMARY KEY,
    customer_id             TEXT NOT NULL,
    current_category        TEXT,
    top_interest_score      DOUBLE PRECISION DEFAULT 0.0,
    products_viewed         INTEGER DEFAULT 0,
    cart_items              INTEGER DEFAULT 0,
    device_type             TEXT,
    started_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_activity           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_customer ON active_sessions (customer_id);

CREATE TABLE IF NOT EXISTS loyalty_status (
    customer_id             TEXT PRIMARY KEY,
    first_name              TEXT,
    last_name               TEXT,
    loyalty_tier            TEXT NOT NULL DEFAULT 'Bronze',
    points_balance          INTEGER NOT NULL DEFAULT 0,
    lifetime_value          DOUBLE PRECISION DEFAULT 0.0,
    total_orders            INTEGER DEFAULT 0,
    next_tier_threshold     INTEGER DEFAULT 0,
    tier_progress_pct       DOUBLE PRECISION DEFAULT 0.0,
    segment                 TEXT,
    churn_risk_level        TEXT DEFAULT 'Low',
    last_purchase_date      DATE,
    preferred_categories    TEXT,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
