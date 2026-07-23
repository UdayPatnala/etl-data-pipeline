CREATE TABLE IF NOT EXISTS customer_metrics (
    customer_id TEXT PRIMARY KEY,
    signup_date DATE,
    country TEXT,
    plan TEXT,
    monthly_spend NUMERIC(10, 2),
    last_login_days INTEGER,
    support_tickets INTEGER,
    activity_band TEXT,
    churn_risk_score NUMERIC(5, 2),
    updated_at TIMESTAMP DEFAULT NOW()
);
