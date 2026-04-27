from __future__ import annotations

import pandas as pd


def _activity_band(days_since_login: int) -> str:
    if days_since_login <= 7:
        return "high"
    if days_since_login <= 30:
        return "medium"
    return "low"


def transform_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "customer_id",
        "signup_date",
        "country",
        "plan",
        "monthly_spend",
        "last_login_days",
        "support_tickets",
    }

    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    transformed = df.copy()

    transformed["signup_date"] = pd.to_datetime(transformed["signup_date"], errors="coerce")
    transformed["monthly_spend"] = pd.to_numeric(transformed["monthly_spend"], errors="coerce")
    transformed["last_login_days"] = pd.to_numeric(transformed["last_login_days"], errors="coerce")
    transformed["support_tickets"] = pd.to_numeric(transformed["support_tickets"], errors="coerce")

    transformed = transformed.dropna(subset=[
        "customer_id",
        "signup_date",
        "country",
        "plan",
        "monthly_spend",
        "last_login_days",
        "support_tickets",
    ])

    transformed["last_login_days"] = transformed["last_login_days"].astype(int)
    transformed["support_tickets"] = transformed["support_tickets"].astype(int)

    transformed["activity_band"] = transformed["last_login_days"].apply(_activity_band)
    transformed["churn_risk_score"] = (
        transformed["last_login_days"] * 0.7 + transformed["support_tickets"] * 4
    ).round(2)

    return transformed
