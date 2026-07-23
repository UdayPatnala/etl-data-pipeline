from __future__ import annotations

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def _activity_band(days_since_login: int) -> str:
    """Determine activity band based on days since last login."""
    if days_since_login <= 7:
        return "high"
    if days_since_login <= 30:
        return "medium"
    return "low"


def transform_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate, clean, and add features to the raw customer dataframe.
    """
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return df

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

    # Coerce data types
    transformed["signup_date"] = pd.to_datetime(transformed["signup_date"], errors="coerce")
    transformed["monthly_spend"] = pd.to_numeric(transformed["monthly_spend"], errors="coerce")
    transformed["last_login_days"] = pd.to_numeric(transformed["last_login_days"], errors="coerce")
    transformed["support_tickets"] = pd.to_numeric(transformed["support_tickets"], errors="coerce")

    # Drop rows with missing values in required columns
    transformed = transformed.dropna(subset=list(required))

    # Convert numeric fields to integers where appropriate
    transformed["last_login_days"] = transformed["last_login_days"].astype(int)
    transformed["support_tickets"] = transformed["support_tickets"].astype(int)

    # Derived features
    transformed["activity_band"] = transformed["last_login_days"].apply(_activity_band)
    
    # Calculate churn risk score (weighted logic)
    transformed["churn_risk_score"] = (
        transformed["last_login_days"] * 0.7 + transformed["support_tickets"] * 4
    ).round(2)

    return transformed

