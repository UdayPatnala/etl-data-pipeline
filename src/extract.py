from __future__ import annotations

import logging
import pandas as pd
import requests

logger = logging.getLogger(__name__)


def extract_from_csv(path: str) -> pd.DataFrame:
    """Extract data from a local CSV file."""
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        logger.error("CSV file not found: %s", path)
        raise
    except pd.errors.EmptyDataError:
        logger.error("CSV file is empty: %s", path)
        raise


def extract_from_api(url: str, timeout: int = 15) -> pd.DataFrame:
    """Extract data from a REST API."""
    try:
        # Use Session for connection pooling (best practice for APIs)
        with requests.Session() as session:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.json()

        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
            return pd.DataFrame(payload["data"])

        raise ValueError("Unsupported API payload format. Expected a list or {\"data\": [...]}.")
    except requests.RequestException as e:
        logger.error("API request failed: %s", e)
        raise
