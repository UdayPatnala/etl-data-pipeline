from __future__ import annotations

import pandas as pd
import requests


def extract_from_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def extract_from_api(url: str, timeout: int = 15) -> pd.DataFrame:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
        return pd.DataFrame(payload["data"])

    raise ValueError("Unsupported API payload format. Expected a list or {\"data\": [...]}.")
