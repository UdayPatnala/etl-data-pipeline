from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from extract import extract_from_api, extract_from_csv
from load import load_to_postgres
from transform import transform_customer_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--source", choices=["csv", "api"], required=True)
    parser.add_argument("--input", required=True, help="CSV file path or API URL")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    if args.source == "csv":
        extracted = extract_from_csv(args.input)
    else:
        extracted = extract_from_api(args.input)

    transformed = transform_customer_data(extracted)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL missing. Add it in .env")

    table_name = os.getenv("TARGET_TABLE", "customer_metrics")
    load_to_postgres(transformed, database_url, table_name)

    print(f"Loaded {len(transformed)} records into table '{table_name}'.")


if __name__ == "__main__":
    main()
