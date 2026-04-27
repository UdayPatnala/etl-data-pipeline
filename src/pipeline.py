from __future__ import annotations

import argparse
import logging
import os

from dotenv import load_dotenv

from extract import extract_from_api, extract_from_csv
from load import load_to_postgres
from transform import transform_customer_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--source", choices=["csv", "api"], required=True)
    parser.add_argument("--input", required=True, help="CSV file path or API URL")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    logger.info("Starting ETL pipeline — source=%s input=%s", args.source, args.input)

    if args.source == "csv":
        extracted = extract_from_csv(args.input)
    else:
        extracted = extract_from_api(args.input)

    logger.info("Extracted %d records", len(extracted))

    transformed = transform_customer_data(extracted)
    logger.info("Transformed %d records (dropped %d invalid)", len(transformed), len(extracted) - len(transformed))

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL missing. Add it in .env")

    table_name = os.getenv("TARGET_TABLE", "customer_metrics")
    load_to_postgres(transformed, database_url, table_name)

    logger.info("Loaded %d records into table '%s'", len(transformed), table_name)
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
