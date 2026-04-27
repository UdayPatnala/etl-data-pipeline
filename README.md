# ETL Data Pipeline

A production-style starter for extracting data, transforming it with Pandas, and loading into PostgreSQL.

## Tech Stack

- Python
- Pandas
- SQLAlchemy
- PostgreSQL

## Flow

1. Extract from CSV or API
2. Transform and validate data
3. Load to PostgreSQL table

## Run

```bash
cp .env.example .env
pip install -r requirements.txt
python src/pipeline.py --source csv --input data/raw/customers.csv
```

## Notes

- For API mode, pass `--source api --input <api_url>`.
- SQL DDL is available in `sql/schema.sql`.
