# ETL Data Pipeline

A production-style ETL pipeline that extracts data from CSV files or REST APIs, transforms it with Pandas (validation, cleaning, feature engineering), and loads into PostgreSQL.

## Tech Stack

- **Python 3.10+**
- **Pandas** — data transformation & validation
- **SQLAlchemy** — database ORM / connection
- **PostgreSQL** — target data store
- **python-dotenv** — environment config

## Pipeline Flow

```
  CSV File / REST API
         │
         ▼
  ┌──────────────┐
  │   Extract     │    extract.py — read CSV or fetch JSON from API
  └──────┬───────┘
         │  raw DataFrame
         ▼
  ┌──────────────┐
  │  Transform    │    transform.py — validate, clean, derive features
  └──────┬───────┘
         │  activity_band, churn_risk_score added
         ▼
  ┌──────────────┐
  │    Load       │    load.py — upsert into PostgreSQL via SQLAlchemy
  └──────────────┘
```

## Setup

```bash
cp .env.example .env        # configure DATABASE_URL
pip install -r requirements.txt
```

## Run

```bash
# CSV mode
python src/pipeline.py --source csv --input data/raw/customers.csv

# API mode
python src/pipeline.py --source api --input https://example.com/api/customers
```

## Schema

The target table is defined in `sql/schema.sql`:

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | TEXT (PK) | Unique customer identifier |
| `signup_date` | DATE | Registration date |
| `country` | TEXT | Country of origin |
| `plan` | TEXT | Basic / Pro / Enterprise |
| `monthly_spend` | NUMERIC | Monthly subscription cost |
| `last_login_days` | INTEGER | Days since last login |
| `support_tickets` | INTEGER | Number of support tickets |
| `activity_band` | TEXT | Derived: high / medium / low |
| `churn_risk_score` | NUMERIC | Derived: weighted risk metric |

## Transformation Rules

- **Type coercion**: dates parsed, numerics validated
- **Null handling**: rows with missing required fields are dropped
- **Activity band**: `≤7d → high`, `≤30d → medium`, `>30d → low`
- **Churn risk score**: `last_login_days × 0.7 + support_tickets × 4`

## Project Structure

```
├── data/
│   └── raw/
│       └── customers.csv       # Sample input (30 records)
├── src/
│   ├── pipeline.py             # CLI entrypoint — orchestrates ETL
│   ├── extract.py              # CSV and API extractors
│   ├── transform.py            # Validation + feature engineering
│   └── load.py                 # PostgreSQL loader
├── sql/
│   └── schema.sql              # DDL for target table
├── .env.example
├── requirements.txt
└── README.md
```

## License

MIT
