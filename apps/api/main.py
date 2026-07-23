import os
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.extract import extract_from_csv
from src.transform import transform_customer_data
from src.load import load_data

load_dotenv()

app = FastAPI(title="ETL Data Pipeline API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./etl.db")
engine = create_engine(DATABASE_URL)

def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customer_metrics (
                customer_id TEXT PRIMARY KEY,
                signup_date DATE,
                country TEXT,
                plan TEXT,
                monthly_spend NUMERIC,
                last_login_days INTEGER,
                support_tickets INTEGER,
                activity_band TEXT,
                churn_risk_score NUMERIC,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

init_db()

def run_pipeline():
    try:
        extracted = extract_from_csv("../../data/raw/customers.csv")
        transformed = transform_customer_data(extracted)
        load_data(transformed, DATABASE_URL, "customer_metrics")
    except Exception as e:
        print(f"Pipeline failed: {e}")

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.post("/api/jobs/run")
def trigger_job(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_pipeline)
    return {"message": "Job started in background"}

@app.get("/api/customers")
def get_customers():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM customer_metrics LIMIT 100"))
            return {"data": [dict(row._mapping) for row in result]}
    except Exception as e:
        return {"data": []}
