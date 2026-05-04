import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from extract import extract_from_csv
from transform import transform_customer_data

def run_demo():
    print("=" * 60)
    print(" ETL Data Pipeline Demo (Dry Run)")
    print("=" * 60)

    # 1. Extraction
    print("\n[1] Extracting data from CSV...")
    csv_path = "data/raw/customers.csv"
    try:
        raw_df = extract_from_csv(csv_path)
        print(f"[OK] Extracted {len(raw_df)} records.")
        print("\nRaw Data Sample:")
        print(raw_df.head(3))
    except Exception as e:
        print(f"[FAIL] Extraction failed: {e}")
        return

    # 2. Transformation
    print("\n" + "=" * 60)
    print("\n[2] Transforming data...")
    try:
        transformed_df = transform_customer_data(raw_df)
        dropped = len(raw_df) - len(transformed_df)
        print(f"[OK] Transformed {len(transformed_df)} records (dropped {dropped} invalid).")
        print("\nTransformed Data Sample (showing new features):")
        print(transformed_df[["customer_id", "last_login_days", "support_tickets", "activity_band", "churn_risk_score"]].head(5))
    except Exception as e:
        print(f"[FAIL] Transformation failed: {e}")
        return

    # 3. Loading
    print("\n" + "=" * 60)
    print("\n[3] Loading data...")
    print("[INFO] Skipping actual PostgreSQL load for demo purposes.")
    print("[INFO] In production, this would perform an upsert (ON CONFLICT DO UPDATE) to the database.")
    print("\n[OK] Pipeline demo completed successfully!")

if __name__ == "__main__":
    run_demo()

