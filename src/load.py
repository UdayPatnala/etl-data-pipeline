from __future__ import annotations

import logging
from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

logger = logging.getLogger(__name__)


def load_to_postgres(df: pd.DataFrame, database_url: str, table_name: str) -> None:
    """
    Load data into PostgreSQL using an upsert (ON CONFLICT DO UPDATE).
    Assumes 'customer_id' is the primary key.
    """
    if df.empty:
        logger.warning("DataFrame is empty. Nothing to load.")
        return

    engine = create_engine(database_url)
    metadata = MetaData()

    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except Exception as e:
        logger.error("Failed to reflect table '%s': %s", table_name, e)
        raise

    records = df.to_dict(orient="records")

    with engine.begin() as connection:
        stmt = insert(table).values(records)

        # Update all columns except customer_id (PK) on conflict
        update_dict = {
            c.name: c
            for c in stmt.excluded
            if c.name != "customer_id"
        }
        
        # If 'updated_at' exists in table, set it to current timestamp on update
        if "updated_at" in table.columns:
            update_dict["updated_at"] = func.now()

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["customer_id"],
            set_=update_dict
        )

        connection.execute(upsert_stmt)
