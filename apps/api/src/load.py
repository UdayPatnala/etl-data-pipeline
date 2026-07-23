from __future__ import annotations

import logging
from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.dialects.sqlite import insert
import pandas as pd

logger = logging.getLogger(__name__)


def load_data(df: pd.DataFrame, database_url: str, table_name: str) -> None:
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

        update_dict = {
            c.name: c
            for c in stmt.excluded
            if c.name != "customer_id"
        }
        
        if "updated_at" in table.columns:
            update_dict["updated_at"] = func.now()

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["customer_id"],
            set_=update_dict
        )

        connection.execute(upsert_stmt)
