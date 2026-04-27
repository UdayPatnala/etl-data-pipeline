from __future__ import annotations

from sqlalchemy import create_engine
import pandas as pd


def load_to_postgres(df: pd.DataFrame, database_url: str, table_name: str) -> None:
    engine = create_engine(database_url)
    with engine.begin() as connection:
        df.to_sql(table_name, connection, if_exists="replace", index=False)
