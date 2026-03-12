import pandas as pd
import sqlite3
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DBLoader:
    """
    Loads a transformed DataFrame into a SQLite database.
    Supports three write modes:
        'replace'  – drop and recreate the table
        'append'   – append rows to existing table
        'fail'     – raise error if table already exists
    Also exports to CSV / JSON.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)

    # ------------------------------------------------------------------ #
    # Database loading                                                     #
    # ------------------------------------------------------------------ #

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        chunksize: int = 1000,
    ) -> int:
        """
        Insert DataFrame into a SQLite table.
        Returns the number of rows inserted.
        """
        # Drop internal metadata columns before loading
        clean_df = df[[c for c in df.columns if not c.startswith("_")]].copy()

        logger.info(
            f"Loading {len(clean_df)} rows into table '{table_name}' "
            f"(mode={if_exists}, db={self.db_path})"
        )

        try:
            conn = sqlite3.connect(self.db_path)
            clean_df.to_sql(
                table_name,
                conn,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
            )
            conn.commit()
            conn.close()
            logger.info(f"Successfully loaded {len(clean_df)} rows into '{table_name}'")
            return len(clean_df)
        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

    # ------------------------------------------------------------------ #
    # File exports                                                         #
    # ------------------------------------------------------------------ #

    def export_csv(self, df: pd.DataFrame, file_path: str, index: bool = False) -> None:
        """Export DataFrame to CSV."""
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)
        df.to_csv(file_path, index=index, encoding="utf-8")
        logger.info(f"Exported {len(df)} rows to CSV: {file_path}")

    def export_json(self, df: pd.DataFrame, file_path: str, orient: str = "records") -> None:
        """Export DataFrame to JSON."""
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)
        df.to_json(file_path, orient=orient, force_ascii=False, indent=2)
        logger.info(f"Exported {len(df)} rows to JSON: {file_path}")

    # ------------------------------------------------------------------ #
    # Validation helpers                                                   #
    # ------------------------------------------------------------------ #

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        result = cursor.fetchone() is not None
        conn.close()
        return result

    def get_row_count(self, table_name: str) -> int:
        """Return the number of rows in a table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a full table back into a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
