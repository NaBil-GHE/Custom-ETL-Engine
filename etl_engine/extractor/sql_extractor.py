import pandas as pd
import sqlite3
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SQLExtractor:
    """
    Extractor for SQL databases.
    Supports SQLite (local) and can be extended to PostgreSQL/MySQL via SQLAlchemy.
    """

    def __init__(self, db_path: str):
        """
        db_path : path to SQLite database file
                  (for other DBs, pass a SQLAlchemy connection string)
        """
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """Open a connection to the database."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed.")

    def extract_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame."""
        if not self.connection:
            self.connect()

        logger.info(f"Executing query: {query[:100]}...")
        try:
            df = pd.read_sql_query(query, self.connection, params=params)
            df["_source_type"] = "SQL"
            df["_source_db"] = self.db_path
            logger.info(f"Extracted {len(df)} rows from SQL query")
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def extract_table(self, table_name: str, where_clause: Optional[str] = None) -> pd.DataFrame:
        """Extract an entire table, optionally filtered."""
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        return self.extract_query(query)

    def list_tables(self) -> list:
        """List all tables in the database."""
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Available tables: {tables}")
        return tables

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
