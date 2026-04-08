"""
Data Warehouse Loader - Star Schema Implementation
Loads data into Fact and Dimension tables with referential integrity
"""

import sqlite3
import pandas as pd
from typing import Dict, Optional
from ..utils import setup_logger

logger = setup_logger("dwh_loader")


class DWHLoader:
    """
    Loads data into Star Schema Data Warehouse
    Handles dimension loading with surrogate key management
    and fact table loading with foreign key resolution
    """
    
    def __init__(self, db_path: str):
        """
        Initialize DWH Loader
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path)
        logger.info(f"✓ Connected to Data Warehouse: {self.db_path}")
        
    def create_schema(self, schema_file: str):
        """
        Execute DDL from schema file to create tables
        
        Args:
            schema_file: Path to SQL file with CREATE TABLE statements
        """
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema creation
            self.conn.executescript(schema_sql)
            self.conn.commit()
            logger.info(f"✓ Star Schema created from: {schema_file}")
            
            # Log created tables
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"  Tables: {', '.join(tables)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            raise
    
    def load_dimension_date(self, df: pd.DataFrame) -> int:
        """
        Load date dimension (special case - no surrogate key needed)
        
        Args:
            df: Date dimension DataFrame
            
        Returns:
            Number of rows loaded
        """
        try:
            # Date dimension uses date_key as natural key
            # Use REPLACE to handle re-runs (idempotent)
            df.to_sql('dim_date', self.conn, if_exists='replace', index=False)
            rows_loaded = len(df)
            logger.info(f"✓ Loaded {rows_loaded:,} rows into dim_date")
            return rows_loaded
            
        except Exception as e:
            logger.error(f"❌ Failed to load dim_date: {e}")
            raise
    
    def load_dimension(self, table_name: str, df: pd.DataFrame, 
                       business_key: str) -> Dict[str, int]:
        """
        Load dimension table with Type 1 SCD (overwrite on change)
        Returns mapping of business keys to surrogate keys
        
        Args:
            table_name: Dimension table name (e.g., "dim_customer")
            df: DataFrame with dimension attributes
            business_key: Column name for natural key (e.g., "customer_id")
            
        Returns:
            Dict mapping business_key value → surrogate_key
        """
        try:
            # Remove duplicates based on business key
            df_unique = df.drop_duplicates(subset=[business_key]).copy()
            initial_count = len(df)
            unique_count = len(df_unique)
            
            if initial_count != unique_count:
                logger.warning(f"⚠ {table_name}: Removed {initial_count - unique_count} duplicates")
            
            # Get surrogate key column name
            surrogate_key_col = table_name.replace('dim_', '') + '_key'
            
            # Get existing dimension data (for incremental loads)
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT {surrogate_key_col}, {business_key} FROM {table_name}")
            existing = {row[1]: row[0] for row in cursor.fetchall()}
            
            key_map = {}
            new_records = 0
            updated_records = 0
            
            for _, row in df_unique.iterrows():
                bkey_value = row[business_key]
                
                if bkey_value in existing:
                    # Existing record - update (SCD Type 1)
                    surrogate_key = existing[bkey_value]
                    key_map[bkey_value] = surrogate_key
                    
                    # Build UPDATE statement
                    set_clause = ', '.join([f"{col} = ?" for col in row.index if col != business_key])
                    values = [row[col] for col in row.index if col != business_key]
                    values.append(bkey_value)
                    
                    cursor.execute(
                        f"UPDATE {table_name} SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE {business_key} = ?",
                        values
                    )
                    updated_records += 1
                    
                else:
                    # New record - insert
                    cols = ', '.join(row.index)
                    placeholders = ', '.join(['?'] * len(row))
                    cursor.execute(
                        f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                        tuple(row.values)
                    )
                    key_map[bkey_value] = cursor.lastrowid
                    new_records += 1
            
            self.conn.commit()
            
            logger.info(f"✓ {table_name}: Loaded {new_records} new, updated {updated_records} existing rows")
            logger.info(f"  Total unique keys: {len(key_map)}")
            
            return key_map
            
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            raise
    
    def load_fact(self, df: pd.DataFrame, dimension_maps: Dict[str, Dict],
                  order_id_col: str = 'order_id') -> int:
        """
        Load fact table with dimension key lookups
        
        Args:
            df: DataFrame with transaction facts
            dimension_maps: Dict of {dimension_name: {business_key: surrogate_key}}
                           e.g., {'customer': {'C0001': 1, 'C0002': 2}, ...}
            order_id_col: Column name for order identifier (degenerate dimension)
            
        Returns:
            Number of fact rows loaded
        """
        try:
            df_fact = df.copy()
            
            # Map customer business key → surrogate key
            if 'customer' not in dimension_maps:
                raise ValueError("Missing 'customer' in dimension_maps")
            
            df_fact['customer_key'] = df_fact['customer_id'].map(dimension_maps['customer'])
            unmapped_customers = df_fact['customer_key'].isna().sum()
            if unmapped_customers > 0:
                logger.warning(f"⚠ {unmapped_customers} rows with unmapped customer_id")
                df_fact = df_fact[df_fact['customer_key'].notna()]
            
            # Map product business key → surrogate key
            if 'product' not in dimension_maps:
                raise ValueError("Missing 'product' in dimension_maps")
            
            df_fact['product_key'] = df_fact['product_id'].map(dimension_maps['product'])
            unmapped_products = df_fact['product_key'].isna().sum()
            if unmapped_products > 0:
                logger.warning(f"⚠ {unmapped_products} rows with unmapped product_id")
                df_fact = df_fact[df_fact['product_key'].notna()]
            
            # Generate date_key from order_date (YYYYMMDD format)
            df_fact['date_key'] = pd.to_datetime(df_fact['order_date']).dt.strftime('%Y%m%d').astype(int)
            
            # Select only fact table columns (remove dimension attributes)
            fact_cols = [
                'date_key', 'customer_key', 'product_key',
                order_id_col, 'quantity', 'unit_price', 'discount', 'total_amount'
            ]
            
            # Ensure all required columns exist
            missing_cols = [col for col in fact_cols if col not in df_fact.columns]
            if missing_cols:
                raise ValueError(f"Missing required fact columns: {missing_cols}")
            
            df_fact_final = df_fact[fact_cols].copy()
            
            # Convert data types
            df_fact_final['customer_key'] = df_fact_final['customer_key'].astype(int)
            df_fact_final['product_key'] = df_fact_final['product_key'].astype(int)
            
            # Load to database (append mode for facts)
            df_fact_final.to_sql('fact_sales', self.conn, if_exists='append', index=False)
            rows_loaded = len(df_fact_final)
            
            logger.info(f"✓ Loaded {rows_loaded:,} rows into fact_sales")
            
            # Log summary statistics
            total_revenue = df_fact_final['total_amount'].sum()
            avg_order = df_fact_final['total_amount'].mean()
            logger.info(f"  Total Revenue: ${total_revenue:,.2f}")
            logger.info(f"  Average Order: ${avg_order:,.2f}")
            
            return rows_loaded
            
        except Exception as e:
            logger.error(f"❌ Failed to load fact_sales: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> dict:
        """
        Get information about a table
        
        Args:
            table_name: Name of table
            
        Returns:
            Dict with table statistics
        """
        try:
            cursor = self.conn.cursor()
            
            # Row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [{'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
            
            return {
                'table': table_name,
                'row_count': row_count,
                'columns': columns
            }
            
        except Exception as e:
            logger.warning(f"Could not get info for {table_name}: {e}")
            return {'table': table_name, 'error': str(e)}
    
    def verify_referential_integrity(self) -> bool:
        """
        Verify that all foreign keys in fact table are valid
        
        Returns:
            True if all references are valid
        """
        try:
            cursor = self.conn.cursor()
            
            # Check customer_key
            cursor.execute("""
                SELECT COUNT(*) FROM fact_sales f
                LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
                WHERE c.customer_key IS NULL
            """)
            orphan_customers = cursor.fetchone()[0]
            
            # Check product_key
            cursor.execute("""
                SELECT COUNT(*) FROM fact_sales f
                LEFT JOIN dim_product p ON f.product_key = p.product_key
                WHERE p.product_key IS NULL
            """)
            orphan_products = cursor.fetchone()[0]
            
            # Check date_key
            cursor.execute("""
                SELECT COUNT(*) FROM fact_sales f
                LEFT JOIN dim_date d ON f.date_key = d.date_key
                WHERE d.date_key IS NULL
            """)
            orphan_dates = cursor.fetchone()[0]
            
            if orphan_customers > 0 or orphan_products > 0 or orphan_dates > 0:
                logger.error(f"❌ Referential integrity violations:")
                logger.error(f"   Orphan customers: {orphan_customers}")
                logger.error(f"   Orphan products: {orphan_products}")
                logger.error(f"   Orphan dates: {orphan_dates}")
                return False
            
            logger.info("✓ Referential integrity verified - all foreign keys valid")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to verify integrity: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("✓ Data Warehouse connection closed")
