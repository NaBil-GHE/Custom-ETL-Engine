"""
Dimension Builders - Extract dimension data from transactional data
Implements dimensional modeling transformations for Star Schema
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple


def build_customer_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique customers from transaction data
    
    Args:
        df: Transaction DataFrame with customer columns
        
    Returns:
        DataFrame with unique customers and attributes
    """
    customer_cols = ['customer_id', 'customer_name', 'email', 'country', 'city']
    
    # Get unique customers (deduplicate by customer_id)
    dim_customer = df[customer_cols].drop_duplicates(subset=['customer_id']).copy()
    
    # Add geographic region (derived attribute)
    def get_region(country):
        regions = {
            'Algeria': 'North Africa',
            'Morocco': 'North Africa',
            'Tunisia': 'North Africa',
            'France': 'Europe',
            'UK': 'Europe',
            'Germany': 'Europe',
            'Spain': 'Europe',
            'USA': 'North America',
        }
        return regions.get(country, 'Other')
    
    dim_customer['region'] = dim_customer['country'].apply(get_region)
    
    # Add customer segment (business rule: Europe/USA = B2B, others = B2C)
    dim_customer['customer_segment'] = dim_customer['country'].apply(
        lambda x: 'B2B' if x in ['USA', 'UK', 'Germany', 'France'] else 'B2C'
    )
    
    # Add first purchase date (from transaction data)
    if 'order_date' in df.columns:
        first_purchase = df.groupby('customer_id')['order_date'].min().reset_index()
        first_purchase.columns = ['customer_id', 'first_purchase_date']
        dim_customer = dim_customer.merge(first_purchase, on='customer_id', how='left')
    
    return dim_customer.reset_index(drop=True)


def build_product_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique products from transaction data
    
    Args:
        df: Transaction DataFrame with product columns
        
    Returns:
        DataFrame with unique products and attributes
    """
    product_cols = ['product_id', 'product_name', 'category', 'subcategory', 'brand']
    
    # Get unique products
    dim_product = df[product_cols].drop_duplicates(subset=['product_id']).copy()
    
    # Calculate cost price (business rule: 60% of average unit price)
    if 'unit_price' in df.columns:
        avg_price = df.groupby('product_id')['unit_price'].mean().reset_index()
        avg_price.columns = ['product_id', 'avg_unit_price']
        dim_product = dim_product.merge(avg_price, on='product_id', how='left')
        dim_product['cost_price'] = (dim_product['avg_unit_price'] * 0.6).round(2)
        dim_product.drop('avg_unit_price', axis=1, inplace=True)
    
    # Add active flag (all products active by default)
    dim_product['is_active'] = 1
    
    return dim_product.reset_index(drop=True)


def build_date_dimension(start_date: str = None, end_date: str = None, 
                         df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Generate complete date dimension table
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format (optional if df provided)
        end_date: End date in 'YYYY-MM-DD' format (optional if df provided)
        df: DataFrame with 'order_date' column to derive date range
        
    Returns:
        DataFrame with date dimension attributes
    """
    # If DataFrame provided, derive date range from data
    if df is not None and 'order_date' in df.columns:
        dates_series = pd.to_datetime(df['order_date'])
        min_date = dates_series.min()
        max_date = dates_series.max()
        
        # Extend range to cover full years
        start_date = f"{min_date.year}-01-01"
        end_date = f"{max_date.year}-12-31"
    
    # Generate date range
    if start_date is None or end_date is None:
        raise ValueError("Must provide either start_date/end_date or DataFrame with order_date")
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Build dimension table
    dim_date = pd.DataFrame({
        'date_key': dates.strftime('%Y%m%d').astype(int),
        'full_date': dates.date,
        'day': dates.day,
        'month': dates.month,
        'year': dates.year,
        'quarter': dates.quarter,
        'day_of_week': dates.day_name(),
        'day_of_week_num': dates.dayofweek + 1,  # 1=Monday, 7=Sunday
        'month_name': dates.month_name(),
        'is_weekend': (dates.dayofweek >= 5).astype(int),
        'is_holiday': 0,  # Can be enhanced with holiday calendar
    })
    
    # Add fiscal calendar (example: fiscal year starts in July)
    # Fiscal year = calendar year if month >= 7, else calendar year - 1
    dim_date['fiscal_year'] = dim_date.apply(
        lambda row: row['year'] if row['month'] >= 7 else row['year'] - 1,
        axis=1
    )
    
    # Fiscal quarter calculation
    def get_fiscal_quarter(month):
        # Assuming fiscal year starts July (month 7)
        if month in [7, 8, 9]:
            return 1
        elif month in [10, 11, 12]:
            return 2
        elif month in [1, 2, 3]:
            return 3
        else:  # 4, 5, 6
            return 4
    
    dim_date['fiscal_quarter'] = dim_date['month'].apply(get_fiscal_quarter)
    
    return dim_date


def validate_dimension_uniqueness(df: pd.DataFrame, key_column: str, 
                                   dimension_name: str) -> Tuple[bool, str]:
    """
    Validate that dimension has unique keys
    
    Args:
        df: Dimension DataFrame
        key_column: Primary key column name
        dimension_name: Name for logging
        
    Returns:
        Tuple of (is_valid, message)
    """
    total_rows = len(df)
    unique_keys = df[key_column].nunique()
    
    if total_rows != unique_keys:
        duplicates = total_rows - unique_keys
        return False, f"{dimension_name}: {duplicates} duplicate keys found in {key_column}"
    
    return True, f"{dimension_name}: ✓ All {total_rows} keys are unique"


def get_dimension_stats(df: pd.DataFrame, dimension_name: str) -> dict:
    """
    Get statistics about a dimension table
    
    Args:
        df: Dimension DataFrame
        dimension_name: Name for reporting
        
    Returns:
        Dictionary with dimension statistics
    """
    return {
        'dimension': dimension_name,
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': df.columns.tolist(),
        'null_counts': df.isnull().sum().to_dict(),
        'memory_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
    }
