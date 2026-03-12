import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Handles all data cleaning operations:
    - Null / missing value treatment
    - Whitespace normalization
    - Data type casting
    - Outlier detection and removal
    - Column name standardization
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the full cleaning pipeline."""
        logger.info(f"Starting cleaning on DataFrame with shape {df.shape}")
        df = df.copy()
        df = self._standardize_column_names(df)
        df = self._strip_whitespace(df)
        df = self._handle_nulls(df)
        df = self._cast_types(df)
        df = self._remove_empty_rows(df)
        logger.info(f"Cleaning complete. Output shape: {df.shape}")
        return df

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase column names, replace spaces with underscores."""
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^\w]", "_", regex=True)
        )
        return df

    def _strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip leading/trailing whitespace from all string columns."""
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace common null-like strings with NaN,
        then fill or drop based on config.
        """
        null_values = self.config.get(
            "null_values", ["N/A", "n/a", "null", "NULL", "None", "none", "-", ""]
        )
        df = df.replace(null_values, np.nan)

        null_strategy = self.config.get("null_strategy", "report")
        fill_values: Dict[str, Any] = self.config.get("fill_values", {})

        if null_strategy == "drop_rows":
            before = len(df)
            df = df.dropna(how="any")
            logger.info(f"Dropped {before - len(df)} rows with nulls")
            df = df.reset_index(drop=True)

        elif null_strategy == "fill":
            for col, val in fill_values.items():
                if col in df.columns:
                    df[col] = df[col].fillna(val)
            # Fill remaining nulls with column mean (numeric) or "Unknown" (str)
            for col in df.columns:
                if df[col].isnull().any():
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(df[col].mean())
                    else:
                        df[col] = df[col].fillna("Unknown")

        else:  # "report" – just log
            null_summary = df.isnull().sum()
            null_cols = null_summary[null_summary > 0]
            if not null_cols.empty:
                logger.warning(f"Null values detected:\n{null_cols}")

        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to types specified in config."""
        type_map: Dict[str, str] = self.config.get("type_map", {})
        for col, dtype in type_map.items():
            if col in df.columns:
                try:
                    if dtype in ("int", "int64"):
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    elif dtype in ("float", "float64"):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif dtype == "datetime":
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif dtype == "bool":
                        df[col] = df[col].map(
                            {"True": True, "False": False, "1": True, "0": False}
                        )
                    else:
                        df[col] = df[col].astype(dtype, errors="ignore")
                    logger.debug(f"Cast column '{col}' to {dtype}")
                except Exception as e:
                    logger.warning(f"Could not cast column '{col}' to {dtype}: {e}")
        return df

    def _remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows where all non-metadata columns are null."""
        data_cols = [c for c in df.columns if not c.startswith("_")]
        before = len(df)
        df = df.dropna(subset=data_cols, how="all").reset_index(drop=True)
        removed = before - len(df)
        if removed:
            logger.info(f"Removed {removed} fully-empty rows")
        return df

    # ------------------------------------------------------------------ #
    # Reporting                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def generate_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a data quality summary report."""
        total = len(df)
        report = {
            "total_rows": total,
            "total_columns": len(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "null_percent": (df.isnull().sum() / total * 100).round(2).to_dict(),
            "duplicate_rows": int(df.duplicated().sum()),
            "data_types": df.dtypes.astype(str).to_dict(),
        }
        return report
