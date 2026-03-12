import pandas as pd
import hashlib
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class Deduplicator:
    """
    Handles de-duplication strategies:
    - Exact row deduplication
    - Key-based deduplication (keep first / keep last / keep none)
    - Fuzzy deduplication using row hash
    """

    def __init__(
        self,
        subset: Optional[List[str]] = None,
        keep: str = "first",
        use_hash: bool = False,
    ):
        """
        subset   : columns to consider for deduplication (None = all columns)
        keep     : 'first' | 'last' | False  (False = remove all duplicates)
        use_hash : create a row hash for quick fingerprinting
        """
        self.subset = subset
        self.keep = keep
        self.use_hash = use_hash

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run deduplication on the DataFrame."""
        df = df.copy()
        original_count = len(df)

        if self.use_hash:
            df = self._add_row_hash(df)

        # Resolve subset – ignore metadata columns if subset not specified
        cols = self.subset
        if cols is None:
            cols = [c for c in df.columns if not c.startswith("_")]

        # Check that all subset columns exist
        missing = [c for c in cols if c not in df.columns]
        if missing:
            logger.warning(f"Subset columns not found, skipping: {missing}")
            cols = [c for c in cols if c in df.columns]

        df = df.drop_duplicates(subset=cols, keep=self.keep).reset_index(drop=True)

        removed = original_count - len(df)
        logger.info(
            f"Deduplication: removed {removed} duplicates "
            f"({original_count} → {len(df)} rows)"
        )
        return df

    def _add_row_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a SHA-256 hash column based on all non-metadata columns."""
        data_cols = [c for c in df.columns if not c.startswith("_")]

        def _hash_row(row):
            row_str = "|".join(str(v) for v in row[data_cols].values)
            return hashlib.sha256(row_str.encode()).hexdigest()

        df["_row_hash"] = df.apply(_hash_row, axis=1)
        logger.debug("Row hash column '_row_hash' added")
        return df

    @staticmethod
    def get_duplicate_report(df: pd.DataFrame, subset: Optional[List[str]] = None) -> dict:
        """Return a summary of duplicate records before removing them."""
        cols = subset or [c for c in df.columns if not c.startswith("_")]
        dupes = df[df.duplicated(subset=cols, keep=False)]
        return {
            "total_rows": len(df),
            "duplicate_rows": len(dupes),
            "unique_rows": len(df) - len(dupes),
            "duplicate_percent": round(len(dupes) / len(df) * 100, 2) if len(df) else 0,
        }
