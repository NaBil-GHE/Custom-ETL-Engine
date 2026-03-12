import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)


class JSONExtractor:
    """
    Extractor for JSON files.
    Supports flat JSON arrays and nested structures with path flattening.
    """

    def __init__(self, record_path=None, meta=None):
        """
        record_path : str or list – path to the list of records (for nested JSON)
        meta        : list – additional metadata fields to extract
        """
        self.record_path = record_path
        self.meta = meta

    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from a single JSON file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        logger.info(f"Extracting JSON: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                if self.record_path:
                    df = pd.json_normalize(
                        data,
                        record_path=self.record_path,
                        meta=self.meta or [],
                        errors="ignore",
                    )
                else:
                    df = pd.json_normalize(data)
            else:
                raise ValueError("Unsupported JSON structure")

            df["_source_file"] = os.path.basename(file_path)
            df["_source_type"] = "JSON"
            logger.info(f"Extracted {len(df)} rows from {file_path}")
            return df

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to extract {file_path}: {e}")
            raise

    def extract_multiple(self, file_paths: list) -> pd.DataFrame:
        """Extract and concatenate multiple JSON files."""
        frames = [self.extract(fp) for fp in file_paths]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
