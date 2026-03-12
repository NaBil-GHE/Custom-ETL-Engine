import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


class CSVExtractor:
    """
    Extractor for CSV files.
    Supports single file and directory-based batch extraction.
    """

    def __init__(self, delimiter=",", encoding="utf-8"):
        self.delimiter = delimiter
        self.encoding = encoding

    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from a single CSV file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        logger.info(f"Extracting CSV: {file_path}")
        try:
            df = pd.read_csv(
                file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                dtype=str,          # read all as string to avoid type errors
            )
            df["_source_file"] = os.path.basename(file_path)
            df["_source_type"] = "CSV"
            logger.info(f"Extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to extract {file_path}: {e}")
            raise

    def extract_directory(self, dir_path: str) -> pd.DataFrame:
        """Extract and concatenate all CSV files from a directory."""
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(f"Not a directory: {dir_path}")

        frames = []
        for fname in os.listdir(dir_path):
            if fname.lower().endswith(".csv"):
                frames.append(self.extract(os.path.join(dir_path, fname)))

        if not frames:
            logger.warning(f"No CSV files found in {dir_path}")
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"Total rows from directory: {len(combined)}")
        return combined
