import pandas as pd
import time
import logging
from typing import Dict, Any, Optional

from .extractor import CSVExtractor, JSONExtractor, SQLExtractor
from .transformer import DataCleaner, Deduplicator, DataValidator
from .loader import DBLoader
from .utils import ETLConfig

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Orchestrates the full ETL process:
      Extract → Clean → Validate → Deduplicate → Load

    Supports multiple source types (CSV, JSON, SQL) simultaneously.
    All steps are logged with timing information.
    """

    def __init__(self, config: Optional[ETLConfig] = None):
        self.config = config or ETLConfig()
        self.report: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Main entry point                                                     #
    # ------------------------------------------------------------------ #

    def run(self) -> Dict[str, Any]:
        """Execute the full ETL pipeline and return a summary report."""
        start_time = time.time()
        logger.info("=" * 60)
        logger.info("ETL PIPELINE STARTED")
        logger.info("=" * 60)

        # -- EXTRACT --
        df = self._extract_step()
        if df.empty:
            logger.error("No data extracted. Pipeline aborted.")
            return {"status": "failed", "reason": "Empty extract"}

        # -- CLEAN --
        df = self._clean_step(df)

        # -- VALIDATE --
        validation_report = self._validate_step(df)

        # -- DEDUPLICATE --
        df = self._deduplicate_step(df)

        # -- LOAD --
        rows_loaded = self._load_step(df)

        elapsed = round(time.time() - start_time, 2)
        logger.info("=" * 60)
        logger.info(f"ETL PIPELINE COMPLETED in {elapsed}s | {rows_loaded} rows loaded")
        logger.info("=" * 60)

        self.report = {
            "status": "success",
            "elapsed_seconds": elapsed,
            "rows_extracted": self.report.get("rows_extracted", 0),
            "rows_after_cleaning": len(df),
            "rows_loaded": rows_loaded,
            "validation": validation_report,
        }
        return self.report

    # ------------------------------------------------------------------ #
    # Step implementations                                                 #
    # ------------------------------------------------------------------ #

    def _extract_step(self) -> pd.DataFrame:
        logger.info("STEP 1/4 – EXTRACT")
        t0 = time.time()
        frames = []

        for source in self.config.sources:
            src_type = source.get("type", "").upper()
            path = source.get("path", "")

            try:
                if src_type == "CSV":
                    extractor = CSVExtractor(
                        delimiter=source.get("delimiter", ","),
                        encoding=source.get("encoding", "utf-8"),
                    )
                    frames.append(extractor.extract(path))

                elif src_type == "JSON":
                    extractor = JSONExtractor(
                        record_path=source.get("record_path"),
                        meta=source.get("meta"),
                    )
                    frames.append(extractor.extract(path))

                elif src_type == "SQL":
                    with SQLExtractor(path) as extractor:
                        table = source.get("table")
                        query = source.get("query")
                        if query:
                            frames.append(extractor.extract_query(query))
                        elif table:
                            frames.append(extractor.extract_table(table))
                else:
                    logger.warning(f"Unknown source type: {src_type}")

            except Exception as e:
                logger.error(f"Extract failed for {src_type} source '{path}': {e}")

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        self.report["rows_extracted"] = len(combined)
        logger.info(
            f"Extract complete: {len(combined)} rows from {len(frames)} source(s) "
            f"in {round(time.time()-t0, 2)}s"
        )
        return combined

    def _clean_step(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("STEP 2/4 – CLEAN")
        t0 = time.time()

        cleaner = DataCleaner(config=self.config.cleaning)
        df = cleaner.clean(df)

        quality_report = DataCleaner.generate_quality_report(df)
        logger.info(
            f"Clean complete: {len(df)} rows, "
            f"{quality_report['duplicate_rows']} duplicates detected, "
            f"in {round(time.time()-t0, 2)}s"
        )
        self.report["quality_report"] = quality_report
        return df

    def _validate_step(self, df: pd.DataFrame) -> Dict[str, Any]:
        logger.info("STEP 3/4 – VALIDATE")
        t0 = time.time()

        rules = self.config.validation_rules
        if not rules:
            logger.info("No validation rules defined, skipping.")
            return {"passed": True, "total_violations": 0, "violations": {}}

        validator = DataValidator(rules=rules)
        report = validator.validate(df)
        logger.info(
            f"Validation {'PASSED' if report['passed'] else 'FAILED'}: "
            f"{report['total_violations']} violation(s) in {round(time.time()-t0, 2)}s"
        )
        return report

    def _deduplicate_step(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("STEP 3.5/4 – DEDUPLICATE")
        t0 = time.time()

        dedup_cfg = self.config.deduplication
        dedup = Deduplicator(
            subset=dedup_cfg.get("subset"),
            keep=dedup_cfg.get("keep", "first"),
            use_hash=dedup_cfg.get("use_hash", True),
        )
        df = dedup.deduplicate(df)
        logger.info(f"Deduplication complete: {len(df)} rows in {round(time.time()-t0, 2)}s")
        return df

    def _load_step(self, df: pd.DataFrame) -> int:
        logger.info("STEP 4/4 – LOAD")
        t0 = time.time()

        output_cfg = self.config.output
        db_path = output_cfg.get("db_path", "data/processed/etl_output.db")
        table_name = output_cfg.get("table_name", "etl_results")

        loader = DBLoader(db_path=db_path)
        rows = loader.load(df, table_name=table_name, if_exists="replace")

        if output_cfg.get("export_csv"):
            csv_path = db_path.replace(".db", ".csv")
            loader.export_csv(df, file_path=csv_path)

        if output_cfg.get("export_json"):
            json_path = db_path.replace(".db", ".json")
            loader.export_json(df, file_path=json_path)

        logger.info(f"Load complete: {rows} rows in {round(time.time()-t0, 2)}s")
        return rows
