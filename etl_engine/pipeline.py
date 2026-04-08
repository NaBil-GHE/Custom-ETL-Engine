import pandas as pd
import time
import logging
from typing import Dict, Any, Optional

from .extractor import CSVExtractor, JSONExtractor, SQLExtractor
from .transformer import (
    DataCleaner, Deduplicator, DataValidator,
    build_customer_dimension, build_product_dimension, build_date_dimension
)
from .loader import DBLoader, DWHLoader
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
        """Execute the full ETL pipeline with enhanced logging and performance tracking."""
        start_time = time.time()
        
        logger.info("=" * 70)
        logger.info("🚀 ETL PIPELINE STARTED")
        logger.info("=" * 70)
        
        # Initialize performance metrics
        metrics = {
            'extract_time': 0,
            'clean_time': 0,
            'validate_time': 0,
            'deduplicate_time': 0,
            'load_time': 0,
        }
        
        try:
            # -- EXTRACT --
            logger.info("\n📥 PHASE 1: EXTRACTION")
            extract_start = time.time()
            df = self._extract_step()
            metrics['extract_time'] = round(time.time() - extract_start, 2)
            
            if df.empty:
                logger.error("❌ No data extracted. Pipeline aborted.")
                return {"status": "failed", "reason": "Empty extract", "metrics": metrics}
            
            logger.info(f"✓ Extracted {len(df):,} rows in {metrics['extract_time']}s")
            
            # -- CLEAN --
            logger.info("\n🧹 PHASE 2: TRANSFORMATION - Cleaning")
            clean_start = time.time()
            df = self._clean_step(df)
            metrics['clean_time'] = round(time.time() - clean_start, 2)
            logger.info(f"✓ Cleaned to {len(df):,} rows in {metrics['clean_time']}s")
            
            # -- VALIDATE --
            logger.info("\n✅ PHASE 3: TRANSFORMATION - Validation")
            validate_start = time.time()
            validation_report = self._validate_step(df)
            metrics['validate_time'] = round(time.time() - validate_start, 2)
            
            if validation_report['passed']:
                logger.info(f"✓ Validation passed in {metrics['validate_time']}s")
            else:
                logger.warning(f"⚠ Validation found {validation_report['total_violations']} violations in {metrics['validate_time']}s")
            
            # -- DEDUPLICATE --
            logger.info("\n🔍 PHASE 4: TRANSFORMATION - Deduplication")
            dedup_start = time.time()
            initial_rows = len(df)
            df = self._deduplicate_step(df)
            metrics['deduplicate_time'] = round(time.time() - dedup_start, 2)
            duplicates_removed = initial_rows - len(df)
            logger.info(f"✓ Removed {duplicates_removed} duplicates, {len(df):,} rows remaining in {metrics['deduplicate_time']}s")
            
            # -- LOAD --
            logger.info("\n💾 PHASE 5: LOADING")
            load_start = time.time()
            rows_loaded = self._load_step(df)
            metrics['load_time'] = round(time.time() - load_start, 2)
            logger.info(f"✓ Loaded {rows_loaded:,} rows in {metrics['load_time']}s")
            
            # Calculate total metrics
            raw_total_time = time.time() - start_time
            total_time = round(raw_total_time, 2)
            metrics['total_time'] = total_time
            metrics['throughput'] = round(rows_loaded / raw_total_time, 0) if raw_total_time > 0 else 0
            
            # Calculate data size
            data_size_mb = round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
            metrics['data_size_mb'] = data_size_mb
            
            # Final summary
            logger.info("\n" + "=" * 70)
            logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"⏱  Total Time: {total_time}s")
            if raw_total_time > 0:
                logger.info(f"   ├─ Extract:      {metrics['extract_time']:>6}s ({metrics['extract_time']/raw_total_time*100:>5.1f}%)")
                logger.info(f"   ├─ Clean:        {metrics['clean_time']:>6}s ({metrics['clean_time']/raw_total_time*100:>5.1f}%)")
                logger.info(f"   ├─ Validate:     {metrics['validate_time']:>6}s ({metrics['validate_time']/raw_total_time*100:>5.1f}%)")
                logger.info(f"   ├─ Deduplicate:  {metrics['deduplicate_time']:>6}s ({metrics['deduplicate_time']/raw_total_time*100:>5.1f}%)")
                logger.info(f"   └─ Load:         {metrics['load_time']:>6}s ({metrics['load_time']/raw_total_time*100:>5.1f}%)")
            else:
                logger.info(f"   ├─ Extract:      {metrics['extract_time']:>6}s")
                logger.info(f"   ├─ Clean:        {metrics['clean_time']:>6}s")
                logger.info(f"   ├─ Validate:     {metrics['validate_time']:>6}s")
                logger.info(f"   ├─ Deduplicate:  {metrics['deduplicate_time']:>6}s")
                logger.info(f"   └─ Load:         {metrics['load_time']:>6}s")
            logger.info(f"📊 Rows Loaded: {rows_loaded:,}")
            logger.info(f"🚀 Throughput: {metrics['throughput']:,.0f} rows/sec")
            logger.info(f"💾 Data Size: {data_size_mb} MB")
            logger.info("=" * 70)
            
            self.report = {
                "status": "success",
                "elapsed_seconds": total_time,
                "rows_extracted": self.report.get("rows_extracted", 0),
                "rows_after_cleaning": len(df),
                "rows_loaded": rows_loaded,
                "validation": validation_report,
                "performance": metrics,
            }
            return self.report
            
        except Exception as e:
            # Error handling with detailed logging
            elapsed = round(time.time() - start_time, 2)
            logger.error("=" * 70)
            logger.error(f"❌ ETL PIPELINE FAILED after {elapsed}s")
            logger.error(f"Error: {str(e)}")
            logger.error("=" * 70, exc_info=True)
            
            return {
                "status": "failed",
                "error": str(e),
                "elapsed_seconds": elapsed,
                "metrics": metrics,
            }

    # ------------------------------------------------------------------ #
    # Step implementations                                                 #
    # ------------------------------------------------------------------ #

    def _extract_step(self) -> pd.DataFrame:
        logger.info("STEP 1/4 – EXTRACT")
        t0 = time.time()
        frames = []
        failed_sources = []

        for idx, source in enumerate(self.config.sources):
            src_type = source.get("type", "").upper()
            path = source.get("path", "")

            try:
                logger.info(f"  [{idx+1}/{len(self.config.sources)}] Extracting {src_type}: {path}")
                
                if src_type == "CSV":
                    extractor = CSVExtractor(
                        delimiter=source.get("delimiter", ","),
                        encoding=source.get("encoding", "utf-8"),
                    )
                    df_source = extractor.extract(path)
                    frames.append(df_source)
                    logger.info(f"      ✓ Extracted {len(df_source):,} rows from CSV")

                elif src_type == "JSON":
                    extractor = JSONExtractor(
                        record_path=source.get("record_path"),
                        meta=source.get("meta"),
                    )
                    df_source = extractor.extract(path)
                    frames.append(df_source)
                    logger.info(f"      ✓ Extracted {len(df_source):,} rows from JSON")

                elif src_type == "SQL":
                    with SQLExtractor(path) as extractor:
                        table = source.get("table")
                        query = source.get("query")
                        if query:
                            df_source = extractor.extract_query(query)
                        elif table:
                            df_source = extractor.extract_table(table)
                        else:
                            raise ValueError("SQL source must specify 'table' or 'query'")
                        frames.append(df_source)
                        logger.info(f"      ✓ Extracted {len(df_source):,} rows from SQL")
                else:
                    logger.warning(f"      ⚠ Unknown source type: {src_type}, skipping")

            except Exception as e:
                error_msg = f"{src_type} source '{path}': {str(e)}"
                failed_sources.append(error_msg)
                logger.error(f"      ❌ Extract failed: {error_msg}")
                
                # Check validation mode
                validation_mode = self.config.get("validation_mode", "permissive")
                if validation_mode == "strict":
                    logger.error("⚠ Validation mode is 'strict' - aborting pipeline due to source failure")
                    raise
                else:
                    logger.warning("⚠ Validation mode is 'permissive' - continuing with remaining sources")

        if not frames:
            if failed_sources:
                logger.error(f"❌ All {len(failed_sources)} sources failed:")
                for err in failed_sources:
                    logger.error(f"   - {err}")
            return pd.DataFrame()

        # Log summary
        if failed_sources:
            logger.warning(f"⚠ {len(failed_sources)}/{len(self.config.sources)} sources failed (continuing with {len(frames)} successful)")

        combined = pd.concat(frames, ignore_index=True)
        self.report["rows_extracted"] = len(combined)
        self.report["failed_sources"] = failed_sources
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
        
        # Check if using Star Schema mode
        use_star_schema = self.config.get("use_star_schema", False)
        
        if use_star_schema:
            logger.info("🌟 Loading into Star Schema Data Warehouse...")
            rows = self._load_star_schema(df, db_path)
        else:
            # Traditional flat table loading
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
    
    def _load_star_schema(self, df: pd.DataFrame, db_path: str) -> int:
        """
        Load data into Star Schema (Fact + Dimension tables)
        
        Args:
            df: Cleaned and validated transaction data
            db_path: Path to data warehouse database
            
        Returns:
            Number of fact rows loaded
        """
        dwh = DWHLoader(db_path)
        dwh.connect()
        
        # Create Star Schema tables
        schema_file = self.config.get("schema_file", "config/star_schema.sql")
        dwh.create_schema(schema_file)
        
        # Build dimension tables from transaction data
        logger.info("📊 Building dimension tables...")
        dim_customer = build_customer_dimension(df)
        dim_product = build_product_dimension(df)
        dim_date = build_date_dimension(df=df)
        
        logger.info(f"  Customers: {len(dim_customer)}, Products: {len(dim_product)}, Dates: {len(dim_date)}")
        
        # Load dimensions and get key mappings
        logger.info("📥 Loading dimension tables...")
        customer_map = dwh.load_dimension('dim_customer', dim_customer, 'customer_id')
        product_map = dwh.load_dimension('dim_product', dim_product, 'product_id')
        dwh.load_dimension_date(dim_date)
        
        # Load fact table with foreign key lookups
        logger.info("📥 Loading fact table...")
        dimension_maps = {
            'customer': customer_map,
            'product': product_map
        }
        fact_rows = dwh.load_fact(df, dimension_maps, order_id_col='order_id')
        
        # Verify referential integrity
        dwh.verify_referential_integrity()
        
        # Log warehouse statistics
        logger.info("\n📊 Data Warehouse Summary:")
        for table in ['dim_date', 'dim_customer', 'dim_product', 'fact_sales']:
            info = dwh.get_table_info(table)
            logger.info(f"  {table}: {info.get('row_count', 0):,} rows")
        
        dwh.close()
        return fact_rows
