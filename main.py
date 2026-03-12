#!/usr/bin/env python3
"""
Custom ETL Engine – Main Entry Point
=====================================
Usage:
    python main.py                  # Run the ETL pipeline using default config
    python main.py --config path    # Run with a custom config file
    python main.py --dashboard      # Start the web dashboard
    python main.py --test           # Run all unit tests
"""

import argparse
import sys
import os

# ── Setup paths ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from etl_engine import ETLPipeline
from etl_engine.utils import setup_logger, ETLConfig

logger = setup_logger("main")


def run_pipeline(config_path: str):
    """Load config and execute the ETL pipeline."""
    logger.info(f"Loading config from: {config_path}")
    config = ETLConfig(config_path)
    pipeline = ETLPipeline(config=config)
    report = pipeline.run()

    print("\n" + "=" * 55)
    print("  ETL PIPELINE REPORT")
    print("=" * 55)
    print(f"  Status          : {report.get('status', 'N/A').upper()}")
    print(f"  Elapsed (s)     : {report.get('elapsed_seconds', 'N/A')}")
    print(f"  Rows Extracted  : {report.get('rows_extracted', 'N/A')}")
    print(f"  Rows After Clean: {report.get('rows_after_cleaning', 'N/A')}")
    print(f"  Rows Loaded     : {report.get('rows_loaded', 'N/A')}")

    val = report.get("validation", {})
    print(f"  Validation      : {'✅ PASSED' if val.get('passed') else '⚠️  FAILED'} "
          f"({val.get('total_violations', 0)} violations)")

    if val.get("violations"):
        for col, msgs in val["violations"].items():
            for msg in msgs:
                print(f"    ⚠  [{col}] {msg}")

    qr = report.get("quality_report", {})
    if qr:
        print(f"\n  Quality Report:")
        print(f"    Duplicate Rows  : {qr.get('duplicate_rows', 0)}")
        null_counts = {k: v for k, v in qr.get("null_counts", {}).items() if v > 0}
        if null_counts:
            print(f"    Nulls per column:")
            for col, cnt in null_counts.items():
                pct = qr.get("null_percent", {}).get(col, 0)
                print(f"      • {col}: {cnt} ({pct}%)")

    print("=" * 55)
    return report


def run_dashboard(config_path: str):
    """Launch the Flask web dashboard."""
    os.chdir(BASE_DIR)
    os.environ["ETL_CONFIG_PATH"] = config_path
    print("Starting ETL Dashboard at http://127.0.0.1:5000")
    print(f"Using config       : {config_path}")
    print("Press Ctrl+C to stop.\n")
    from dashboard.app import app
    app.run(debug=False, port=5000)


def run_tests():
    """Run all unit tests."""
    import unittest
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(BASE_DIR, "tests"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Custom ETL Engine – BI Project | USTO Oran 2025-2026"
    )
    parser.add_argument(
        "--config",
        default=os.path.join(BASE_DIR, "config", "etl_config.yaml"),
        help="Path to the ETL configuration YAML file",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the web dashboard instead of running the pipeline",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run all unit tests",
    )

    args = parser.parse_args()

    if args.dashboard:
        run_dashboard(args.config)
    elif args.test:
        sys.exit(run_tests())
    else:
        report = run_pipeline(args.config)
        sys.exit(0 if report.get("status") == "success" else 1)
