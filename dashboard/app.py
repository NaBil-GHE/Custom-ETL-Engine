from flask import Flask, render_template, jsonify, request
import sqlite3
import pandas as pd
import os
import sys

# Allow importing from parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_engine import ETLPipeline
from etl_engine.utils import ETLConfig, setup_logger

logger = setup_logger("dashboard")
app = Flask(__name__)

# DB path is derived from the active config (set via ETL_CONFIG_PATH env var)
def _get_db_path() -> str:
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/ecommerce_dwh_config.yaml")
    try:
        from etl_engine.utils import ETLConfig
        cfg = ETLConfig(config_path)
        return cfg.output.get("db_path", os.path.join("data", "processed", "ecommerce_dwh.db"))
    except Exception:
        return os.path.join("data", "processed", "ecommerce_dwh.db")


DB_PATH = _get_db_path()


def _get_table_name() -> str:
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/ecommerce_dwh_config.yaml")
    try:
        cfg = ETLConfig(config_path)
        # For Star Schema, use fact_sales table
        use_star = cfg.get("use_star_schema", False)
        if use_star:
            return "fact_sales"
        return cfg.output.get("table_name", "etl_results")
    except Exception:
        return "fact_sales"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(table_name: str) -> bool:
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        result = cursor.fetchone() is not None
        conn.close()
        return result
    except Exception:
        return False


# ------------------------------------------------------------------ #
# Routes                                                               #
# ------------------------------------------------------------------ #

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    """Return summary statistics for the dashboard."""
    table = _get_table_name()
    if not table_exists(table):
        return jsonify({"error": "No data found. Run the ETL pipeline first."}), 404

    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()

    stats = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": df.columns.tolist(),
        "null_counts": df.isnull().sum().to_dict(),
        "data_types": df.dtypes.astype(str).to_dict(),
    }
    return jsonify(stats)


@app.route("/api/data")
def api_data():
    """Return paginated data from the ETL results table."""
    table = _get_table_name()
    if not table_exists(table):
        return jsonify({"error": "No data found. Run the ETL pipeline first."}), 404

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    offset = (page - 1) * per_page

    conn = get_db_connection()
    df = pd.read_sql_query(
        f"SELECT * FROM {table} LIMIT {per_page} OFFSET {offset}", conn
    )
    total = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn)["cnt"][0]
    conn.close()

    return jsonify({
        "page": page,
        "per_page": per_page,
        "total": int(total),
        "columns": df.columns.tolist(),
        "data": df.fillna("").to_dict(orient="records"),
    })


@app.route("/api/run_etl", methods=["POST"])
def api_run_etl():
    """Trigger the ETL pipeline via HTTP POST."""
    try:
        config_path = os.environ.get("ETL_CONFIG_PATH", "config/etl_config.yaml")
        config = ETLConfig(config_path)
        pipeline = ETLPipeline(config=config)
        report = pipeline.run()
        # refresh DB_PATH after pipeline runs
        global DB_PATH
        DB_PATH = config.output.get("db_path", DB_PATH)
        return jsonify(report)
    except Exception as e:
        logger.error(f"ETL run failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/source_breakdown")
def api_source_breakdown():
    """Count rows per source type if _source_type column exists."""
    table = _get_table_name()
    if not table_exists(table):
        return jsonify({}), 200

    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()

    if "_source_type" not in df.columns:
        return jsonify({})

    breakdown = df["_source_type"].value_counts().to_dict()
    return jsonify(breakdown)


# ------------------------------------------------------------------ #
# Setup Wizard Routes                                                  #
# ------------------------------------------------------------------ #



if __name__ == "__main__":
    app.run(debug=False, port=5000)
