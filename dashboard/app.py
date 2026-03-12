from flask import Flask, render_template, jsonify, request
from werkzeug.utils import secure_filename
import sqlite3
import pandas as pd
import yaml
import json
import os
import sys

# Allow importing from parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_engine import ETLPipeline
from etl_engine.utils import ETLConfig, setup_logger

logger = setup_logger("dashboard")
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB max upload

UPLOAD_DIR = os.path.join("data", "raw")
ALLOWED_EXTENSIONS = {"csv", "json", "db"}


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

# DB path is derived from the active config (set via ETL_CONFIG_PATH env var)
def _get_db_path() -> str:
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/etl_config.yaml")
    try:
        from etl_engine.utils import ETLConfig
        cfg = ETLConfig(config_path)
        return cfg.output.get("db_path", os.path.join("data", "processed", "etl_output.db"))
    except Exception:
        return os.path.join("data", "processed", "etl_output.db")


DB_PATH = _get_db_path()


def _get_table_name() -> str:
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/etl_config.yaml")
    try:
        cfg = ETLConfig(config_path)
        return cfg.output.get("table_name", "etl_results")
    except Exception:
        return "etl_results"


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

@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Upload a data file (CSV / JSON / DB) to data/raw/."""
    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not _allowed_file(file.filename):
        return jsonify({"error": "Only CSV, JSON, and DB files are allowed"}), 400

    filename = secure_filename(file.filename)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    save_path = os.path.join(UPLOAD_DIR, filename)
    file.save(save_path)

    # Return a preview of the file
    preview = []
    ext = filename.rsplit(".", 1)[1].lower()
    try:
        if ext == "csv":
            delimiter = request.form.get("delimiter", ",")
            df = pd.read_csv(save_path, delimiter=delimiter, nrows=5, dtype=str)
            preview = df.fillna("").to_dict(orient="records")
        elif ext == "json":
            df = pd.read_json(save_path)
            if isinstance(df, pd.DataFrame):
                preview = df.head(5).fillna("").to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Could not generate preview for {filename}: {e}")

    logger.info(f"File uploaded: {save_path}")
    return jsonify({
        "filename": filename,
        "path": save_path.replace("\\", "/"),
        "size_kb": round(os.path.getsize(save_path) / 1024, 1),
        "preview": preview,
    })


@app.route("/api/list_uploads")
def api_list_uploads():
    """List all files currently in data/raw/."""
    if not os.path.isdir(UPLOAD_DIR):
        return jsonify([])
    files = []
    for fname in os.listdir(UPLOAD_DIR):
        ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
        if ext in ALLOWED_EXTENSIONS:
            fpath = os.path.join(UPLOAD_DIR, fname)
            files.append({
                "filename": fname,
                "path": fpath.replace("\\", "/"),
                "size_kb": round(os.path.getsize(fpath) / 1024, 1),
                "type": ext.upper(),
            })
    return jsonify(files)


@app.route("/api/generate_config", methods=["POST"])
def api_generate_config():
    """
    Generate a YAML config file from the wizard form data and set it as active.
    Expected JSON body:
    {
        "config_name": "my_pipeline",
        "sources": [{"type":"CSV","path":"data/raw/x.csv","delimiter":","}],
        "null_strategy": "fill",
        "dedup_keep": "first",
        "output_table": "results",
        "validation_rules": {}   // optional
    }
    """
    body = request.get_json(force=True)
    if not body:
        return jsonify({"error": "Empty request body"}), 400

    config_name = secure_filename(body.get("config_name", "wizard_config"))
    if not config_name.endswith(".yaml"):
        config_name += ".yaml"

    sources = body.get("sources", [])
    if not sources:
        return jsonify({"error": "At least one source is required"}), 400

    # Build config dict
    cfg = {
        "sources": sources,
        "cleaning": {
            "null_strategy": body.get("null_strategy", "fill"),
            "null_values": ["N/A", "n/a", "null", "NULL", "None", "", "-"],
            "type_map": body.get("type_map", {}),
        },
        "deduplication": {
            "keep": body.get("dedup_keep", "first"),
            "use_hash": True,
        },
        "validation_rules": body.get("validation_rules", {}),
        "output": {
            "db_path": f"data/processed/{config_name.replace('.yaml','')}_output.db",
            "table_name": body.get("output_table", "etl_results"),
            "export_csv": True,
            "export_json": body.get("export_json", False),
        },
    }

    config_dir = "config"
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.join(config_dir, config_name)

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    # Set as active config
    os.environ["ETL_CONFIG_PATH"] = config_path
    global DB_PATH
    DB_PATH = cfg["output"]["db_path"]

    logger.info(f"Config generated and activated: {config_path}")
    return jsonify({
        "config_path": config_path,
        "message": f"Config '{config_name}' generated and set as active pipeline.",
    })


@app.route("/api/active_config")
def api_active_config():
    """Return the currently active config file path and its contents."""
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/etl_config.yaml")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
        return jsonify({"path": config_path, "config": content})
    except Exception as e:
        return jsonify({"error": str(e)}), 404


if __name__ == "__main__":
    app.run(debug=False, port=5000)
