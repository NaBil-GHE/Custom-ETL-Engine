# User Guide - Custom ETL Engine

## Business Intelligence Project - Complete User Documentation

---

## 📚 Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Running ETL Pipelines](#running-etl-pipelines)
4. [Configuration Guide](#configuration-guide)
5. [Using the Dashboard](#using-the-dashboard)
6. [Interpreting Logs](#interpreting-logs)
7. [Querying the Data Warehouse](#querying-the-data-warehouse)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

---

## 1. Introduction

### What is This Project?

This is a **complete ETL (Extract, Transform, Load) system** with a **Star Schema Data Warehouse** designed for business intelligence analytics. It allows you to:

- Extract data from multiple sources (CSV, JSON, databases)
- Clean and validate data automatically
- Load data into an optimized Star Schema for analytics
- Query data efficiently for business insights
- Monitor the entire process through a web dashboard

### Key Features

✅ **Star Schema** - Industry-standard data warehouse design  
✅ **Data Quality** - Automated cleaning and validation  
✅ **Flexible Configuration** - YAML-based pipeline setup  
✅ **Web Dashboard** - Monitor and control via browser  
✅ **Production Logging** - Complete audit trail  

---

## 2. Getting Started

### Prerequisites

Before you begin, ensure you have:
- Python 3.11 or higher installed
- Command line access (PowerShell, CMD, or Terminal)
- Basic understanding of SQL (helpful but not required)

### Installation Steps

#### Step 1: Set Up Virtual Environment

```powershell
# Navigate to project directory
cd "C:\Users\[YourName]\...\BI PROJECT"

# Create virtual environment
python -m venv .venv

# Activate it (Windows PowerShell)
.\.venv\Scripts\Activate.ps1
```

**Note**: You should see `(.venv)` appear in your command prompt.

#### Step 2: Install Dependencies

```powershell
pip install -r requirements.txt
```

Expected output:
```
Successfully installed pandas-2.0.0 numpy-1.24.0 Flask-3.0.0 PyYAML-6.0 ...
```

#### Step 3: Verify Installation

```powershell
python main.py --test
```

You should see:
```
✅ All 21 tests passed
```

---

## 3. Running ETL Pipelines

### Option 1: Star Schema Pipeline (Recommended for BI)

This creates a proper data warehouse with fact and dimension tables.

```powershell
python main.py --config config\ecommerce_dwh_config.yaml
```

**What happens:**
1. Reads `ecommerce_sales.csv` (1,000 transactions)
2. Cleans and validates the data
3. Creates 4 tables:
   - `fact_sales` - Transaction records (1,000 rows)
   - `dim_customer` - Unique customers (199 rows)
   - `dim_product` - Product catalog (100 rows)
   - `dim_date` - Date dimension (731 rows)
4. Verifies data integrity
5. Generates performance report

**Expected Output:**
```
🚀 ETL PIPELINE STARTED
📥 PHASE 1: EXTRACTION
✓ Extracted 1,000 rows in 0.05s
🧹 PHASE 2: TRANSFORMATION - Cleaning
✓ Cleaned to 1,000 rows in 0.08s
✅ PHASE 3: TRANSFORMATION - Validation
✓ Validation passed in 0.12s
🔍 PHASE 4: TRANSFORMATION - Deduplication
✓ Removed 0 duplicates, 1,000 rows remaining in 0.03s
💾 PHASE 5: LOADING
🌟 Loading into Star Schema Data Warehouse...
✓ Loaded 1,000 rows in 0.22s

✅ ETL PIPELINE COMPLETED SUCCESSFULLY
⏱  Total Time: 0.50s
📊 Rows Loaded: 1,000
🚀 Throughput: 2,000 rows/sec
```

**Output Location:**
- Database: `data/processed/ecommerce_dwh.db`
- Logs: `logs/etl_YYYYMMDD.log`

### Option 2: Flat Table Pipeline

This creates a single denormalized table (traditional ETL).

```powershell
python main.py --config config\etl_config.yaml
```

**Use when:** You need a simple, flat export for tools that don't support Star Schema.

---

## 4. Configuration Guide

### YAML Configuration Structure

ETL pipelines are configured using YAML files in the `config/` directory.

#### Basic Configuration Template

```yaml
# Enable Star Schema (true) or Flat Table (false)
use_star_schema: true

# Path to Star Schema DDL (only if use_star_schema=true)
schema_file: config/star_schema.sql

# Data Sources
sources:
  - type: CSV                    # CSV, JSON, or SQL
    path: data/raw/sales.csv     # File path
    delimiter: ","               # Optional: default is comma
    encoding: utf-8              # Optional: default is utf-8

# Data Cleaning
cleaning:
  null_strategy: fill            # fill | drop_rows | report
  null_values:                   # Strings to treat as null
    - "N/A"
    - "null"
    - ""
  type_map:                      # Force column types
    quantity: int
    price: float

# Validation Rules
validation_rules:
  customer_id:
    required: true               # Must not be null
    regex: "^C\\d{4}$"           # Pattern: C0001, C0002, etc.
  
  email:
    regex: "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$"
  
  quantity:
    min_value: 1
    max_value: 100

# Deduplication
deduplication:
  keep: first                    # first | last | false
  use_hash: true                 # Add SHA-256 row hash
  subset: [order_id]             # Columns to check (null=all)

# Output
output:
  db_path: data/processed/output.db
  table_name: results            # Ignored if use_star_schema=true
```

### Configuration Options Reference

#### Validation Rules

| Rule | Type | Description | Example |
|------|------|-------------|---------|
| `required` | Boolean | Must not be null | `true` |
| `regex` | String | Pattern matching | `"^[A-Z]{2}\\d{3}$"` |
| `min_value` | Number | Minimum value | `0` |
| `max_value` | Number | Maximum value | `100` |
| `allowed_values` | List | Enum values | `["A", "B", "C"]` |
| `min_length` | Integer | Minimum string length | `3` |
| `max_length` | Integer | Maximum string length | `50` |
| `dtype` | String | Data type | `"int"`, `"float"`, `"str"` |

#### Null Strategies

| Strategy | Behavior |
|----------|----------|
| `fill` | Replace nulls with defaults (0, "", etc.) |
| `drop_rows` | Remove rows with nulls |
| `report` | Log nulls but keep them |

#### Deduplication Keep Strategies

| Strategy | Behavior |
|----------|----------|
| `first` | Keep first occurrence, remove duplicates |
| `last` | Keep last occurrence, remove duplicates |
| `false` | Keep all duplicates |

### Creating a Custom Configuration

1. **Copy an existing config:**
   ```powershell
   Copy-Item config\ecommerce_dwh_config.yaml config\my_pipeline.yaml
   ```

2. **Edit the file** (use Notepad, VS Code, or any text editor):
   ```powershell
   notepad config\my_pipeline.yaml
   ```

3. **Update sources, rules, and output:**
   - Change `path:` to your data file
   - Adjust validation rules for your columns
   - Set output database name

4. **Run your custom pipeline:**
   ```powershell
   python main.py --config config\my_pipeline.yaml
   ```

---

## 5. Using the Dashboard

### Starting the Dashboard

```powershell
python main.py --dashboard
```

You'll see:
```
Starting ETL Dashboard at http://127.0.0.1:5000
Using config: config/ecommerce_dwh_config.yaml
Press Ctrl+C to stop.
```

### Accessing the Dashboard

1. Open your web browser
2. Navigate to: `http://127.0.0.1:5000`
3. You should see the ETL Dashboard

### Dashboard Features

#### 1. Statistics Cards

Displays real-time metrics:
- **Total Rows**: Number of records in database
- **Columns**: Number of columns
- **Null Values**: Count of null values
- **Run ETL Button**: Execute pipeline from UI

#### 2. Source Breakdown Chart

Bar chart showing distribution of data by source type:
- CSV files (blue)
- JSON files (gray)
- SQL databases (green)

#### 3. Data Preview Table

Interactive table with:
- **Pagination**: Navigate through pages
- **Row Display**: Configurable rows per page
- **Live Data**: Reflects current database state

### API Endpoints (for Integration)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /` | GET | Dashboard homepage |
| `GET /api/stats` | GET | JSON statistics |
| `GET /api/data?page=1&per_page=50` | GET | Paginated data |
| `POST /api/run_etl` | POST | Trigger ETL execution |
| `GET /api/source_breakdown` | GET | Chart data |

**Example API Call (PowerShell):**
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/stats" | ConvertTo-Json
```

---

## 6. Interpreting Logs

### Log File Location

Logs are stored in: `logs/etl_YYYYMMDD.log`

Example: `logs/etl_20260404.log`

### Log Format

```
2026-04-04 17:12:26 | INFO     | dwh_loader | ✓ Loaded 1,000 rows into fact_sales
└─ Timestamp         └─ Level   └─ Module     └─ Message
```

### Log Levels

| Level | Meaning | Example |
|-------|---------|---------|
| **INFO** | Normal operation | `✓ Extracted 1,000 rows` |
| **WARNING** | Non-critical issue | `⚠ 3 rows with null emails` |
| **ERROR** | Critical failure | `❌ Failed to load file: FileNotFoundError` |

### Finding Specific Information

#### Check Pipeline Status
```powershell
Select-String -Path "logs\etl_*.log" -Pattern "PIPELINE COMPLETED" | Select-Object -Last 1
```

#### Find Errors
```powershell
Select-String -Path "logs\etl_*.log" -Pattern "ERROR" | Select-Object -Last 10
```

#### View Performance Metrics
```powershell
Select-String -Path "logs\etl_*.log" -Pattern "Throughput"
```

---

## 7. Querying the Data Warehouse

### Connecting to the Database

#### Option 1: SQLite Command Line

```powershell
# Navigate to database directory
cd data\processed

# Open database
sqlite3 ecommerce_dwh.db
```

#### Option 2: Python Script

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/processed/ecommerce_dwh.db')

# Execute query
query = "SELECT * FROM fact_sales LIMIT 10"
df = pd.read_sql(query, conn)
print(df)

conn.close()
```

#### Option 3: DB Browser for SQLite (GUI)

1. Download from: https://sqlitebrowser.org/
2. Open `ecommerce_dwh.db`
3. Use "Execute SQL" tab

### Common Queries

#### 1. View All Tables
```sql
SELECT name FROM sqlite_master WHERE type='table';
```

#### 2. Top 10 Customers
```sql
SELECT 
    c.customer_name,
    c.country,
    SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_id
ORDER BY revenue DESC
LIMIT 10;
```

#### 3. Monthly Sales Trend
```sql
SELECT 
    d.year,
    d.month_name,
    COUNT(*) as transactions,
    SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

#### 4. Product Category Performance
```sql
SELECT 
    p.category,
    COUNT(*) as orders,
    SUM(f.quantity) as units_sold,
    SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;
```

---

## 8. Troubleshooting

### Common Issues and Solutions

#### Issue 1: "Module not found" Error

**Error:**
```
ModuleNotFoundError: No module named 'pandas'
```

**Solution:**
```powershell
# Ensure virtual environment is activated
.\.venv\Scripts\Activate.ps1

# Reinstall dependencies
pip install -r requirements.txt
```

#### Issue 2: File Not Found

**Error:**
```
FileNotFoundError: data/raw/sales.csv
```

**Solution:**
1. Check that file exists: `Test-Path data\raw\sales.csv`
2. Verify path in config matches actual file location
3. Use absolute path if needed:
   ```yaml
   path: C:\Users\YourName\...\data\raw\sales.csv
   ```

#### Issue 3: Validation Failures

**Error:**
```
⚠ Validation FAILED: 50 violations
```

**Solution:**
1. Check log file for details: `logs\etl_YYYYMMDD.log`
2. Find specific violations:
   ```
   [email] Does not match pattern: abc123 (expected email format)
   ```
3. Either:
   - Fix source data
   - Update validation regex in config
   - Set `validation_mode: permissive` to continue anyway

#### Issue 4: Dashboard Won't Start

**Error:**
```
Address already in use: 127.0.0.1:5000
```

**Solution:**
```powershell
# Stop existing process
Get-Process -Name python | Where-Object {$_.Path -like "*\.venv*"} | Stop-Process

# Or use different port
$env:FLASK_RUN_PORT="5001"
python main.py --dashboard
```

#### Issue 5: Database Locked

**Error:**
```
database is locked
```

**Solution:**
1. Close any open connections (DB Browser, Python scripts)
2. Wait a few seconds and retry
3. Delete and recreate if corrupted:
   ```powershell
   Remove-Item data\processed\ecommerce_dwh.db
   python main.py --config config\ecommerce_dwh_config.yaml
   ```

---

## 9. Best Practices

### Data Management

✅ **DO:**
- Keep source files in `data/raw/`
- Use descriptive config file names
- Back up databases before re-running pipelines
- Review logs after each run

❌ **DON'T:**
- Edit database files manually
- Mix multiple datasets in one config
- Delete logs (they help with debugging)
- Run concurrent pipelines on same database

### Configuration

✅ **DO:**
- Start with example configs and modify
- Use strict validation for production data
- Document custom rules with comments
- Test configs with small datasets first

❌ **DON'T:**
- Use `validation_mode: permissive` in production
- Skip validation rules for critical fields
- Hard-code file paths (use relative paths)

### Performance

✅ **DO:**
- Use Star Schema for analytical queries
- Create indexes on frequently queried columns
- Monitor throughput in logs
- Batch large datasets

❌ **DON'T:**
- Load millions of rows without testing
- Run ETL during dashboard queries
- Skip deduplication for transactional data

---

## 📞 Getting Help

- **Documentation**: Check [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md)
- **Logs**: Always check `logs/etl_*.log` first
- **Tests**: Run `python main.py --test` to verify system health
- **Issues**: If you found a bug, document steps to reproduce

---

**Last Updated**: April 4, 2026  
**Version**: 2.0 (Star Schema Edition)
