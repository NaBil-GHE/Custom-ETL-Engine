# 🏢 Custom ETL Engine with Star Schema Data Warehouse

> **Business Intelligence Project** - University Course 2025-2026  
> A production-ready ETL pipeline implementing dimensional modeling for analytics

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-21%20passing-success.svg)](tests/)
[![Coverage](https://img.shields.io/badge/Coverage-95%25-brightgreen.svg)](tests/)

---

## 📋 Table of Contents

- [Features](#-features)
- [Star Schema Design](#-star-schema-design)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Usage](#-usage)
- [Configuration](#-configuration)
- [Architecture](#-architecture)
- [Dashboard](#-dashboard)
- [Testing](#-testing)
- [Project Structure](#-project-structure)
- [Sample Queries](#-sample-queries)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Features

### 🌟 Core Capabilities

- **⭐ Star Schema Data Warehouse**
  - Fact table: `fact_sales` (transactions with measures)
  - Dimension tables: `dim_date`, `dim_customer`, `dim_product`
  - Surrogate key management with automatic generation
  - Referential integrity enforcement
  - Optimized for analytical queries (OLAP)

- **🔄 Modular ETL Pipeline**
  - **Extract**: CSV, JSON, SQLite support (batch & single file)
  - **Transform**: Cleaning, validation, deduplication, dimension building
  - **Load**: Star Schema or flat table modes
  - YAML-driven configuration for flexibility

- **✅ Data Quality Management**
  - 11 validation rule types (regex, bounds, enums, required fields)
  - Automated data cleaning (null handling, type casting, normalization)
  - Deduplication with SHA-256 row hashing
  - Quality reporting with detailed violation tracking

- **📊 Web Dashboard**
  - Real-time statistics and metrics
  - Interactive data preview with pagination
  - Source breakdown visualization
  - One-click ETL execution
  - RESTful API for integration

- **🧪 Comprehensive Testing**
  - 21 unit tests covering all modules
  - 95% code coverage
  - Integration tests for end-to-end validation
  - Automated test suite with pytest

- **📝 Production-Ready Logging**
  - Daily rotating log files
  - Structured logging (INFO, WARNING, ERROR)
  - Performance metrics tracking
  - Detailed audit trail

---

## ⭐ Star Schema Design

Our data warehouse implements a **Star Schema** optimized for BI analytics:

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (731 rows) │
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
    ┌────▼────┐     ┌──────▼──────┐    ┌────▼────┐
    │dim_     │     │ fact_sales  │    │  dim_   │
    │customer │◄────┤ (1000 rows) │────►│ product │
    │(199 rows)│    │             │    │(100 rows)│
    └─────────┘     └─────────────┘    └─────────┘
```

### Why Star Schema?

- **Fast Queries**: Denormalized dimensions = fewer joins
- **BI Tool Compatible**: Works with Power BI, Tableau, Qlik
- **Easy to Understand**: Clear separation of facts and dimensions
- **Scalable**: Add dimensions without touching fact table

### Sample Analysis

```sql
-- Monthly revenue trend
SELECT 
    d.year,
    d.month_name,
    SUM(f.total_amount) as revenue,
    COUNT(*) as transactions
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## 🚀 Quick Start

Get up and running in 3 minutes:

```bash
# 1. Clone the repository
git clone <repository-url>
cd "BI PROJECT"

# 2. Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1  # Windows
# source .venv/bin/activate    # Linux/Mac

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run Star Schema ETL pipeline
python main.py --config config\ecommerce_dwh_config.yaml

# 5. Launch web dashboard
python main.py --dashboard
# Open browser: http://127.0.0.1:5000
```

**Expected Output:**
```
✅ ETL PIPELINE COMPLETED SUCCESSFULLY
⏱  Total Time: 0.44s
📊 Rows Loaded: 1,000
🚀 Throughput: 2,273 rows/sec
💾 Data Size: 0.23 MB
```

---

## 📦 Installation

### Prerequisites

- Python 3.11 or higher
- pip package manager
- 50 MB free disk space

### Step-by-Step Installation

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd "BI PROJECT"
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv .venv
   ```

3. **Activate Virtual Environment**
   - **Windows (PowerShell)**:
     ```powershell
     .\.venv\Scripts\Activate.ps1
     ```
   - **Windows (CMD)**:
     ```cmd
     .venv\Scripts\activate.bat
     ```
   - **Linux/Mac**:
     ```bash
     source .venv/bin/activate
     ```

4. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Verify Installation**
   ```bash
   python main.py --test
   ```
   All 21 tests should pass ✅

---

## 🎯 Usage

### Running ETL Pipelines

#### Star Schema Mode (Recommended for BI)
```bash
python main.py --config config\ecommerce_dwh_config.yaml
```

Creates data warehouse with:
- `fact_sales`: 1,000 transaction records
- `dim_customer`: 199 unique customers
- `dim_product`: 100 products
- `dim_date`: 731 dates (2024-2025)

#### Flat Table Mode (Traditional ETL)
```bash
python main.py --config config\etl_config.yaml
```

Creates single denormalized table.

### Launching Dashboard

```bash
python main.py --dashboard
```

Access at: http://127.0.0.1:5000

Features:
- 📊 Live statistics cards
- 📋 Paginated data table
- 📈 Source breakdown chart
- ▶️ Run ETL button

### Running Tests

```bash
# All tests
python main.py --test

# Specific test module
python -m pytest tests/test_dwh_loader.py -v

# With coverage report
python -m pytest --cov=etl_engine tests/
```

---

## ⚙️ Configuration

ETL pipelines are configured via YAML files in the `config/` directory.

### Star Schema Configuration Example

```yaml
# config/ecommerce_dwh_config.yaml

# Enable Star Schema mode
use_star_schema: true
schema_file: config/star_schema.sql

# Data sources
sources:
  - type: CSV
    path: data/raw/ecommerce_sales.csv
    delimiter: ","
    encoding: utf-8

# Data cleaning
cleaning:
  null_strategy: fill  # Options: fill | drop_rows | report
  null_values: ["N/A", "null", ""]
  type_map:
    quantity: int
    unit_price: float
    total_amount: float

# Validation rules
validation_rules:
  customer_id:
    required: true
    regex: "^C\\d{4}$"
  
  email:
    regex: "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$"
  
  quantity:
    min_value: 1
    max_value: 100

# Deduplication
deduplication:
  keep: first
  use_hash: true
  subset: [order_id]

# Output
output:
  db_path: data/processed/ecommerce_dwh.db
```

### Configuration Options

| Option | Values | Description |
|--------|--------|-------------|
| `use_star_schema` | true/false | Enable Star Schema loading |
| `null_strategy` | fill/drop_rows/report | How to handle nulls |
| `keep` | first/last/false | Deduplication strategy |
| `validation_mode` | strict/permissive | Fail or continue on errors |

---

## 🏗️ Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed system design.

### High-Level Flow

```
Data Sources → Extractors → Transformers → DWH Loader → Star Schema → Dashboard
```

### Key Components

1. **Extractors** (`etl_engine/extractor/`)
   - CSV, JSON, SQL extractors
   - Batch file processing
   - Source metadata tagging

2. **Transformers** (`etl_engine/transformer/`)
   - `DataCleaner`: Column normalization, null handling
   - `DataValidator`: 11 rule types, detailed reports
   - `Deduplicator`: SHA-256 hashing, flexible strategies
   - `DimensionBuilder`: Star Schema dimension extraction

3. **Loaders** (`etl_engine/loader/`)
   - `DWHLoader`: Star Schema with surrogate keys
   - `DBLoader`: Flat table loading with exports

4. **Pipeline Orchestrator** (`etl_engine/pipeline.py`)
   - Phase-by-phase execution
   - Error recovery
   - Performance tracking

---

## 📊 Dashboard

Flask-based web interface for monitoring and control.

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard homepage |
| `/api/stats` | GET | Database statistics |
| `/api/data` | GET | Paginated table data |
| `/api/run_etl` | POST | Execute ETL pipeline |
| `/api/source_breakdown` | GET | Chart data |

### Dashboard Features

- **Statistics Cards**: Row count, column count, null percentage
- **Data Table**: Paginated view with search/filter
- **Charts**: Bar chart for source distribution
- **ETL Control**: Run pipeline from UI

---

## 🧪 Testing

Comprehensive test suite with 95% coverage.

### Test Modules

```
tests/
├── test_extractor.py       # CSV/JSON/SQL extraction
├── test_transformer.py     # Cleaner, validator, deduplicator
├── test_loader.py          # DBLoader tests
├── test_dwh_loader.py      # Star Schema loader tests
└── test_integration.py     # End-to-end pipeline tests
```

### Running Tests

```bash
# All tests
python -m pytest tests/ -v

# Specific module
python -m pytest tests/test_dwh_loader.py -v

# With coverage
python -m pytest --cov=etl_engine --cov-report=html tests/

# Integration test only
python -m pytest tests/test_integration.py -v
```

---

## 📁 Project Structure

```
BI PROJECT/
├── config/                         # Configuration files
│   ├── star_schema.sql             # Star Schema DDL
│   ├── ecommerce_dwh_config.yaml   # DWH pipeline config
│   └── etl_config.yaml             # Flat table config
├── data/
│   ├── raw/                        # Source data files
│   │   └── ecommerce_sales.csv     # Sample dataset (1000 rows)
│   └── processed/                  # Output databases
│       └── ecommerce_dwh.db        # Star Schema warehouse
├── etl_engine/                     # Core ETL engine
│   ├── extractor/                  # Data extraction
│   │   ├── csv_extractor.py
│   │   ├── json_extractor.py
│   │   └── sql_extractor.py
│   ├── transformer/                # Data transformation
│   │   ├── cleaner.py
│   │   ├── validator.py
│   │   ├── deduplicator.py
│   │   └── dimension_builder.py    # ⭐ Star Schema builder
│   ├── loader/                     # Data loading
│   │   ├── db_loader.py
│   │   └── dwh_loader.py           # ⭐ Star Schema loader
│   ├── utils/                      # Utilities
│   │   ├── logger.py
│   │   └── config.py
│   └── pipeline.py                 # Main orchestrator
├── dashboard/                      # Web interface
│   ├── app.py                      # Flask server
│   └── templates/
│       └── index.html
├── tests/                          # Test suite
│   ├── test_extractor.py
│   ├── test_transformer.py
│   ├── test_loader.py
│   ├── test_dwh_loader.py
│   └── test_integration.py
├── logs/                           # ETL logs (auto-generated)
│   └── etl_YYYYMMDD.log
├── main.py                         # CLI entry point
├── requirements.txt                # Python dependencies
├── ARCHITECTURE.md                 # Detailed system design
├── USER_GUIDE.md                   # User documentation
└── README.md                       # This file
```

---

## 📊 Sample Queries

### Top 10 Customers by Revenue
```sql
SELECT 
    c.customer_name,
    c.country,
    c.customer_segment,
    SUM(f.total_amount) as total_revenue,
    COUNT(*) as order_count
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_key
ORDER BY total_revenue DESC
LIMIT 10;
```

### Category Performance Analysis
```sql
SELECT 
    p.category,
    COUNT(*) as transactions,
    SUM(f.quantity) as units_sold,
    SUM(f.total_amount) as revenue,
    SUM(f.total_amount - f.quantity * p.cost_price) as profit,
    ROUND(AVG(f.total_amount), 2) as avg_order_value
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;
```

### Monthly Sales Trend
```sql
SELECT 
    d.year,
    d.quarter,
    d.month_name,
    COUNT(*) as transactions,
    SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month
ORDER BY d.year, d.month;
```

### Weekend vs Weekday Sales
```sql
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as transactions,
    SUM(f.total_amount) as revenue,
    ROUND(AVG(f.total_amount), 2) as avg_transaction
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend;
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`python main.py --test`)
6. Commit your changes (`git commit -m 'Add AmazingFeature'`)
7. Push to the branch (`git push origin feature/AmazingFeature`)
8. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 👥 Authors

**University BI Project Team**  
- **Course**: Business Intelligence  
- **Year**: 2025-2026  
- **Institution**: University of Science and Technology

---

## 📞 Support

For questions or issues:
- **Documentation**: See [USER_GUIDE.md](USER_GUIDE.md)
- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Issues**: Open a GitHub issue
- **Email**: [Your university email]

---

## 🙏 Acknowledgments

- Course instructors for guidance on dimensional modeling
- Python community for excellent libraries (Pandas, Flask)
- Ralph Kimball for Star Schema methodology
- Open source contributors

---

## 🔮 Roadmap

### Phase 1: Core ETL ✅
- [x] Modular extractors
- [x] Data transformation pipeline
- [x] Star Schema implementation
- [x] Web dashboard
- [x] Comprehensive testing

### Phase 2: Enhancements 🔄
- [ ] Incremental loading (SCD Type 2)
- [ ] Real-time streaming ETL
- [ ] Data lineage tracking
- [ ] Advanced analytics dashboard

### Phase 3: Enterprise 🚀
- [ ] Cloud deployment (AWS/Azure)
- [ ] PostgreSQL/MySQL support
- [ ] Apache Airflow orchestration
- [ ] Machine learning integration

---

**⭐ Star Schema Implementation - Production Ready**

Built with ❤️ for Business Intelligence excellence
