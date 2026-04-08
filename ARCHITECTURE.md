# System Architecture

## Custom ETL Engine with Star Schema Data Warehouse
### Business Intelligence Project - University Course 2025-2026

---

## 🏗️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────┐  │
│  │   CSV Files │  │ JSON Files  │  │  SQLite DB  │  │   APIs   │  │
│  │  (Batch)    │  │  (Nested)   │  │  (Queries)  │  │ (Future) │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └────┬─────┘  │
└─────────┼─────────────────┼─────────────────┼──────────────┼────────┘
          │                 │                 │              │
          └─────────────────┴─────────────────┴──────────────┘
                                    │
               ┌────────────────────▼────────────────────┐
               │        EXTRACTION LAYER                 │
               │  ┌──────────────────────────────────┐  │
               │  │  CSVExtractor                    │  │
               │  │  • Batch file loading            │  │
               │  │  • Delimiter/encoding config     │  │
               │  │  • Source metadata tagging       │  │
               │  ├──────────────────────────────────┤  │
               │  │  JSONExtractor                   │  │
               │  │  • Nested record flattening      │  │
               │  │  • record_path support           │  │
               │  │  • Metadata field extraction     │  │
               │  ├──────────────────────────────────┤  │
               │  │  SQLExtractor (Optional)         │  │
               │  │  • Parameterized queries         │  │
               │  │  • Full table extraction         │  │
               │  └──────────────────────────────────┘  │
               └────────────────┬────────────────────────┘
                                │
               ┌────────────────▼────────────────────┐
               │     TRANSFORMATION LAYER            │
               │  ┌──────────────────────────────────┐  │
               │  │  1. DataCleaner                  │  │
               │  │     • Column name normalization  │  │
               │  │     • Whitespace removal         │  │
               │  │     • Null value handling        │  │
               │  │     • Type casting               │  │
               │  │     • Quality reporting          │  │
               │  ├──────────────────────────────────┤  │
               │  │  2. DataValidator                │  │
               │  │     • Required field checks      │  │
               │  │     • Regex pattern matching     │  │
               │  │     • Numeric bounds validation  │  │
               │  │     • Enum value validation      │  │
               │  │     • Type consistency checks    │  │
               │  ├──────────────────────────────────┤  │
               │  │  3. Deduplicator                 │  │
               │  │     • Exact duplicate removal    │  │
               │  │     • Key-based deduplication    │  │
               │  │     • SHA-256 row hashing        │  │
               │  │     • Configurable keep strategy │  │
               │  ├──────────────────────────────────┤  │
               │  │  4. DimensionBuilder (Star)      │  │
               │  │     • Extract unique customers   │  │
               │  │     • Extract unique products    │  │
               │  │     • Generate date dimension    │  │
               │  │     • Apply business rules       │  │
               │  └──────────────────────────────────┘  │
               └────────────────┬────────────────────────┘
                                │
               ┌────────────────▼────────────────────┐
               │         LOADING LAYER               │
               │  ┌──────────────────────────────────┐  │
               │  │  DWHLoader (Star Schema Mode)    │  │
               │  │  • Create dimension tables       │  │
               │  │  • Surrogate key management      │  │
               │  │  • SCD Type 1 loading            │  │
               │  │  • Foreign key resolution        │  │
               │  │  • Referential integrity check   │  │
               │  ├──────────────────────────────────┤  │
               │  │  DBLoader (Flat Table Mode)      │  │
               │  │  • Single table loading          │  │
               │  │  • CSV/JSON export               │  │
               │  │  • Replace/append modes          │  │
               │  └──────────────────────────────────┘  │
               └────────────────┬────────────────────────┘
                                │
    ┌───────────────────────────▼────────────────────────────┐
    │              SQLITE DATA WAREHOUSE                     │
    │  ┌─────────────────────────────────────────────────┐  │
    │  │          ⭐ STAR SCHEMA DESIGN ⭐                │  │
    │  │                                                  │  │
    │  │  ┌────────────────────────────────────────────┐ │  │
    │  │  │         FACT TABLE: fact_sales             │ │  │
    │  │  │  ┌──────────────────────────────────────┐  │ │  │
    │  │  │  │  sale_id (PK)                        │  │ │  │
    │  │  │  │  date_key (FK) ──────────────┐       │  │ │  │
    │  │  │  │  customer_key (FK) ───────┐  │       │  │ │  │
    │  │  │  │  product_key (FK) ─────┐  │  │       │  │ │  │
    │  │  │  │  order_id             │  │  │       │  │ │  │
    │  │  │  │  quantity             │  │  │       │  │ │  │
    │  │  │  │  unit_price           │  │  │       │  │ │  │
    │  │  │  │  discount             │  │  │       │  │ │  │
    │  │  │  │  total_amount         │  │  │       │  │ │  │
    │  │  │  └──────────────────────────────────────┘  │ │  │
    │  │  └────────────────┬───────────┬───────────────┘ │  │
    │  │                   │           │          │       │  │
    │  │   ┌───────────────┘           │          │       │  │
    │  │   │  DIMENSIONS                │          │       │  │
    │  │   │                            │          │       │  │
    │  │   ▼                            ▼          ▼       │  │
    │  │  ┌───────────┐   ┌──────────────┐  ┌──────────┐ │  │
    │  │  │dim_date   │   │dim_customer  │  │dim_product│ │  │
    │  │  ├───────────┤   ├──────────────┤  ├──────────┤ │  │
    │  │  │date_key PK│   │customer_key  │  │product_  │ │  │
    │  │  │full_date  │   │customer_id   │  │  key PK  │ │  │
    │  │  │year       │   │name          │  │product_id│ │  │
    │  │  │quarter    │   │email         │  │name      │ │  │
    │  │  │month      │   │country       │  │category  │ │  │
    │  │  │day        │   │city          │  │subcategory│ │  │
    │  │  │is_weekend │   │segment       │  │brand     │ │  │
    │  │  │...        │   │...           │  │cost_price│ │  │
    │  │  └───────────┘   └──────────────┘  └──────────┘ │  │
    │  │                                                  │  │
    │  │  ✓ Optimized for analytical queries             │  │
    │  │  ✓ Denormalized dimensions for fast joins       │  │
    │  │  ✓ Surrogate keys for flexibility               │  │
    │  │  ✓ Time dimension for trend analysis            │  │
    │  └─────────────────────────────────────────────────┘  │
    └───────────────────────────┬────────────────────────────┘
                                │
               ┌────────────────▼────────────────────┐
               │       PRESENTATION LAYER            │
               │  ┌──────────────────────────────────┐  │
               │  │  Flask Web Dashboard             │  │
               │  │  • Real-time statistics cards    │  │
               │  │  • Paginated data table          │  │
               │  │  • Source breakdown chart        │  │
               │  │  • One-click ETL execution       │  │
               │  │  • RESTful API endpoints         │  │
               │  └──────────────────────────────────┘  │
               └─────────────────────────────────────────┘
```

---

## 📊 Star Schema Design Rationale

### Why Star Schema?

1. **Query Performance**
   - Denormalized dimensions = fewer joins
   - Optimized for analytical queries (OLAP)
   - Fast aggregations and filtering

2. **Simplicity**
   - Easy to understand for business users
   - Clear separation of facts and dimensions
   - Simple to maintain and extend

3. **BI Tool Compatibility**
   - Industry standard for data warehouses
   - Works seamlessly with Power BI, Tableau, Qlik
   - Supports drill-down and roll-up analysis

4. **Flexibility**
   - Surrogate keys allow source system changes
   - Easy to add new dimensions
   - Supports slowly changing dimensions (SCD)

### Schema Components

**Fact Table**: `fact_sales`
- **Grain**: One row per transaction (order line)
- **Measures**: quantity, unit_price, discount, total_amount (additive)
- **Foreign Keys**: Links to all dimension tables
- **Degenerate Dimension**: order_id (transaction identifier)

**Dimension Tables**:
- `dim_date`: Time dimension (365+ rows per year)
  - Enables temporal analysis (trends, seasonality, YoY comparisons)
  - Pre-calculated attributes (quarter, is_weekend, fiscal_year)
  
- `dim_customer`: Who bought (SCD Type 1)
  - Geographic hierarchy: country → city → region
  - Customer segmentation (B2B, B2C, Premium)
  - Contact information (email)
  
- `dim_product`: What was bought (SCD Type 1)
  - Product hierarchy: category → subcategory → brand
  - Pricing information (cost_price for margin analysis)
  - Active/inactive flag

---

## 🔄 ETL Pipeline Flow

### Phase-by-Phase Execution

```
1. EXTRACTION (Parallel)
   ├─ Load CSV files (batch support)
   ├─ Parse JSON (nested flattening)
   ├─ Query SQL databases
   └─ Add source metadata tags

2. TRANSFORMATION (Sequential)
   ├─ Cleaner: Standardize & clean data
   ├─ Validator: Check data quality rules
   ├─ Deduplicator: Remove duplicate records
   └─ DimensionBuilder: Extract dimensions (Star mode only)

3. LOADING (Transaction-safe)
   ├─ Create Star Schema tables (if not exists)
   ├─ Load dimensions with surrogate key management
   ├─ Load fact table with foreign key resolution
   └─ Verify referential integrity

4. VERIFICATION
   ├─ Check row counts
   ├─ Validate foreign key relationships
   ├─ Generate quality report
   └─ Log performance metrics
```

### Performance Optimization

- **Batch Processing**: Load data in configurable chunks (default 1000 rows)
- **Index Creation**: Automatic indexes on foreign keys and frequently queried columns
- **Type Inference**: Smart data type detection for optimal storage
- **Parallel Extraction**: Multiple sources loaded concurrently (future enhancement)

---

## 🔧 Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Language** | Python 3.11+ | Core ETL logic |
| **Data Processing** | Pandas, NumPy | DataFrame operations |
| **Database** | SQLite | Data warehouse storage |
| **Web Framework** | Flask | Dashboard backend |
| **Visualization** | Chart.js | Interactive charts |
| **Configuration** | PyYAML | Pipeline configuration |
| **Testing** | unittest, pytest | Quality assurance |
| **Logging** | Python logging | Audit trail |

---

## 📁 Project Structure

```
BI PROJECT/
├── config/
│   ├── star_schema.sql           # Star Schema DDL
│   ├── ecommerce_dwh_config.yaml # Star Schema pipeline config
│   └── etl_config.yaml            # Flat table pipeline config
├── data/
│   ├── raw/                       # Source data files
│   └── processed/                 # Output databases
├── etl_engine/
│   ├── extractor/                 # Data extraction modules
│   ├── transformer/               # Data transformation modules
│   │   ├── cleaner.py
│   │   ├── validator.py
│   │   ├── deduplicator.py
│   │   └── dimension_builder.py   # ⭐ Star Schema builder
│   ├── loader/                    # Data loading modules
│   │   ├── db_loader.py           # Flat table loader
│   │   └── dwh_loader.py          # ⭐ Star Schema loader
│   ├── utils/                     # Helper utilities
│   └── pipeline.py                # Main orchestrator
├── dashboard/
│   ├── app.py                     # Flask web server
│   └── templates/                 # HTML templates
├── tests/                         # Unit & integration tests
├── logs/                          # ETL execution logs
├── main.py                        # CLI entry point
└── requirements.txt               # Python dependencies
```

---

## 🎯 Design Principles

1. **Modularity**: Each component has a single responsibility
2. **Configurability**: YAML-driven pipeline configuration
3. **Extensibility**: Easy to add new extractors, transformers, loaders
4. **Observability**: Comprehensive logging at all stages
5. **Data Quality**: Built-in validation and cleaning
6. **Performance**: Optimized for batch processing
7. **Reliability**: Error handling and recovery mechanisms

---

## 📈 Typical Use Cases

### 1. Sales Analysis
```sql
-- Monthly sales trend
SELECT d.year, d.month_name, SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

### 2. Customer Segmentation
```sql
-- Top customers by segment
SELECT c.customer_segment, COUNT(DISTINCT c.customer_key) as customers,
       SUM(f.total_amount) as revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment;
```

### 3. Product Performance
```sql
-- Category profitability
SELECT p.category, 
       SUM(f.total_amount) as revenue,
       SUM(f.quantity * p.cost_price) as cost,
       SUM(f.total_amount - f.quantity * p.cost_price) as profit
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY profit DESC;
```

---

## 🔮 Future Enhancements

1. **Scalability**
   - Migrate to PostgreSQL/MySQL for larger datasets
   - Implement distributed processing (Apache Spark)
   - Add data partitioning strategies

2. **Real-Time Processing**
   - Apache Kafka for streaming ETL
   - Change Data Capture (CDC)
   - Incremental loading with upserts

3. **Advanced Analytics**
   - Integration with ML pipelines
   - Predictive analytics
   - Anomaly detection

4. **Cloud Deployment**
   - AWS Redshift / Google BigQuery
   - Azure Data Factory
   - Airflow for orchestration

5. **Data Governance**
   - Data lineage tracking
   - Schema versioning
   - Audit logging
   - Data masking/encryption

---

**Last Updated**: April 4, 2026  
**Version**: 2.0 (Star Schema Implementation)
