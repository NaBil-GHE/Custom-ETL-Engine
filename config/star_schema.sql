-- ================================================================
-- STAR SCHEMA DESIGN FOR E-COMMERCE SALES DATA WAREHOUSE
-- ================================================================
-- This schema implements dimensional modeling for BI analytics
-- Optimized for analytical queries and reporting
-- ================================================================
-- ----------------------------------------------------------------
-- FACT TABLE: Sales Transactions (Grain: One row per order line)
-- ----------------------------------------------------------------
CREATE TABLE
    IF NOT EXISTS fact_sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        -- Foreign Keys to Dimensions (surrogate keys)
        date_key INTEGER NOT NULL,
        customer_key INTEGER NOT NULL,
        product_key INTEGER NOT NULL,
        -- Degenerate Dimension (transaction identifier kept in fact)
        order_id TEXT NOT NULL,
        -- Measures (numeric facts - additive)
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price REAL NOT NULL CHECK (unit_price >= 0),
        discount REAL DEFAULT 0 CHECK (
            discount >= 0
            AND discount <= 100
        ),
        total_amount REAL NOT NULL CHECK (total_amount >= 0),
        -- Audit columns
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        -- Foreign Key Constraints
        FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
        FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
        FOREIGN KEY (product_key) REFERENCES dim_product (product_key)
    );

-- ----------------------------------------------------------------
-- DIMENSION TABLE: Date (Time Dimension - Conformed)
-- ----------------------------------------------------------------
-- Critical for time-based analysis (trends, seasonality, YoY)
-- Pre-populated with all dates in range
-- ----------------------------------------------------------------
CREATE TABLE
    IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY, -- Format: YYYYMMDD (e.g., 20260404)
        full_date DATE NOT NULL UNIQUE,
        -- Date components
        day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
        -- Descriptive attributes
        day_of_week TEXT NOT NULL, -- Monday, Tuesday, etc.
        day_of_week_num INTEGER NOT NULL CHECK (day_of_week_num BETWEEN 1 AND 7),
        month_name TEXT NOT NULL, -- January, February, etc.
        -- Flags for filtering
        is_weekend BOOLEAN NOT NULL,
        is_holiday BOOLEAN DEFAULT 0,
        -- Fiscal calendar (if different from calendar year)
        fiscal_year INTEGER,
        fiscal_quarter INTEGER
    );

-- ----------------------------------------------------------------
-- DIMENSION TABLE: Customer (Who bought)
-- ----------------------------------------------------------------
-- Slowly Changing Dimension Type 1 (overwrite on change)
-- Could be extended to SCD Type 2 for historical tracking
-- ----------------------------------------------------------------
CREATE TABLE
    IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        -- Natural/Business Key
        customer_id TEXT UNIQUE NOT NULL,
        -- Descriptive attributes
        customer_name TEXT NOT NULL,
        email TEXT,
        -- Geographic hierarchy
        country TEXT,
        city TEXT,
        region TEXT, -- Can be derived: North Africa, Europe, Americas
        -- Segmentation (for analytics)
        customer_segment TEXT DEFAULT 'B2C', -- B2B, B2C, Premium, VIP
        -- Audit columns
        first_purchase_date DATE,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- ----------------------------------------------------------------
-- DIMENSION TABLE: Product (What was bought)
-- ----------------------------------------------------------------
-- Contains product master data and hierarchy
-- ----------------------------------------------------------------
CREATE TABLE
    IF NOT EXISTS dim_product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        -- Natural/Business Key
        product_id TEXT UNIQUE NOT NULL,
        -- Descriptive attributes
        product_name TEXT NOT NULL,
        -- Product hierarchy (for drill-down analysis)
        category TEXT NOT NULL, -- Electronics, Fashion, Home, etc.
        subcategory TEXT, -- Phones, Laptops, Shoes, etc.
        brand TEXT,
        -- Financial attributes
        cost_price REAL, -- For margin analysis
        -- Product attributes
        is_active BOOLEAN DEFAULT 1,
        -- Audit columns
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- ================================================================
-- INDEXES FOR QUERY PERFORMANCE
-- ================================================================
-- Fact table indexes (for fast joins and filtering)
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_sales (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales (product_key);

CREATE INDEX IF NOT EXISTS idx_fact_order ON fact_sales (order_id);

-- Dimension indexes (for lookups)
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer (customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_country ON dim_customer (country);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product (product_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product (category);

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON dim_date (full_date);

-- ================================================================
-- VIEWS FOR COMMON ANALYTICS QUERIES
-- ================================================================
-- Complete sales view with dimension attributes (star join)
CREATE VIEW
    IF NOT EXISTS v_sales_analysis AS
SELECT
    f.sale_id,
    f.order_id,
    -- Date attributes
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_of_week,
    -- Customer attributes
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.city,
    c.customer_segment,
    -- Product attributes
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    -- Measures
    f.quantity,
    f.unit_price,
    f.discount,
    f.total_amount,
    -- Calculated measures
    (
        f.total_amount - COALESCE(p.cost_price, 0) * f.quantity
    ) AS profit,
    (f.discount / 100.0 * f.unit_price * f.quantity) AS discount_amount
FROM
    fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_product p ON f.product_key = p.product_key;

-- ================================================================
-- SAMPLE ANALYTICAL QUERIES
-- ================================================================
-- Query 1: Monthly sales trend
-- SELECT year, month_name, SUM(total_amount) as revenue
-- FROM v_sales_analysis
-- GROUP BY year, month, month_name
-- ORDER BY year, month;
-- Query 2: Top 10 customers by revenue
-- SELECT customer_name, country, SUM(total_amount) as total_revenue
-- FROM v_sales_analysis
-- GROUP BY customer_id, customer_name, country
-- ORDER BY total_revenue DESC
-- LIMIT 10;
-- Query 3: Product category performance
-- SELECT category, 
--        COUNT(*) as order_count,
--        SUM(quantity) as units_sold,
--        SUM(total_amount) as revenue,
--        AVG(total_amount) as avg_order_value
-- FROM v_sales_analysis
-- GROUP BY category
-- ORDER BY revenue DESC;
-- Query 4: Weekend vs Weekday sales
-- SELECT is_weekend,
--        COUNT(*) as transactions,
--        SUM(total_amount) as revenue
-- FROM v_sales_analysis
-- JOIN dim_date ON v_sales_analysis.full_date = dim_date.full_date
-- GROUP BY is_weekend;