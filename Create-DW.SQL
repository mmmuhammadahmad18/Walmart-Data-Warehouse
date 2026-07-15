-- Drop existing tables if they exist
use db;
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;

-- DIMENSION TABLES

-- Date Dimension Table
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month INT,
    month_name VARCHAR(15),
    quarter INT,
    quarter_name VARCHAR(10),
    year INT,
    is_weekend BOOLEAN,
    season VARCHAR(10)
);

-- Customer Dimension Table
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT UNIQUE NOT NULL,
    gender CHAR(1),
    age INT,
    age_group VARCHAR(20),
    occupation VARCHAR(50),
    city_category CHAR(1),
    stay_in_current_city_years VARCHAR(10),
    marital_status INT
);

-- Product Dimension Table
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_category_1 VARCHAR(100),
    product_category_2 VARCHAR(100),
    product_category_3 VARCHAR(100),
    product_name VARCHAR(100),
    supplier_name VARCHAR(100)
);

-- Store Dimension Table
CREATE TABLE dim_store (
    store_key INT PRIMARY KEY,
    store_id VARCHAR(20) UNIQUE NOT NULL,
    store_name VARCHAR(100),
    store_location VARCHAR(100)
);

-- FACT TABLE

-- Sales Fact Table
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    user_id INT NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    purchase_amount DECIMAL(10, 2) NOT NULL,
    quantity INT DEFAULT 1,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_date FOREIGN KEY (date_key) 
        REFERENCES dim_date(date_key) ON DELETE CASCADE,
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) 
        REFERENCES dim_customer(customer_key) ON DELETE CASCADE,
    CONSTRAINT fk_product FOREIGN KEY (product_key) 
        REFERENCES dim_product(product_key) ON DELETE CASCADE,
    CONSTRAINT fk_store FOREIGN KEY (store_key) 
        REFERENCES dim_store(store_key) ON DELETE CASCADE
);


-- INDEXES FOR PERFORMANCE OPTIMIZATION

-- Indexes on fact table foreign keys
CREATE INDEX idx_fact_date ON fact_sales(date_key);
CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_product ON fact_sales(product_key);
CREATE INDEX idx_fact_store ON fact_sales(store_key);

-- Indexes on dimension table business keys
CREATE INDEX idx_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_product_id ON dim_product(product_id);
CREATE INDEX idx_store_id ON dim_store(store_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_date_product ON fact_sales(date_key, product_key);
CREATE INDEX idx_fact_date_customer ON fact_sales(date_key, customer_key);


-- VIEWS FOR ANALYSIS

-- View for Store Quarterly Sales (Q20)
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.store_name,
    d.year,
    d.quarter,
    d.quarter_name,
    SUM(f.purchase_amount) AS total_sales,
    COUNT(*) AS transaction_count
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY s.store_name, d.year, d.quarter, d.quarter_name
ORDER BY s.store_name, d.year, d.quarter;


DO $$
BEGIN
    RAISE NOTICE 'Data Warehouse schema created successfully!';
    RAISE NOTICE 'Tables created: dim_date, dim_customer, dim_product, dim_store, fact_sales';
    RAISE NOTICE 'View created: STORE_QUARTERLY_SALES';
END $$;