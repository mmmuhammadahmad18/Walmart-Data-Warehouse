-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
WITH ranked_products AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        p.product_id,
        p.product_name,
        SUM(f.purchase_amount) AS total_revenue,
        COUNT(*) AS transaction_count,
        RANK() OVER (PARTITION BY d.year, d.month, d.is_weekend 
                     ORDER BY SUM(f.purchase_amount) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2017  -- Changed to 2017 (your data range)
    GROUP BY d.year, d.month, d.month_name, d.is_weekend, p.product_id, p.product_name
)
SELECT 
    year,
    month,
    month_name,
    day_type,
    product_id,
    product_name,
    total_revenue,
    transaction_count,
    revenue_rank
FROM ranked_products
WHERE revenue_rank <= 5
ORDER BY month, day_type, revenue_rank;

-- Q2: Customer Demographics by Purchase Amount with City Category Breakdown
SELECT 
    c.gender,
    c.age_group,
    c.city_category,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    SUM(f.purchase_amount) AS total_purchase_amount,
    AVG(f.purchase_amount) AS avg_purchase_amount,
    MIN(f.purchase_amount) AS min_purchase,
    MAX(f.purchase_amount) AS max_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY c.gender, c.age_group, c.city_category;

-- Q3: Product Category Sales by Occupation
SELECT 
    c.occupation,
    p.product_category_1,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_sales,
    AVG(f.purchase_amount) AS avg_transaction_value,
    SUM(f.quantity) AS total_quantity_sold
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_category_1 IS NOT NULL
GROUP BY c.occupation, p.product_category_1
ORDER BY c.occupation, total_sales DESC;

-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
SELECT 
    d.year,
    d.quarter,
    d.quarter_name,
    c.gender,
    c.age_group,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_purchase_amount,
    AVG(f.purchase_amount) AS avg_purchase_amount
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2017
GROUP BY d.year, d.quarter, d.quarter_name, c.gender, c.age_group
ORDER BY d.quarter, c.gender, c.age_group;

-- Q5: Top Occupations by Product Category Sales
WITH occupation_category_sales AS (
    SELECT 
        p.product_category_1,
        c.occupation,
        SUM(f.purchase_amount) AS total_sales,
        COUNT(*) AS transaction_count,
        RANK() OVER (PARTITION BY p.product_category_1 
                     ORDER BY SUM(f.purchase_amount) DESC) AS sales_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_product p ON f.product_key = p.product_key
    WHERE p.product_category_1 IS NOT NULL
    GROUP BY p.product_category_1, c.occupation
)
SELECT 
    product_category_1,
    occupation,
    total_sales,
    transaction_count,
    sales_rank
FROM occupation_category_sales
WHERE sales_rank <= 5
ORDER BY product_category_1, sales_rank;

-- Q6: City Category Performance by Marital Status with Monthly Breakdown
SELECT 
    d.year,
    d.month,
    d.month_name,
    c.city_category,
    c.marital_status,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_purchase_amount,
    AVG(f.purchase_amount) AS avg_purchase_amount
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year=2016
GROUP BY d.year, d.month, d.month_name, c.city_category, c.marital_status
ORDER BY d.year, d.month, c.city_category, c.marital_status;

-- Q7: Average Purchase Amount by Stay Duration and Gender
SELECT 
    c.stay_in_current_city_years,
    c.gender,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(f.purchase_amount) AS total_purchase_amount,
    AVG(f.purchase_amount) AS avg_purchase_amount,
    STDDEV(f.purchase_amount) AS stddev_purchase_amount
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;

-- Q8: Top 5 Revenue-Generating Cities by Product Category
WITH city_product_revenue AS (
    SELECT 
        p.product_category_1,
        c.city_category,
        SUM(f.purchase_amount) AS total_revenue,
        COUNT(*) AS transaction_count,
        RANK() OVER (PARTITION BY p.product_category_1 
                     ORDER BY SUM(f.purchase_amount) DESC) AS revenue_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_product p ON f.product_key = p.product_key
    WHERE p.product_category_1 IS NOT NULL
    GROUP BY p.product_category_1, c.city_category
)
SELECT 
    product_category_1,
    city_category,
    total_revenue,
    transaction_count,
    revenue_rank
FROM city_product_revenue
WHERE revenue_rank <= 5
ORDER BY product_category_1, revenue_rank;

-- Q9: Monthly Sales Growth by Product Category
WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        p.product_category_1,
        SUM(f.purchase_amount) AS monthly_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2016
        AND p.product_category_1 IS NOT NULL
    GROUP BY d.year, d.month, d.month_name, p.product_category_1
),
growth_calculation AS (
    SELECT 
        year,
        month,
        month_name,
        product_category_1,
        monthly_revenue,
        LAG(monthly_revenue) OVER (PARTITION BY product_category_1 ORDER BY month) AS prev_month_revenue
    FROM monthly_sales
)
SELECT 
    year,
    month,
    month_name,
    product_category_1,
    monthly_revenue,
    prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NULL THEN NULL
        WHEN prev_month_revenue = 0 THEN NULL
        ELSE ROUND(((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100)::NUMERIC, 2)
    END AS growth_percentage
FROM growth_calculation
ORDER BY product_category_1, month;

-- Q10: Weekend vs. Weekday Sales by Age Group
SELECT 
    c.age_group,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_sales,
    AVG(f.purchase_amount) AS avg_transaction_value,
    SUM(f.quantity) AS total_quantity
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2016
GROUP BY c.age_group, d.is_weekend
ORDER BY c.age_group, day_type;

-- Q11: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down 
WITH product_revenue AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        d.is_weekend,
        p.product_id,
        p.product_name,
        SUM(f.purchase_amount) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY d.year, d.month, d.is_weekend 
                          ORDER BY SUM(f.purchase_amount) DESC) AS rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2017  -- Specify year as needed
    GROUP BY d.year, d.month, d.month_name, d.is_weekend, p.product_id, p.product_name
)
SELECT 
    year,
    month,
    month_name,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    product_id,
    product_name,
    total_revenue,
    rank
FROM product_revenue
WHERE rank <= 5
ORDER BY year, month, is_weekend, rank;

-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH quarterly_revenue AS (
    SELECT 
        s.store_id,
        s.store_name,
        d.year,
        d.quarter,
        d.quarter_name,
        SUM(f.purchase_amount) AS quarterly_revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_key = s.store_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = 2017
    GROUP BY s.store_id, s.store_name, d.year, d.quarter, d.quarter_name
),
growth_calc AS (
    SELECT 
        store_id,
        store_name,
        year,
        quarter,
        quarter_name,
        quarterly_revenue,
        LAG(quarterly_revenue) OVER (PARTITION BY store_id ORDER BY quarter) AS prev_quarter_revenue
    FROM quarterly_revenue
)
SELECT 
    store_id,
    store_name,
    year,
    quarter,
    quarter_name,
    quarterly_revenue,
    prev_quarter_revenue,
    CASE 
        WHEN prev_quarter_revenue IS NULL THEN NULL
        WHEN prev_quarter_revenue = 0 THEN NULL
        ELSE ROUND(((quarterly_revenue - prev_quarter_revenue) / prev_quarter_revenue * 100)::NUMERIC, 2)
    END AS growth_rate_percentage
FROM growth_calc
ORDER BY store_id, quarter;

-- Q13: Detailed Supplier Sales Contribution by Store and Product Name
SELECT 
    s.store_id,
    s.store_name,
    p.supplier_name,
    p.product_name,
    p.product_id,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity,
    AVG(f.purchase_amount) AS avg_sale_amount
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.supplier_name IS NOT NULL
GROUP BY s.store_id, s.store_name, p.supplier_name, p.product_name, p.product_id
ORDER BY s.store_id, p.supplier_name, total_sales DESC;

-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT 
    d.season,
    p.product_category_1,
    p.product_id,
    p.product_name,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_sales,
    AVG(f.purchase_amount) AS avg_sale_amount,
    SUM(f.quantity) AS total_quantity
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE p.product_category_1 IS NOT NULL
GROUP BY d.season, p.product_category_1, p.product_id, p.product_name
ORDER BY d.season, p.product_category_1, total_sales DESC;

-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH monthly_revenue AS (
    SELECT 
        s.store_id,
        s.store_name,
        p.supplier_name,
        d.year,
        d.month,
        d.month_name,
        SUM(f.purchase_amount) AS monthly_revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_key = s.store_key
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE p.supplier_name IS NOT NULL
    GROUP BY s.store_id, s.store_name, p.supplier_name, d.year, d.month, d.month_name
),
volatility_calc AS (
    SELECT 
        store_id,
        store_name,
        supplier_name,
        year,
        month,
        month_name,
        monthly_revenue,
        LAG(monthly_revenue) OVER (PARTITION BY store_id, supplier_name ORDER BY year, month) AS prev_month_revenue
    FROM monthly_revenue
)
SELECT 
    store_id,
    store_name,
    supplier_name,
    year,
    month,
    month_name,
    monthly_revenue,
    prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NULL THEN NULL
        WHEN prev_month_revenue = 0 THEN NULL
        ELSE ROUND(((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100)::NUMERIC, 2)
    END AS volatility_percentage
FROM volatility_calc
ORDER BY store_id, supplier_name, year, month;

-- Q16: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
WITH product_pairs AS (
    SELECT 
        f1.product_id AS product_1,
        f2.product_id AS product_2,
        p1.product_name AS product_1_name,
        p2.product_name AS product_2_name,
        COUNT(*) AS co_purchase_count
    FROM fact_sales f1
    JOIN fact_sales f2 ON f1.date_key = f2.date_key 
                       AND f1.customer_key = f2.customer_key
                       AND f1.product_id < f2.product_id  -- Avoid duplicates
    JOIN dim_product p1 ON f1.product_key = p1.product_key
    JOIN dim_product p2 ON f2.product_key = p2.product_key
    GROUP BY f1.product_id, f2.product_id, p1.product_name, p2.product_name
)
SELECT 
    product_1,
    product_1_name,
    product_2,
    product_2_name,
    co_purchase_count,
    RANK() OVER (ORDER BY co_purchase_count DESC) AS affinity_rank
FROM product_pairs
ORDER BY co_purchase_count DESC
LIMIT 5;

-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT 
    COALESCE(s.store_name, 'ALL STORES') AS store_name,
    COALESCE(p.supplier_name, 'ALL SUPPLIERS') AS supplier_name,
    COALESCE(p.product_name, 'ALL PRODUCTS') AS product_name,
    d.year,
    COUNT(*) AS transaction_count,
    SUM(f.purchase_amount) AS total_revenue,
    AVG(f.purchase_amount) AS avg_transaction_value
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE p.supplier_name IS NOT NULL
GROUP BY ROLLUP(s.store_name, p.supplier_name, p.product_name, d.year)
ORDER BY store_name, supplier_name, product_name, d.year;

-- Q18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
SELECT 
    p.product_id,
    p.product_name,
    p.product_category_1,
    d.year,
    SUM(CASE WHEN d.month <= 6 THEN f.purchase_amount ELSE 0 END) AS h1_revenue,
    SUM(CASE WHEN d.month > 6 THEN f.purchase_amount ELSE 0 END) AS h2_revenue,
    SUM(CASE WHEN d.month <= 6 THEN f.quantity ELSE 0 END) AS h1_quantity,
    SUM(CASE WHEN d.month > 6 THEN f.quantity ELSE 0 END) AS h2_quantity,
    SUM(f.purchase_amount) AS yearly_revenue,
    SUM(f.quantity) AS yearly_quantity,
    CASE 
        WHEN SUM(CASE WHEN d.month <= 6 THEN f.purchase_amount ELSE 0 END) = 0 THEN NULL
        ELSE ROUND(((SUM(CASE WHEN d.month > 6 THEN f.purchase_amount ELSE 0 END) - 
                     SUM(CASE WHEN d.month <= 6 THEN f.purchase_amount ELSE 0 END)) / 
                     SUM(CASE WHEN d.month <= 6 THEN f.purchase_amount ELSE 0 END) * 100)::NUMERIC, 2)
    END AS h1_to_h2_growth_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY p.product_id, p.product_name, p.product_category_1, d.year
ORDER BY d.year, yearly_revenue DESC;

-- Q19: Identify High Revenue Spikes in Product Sales and Highlight Outliers
WITH daily_product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        d.full_date,
        d.day_of_week,
        SUM(f.purchase_amount) AS daily_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY p.product_id, p.product_name, d.full_date, d.day_of_week
),
product_avg_sales AS (
    SELECT 
        product_id,
        product_name,
        AVG(daily_revenue) AS avg_daily_revenue,
        STDDEV(daily_revenue) AS stddev_daily_revenue
    FROM daily_product_sales
    GROUP BY product_id, product_name
)
SELECT 
    dps.product_id,
    dps.product_name,
    dps.full_date,
    dps.day_of_week,
    dps.daily_revenue,
    pas.avg_daily_revenue,
    pas.stddev_daily_revenue,
    ROUND((dps.daily_revenue / NULLIF(pas.avg_daily_revenue, 0))::NUMERIC, 2) AS revenue_multiplier,
    CASE 
        WHEN dps.daily_revenue > (2 * pas.avg_daily_revenue) THEN 'SPIKE - High Outlier'
        WHEN dps.daily_revenue < (pas.avg_daily_revenue * 0.5) THEN 'LOW - Potential Issue'
        ELSE 'Normal'
    END AS anomaly_flag
FROM daily_product_sales dps
JOIN product_avg_sales pas ON dps.product_id = pas.product_id
WHERE dps.daily_revenue > (2 * pas.avg_daily_revenue)
    OR dps.daily_revenue < (pas.avg_daily_revenue * 0.5)
ORDER BY dps.product_id, dps.full_date;

-- Q20: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
SELECT 
    store_name,
    year,
    quarter,
    quarter_name,
    total_sales,
    transaction_count,
    LAG(total_sales) OVER (PARTITION BY store_name ORDER BY year, quarter) AS prev_quarter_sales,
    CASE 
        WHEN LAG(total_sales) OVER (PARTITION BY store_name ORDER BY year, quarter) IS NULL THEN NULL
        WHEN LAG(total_sales) OVER (PARTITION BY store_name ORDER BY year, quarter) = 0 THEN NULL
        ELSE ROUND(((total_sales - LAG(total_sales) OVER (PARTITION BY store_name ORDER BY year, quarter)) / 
                     LAG(total_sales) OVER (PARTITION BY store_name ORDER BY year, quarter) * 100)::NUMERIC, 2)
    END AS quarter_over_quarter_growth
FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter;