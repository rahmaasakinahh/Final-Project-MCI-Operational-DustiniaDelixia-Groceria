CREATE DATABASE IF NOT EXISTS dustinia_db;

-- fact_deliveries — tabel utama denormalized

CREATE TABLE IF NOT EXISTS dustinia_db.fact_deliveries (
    order_id  String,
    customer_id  String,
    seller_id  String,
    product_id  String,
    order_purchase_timestamp  Nullable(DateTime),
    order_approved_at  Nullable(DateTime),
    order_delivered_carrier_date  Nullable(DateTime),
    order_delivered_customer_date  Nullable(DateTime),
    order_estimated_delivery_date  Nullable(DateTime),
    t1_approval_h  Nullable(Float32),
    t2_processing_h  Nullable(Float32),
    t3_shipping_h  Nullable(Float32),
    delivery_days  Nullable(Int32),
    estimated_days  Nullable(Int32),
    days_late  Nullable(Int32),
    actual_vs_est_ratio  Nullable(Float32),
    is_late  UInt8,
    purchase_month  UInt8,
    purchase_dayofweek  UInt8,
    purchase_hour  UInt8,
    customer_state  String,
    customer_city  String,
    seller_state  String,
    seller_city  String,
    seller_cohort  String,
    freight_bucket  String,
    product_category_name  String,
    product_category_name_en  String,
    price  Nullable(Float32),
    freight_value  Nullable(Float32),
    review_score  Float32
) ENGINE = MergeTree()
PARTITION BY purchase_month
ORDER BY (order_id)
SETTINGS index_granularity = 8192;

-- dim_sellers

CREATE TABLE IF NOT EXISTS dustinia_db.dim_sellers (
    seller_id  String,
    seller_zip_code_prefix  String,
    seller_city  String,
    seller_state  String
) ENGINE = MergeTree()
ORDER BY seller_id;

-- dim_customers

CREATE TABLE IF NOT EXISTS dustinia_db.dim_customers (
    customer_id  String,
    customer_unique_id  String,
    customer_zip_code_prefix  String,
    customer_city  String,
    customer_state  String
) ENGINE = MergeTree()
ORDER BY customer_id;

-- dim_products

CREATE TABLE IF NOT EXISTS dustinia_db.dim_products (
    product_id  String,
    product_category_name  String,
    product_category_name_en  String,
    product_weight_g  Nullable(Float32),
    product_length_cm  Nullable(Float32),
    product_height_cm  Nullable(Float32),
    product_width_cm  Nullable(Float32)
) ENGINE = MergeTree()
ORDER BY product_id;

-- agg_seller_performance — hanya seller aktif >=20 orders
-- is_chronic = >=20 orders DAN >20% late rate

CREATE TABLE IF NOT EXISTS dustinia_db.agg_seller_performance (
    seller_id  String,
    seller_state  String,
    seller_city  String,
    seller_cohort  String,
    total_orders  UInt32,
    late_orders  UInt32,
    late_rate  Float32,
    avg_t2_processing_h  Float32,
    avg_delivery_days  Float32,
    total_revenue  Float32,
    is_chronic  UInt8,
    is_new_problem  UInt8
) ENGINE = MergeTree()
ORDER BY seller_id;

-- agg_regional_stats

CREATE TABLE IF NOT EXISTS dustinia_db.agg_regional_stats (
    customer_state  String,
    total_orders  UInt32,
    late_orders  UInt32,
    late_rate  Float32,
    avg_delivery_days  Float32,
    avg_t3_shipping_h  Float32
) ENGINE = MergeTree()
ORDER BY customer_state;

-- agg_monthly_trend — per year_month bukan hanya bulan

CREATE TABLE IF NOT EXISTS dustinia_db.agg_monthly_trend (
    year_month  String,
    total_orders  UInt32,
    late_orders  UInt32,
    late_rate  Float32,
    avg_delivery_days  Float32
) ENGINE = MergeTree()
ORDER BY year_month;

-- agg_cohort_performance — performa per cohort seller

CREATE TABLE IF NOT EXISTS dustinia_db.agg_cohort_performance (
    seller_cohort  String,
    seller_count  UInt32,
    total_orders  UInt32,
    late_orders  UInt32,
    late_rate  Float32,
    avg_delivery_days  Float32
) ENGINE = MergeTree()
ORDER BY seller_cohort;

-- kpi_summary — 5 KPI pre-computed untuk dashboard Metabase

CREATE TABLE IF NOT EXISTS dustinia_db.kpi_summary (
    kpi_name  String,
    actual_value  Float32,
    target_value  Float32,
    target_operator  String,
    status  String,
    unit  String
) ENGINE = MergeTree()
ORDER BY kpi_name;

-- fact_late_risk — hasil prediksi XGBoost per order

CREATE TABLE IF NOT EXISTS dustinia_db.fact_late_risk (
    order_id  String,
    customer_state  String,
    seller_state  String,
    seller_cohort  String,
    purchase_month  UInt8,
    freight_bucket  String,
    late_probability  Float32,
    late_prediction  UInt8,
    risk_level  String,
    is_late  UInt8
) ENGINE = MergeTree()
ORDER BY order_id;