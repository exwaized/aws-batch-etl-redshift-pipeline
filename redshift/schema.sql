-- Star schema for the sales warehouse. Target: Amazon Redshift (or Redshift
-- Serverless). Loaded from Parquet in s3://<processed-bucket>/warehouse/
-- (written by glue_job/etl_job.py) via the COPY commands in
-- scripts/load_to_redshift.py.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,
    order_date   DATE,
    year         SMALLINT,
    month        SMALLINT,
    day          SMALLINT,
    quarter      SMALLINT,
    day_of_week  SMALLINT
)
DISTSTYLE ALL
SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key   INTEGER PRIMARY KEY,
    product_id    VARCHAR(16),
    product_name  VARCHAR(256),
    category      VARCHAR(64),
    unit_price     NUMERIC(10, 2)
)
DISTSTYLE ALL
SORTKEY (product_key);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key   INTEGER PRIMARY KEY,
    store_id    VARCHAR(16),
    store_name  VARCHAR(256),
    region      VARCHAR(32)
)
DISTSTYLE ALL
SORTKEY (store_key);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id      VARCHAR(16),
    date_key      INTEGER REFERENCES dim_date(date_key),
    product_key   INTEGER REFERENCES dim_product(product_key),
    store_key     INTEGER REFERENCES dim_store(store_key),
    customer_id   VARCHAR(16),
    quantity      INTEGER,
    unit_price    NUMERIC(10, 2),
    total_amount  NUMERIC(12, 2)
)
DISTSTYLE KEY
DISTKEY (product_key)
SORTKEY (date_key);

-- Example dashboard queries (what a BI tool / QuickSight would run)
-- Revenue by category:
-- SELECT p.category, SUM(f.total_amount) AS revenue
-- FROM fact_sales f JOIN dim_product p USING (product_key)
-- GROUP BY 1 ORDER BY 2 DESC;
