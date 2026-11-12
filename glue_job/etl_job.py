"""
AWS Glue ETL job (PySpark).

Entry point for an AWS Glue Job (Glue version 4.0, Python 3 / Spark 3.x).
Reads raw sales CSV from the raw S3 bucket, cleans and conforms it into a
star schema (dim_date, dim_product, dim_store, fact_sales), and writes the
result as partitioned Parquet to the processed S3 bucket, ready to be loaded
into Redshift via COPY (see scripts/load_to_redshift.py).

Run on AWS Glue with job arguments:
    --RAW_PATH   s3://<raw-bucket>/sales/
    --OUTPUT_PATH s3://<processed-bucket>/warehouse/

For local development/testing without a Glue/Spark environment, use
local_test/run_pipeline_local.py, which implements the same cleaning and
star-schema logic with pandas + DuckDB.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "OUTPUT_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = args["RAW_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"].rstrip("/")


def load_raw(path: str):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def clean(df):
    """Mirrors the cleaning rules in local_test/transform.py::clean_raw."""
    df = df.dropDuplicates(["order_id"])
    df = df.filter(F.col("product_name").isNotNull() & (F.col("product_name") != ""))
    df = df.filter(F.col("quantity") > 0)
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("order_date", F.to_date("order_date"))
    return df


def build_dims_and_fact(df):
    dim_product = (
        df.select("product_id", "product_name", "category", "unit_price")
        .dropDuplicates(["product_id"])
        .withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
    )

    dim_store = (
        df.select("store_id", "store_name", "region")
        .dropDuplicates(["store_id"])
        .withColumn("store_key", F.row_number().over(Window.orderBy("store_id")))
    )

    dim_date = (
        df.select("order_date")
        .dropDuplicates()
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .withColumn("day", F.dayofmonth("order_date"))
        .withColumn("quarter", F.quarter("order_date"))
        .withColumn("day_of_week", F.dayofweek("order_date"))
    )

    fact_sales = (
        df.join(dim_product, "product_id")
        .join(dim_store, "store_id")
        .join(
            dim_date.select("order_date", "date_key"),
            "order_date",
        )
        .withColumn("total_amount", F.col("quantity") * df["unit_price"])
        .select(
            "order_id",
            "date_key",
            dim_product["product_key"],
            dim_store["store_key"],
            "customer_id",
            "quantity",
            df["unit_price"],
            "total_amount",
        )
    )

    return dim_date, dim_product, dim_store, fact_sales


def main():
    raw = load_raw(RAW_PATH)
    cleaned = clean(raw)
    dim_date, dim_product, dim_store, fact_sales = build_dims_and_fact(cleaned)

    dim_date.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/dim_date/")
    dim_product.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/dim_product/")
    dim_store.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/dim_store/")
    fact_sales.write.mode("overwrite").partitionBy("date_key").parquet(
        f"{OUTPUT_PATH}/fact_sales/"
    )

    job.commit()


if __name__ == "__main__":
    main()
