"""
Pandas/DuckDB re-implementation of glue_job/etl_job.py's cleaning and
star-schema logic, so the pipeline can be built and verified on a laptop
with no Spark/Glue/AWS dependency. The business rules here must stay in
sync with the Glue job.
"""
import pandas as pd


def clean_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["order_id"])
    df = df[df["product_name"].notna() & (df["product_name"] != "")]
    df = df[df["quantity"] > 0]
    df["unit_price"] = df["unit_price"].astype(float)
    df["quantity"] = df["quantity"].astype(int)
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df.reset_index(drop=True)


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["product_id", "product_name", "category", "unit_price"]]
        .drop_duplicates(subset=["product_id"])
        .reset_index(drop=True)
    )
    dim.insert(0, "product_key", dim.index + 1)
    return dim


def build_dim_store(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["store_id", "store_name", "region"]]
        .drop_duplicates(subset=["store_id"])
        .reset_index(drop=True)
    )
    dim.insert(0, "store_key", dim.index + 1)
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = df["order_date"].drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame({"order_date": dates})
    dim["date_key"] = dim["order_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["order_date"].dt.year
    dim["month"] = dim["order_date"].dt.month
    dim["day"] = dim["order_date"].dt.day
    dim["quarter"] = dim["order_date"].dt.quarter
    dim["day_of_week"] = dim["order_date"].dt.dayofweek + 1
    return dim


def build_fact_sales(
    df: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    merged = df.merge(
        dim_product[["product_id", "product_key"]], on="product_id"
    ).merge(dim_store[["store_id", "store_key"]], on="store_id").merge(
        dim_date[["order_date", "date_key"]], on="order_date"
    )
    merged["total_amount"] = merged["quantity"] * merged["unit_price"]
    return merged[
        [
            "order_id",
            "date_key",
            "product_key",
            "store_key",
            "customer_id",
            "quantity",
            "unit_price",
            "total_amount",
        ]
    ]


def run_pipeline(raw_df: pd.DataFrame):
    cleaned = clean_raw(raw_df)
    dim_product = build_dim_product(cleaned)
    dim_store = build_dim_store(cleaned)
    dim_date = build_dim_date(cleaned)
    fact_sales = build_fact_sales(cleaned, dim_product, dim_store, dim_date)
    return dim_date, dim_product, dim_store, fact_sales
