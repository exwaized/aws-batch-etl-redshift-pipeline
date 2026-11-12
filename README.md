# Batch ETL Pipeline — S3 → Glue → Redshift → Dashboard

A batch data-warehousing pipeline: raw sales data lands in S3, an AWS Glue
(PySpark) job cleans it and conforms it into a star schema, the result is
loaded into Redshift, and a BI layer queries it for reporting.

```
S3 (raw CSV) --> Glue ETL job (clean + star schema) --> S3 (Parquet)
                                                            |
                                                            v
                                          Redshift (COPY from S3) --> Dashboard
```

## Why this design
- **S3 raw/processed split**: raw zone is immutable/append-only (audit trail,
  replay-able); processed zone holds analytics-ready Parquet, partitioned by
  `date_key` for partition pruning.
- **Star schema**: `fact_sales` + `dim_date` / `dim_product` / `dim_store` —
  standard Kimball modeling so BI tools can do simple joins instead of
  re-deriving business logic.
- **Glue over EMR**: serverless, pay-per-job, no cluster to manage — right
  fit for a daily/hourly batch job at this scale.
- **Redshift Serverless**: avoids paying for an always-on cluster during
  development/demo.

## Run it locally (no AWS account needed)
This reproduces the same cleaning + star-schema logic as the Glue job
(`local_test/transform.py` mirrors `glue_job/etl_job.py`), using DuckDB as a
stand-in warehouse and a generated HTML dashboard as a stand-in for
QuickSight.

```bash
python raw_data/generate_sample_data.py
python local_test/run_pipeline_local.py
```

This prints revenue-by-category / top-products tables to the console and
writes:
- `local_test/output/*.csv` — the query results
- `local_test/output/dashboard.html` — an interactive chart dashboard (open
  it in a browser)
- `local_test/warehouse.duckdb` — the "warehouse" database file

## Deploy to AWS
1. `aws cloudformation deploy --template-file infra/cloudformation.yaml --stack-name de-intern-batch-etl --parameter-overrides RedshiftAdminPassword=<pw> --capabilities CAPABILITY_NAMED_IAM`
2. Upload `glue_job/etl_job.py` to `s3://<processed-bucket>/scripts/etl_job.py`
   and raw CSVs to `s3://<raw-bucket>/sales/`.
3. Start the Glue job: `aws glue start-job-run --job-name de-intern-batch-etl-etl-job`
4. Load Redshift: `python scripts/load_to_redshift.py --workgroup de-intern-batch-etl-wg --database salesdb --s3-path s3://<processed-bucket>/warehouse --iam-role <RedshiftCopyRoleArn>`
5. Point QuickSight (or any BI tool) at the Redshift workgroup and build
   the same revenue/top-products visuals as `local_test/output/dashboard.html`.

## Files
| Path | Purpose |
|---|---|
| `raw_data/generate_sample_data.py` | Synthetic, intentionally-dirty sales data (dupes, nulls, bad quantities) |
| `glue_job/etl_job.py` | AWS Glue (PySpark) ETL job — cleans raw data, builds star schema, writes Parquet |
| `local_test/transform.py` | Pandas port of the same logic, for local dev/testing |
| `local_test/run_pipeline_local.py` | Runs the full pipeline locally end-to-end |
| `redshift/schema.sql` | Star-schema DDL for Redshift |
| `scripts/load_to_redshift.py` | COPYs Parquet from S3 into Redshift via the Data API |
| `infra/cloudformation.yaml` | S3 buckets, Glue job + IAM role, Redshift Serverless namespace/workgroup |
