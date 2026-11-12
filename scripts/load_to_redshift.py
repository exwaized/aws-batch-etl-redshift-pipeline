"""
Loads the Parquet star schema produced by glue_job/etl_job.py from S3 into
Redshift using the Redshift Data API (no persistent DB connection/driver
needed from this script's host — IAM auth, works great from a Lambda,
Step Function, or CI job).

Usage:
    python scripts/load_to_redshift.py \\
        --workgroup my-redshift-serverless-wg \\
        --database salesdb \\
        --s3-path s3://my-processed-bucket/warehouse \\
        --iam-role arn:aws:iam::123456789012:role/RedshiftCopyRole

Requires: boto3, and credentials with redshift-data:ExecuteStatement on the
target workgroup/cluster.
"""
import argparse
import time

import boto3

COPY_TEMPLATE = """
COPY {table} FROM '{s3_path}/{table}/'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET;
"""

TABLES = ["dim_date", "dim_product", "dim_store", "fact_sales"]


def run_statement(client, workgroup, database, sql):
    resp = client.execute_statement(
        WorkgroupName=workgroup, Database=database, Sql=sql
    )
    statement_id = resp["Id"]
    while True:
        status = client.describe_statement(Id=statement_id)
        if status["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            return status
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--s3-path", required=True, help="e.g. s3://bucket/warehouse")
    parser.add_argument("--iam-role", required=True)
    parser.add_argument("--schema-file", default="redshift/schema.sql")
    args = parser.parse_args()

    client = boto3.client("redshift-data")

    with open(args.schema_file) as f:
        ddl = f.read()
    print("Applying schema...")
    status = run_statement(client, args.workgroup, args.database, ddl)
    if status["Status"] != "FINISHED":
        raise RuntimeError(f"Schema creation failed: {status}")

    for table in TABLES:
        sql = COPY_TEMPLATE.format(
            table=table, s3_path=args.s3_path.rstrip("/"), iam_role=args.iam_role
        )
        print(f"Loading {table}...")
        status = run_statement(client, args.workgroup, args.database, sql)
        if status["Status"] != "FINISHED":
            raise RuntimeError(f"Load failed for {table}: {status}")
        print(f"  OK ({table})")

    print("All tables loaded.")


if __name__ == "__main__":
    main()
