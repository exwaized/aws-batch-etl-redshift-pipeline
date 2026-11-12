"""
End-to-end local run of Project 1: simulates S3 raw -> Glue ETL -> Redshift
warehouse -> BI dashboard, entirely on disk with DuckDB standing in for
Redshift.

Usage:
    python local_test/run_pipeline_local.py
"""
import sys
import webbrowser
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from transform import run_pipeline  # noqa: E402

BASE = Path(__file__).parent.parent
RAW_CSV = BASE / "raw_data" / "sales_raw.csv"
WAREHOUSE_DB = Path(__file__).parent / "warehouse.duckdb"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def main():
    if not RAW_CSV.exists():
        print(f"Raw data not found at {RAW_CSV}. Run raw_data/generate_sample_data.py first.")
        sys.exit(1)

    print(f"[1/4] Loading raw data from {RAW_CSV} (simulates reading from S3 raw zone)")
    raw_df = pd.read_csv(RAW_CSV)
    print(f"       {len(raw_df)} raw rows (includes dupes/nulls/bad quantities)")

    print("[2/4] Running ETL (clean + build star schema) — mirrors glue_job/etl_job.py")
    dim_date, dim_product, dim_store, fact_sales = run_pipeline(raw_df)
    print(
        f"       cleaned to {len(fact_sales)} fact rows, "
        f"{len(dim_product)} products, {len(dim_store)} stores, {len(dim_date)} dates"
    )

    print(f"[3/4] Loading star schema into warehouse ({WAREHOUSE_DB}) — simulates Redshift COPY")
    con = duckdb.connect(str(WAREHOUSE_DB))
    con.execute("CREATE OR REPLACE TABLE dim_date AS SELECT * FROM dim_date")
    con.execute("CREATE OR REPLACE TABLE dim_product AS SELECT * FROM dim_product")
    con.execute("CREATE OR REPLACE TABLE dim_store AS SELECT * FROM dim_store")
    con.execute("CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM fact_sales")

    print("[4/4] Running analytical queries (the kind a BI dashboard would issue)")
    revenue_by_category = con.execute(
        """
        SELECT p.category, ROUND(SUM(f.total_amount), 2) AS revenue, SUM(f.quantity) AS units
        FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category ORDER BY revenue DESC
        """
    ).df()

    revenue_by_region_month = con.execute(
        """
        SELECT s.region, d.year, d.month, ROUND(SUM(f.total_amount), 2) AS revenue
        FROM fact_sales f
        JOIN dim_store s ON f.store_key = s.store_key
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY s.region, d.year, d.month
        ORDER BY d.year, d.month, s.region
        """
    ).df()

    top_products = con.execute(
        """
        SELECT p.product_name, p.category, ROUND(SUM(f.total_amount), 2) AS revenue
        FROM fact_sales f JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_name, p.category
        ORDER BY revenue DESC LIMIT 10
        """
    ).df()

    revenue_by_category.to_csv(OUTPUT_DIR / "revenue_by_category.csv", index=False)
    revenue_by_region_month.to_csv(OUTPUT_DIR / "revenue_by_region_month.csv", index=False)
    top_products.to_csv(OUTPUT_DIR / "top_products.csv", index=False)

    print("\nRevenue by category:")
    print(revenue_by_category.to_string(index=False))
    print("\nTop 10 products:")
    print(top_products.to_string(index=False))

    build_dashboard_html(revenue_by_category, revenue_by_region_month, top_products)
    con.close()
    print(f"\nDone. CSVs + dashboard written to {OUTPUT_DIR}")


def build_dashboard_html(revenue_by_category, revenue_by_region_month, top_products):
    """Writes a static HTML dashboard (Chart.js) as a stand-in for QuickSight."""
    categories = revenue_by_category["category"].tolist()
    cat_revenue = revenue_by_category["revenue"].tolist()

    months = sorted(revenue_by_region_month["month"].unique().tolist())
    regions = revenue_by_region_month["region"].unique().tolist()
    series = []
    for region in regions:
        sub = revenue_by_region_month[revenue_by_region_month["region"] == region]
        by_month = {row["month"]: row["revenue"] for _, row in sub.iterrows()}
        series.append({"label": region, "data": [by_month.get(m, 0) for m in months]})

    top_names = top_products["product_name"].tolist()
    top_revenue = top_products["revenue"].tolist()

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Sales Warehouse Dashboard (local)</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #0b1021; color: #e8e8f0; }}
h1 {{ font-size: 20px; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-top: 16px; }}
.card {{ background: #131a33; border-radius: 10px; padding: 16px; }}
.full {{ grid-column: 1 / -1; }}
</style></head>
<body>
<h1>Sales Warehouse Dashboard — local DuckDB stand-in for Redshift + QuickSight</h1>
<div class="grid">
  <div class="card"><canvas id="catChart"></canvas></div>
  <div class="card"><canvas id="topChart"></canvas></div>
  <div class="card full"><canvas id="regionChart"></canvas></div>
</div>
<script>
new Chart(document.getElementById('catChart'), {{
  type: 'bar',
  data: {{ labels: {categories!r}, datasets: [{{ label: 'Revenue by category', data: {cat_revenue!r} }}] }}
}});
new Chart(document.getElementById('topChart'), {{
  type: 'bar',
  data: {{ labels: {top_names!r}, datasets: [{{ label: 'Top 10 products by revenue', data: {top_revenue!r} }}] }},
  options: {{ indexAxis: 'y' }}
}});
new Chart(document.getElementById('regionChart'), {{
  type: 'line',
  data: {{ labels: {months!r}, datasets: {series!r} }},
  options: {{ plugins: {{ title: {{ display: true, text: 'Monthly revenue by region' }} }} }}
}});
</script>
</body></html>"""
    out_path = OUTPUT_DIR / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")


if __name__ == "__main__":
    main()
