"""
Generates a synthetic, intentionally messy retail-sales dataset to stand in for
a raw landing-zone file (what would normally arrive in s3://<raw-bucket>/sales/).

Messiness is deliberate: duplicate order_ids, a few nulls, a few negative
quantities (returns/data errors). The ETL job is responsible for cleaning
these before they reach the warehouse.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
random.seed(7)
Faker.seed(7)

OUT_PATH = Path(__file__).parent / "sales_raw.csv"

CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Books", "Sports", "Toys"]
REGIONS = ["US-EAST", "US-WEST", "EU-CENTRAL", "APAC"]

N_PRODUCTS = 40
N_STORES = 12
N_ORDERS = 5000

products = [
    {
        "product_id": f"P{i:04d}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "unit_price": round(random.uniform(5, 500), 2),
    }
    for i in range(N_PRODUCTS)
]

stores = [
    {
        "store_id": f"S{i:03d}",
        "store_name": f"{fake.city()} Store",
        "region": random.choice(REGIONS),
    }
    for i in range(N_STORES)
]

start_date = datetime(2025, 1, 1)
rows = []
for i in range(N_ORDERS):
    order_date = start_date + timedelta(days=random.randint(0, 364))
    product = random.choice(products)
    store = random.choice(stores)
    quantity = random.randint(1, 8)

    row = {
        "order_id": f"O{i:06d}",
        "order_date": order_date.strftime("%Y-%m-%d"),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "unit_price": product["unit_price"],
        "store_id": store["store_id"],
        "store_name": store["store_name"],
        "region": store["region"],
        "quantity": quantity,
        "customer_id": f"C{random.randint(1, 1500):05d}",
    }
    rows.append(row)

    # inject dirtiness: ~2% exact duplicate rows
    if random.random() < 0.02:
        rows.append(dict(row))

    # ~1% missing product_name
    if random.random() < 0.01:
        rows[-1] = dict(rows[-1])
        rows[-1]["product_name"] = ""

    # ~0.5% negative quantity (bad data entry)
    if random.random() < 0.005:
        rows[-1] = dict(rows[-1])
        rows[-1]["quantity"] = -row["quantity"]

random.shuffle(rows)

fieldnames = list(rows[0].keys())
with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH}")
