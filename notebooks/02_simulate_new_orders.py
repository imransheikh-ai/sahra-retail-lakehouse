# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Simulate new data arriving
# MAGIC
# MAGIC *Sahra Retail Lakehouse · by Imran Sheikh · [LinkedIn](https://www.linkedin.com/in/imranazhar/)*
# MAGIC
# MAGIC Real pipelines never run once. This notebook plays the role of the store POS systems and the CRM:
# MAGIC it writes **new** order files (and a small CRM change file) into the landing volume, continuing
# MAGIC from the last order date already in Silver.
# MAGIC
# MAGIC Then refresh the pipeline and watch what happens:
# MAGIC * Bronze streaming tables read **only the new files** (Auto Loader remembers what it has seen).
# MAGIC * Silver and Gold materialized views update. The customer history gets new SCD2 rows.
# MAGIC * The dashboard shows the new days after its next refresh.
# MAGIC
# MAGIC Run the pipeline at least once before using this notebook. Put it as the first task of a Job to simulate a daily feed.

# COMMAND ----------

dbutils.widgets.text("days", "7", "Days of new orders")
dbutils.widgets.text("orders_per_day", "190", "Average orders per day")
DAYS = int(dbutils.widgets.get("days"))
PER_DAY = int(dbutils.widgets.get("orders_per_day"))

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/sahra_bronze/landing"

# COMMAND ----------

import json, random, csv, os
from datetime import datetime, timedelta

last_ts = spark.sql(f"SELECT max(order_ts) AS t FROM {CATALOG}.sahra_silver.orders").first()["t"]
start_day = (last_ts + timedelta(days=1)).date()
last_seq = spark.sql(f"""
  SELECT max(CAST(element_at(split(order_id, '-'), 3) AS BIGINT)) AS s FROM {CATALOG}.sahra_silver.orders
""").first()["s"] or 0

products = [r.asDict() for r in spark.table(f"{CATALOG}.sahra_silver.products").collect()]
stores = [r.asDict() for r in spark.table(f"{CATALOG}.sahra_silver.stores").collect()]
customers = [r.asDict() for r in spark.table(f"{CATALOG}.sahra_silver.customers_current").collect()]
print(f"Continuing from {start_day}, last order number {last_seq:,}")

# COMMAND ----------

STORE_WEIGHT = {"Hypermarket": 1.2, "Express": 0.45, "Online": 1.2}
CAT_WEIGHT = {"Grocery": 0.44, "Beverages": 0.20, "Electronics": 0.07, "Home": 0.12, "Beauty": 0.09, "Fashion": 0.08}
by_cat = {}
for p in products:
    by_cat.setdefault(p["category"], []).append(p)

seq = last_seq
written = []
for d in range(DAYS):
    day = start_day + timedelta(days=d)
    weekend = day.weekday() in (5, 6)
    n = int(random.gauss(PER_DAY, 15) * (1.25 if weekend else 1.0))
    rows = []
    for _ in range(n):
        seq += 1
        s = random.choices(stores, weights=[STORE_WEIGHT[x["store_format"]] for x in stores])[0]
        ts = datetime(day.year, day.month, day.day, random.randint(8, 23), random.randint(0, 59), random.randint(0, 59))
        cust = None if (s["store_format"] != "Online" and random.random() < 0.18) else random.choice(customers)["customer_id"]
        items = []
        for i in range(random.choices([1, 2, 3, 4, 5], weights=[30, 28, 20, 14, 8])[0]):
            cat = random.choices(list(CAT_WEIGHT), weights=list(CAT_WEIGHT.values()))[0]
            p = random.choice(by_cat[cat])
            items.append({"line_no": i + 1, "product_id": p["product_id"],
                          "qty": 1 if cat == "Electronics" else random.choice([1, 1, 1, 2, 2, 3]),
                          "unit_price": p["unit_price_aed"], "discount_pct": random.choice([0, 0, 0, 5, 10])})
        o = {"order_id": f"SO-{day.year}-{seq:07d}", "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
             "store_id": s["store_id"], "customer_id": cust,
             "channel": "Online" if s["store_format"] == "Online" else "In-Store",
             "payment_method": random.choice(["Card", "Card", "Cash", "Apple Pay", "Tabby BNPL"]),
             "status": random.choices(["COMPLETED", "RETURNED", "CANCELLED"], weights=[92, 4, 4])[0],
             "currency": "AED", "items": items}
        if random.random() < 0.01:          # keep the data honest: a few messy records
            o["status"] = "completed "
        if random.random() < 0.005:
            o["items"][0]["qty"] = 0
        rows.append(o)
        if random.random() < 0.008:         # POS re-send
            rows.append(o)
    path = f"{LANDING}/orders/orders_{day.isoformat()}.json"
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n")
    written.append((path, len(rows)))

for p, n in written:
    print(f"wrote {n:>4} orders -> {p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A small CRM change file
# MAGIC 25 customers get upgraded one loyalty tier. After the pipeline refresh, `customers_history`
# MAGIC closes their old row and opens a new one. That is SCD Type 2 at work.

# COMMAND ----------

ladder = ["Bronze", "Silver", "Gold", "Platinum"]
changed = random.sample([c for c in customers if c["loyalty_tier"] != "Platinum"], 25)
change_ts = datetime(start_day.year, start_day.month, start_day.day, 6, 0, 0)
path = f"{LANDING}/customers/customers_{start_day.isoformat()}_changes.csv"
fields = ["customer_id", "first_name", "last_name", "email", "phone", "city", "emirate",
          "loyalty_tier", "marketing_opt_in", "signup_date", "updated_at"]
with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for c in changed:
        w.writerow({"customer_id": c["customer_id"], "first_name": c["first_name"], "last_name": c["last_name"],
                    "email": c["email"], "phone": "+97150" + str(random.randint(1000000, 9999999)),
                    "city": c["city"], "emirate": c["emirate"],
                    "loyalty_tier": ladder[ladder.index(c["loyalty_tier"]) + 1],
                    "marketing_opt_in": str(c["marketing_opt_in"]).lower(), "signup_date": c["signup_date"],
                    "updated_at": change_ts.strftime("%Y-%m-%dT%H:%M:%S")})
print(f"wrote 25 customer changes -> {path}")
print("Now refresh the pipeline (or let the Job do it).")
