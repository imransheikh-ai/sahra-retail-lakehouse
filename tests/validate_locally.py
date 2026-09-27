"""Sahra Retail Lakehouse · Author: Imran Sheikh · https://www.linkedin.com/in/imranazhar/


Local check of the pipeline SQL with plain Apache Spark (no Databricks needed).

It emulates what Lakeflow does:
  * Bronze streaming tables  -> spark.read on the landing files (+ lineage columns)
  * AUTO CDC SCD Type 2      -> window functions over the customer exports
  * Materialized views       -> the SELECT from pipeline/02_silver.sql and 03_gold.sql, run as-is
  * Expectations             -> counted; DROP ROW applied as a filter, FAIL UPDATE raises

Run:  pip install pyspark==4.0.1  &&  python tests/validate_locally.py
"""
import json, os, re, sys
from pyspark.sql import SparkSession, functions as F, Window

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("SAHRA_DATA", os.path.join(ROOT, "data"))
spark = (SparkSession.builder.master("local[4]").appName("sahra-validate")
         .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
report = {}


def reg(name, df):
    df.cache()
    df.createOrReplaceTempView(name.replace(".", "__"))
    report.setdefault("row_counts", {})[name] = df.count()
    return df


# ------------------------------------------------------------------ bronze (emulated read_files)
lineage = [F.col("_metadata.file_path").alias("source_file"), F.current_timestamp().alias("ingested_at")]
items_t = "ARRAY<STRUCT<line_no INT, product_id STRING, qty INT, unit_price DOUBLE, discount_pct INT>>"
orders_schema = (f"order_id STRING, order_ts STRING, store_id STRING, customer_id STRING, channel STRING, "
                 f"payment_method STRING, status STRING, currency STRING, items {items_t}")
reg("sahra_bronze.orders_raw", spark.read.schema(orders_schema).json(f"{DATA}/orders/").select("*", *lineage))
reg("sahra_bronze.customers_raw",
    spark.read.option("header", True).option("inferSchema", True).csv(f"{DATA}/customers/")
    .withColumn("customer_id", F.col("customer_id").cast("string")).withColumn("phone", F.col("phone").cast("string"))
    .withColumn("signup_date", F.col("signup_date").cast("date")).withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    .select("*", *lineage))
reg("sahra_bronze.products_raw", spark.read.option("header", True).csv(f"{DATA}/products/").select("*", *lineage))
reg("sahra_bronze.stores_raw", spark.read.option("header", True).csv(f"{DATA}/stores/").select("*", *lineage))

# ------------------------------------------------------------------ AUTO CDC ... SCD TYPE 2 (emulated)
c = spark.table("sahra_bronze__customers_raw").drop("source_file", "ingested_at")
w = Window.partitionBy("customer_id").orderBy("updated_at")
c = (c.withColumn("sig", F.concat_ws("|", "loyalty_tier", "emirate", "city"))
       .withColumn("prev_sig", F.lag("sig").over(w))
       .filter(F.col("prev_sig").isNull() | (F.col("sig") != F.col("prev_sig")))
       .withColumn("__START_AT", F.col("updated_at"))
       .withColumn("__END_AT", F.lead("updated_at").over(w))
       .drop("sig", "prev_sig"))
reg("sahra_silver.customers_history", c)


# ------------------------------------------------------------------ materialized views from the real SQL
def strip_comments(sql):
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql.splitlines())


def matching_paren(s, i):
    depth = 0
    for j in range(i, len(s)):
        if s[j] == "(":
            depth += 1
        elif s[j] == ")":
            depth -= 1
            if depth == 0:
                return j
    raise ValueError("unbalanced")


def run_file(path):
    sql = strip_comments(open(path).read())
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        m = re.match(r"CREATE OR REFRESH MATERIALIZED VIEW\s+([\w.]+)\s*", stmt)
        if not m:
            continue  # streaming table / flow handled above
        name, rest = m.group(1), stmt[m.end():]
        constraints = []
        if rest.startswith("("):
            j = matching_paren(rest, 0)
            block, rest = rest[1:j], rest[j + 1:]
            for cm in re.finditer(r"CONSTRAINT\s+(\w+)\s+EXPECT\s*", block):
                k = cm.end()
                e = matching_paren(block, k)
                expr = block[k + 1:e]
                tail = block[e + 1:].split("CONSTRAINT")[0]
                action = "DROP" if "DROP ROW" in tail else "FAIL" if "FAIL UPDATE" in tail else "WARN"
                constraints.append((cm.group(1), expr, action))
        tp = rest.index("TBLPROPERTIES")
        j = matching_paren(rest, rest.index("(", tp))
        body = rest[j + 1:].strip()
        assert body.startswith("AS"), name
        body = re.sub(r"\b(sahra_\w+)\.(\w+)", r"\1__\2", body[2:])
        df = spark.sql(body)
        for cname, expr, action in constraints:
            failed = df.filter(f"NOT ({expr})").count()
            report.setdefault("expectations", {})[f"{name}.{cname}"] = {"action": action, "failed_rows": failed}
            if action == "DROP":
                df = df.filter(f"coalesce({expr}, false)")
            if action == "FAIL" and failed:
                raise SystemExit(f"FAIL UPDATE: {name}.{cname} failed on {failed} rows")
        reg(name, df)
        print(f"  ok  {name:40s} {report['row_counts'][name]:>8,} rows")


print("Running silver + gold SQL ...")
run_file(os.path.join(ROOT, "pipeline", "02_silver.sql"))
run_file(os.path.join(ROOT, "pipeline", "03_gold.sql"))

# ------------------------------------------------------------------ headline numbers for the guide
q = lambda s: [r.asDict() for r in spark.sql(s).collect()]
report["kpis"] = q("""
  SELECT round(sum(net_amount),0) AS net_revenue, round(sum(gross_margin),0) AS gross_margin,
         count(DISTINCT order_id) AS orders, sum(qty) AS units,
         round(sum(net_amount)/count(DISTINCT order_id),2) AS avg_basket
  FROM sahra_gold__sales_enriched WHERE is_revenue""")[0]
report["by_category"] = q("""SELECT category, round(sum(net_amount),0) AS revenue, round(sum(gross_margin)/sum(net_amount),3) AS margin
  FROM sahra_gold__sales_enriched WHERE is_revenue GROUP BY 1 ORDER BY 2 DESC""")
report["by_emirate"] = q("""SELECT store_emirate, round(sum(net_revenue),0) AS revenue
  FROM sahra_gold__daily_store_sales GROUP BY 1 ORDER BY 2 DESC""")
report["by_month"] = q("""SELECT month(order_date) AS m, round(sum(net_revenue),0) AS revenue, sum(orders) AS orders
  FROM sahra_gold__daily_store_sales GROUP BY 1 ORDER BY 1""")
report["by_event"] = q("""SELECT d.retail_event, round(sum(s.net_revenue)/count(DISTINCT s.order_date),0) AS revenue_per_day
  FROM sahra_gold__daily_store_sales s JOIN sahra_gold__dim_date d ON s.order_date = d.date GROUP BY 1 ORDER BY 2 DESC""")
report["segments"] = q("SELECT segment, count(*) AS customers, round(sum(monetary),0) AS revenue FROM sahra_gold__customer_rfm GROUP BY 1 ORDER BY 3 DESC")
report["tiers_at_order"] = q("SELECT loyalty_tier_at_order, count(DISTINCT order_id) AS orders FROM sahra_gold__sales_enriched GROUP BY 1 ORDER BY 2 DESC")
report["top_products"] = q("SELECT product_name, category, net_revenue FROM sahra_gold__product_performance ORDER BY net_revenue DESC LIMIT 5")
report["quarantine"] = q("SELECT reject_reason, count(*) AS n FROM sahra_silver__order_items_quarantine GROUP BY 1")
report["dupes_removed"] = report["row_counts"]["sahra_bronze.orders_raw"] - report["row_counts"]["sahra_silver.orders"]
report["scd2_customers_with_history"] = q("SELECT count(*) AS n FROM (SELECT customer_id FROM sahra_silver__customers_history GROUP BY 1 HAVING count(*) > 1)")[0]["n"]

out = os.path.join(ROOT, "tests", "expected_results.json")
json.dump(report, open(out, "w"), indent=2, default=str)
print(json.dumps({k: report[k] for k in ("row_counts", "expectations", "kpis", "dupes_removed")}, indent=2, default=str))
print(f"\nFull report: {out}")

# ------------------------------------------------------------------ dashboard datasets must run too
print("\nRunning dashboard datasets ...")
dash = strip_comments(open(os.path.join(ROOT, "dashboard", "dashboard_datasets.sql")).read())
for i, stmt in enumerate([s.strip() for s in dash.split(";") if s.strip()], 1):
    stmt = re.sub(r"\bworkspace\.(sahra_\w+)\.(\w+)", r"\1__\2", stmt)
    n = spark.sql(stmt).count()
    print(f"  ok  dataset {i}: {n:,} rows")
