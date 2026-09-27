# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schemas, landing volume, folders
# MAGIC
# MAGIC *Sahra Retail Lakehouse · by Imran Sheikh · [LinkedIn](https://www.linkedin.com/in/imranazhar/)*
# MAGIC
# MAGIC Run this once. It creates the Unity Catalog structure for the Sahra Retail lakehouse:
# MAGIC
# MAGIC | Object | Name | Purpose |
# MAGIC |---|---|---|
# MAGIC | Schema | `workspace.sahra_bronze` | raw tables + the landing volume |
# MAGIC | Schema | `workspace.sahra_silver` | clean, typed, de-duplicated tables |
# MAGIC | Schema | `workspace.sahra_gold` | business-ready tables for dashboards and Genie |
# MAGIC | Volume | `workspace.sahra_bronze.landing` | where raw files are dropped |
# MAGIC
# MAGIC Free Edition gives you a catalog called `workspace`, so we use it. On a paid workspace you could create a dedicated catalog instead.

# COMMAND ----------

CATALOG = "workspace"
SCHEMAS = {
    "sahra_bronze": "Bronze: raw data exactly as received, plus lineage columns",
    "sahra_silver": "Silver: cleaned, typed, de-duplicated, quality-checked data",
    "sahra_gold":   "Gold: business-ready aggregates for BI, Genie and ML",
}

spark.sql(f"USE CATALOG {CATALOG}")
for schema, comment in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {CATALOG}.sahra_bronze.landing
  COMMENT 'Landing zone for raw files: orders (JSON), customers/products/stores (CSV)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create one folder per source
# MAGIC Auto Loader watches a folder, so each entity gets its own.

# COMMAND ----------

LANDING = f"/Volumes/{CATALOG}/sahra_bronze/landing"
for folder in ["orders", "customers", "products", "stores"]:
    dbutils.fs.mkdirs(f"{LANDING}/{folder}")

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check
# MAGIC You should see three schemas starting with `sahra_` and four folders above.
# MAGIC
# MAGIC **Next:** either upload the files from the project's `data/` folder into the matching volume folders,
# MAGIC or run notebook `01_generate_data` to create them directly in the volume.

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'sahra*'"))
