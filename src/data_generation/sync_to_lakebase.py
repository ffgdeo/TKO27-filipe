# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Gold Tables to Lakebase
# MAGIC Reads from Gold layer and writes to Lakebase for sub-second serving

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
import json

CATALOG = spark.conf.get("spark.databricks.unityCatalog.catalog", "tko27_filipe_catalog")
SCHEMA = "loyalty_engine"

# Lakebase connection - set via job parameters or secrets
LAKEBASE_HOST = dbutils.secrets.get(scope="loyalty-engine", key="lakebase-host")
LAKEBASE_PORT = dbutils.secrets.get(scope="loyalty-engine", key="lakebase-port")
LAKEBASE_DB = dbutils.secrets.get(scope="loyalty-engine", key="lakebase-db")
LAKEBASE_USER = dbutils.secrets.get(scope="loyalty-engine", key="lakebase-user")
LAKEBASE_PASSWORD = dbutils.secrets.get(scope="loyalty-engine", key="lakebase-password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Customer Segments -> loyalty_status

# COMMAND ----------

segments_df = spark.table(f"{CATALOG}.{SCHEMA}.gold_customer_segments").toPandas()

TIER_THRESHOLDS = {"Bronze": 5000, "Silver": 15000, "Gold": 50000, "Platinum": 100000}

conn = psycopg2.connect(
    host=LAKEBASE_HOST, port=LAKEBASE_PORT,
    dbname=LAKEBASE_DB, user=LAKEBASE_USER, password=LAKEBASE_PASSWORD,
    sslmode="require",
)
cur = conn.cursor()

for _, row in segments_df.iterrows():
    threshold = TIER_THRESHOLDS.get(row["loyalty_tier"], 5000)
    progress = min(100.0, (row.get("loyalty_points", 0) / threshold) * 100) if threshold > 0 else 0

    cur.execute("""
        INSERT INTO loyalty_status (
            customer_id, first_name, last_name, loyalty_tier, points_balance,
            lifetime_value, total_orders, next_tier_threshold, tier_progress_pct,
            segment, churn_risk_level, last_purchase_date, preferred_categories, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (customer_id) DO UPDATE SET
            loyalty_tier = EXCLUDED.loyalty_tier,
            points_balance = EXCLUDED.points_balance,
            lifetime_value = EXCLUDED.lifetime_value,
            total_orders = EXCLUDED.total_orders,
            next_tier_threshold = EXCLUDED.next_tier_threshold,
            tier_progress_pct = EXCLUDED.tier_progress_pct,
            segment = EXCLUDED.segment,
            churn_risk_level = EXCLUDED.churn_risk_level,
            last_purchase_date = EXCLUDED.last_purchase_date,
            preferred_categories = EXCLUDED.preferred_categories,
            updated_at = NOW()
    """, (
        row["customer_id"], row.get("first_name"), row.get("last_name"),
        row["loyalty_tier"], int(row.get("loyalty_points", 0)),
        float(row.get("lifetime_value", 0)), int(row.get("total_orders", 0)),
        threshold, round(progress, 1),
        row.get("segment"), row.get("churn_risk_level"),
        row.get("last_purchase_date"),
        row.get("preferred_categories"),
    ))

conn.commit()
print(f"Synced {len(segments_df)} customers to loyalty_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate & Sync Personalized Offers

# COMMAND ----------

import uuid
from datetime import datetime, timedelta

interest_df = spark.table(f"{CATALOG}.{SCHEMA}.gold_category_interest").toPandas()
products_df = spark.table(f"{CATALOG}.{SCHEMA}.bronze_products").toPandas()

# For each customer's top 3 categories, generate an offer from a matching product
top_interests = interest_df[interest_df["interest_rank"] <= 3]

OFFER_TYPES = ["flash_sale", "loyalty_reward", "recommendation", "bundle_deal"]
offers_inserted = 0

for _, row in top_interests.iterrows():
    category_products = products_df[products_df["category"] == row["category"]]
    if category_products.empty:
        continue

    prod = category_products.sample(n=1).iloc[0]
    discount = min(30, max(5, int(row["interest_score"] / 5)))

    cur.execute("""
        INSERT INTO personalized_offers (
            offer_id, customer_id, offer_code, product_id, product_name,
            category, relevance_score, offer_type, discount_pct,
            expires_at, status, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', NOW())
        ON CONFLICT (offer_id) DO NOTHING
    """, (
        str(uuid.uuid4()), row["customer_id"],
        f"OFFER-{row['category'][:3].upper()}-{discount}OFF",
        prod["product_id"], prod["product_name"],
        row["category"], float(row["interest_score"]),
        OFFER_TYPES[int(row["interest_rank"]) % len(OFFER_TYPES)],
        float(discount),
        (datetime.now() + timedelta(days=7)).isoformat(),
    ))
    offers_inserted += 1

conn.commit()
cur.close()
conn.close()

print(f"Synced {offers_inserted} personalized offers to Lakebase")
