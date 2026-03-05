# Databricks notebook source
# MAGIC %md
# MAGIC # Loyalty Engine - Spark Declarative Pipeline
# MAGIC Bronze (Autoloader from Volume) -> Silver (Enriched + Intent) -> Gold (Segments + Churn)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog", "tko27_filipe_catalog")
SCHEMA = spark.conf.get("schema", "loyalty_engine")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

# =============================================================================
# BRONZE LAYER - Autoloader from Volume
# =============================================================================

@dlt.table(
    name="bronze_products",
    comment="Raw products catalog ingested from Volume via Autoloader",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/products_catalog.csv")
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer profiles ingested from Volume via Autoloader",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/customer_profiles.csv")
    )


@dlt.table(
    name="bronze_purchases",
    comment="Raw purchase history ingested from Volume via Autoloader",
    table_properties={"quality": "bronze"},
)
def bronze_purchases():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/purchase_history.csv")
    )


@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested from Volume via Autoloader",
    table_properties={"quality": "bronze"},
)
def bronze_clickstream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/clickstream_events.csv")
    )


# =============================================================================
# SILVER LAYER - Enriched & Cleaned
# =============================================================================

@dlt.table(
    name="silver_clickstream_enriched",
    comment="Clickstream events enriched with product details and category context",
    table_properties={"quality": "silver"},
)
def silver_clickstream_enriched():
    clickstream = dlt.read("bronze_clickstream")
    products = dlt.read("bronze_products")

    event_weights = {
        "page_view": 1,
        "product_view": 2,
        "search": 3,
        "add_to_wishlist": 4,
        "add_to_cart": 5,
        "checkout_start": 6,
        "remove_from_cart": -1,
    }

    weight_expr = F.coalesce(
        *[F.when(F.col("event_type") == k, F.lit(v)) for k, v in event_weights.items()],
        F.lit(1)
    )

    return (
        clickstream
        .join(
            products.select("product_id", "product_name", "subcategory", "price", "material"),
            on="product_id",
            how="left",
        )
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_weight", weight_expr)
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
    )


@dlt.table(
    name="silver_purchase_enriched",
    comment="Purchase history enriched with product details",
    table_properties={"quality": "silver"},
)
def silver_purchase_enriched():
    purchases = dlt.read("bronze_purchases")
    products = dlt.read("bronze_products")

    return (
        purchases
        .join(
            products.select("product_id", "product_name", "subcategory", "material"),
            on="product_id",
            how="left",
        )
        .withColumn("purchase_date", F.to_timestamp("purchase_date"))
        .withColumn("purchase_month", F.date_format("purchase_date", "yyyy-MM"))
    )


@dlt.table(
    name="silver_customer_360",
    comment="Customer golden record combining profiles, purchase aggregates, and intent signals",
    table_properties={"quality": "silver"},
)
def silver_customer_360():
    customers = dlt.read("bronze_customers")
    purchases = dlt.read("bronze_purchases")
    clickstream = dlt.read("bronze_clickstream")

    # Purchase aggregates
    purchase_agg = (
        purchases
        .withColumn("purchase_date", F.to_timestamp("purchase_date"))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.max("purchase_date").alias("last_purchase_date"),
            F.min("purchase_date").alias("first_purchase_date"),
            F.countDistinct("category").alias("distinct_categories_purchased"),
        )
    )

    # Clickstream aggregates
    click_agg = (
        clickstream
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.max("event_timestamp").alias("last_activity"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_cart_adds"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("total_searches"),
        )
    )

    return (
        customers
        .join(purchase_agg, on="customer_id", how="left")
        .join(click_agg, on="customer_id", how="left")
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("days_since_last_purchase",
            F.datediff(F.current_date(), F.col("last_purchase_date")))
        .withColumn("days_since_last_activity",
            F.datediff(F.current_date(), F.to_date("last_activity")))
    )


# =============================================================================
# GOLD LAYER - Business Metrics & Segments
# =============================================================================

@dlt.table(
    name="gold_category_interest",
    comment="Real-time category affinity scores per customer based on recent clickstream",
    table_properties={"quality": "gold"},
)
def gold_category_interest():
    enriched = dlt.read("silver_clickstream_enriched")

    event_weights = {
        "page_view": 1, "product_view": 2, "search": 3,
        "add_to_wishlist": 4, "add_to_cart": 5, "checkout_start": 6, "remove_from_cart": -1,
    }
    weight_expr = F.coalesce(
        *[F.when(F.col("event_type") == k, F.lit(v)) for k, v in event_weights.items()],
        F.lit(1)
    )

    # Recency decay: events in last 48h get 3x weight, last 7d get 2x, rest get 1x
    recency_weight = (
        F.when(F.col("event_timestamp") >= F.date_sub(F.current_timestamp(), 2), 3.0)
        .when(F.col("event_timestamp") >= F.date_sub(F.current_timestamp(), 7), 2.0)
        .otherwise(1.0)
    )

    return (
        enriched
        .withColumn("weighted_score", weight_expr * recency_weight)
        .groupBy("customer_id", "category")
        .agg(
            F.sum("weighted_score").alias("interest_score"),
            F.count("*").alias("event_count"),
            F.max("event_timestamp").alias("last_interaction"),
            F.countDistinct("product_id").alias("unique_products_viewed"),
        )
        .withColumn("interest_rank",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.desc("interest_score"))
            ))
    )


@dlt.table(
    name="gold_customer_segments",
    comment="Customer segmentation: High-Value, At-Risk, Dormant, New based on LTV and recency",
    table_properties={"quality": "gold"},
)
def gold_customer_segments():
    c360 = dlt.read("silver_customer_360")

    return (
        c360
        .withColumn("segment",
            F.when(
                (F.col("loyalty_tier").isin("Gold", "Platinum")) & (F.col("days_since_last_purchase") <= 30),
                "High-Value Active"
            ).when(
                (F.col("loyalty_tier").isin("Gold", "Platinum")) & (F.col("days_since_last_purchase") > 30),
                "High-Value At-Risk"
            ).when(
                (F.col("days_since_last_purchase") > 60) | (F.col("days_since_last_activity") > 30),
                "Dormant"
            ).when(
                F.datediff(F.current_date(), F.col("signup_date")) <= 90,
                "New"
            ).when(
                (F.col("days_since_last_purchase") > 30) & (F.col("days_since_last_purchase") <= 60),
                "At-Risk"
            ).otherwise("Active")
        )
        .withColumn("churn_risk_score",
            F.least(F.lit(100),
                F.coalesce(F.col("days_since_last_purchase"), F.lit(90)) * 1.0 +
                F.coalesce(F.col("days_since_last_activity"), F.lit(30)) * 0.5 -
                F.coalesce(F.col("total_orders"), F.lit(0)) * 2.0
            )
        )
        .withColumn("churn_risk_level",
            F.when(F.col("churn_risk_score") >= 70, "High")
            .when(F.col("churn_risk_score") >= 40, "Medium")
            .otherwise("Low")
        )
        .select(
            "customer_id", "first_name", "last_name", "email",
            "loyalty_tier", "loyalty_points", "lifetime_value",
            "total_orders", "total_spend", "avg_order_value",
            "last_purchase_date", "days_since_last_purchase",
            "total_events", "last_activity", "days_since_last_activity",
            "segment", "churn_risk_score", "churn_risk_level",
            "preferred_categories",
        )
    )
