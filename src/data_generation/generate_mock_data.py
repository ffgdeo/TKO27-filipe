# Databricks notebook source
# MAGIC %md
# MAGIC # Mock Retail Data Generator
# MAGIC Generates synthetic retail data: products catalog, customer profiles,
# MAGIC purchase history, and clickstream events. Writes to Unity Catalog Volume.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import csv
import io
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CATALOG = spark.conf.get("spark.databricks.unityCatalog.catalog", "tko27_filipe_catalog")
SCHEMA = "loyalty_engine"
VOLUME = "raw_data"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Products Catalog (~500 products)

# COMMAND ----------

CATEGORIES = {
    "Denim": ["Slim Jeans", "Bootcut Jeans", "Skinny Jeans", "Relaxed Jeans", "Denim Jacket", "Denim Shorts", "Denim Skirt", "Wide Leg Jeans"],
    "Tops": ["T-Shirt", "Blouse", "Polo Shirt", "Tank Top", "Henley", "Crop Top", "Button-Down Shirt", "Sweater"],
    "Shoes": ["Sneakers", "Boots", "Sandals", "Loafers", "Running Shoes", "Heels", "Flats", "Slip-Ons"],
    "Accessories": ["Watch", "Sunglasses", "Belt", "Handbag", "Scarf", "Hat", "Wallet", "Bracelet"],
    "Outerwear": ["Winter Jacket", "Trench Coat", "Blazer", "Parka", "Windbreaker", "Vest", "Peacoat", "Rain Jacket"],
    "Activewear": ["Leggings", "Sports Bra", "Running Shorts", "Yoga Pants", "Track Jacket", "Compression Shirt", "Gym Tank", "Sweatpants"],
}

BRANDS = ["UrbanEdge", "NorthTrail", "LuxeVibe", "ActivePulse", "ClassicThread", "StreetCraft", "EcoWear", "PrimeFit"]
COLORS = ["Black", "White", "Navy", "Gray", "Red", "Blue", "Green", "Beige", "Brown", "Pink", "Olive", "Burgundy"]
SIZES = ["XS", "S", "M", "L", "XL", "XXL"]
MATERIALS = ["Cotton", "Polyester", "Denim", "Leather", "Nylon", "Wool", "Linen", "Silk"]

products = []
product_id = 1000
for category, items in CATEGORIES.items():
    for _ in range(83):  # ~500 total
        item = random.choice(items)
        brand = random.choice(BRANDS)
        color = random.choice(COLORS)
        material = random.choice(MATERIALS)
        base_price = {
            "Denim": (39.99, 149.99), "Tops": (19.99, 89.99), "Shoes": (49.99, 199.99),
            "Accessories": (14.99, 129.99), "Outerwear": (59.99, 299.99), "Activewear": (24.99, 99.99),
        }
        price_range = base_price[category]
        price = round(random.uniform(*price_range), 2)
        product_id += 1
        products.append({
            "product_id": f"PRD-{product_id}",
            "product_name": f"{brand} {color} {item}",
            "category": category,
            "subcategory": item,
            "brand": brand,
            "color": color,
            "size": random.choice(SIZES),
            "material": material,
            "price": price,
            "rating": round(random.uniform(3.0, 5.0), 1),
            "review_count": random.randint(5, 500),
            "tags": json.dumps([category.lower(), item.lower().replace(" ", "_"), brand.lower(), color.lower()]),
            "description": f"Premium {material.lower()} {item.lower()} by {brand}. Available in {color.lower()}. Perfect for any occasion.",
            "in_stock": random.choice([True, True, True, False]),
        })

print(f"Generated {len(products)} products")

# Write to Volume as CSV
buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=products[0].keys())
writer.writeheader()
writer.writerows(products)
dbutils.fs.put(f"{VOLUME_PATH}/products_catalog.csv", buf.getvalue(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Profiles (~1,000 customers)

# COMMAND ----------

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
TIER_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

customers = []
for i in range(1, 1001):
    tier = random.choices(LOYALTY_TIERS, weights=TIER_WEIGHTS, k=1)[0]
    signup_date = fake.date_between(start_date="-3y", end_date="-30d")
    ltv = {
        "Bronze": random.uniform(50, 500),
        "Silver": random.uniform(300, 1500),
        "Gold": random.uniform(1000, 5000),
        "Platinum": random.uniform(3000, 15000),
    }[tier]

    customers.append({
        "customer_id": f"CUST-{i:05d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "signup_date": signup_date.isoformat(),
        "loyalty_tier": tier,
        "loyalty_points": random.randint(100, 25000),
        "lifetime_value": round(ltv, 2),
        "preferred_categories": json.dumps(random.sample(list(CATEGORIES.keys()), k=random.randint(1, 3))),
        "credit_card_last4": fake.credit_card_number()[-4:],
        "credit_card_masked": f"****-****-****-{fake.credit_card_number()[-4:]}",
    })

print(f"Generated {len(customers)} customers")

buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=customers[0].keys())
writer.writeheader()
writer.writerows(customers)
dbutils.fs.put(f"{VOLUME_PATH}/customer_profiles.csv", buf.getvalue(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Purchase History (~10,000 transactions)

# COMMAND ----------

purchases = []
for _ in range(10000):
    cust = random.choice(customers)
    prod = random.choice(products)
    purchase_date = fake.date_time_between(start_date="-12M", end_date="now")
    quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1], k=1)[0]

    purchases.append({
        "transaction_id": str(uuid.uuid4()),
        "customer_id": cust["customer_id"],
        "product_id": prod["product_id"],
        "category": prod["category"],
        "brand": prod["brand"],
        "quantity": quantity,
        "unit_price": prod["price"],
        "total_amount": round(prod["price"] * quantity, 2),
        "purchase_date": purchase_date.isoformat(),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
        "store_type": random.choice(["online", "online", "in_store"]),
    })

print(f"Generated {len(purchases)} purchases")

buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=purchases[0].keys())
writer.writeheader()
writer.writerows(purchases)
dbutils.fs.put(f"{VOLUME_PATH}/purchase_history.csv", buf.getvalue(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clickstream Events (~5,000+ events)

# COMMAND ----------

EVENT_TYPES = {
    "page_view": 0.45,
    "product_view": 0.25,
    "search": 0.10,
    "add_to_cart": 0.10,
    "add_to_wishlist": 0.05,
    "remove_from_cart": 0.03,
    "checkout_start": 0.02,
}

SEARCH_TERMS = [
    "blue denim jacket", "summer dress", "running shoes sale", "casual sneakers",
    "black leather boots", "yoga pants", "winter coat", "cotton t-shirt",
    "slim fit jeans", "designer handbag", "workout gear", "men's polo",
    "women's activewear", "denim shorts", "linen blazer", "cozy sweater",
]

clickstream = []
# Create sessions concentrated in recent 48 hours for some customers (for Genie demo)
recent_customers = random.sample(customers, 200)

for _ in range(5500):
    if random.random() < 0.3 and recent_customers:
        cust = random.choice(recent_customers)
        event_time = fake.date_time_between(start_date="-48h", end_date="now")
    else:
        cust = random.choice(customers)
        event_time = fake.date_time_between(start_date="-30d", end_date="now")

    event_type = random.choices(
        list(EVENT_TYPES.keys()),
        weights=list(EVENT_TYPES.values()),
        k=1
    )[0]

    prod = random.choice(products)
    session_id = f"SES-{fake.uuid4()[:8]}"

    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "customer_id": cust["customer_id"],
        "event_type": event_type,
        "event_timestamp": event_time.isoformat(),
        "product_id": prod["product_id"] if event_type != "search" else "",
        "category": prod["category"],
        "brand": prod["brand"],
        "page_url": f"/products/{prod['category'].lower()}/{prod['product_id'].lower()}" if event_type != "search" else f"/search?q={random.choice(SEARCH_TERMS).replace(' ', '+')}",
        "search_term": random.choice(SEARCH_TERMS) if event_type == "search" else "",
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "referrer": random.choice(["google", "direct", "instagram", "facebook", "email", "tiktok"]),
        "duration_seconds": random.randint(3, 300) if event_type in ("page_view", "product_view") else 0,
    }
    clickstream.append(event)

print(f"Generated {len(clickstream)} clickstream events")

buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=clickstream[0].keys())
writer.writeheader()
writer.writerows(clickstream)
dbutils.fs.put(f"{VOLUME_PATH}/clickstream_events.csv", buf.getvalue(), overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
Data Generation Complete!
========================
Products Catalog: {len(products)} products across {len(CATEGORIES)} categories
Customer Profiles: {len(customers)} customers
Purchase History: {len(purchases)} transactions
Clickstream Events: {len(clickstream)} events

All files written to: {VOLUME_PATH}/
  - products_catalog.csv
  - customer_profiles.csv
  - purchase_history.csv
  - clickstream_events.csv
""")
