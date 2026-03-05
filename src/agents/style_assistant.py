# Databricks notebook source
# MAGIC %md
# MAGIC # Style Assistant Agent
# MAGIC RAG-powered agent that uses Vector Search to match products
# MAGIC to a shopper's browsing "vibe" and past preferences.

# COMMAND ----------

# MAGIC %pip install mlflow databricks-agents databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from databricks.vector_search.client import VectorSearchClient
from mlflow.models import infer_signature
import json

CATALOG = spark.conf.get("spark.databricks.unityCatalog.catalog", "tko27_filipe_catalog")
SCHEMA = "loyalty_engine"
VS_ENDPOINT = "loyalty-engine-vs"
VS_INDEX = f"{CATALOG}.{SCHEMA}.products_vs_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_products"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Vector Search Endpoint & Index

# COMMAND ----------

vsc = VectorSearchClient()

# Create endpoint if it doesn't exist
try:
    vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint '{VS_ENDPOINT}' already exists")
except Exception:
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print(f"Created endpoint '{VS_ENDPOINT}'")

# COMMAND ----------

# Prepare the source table with a searchable text column
spark.sql(f"""
    CREATE OR REPLACE TABLE {SOURCE_TABLE}_searchable AS
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        color,
        material,
        price,
        rating,
        description,
        tags,
        CONCAT(
            product_name, '. ', category, ' - ', subcategory, '. ',
            'Made of ', material, '. ', description
        ) AS search_text
    FROM {SOURCE_TABLE}
""")

# Enable CDF for sync
spark.sql(f"ALTER TABLE {SOURCE_TABLE}_searchable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# Create or sync the Vector Search index
try:
    vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    print(f"Index '{VS_INDEX}' already exists, syncing...")
    vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX).sync()
except Exception:
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=f"{SOURCE_TABLE}_searchable",
        pipeline_type="TRIGGERED",
        primary_key="product_id",
        embedding_source_column="search_text",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )
    print(f"Created index '{VS_INDEX}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define the Style Assistant Agent

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

class StyleAssistantAgent(mlflow.pyfunc.PythonModel):
    def __init__(self):
        self.vs_endpoint = VS_ENDPOINT
        self.vs_index = VS_INDEX
        self.catalog = CATALOG
        self.schema = SCHEMA

    def load_context(self, context):
        from databricks.vector_search.client import VectorSearchClient
        from databricks.sdk import WorkspaceClient
        self.vsc = VectorSearchClient()
        self.w = WorkspaceClient()

    def _search_products(self, query: str, filters: dict = None, num_results: int = 5):
        index = self.vsc.get_index(
            endpoint_name=self.vs_endpoint,
            index_name=self.vs_index,
        )
        results = index.similarity_search(
            query_text=query,
            columns=["product_id", "product_name", "category", "subcategory",
                     "brand", "color", "material", "price", "rating", "description"],
            num_results=num_results,
            filters=filters,
        )
        return results.get("result", {}).get("data_array", [])

    def _get_customer_context(self, customer_id: str):
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        result = w.statement_execution.execute_statement(
            warehouse_id=self._get_warehouse_id(),
            statement=f"""
                SELECT c.preferred_categories, c.loyalty_tier,
                       ci.category, ci.interest_score
                FROM {self.catalog}.{self.schema}.silver_customer_360 c
                LEFT JOIN {self.catalog}.{self.schema}.gold_category_interest ci
                    ON c.customer_id = ci.customer_id AND ci.interest_rank <= 3
                WHERE c.customer_id = '{customer_id}'
            """,
        )
        return result

    def _get_warehouse_id(self):
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                return wh.id
        return warehouses[0].id if warehouses else None

    def predict(self, context, model_input, params=None):
        import json

        messages = model_input.get("messages", [])
        customer_id = model_input.get("customer_id", "")

        user_message = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                user_message = msg.get("content", "")
                break

        # Search for matching products
        products = self._search_products(user_message, num_results=6)

        product_context = "\n".join([
            f"- {p[1]} ({p[3]}) by {p[4]} in {p[5]} {p[6]} - ${p[7]:.2f}, Rating: {p[8]}/5"
            for p in products
        ]) if products else "No matching products found."

        system_prompt = f"""You are a friendly and knowledgeable Style Assistant for a retail apparel brand.
Your job is to help shoppers find products that match their style, preferences, and current mood.

Based on the shopper's query, here are the most relevant products from our catalog:

{product_context}

Guidelines:
- Be enthusiastic but not pushy
- Explain WHY each recommendation fits their style/query
- Suggest complementary items when appropriate
- Mention any deals or loyalty benefits if applicable
- Keep responses concise (3-5 product suggestions max)
- If the query is vague, ask a clarifying question about their style preference
"""

        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        response = w.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            max_tokens=512,
        )

        return {"content": response.choices[0].message.content}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log and Register the Agent

# COMMAND ----------

with mlflow.start_run(run_name="style-assistant-v1"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="style-assistant",
        python_model=StyleAssistantAgent(),
        pip_requirements=[
            "mlflow",
            "databricks-vectorsearch",
            "databricks-sdk",
        ],
        registered_model_name=f"{CATALOG}.{SCHEMA}.style_assistant",
    )
    print(f"Model logged: {model_info.model_uri}")
