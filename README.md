# Personalized Shopper Recommendations & Loyalty Engine

End-to-end retail demo showcasing hyper-personalized customer engagement using the Databricks Data Intelligence Platform.

## Architecture

```
UC Volume (CSVs) --> Autoloader --> Bronze --> Silver --> Gold
                                                          |
                                                    Lakebase (Postgres)
                                                          |
                                              Databricks App (FastAPI + React)
                                                          |
                                              Style Assistant (RAG + Vector Search)
                                                          |
                                              Genie Space (Conversational BI)
```

## Components

| Component | Description | Location |
|-----------|-------------|----------|
| Data Generation | Mock retail data (products, customers, purchases, clickstream) | `src/data_generation/` |
| SDP Pipeline | Autoloader -> Bronze -> Silver -> Gold with intent scoring | `src/pipelines/` |
| Lakebase | Operational state store for sub-second reads | `lakebase/` |
| Style Assistant | RAG agent with Vector Search for product recommendations | `src/agents/` |
| Genie Space | Conversational BI for retail metrics (LTV, Churn, Interest) | `genie/` |
| Customer Portal | FastAPI + React app showing offers, loyalty status, chat | `src/app/` |

## Deployment

This project uses **Databricks Asset Bundles (DABs)**.

```bash
# Configure workspace
databricks auth login https://fevm-tko27-filipe.cloud.databricks.com

# Deploy all resources
databricks bundle deploy -t dev

# Run data generation
databricks bundle run data_generation -t dev

# Start the pipeline
databricks bundle run loyalty_pipeline -t dev
```

## Data Flow

1. **Generate** mock data -> UC Volume
2. **Ingest** via Autoloader into Bronze streaming tables
3. **Enrich** in Silver: join clickstream + products, build customer 360
4. **Aggregate** in Gold: customer segments, category interest, churn risk
5. **Sync** Gold -> Lakebase for low-latency serving
6. **Serve** via Databricks App with Style Assistant chat

## Key Metrics

- **Intent Score**: Weighted clickstream events (cart_add=5, wishlist=4, search=3, view=2, page=1) with recency decay
- **Churn Risk**: Composite of days since last purchase/activity and order frequency
- **LTV**: Historical total spend per customer
- **Category Interest**: Real-time affinity scores per customer x category
