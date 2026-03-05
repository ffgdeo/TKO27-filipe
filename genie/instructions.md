# Genie Space Instructions: Certified SQL - Retail Analytics

## Overview
This Genie Space provides conversational BI for the Loyalty Engine retail demo. Marketers can ask natural language questions about customer behavior, purchase patterns, and engagement metrics.

## Tables Available

### silver_clickstream_enriched
Clickstream events enriched with product details. Key columns:
- `customer_id`, `event_type`, `event_timestamp`, `category`, `brand`, `product_name`, `event_weight`
- Event types: page_view, product_view, search, add_to_cart, add_to_wishlist, checkout_start, remove_from_cart

### silver_customer_360
Customer golden record with profile + aggregated metrics:
- `customer_id`, `first_name`, `last_name`, `loyalty_tier`, `loyalty_points`, `lifetime_value`
- `total_orders`, `total_spend`, `avg_order_value`, `last_purchase_date`, `days_since_last_purchase`
- `total_events`, `last_activity`, `total_sessions`, `preferred_categories`

### gold_customer_segments
Customer segments with churn risk:
- `segment`: High-Value Active, High-Value At-Risk, Dormant, New, At-Risk, Active
- `churn_risk_score` (0-100), `churn_risk_level` (Low/Medium/High)

### gold_category_interest
Category affinity scores per customer:
- `interest_score` (weighted by recency and event type), `interest_rank`, `event_count`

### silver_purchase_enriched
Purchase history with product details

## Key Metric Definitions

### Lifetime Value (LTV)
The total historical spend by a customer. Found in `silver_customer_360.lifetime_value` or calculated as `SUM(total_amount)` from `silver_purchase_enriched`.

### Churn Risk
Calculated from days since last purchase, days since last activity, and total orders. Score 0-100.
- High Risk: score >= 70 (no purchases in 60+ days, low activity)
- Medium Risk: score 40-70
- Low Risk: score < 40

### Recent Interest
To calculate "Recent Interest," look at the last 48 hours of clickstream data:
```sql
SELECT customer_id, category,
  SUM(CASE
    WHEN event_type = 'add_to_cart' THEN 5
    WHEN event_type = 'add_to_wishlist' THEN 4
    WHEN event_type = 'search' THEN 3
    WHEN event_type = 'product_view' THEN 2
    WHEN event_type = 'page_view' THEN 1
    ELSE 0
  END) AS recent_interest_score,
  COUNT(*) AS events_48h
FROM silver_clickstream_enriched
WHERE event_timestamp >= current_timestamp() - INTERVAL 48 HOURS
GROUP BY customer_id, category
ORDER BY recent_interest_score DESC
```

### Top Shoppers
"Top 10% of shoppers" means customers whose `lifetime_value` is in the 90th percentile or above.

## Sample Questions & SQL Patterns

### "Show me top 10% shoppers who haven't bought in 30 days but browsed denim recently"
```sql
WITH top_shoppers AS (
  SELECT customer_id, lifetime_value,
    PERCENT_RANK() OVER (ORDER BY lifetime_value) AS ltv_pctile
  FROM silver_customer_360
),
recent_denim AS (
  SELECT DISTINCT customer_id
  FROM silver_clickstream_enriched
  WHERE category = 'Denim'
    AND event_timestamp >= current_timestamp() - INTERVAL 48 HOURS
)
SELECT c.customer_id, c.first_name, c.last_name, c.loyalty_tier,
  c.lifetime_value, c.days_since_last_purchase, c.last_activity
FROM silver_customer_360 c
JOIN top_shoppers t ON c.customer_id = t.customer_id
JOIN recent_denim d ON c.customer_id = d.customer_id
WHERE t.ltv_pctile >= 0.90
  AND c.days_since_last_purchase >= 30
ORDER BY c.lifetime_value DESC
```

### "What categories are trending this week?"
```sql
SELECT category,
  COUNT(*) AS total_events,
  COUNT(DISTINCT customer_id) AS unique_shoppers,
  SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_adds
FROM silver_clickstream_enriched
WHERE event_timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY category
ORDER BY total_events DESC
```

### "Which customers are at high risk of churning?"
```sql
SELECT customer_id, first_name, last_name, loyalty_tier,
  lifetime_value, segment, churn_risk_score, churn_risk_level,
  days_since_last_purchase
FROM gold_customer_segments
WHERE churn_risk_level = 'High'
ORDER BY lifetime_value DESC
```

## Business Context
- This is a retail apparel business with 6 categories: Denim, Tops, Shoes, Accessories, Outerwear, Activewear
- Loyalty tiers: Bronze, Silver, Gold, Platinum
- The goal is to identify high-value customers showing disengagement and re-engage them with personalized offers
- "Relevance Fatigue" is the key business challenge - avoid generic recommendations
