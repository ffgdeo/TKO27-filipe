"""
Loyalty Engine - Customer Portal API
FastAPI backend serving personalized offers, loyalty status, and Style Assistant.
"""

import os
import json
import psycopg2
from contextlib import contextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import httpx

app = FastAPI(title="Loyalty Engine API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration from environment
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_PORT = os.environ.get("LAKEBASE_PORT", "5432")
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "loyalty_engine")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "")
LAKEBASE_PASSWORD = os.environ.get("LAKEBASE_PASSWORD", "")
MODEL_SERVING_URL = os.environ.get("MODEL_SERVING_URL", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")


@contextmanager
def get_db():
    conn = psycopg2.connect(
        host=LAKEBASE_HOST, port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB, user=LAKEBASE_USER, password=LAKEBASE_PASSWORD,
        sslmode="require",
    )
    try:
        yield conn
    finally:
        conn.close()


# --- Models ---

class ChatRequest(BaseModel):
    message: str
    customer_id: Optional[str] = None


# --- Routes ---

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "loyalty-engine"}


@app.get("/api/customer/{customer_id}/profile")
def get_customer_profile(customer_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM loyalty_status WHERE customer_id = %s",
            (customer_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Customer not found")
        cols = [desc[0] for desc in cur.description]
        profile = dict(zip(cols, row))
        # Convert non-serializable types
        for k, v in profile.items():
            if hasattr(v, "isoformat"):
                profile[k] = v.isoformat()
        return profile


@app.get("/api/customer/{customer_id}/offers")
def get_customer_offers(customer_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT offer_id, offer_code, product_id, product_name, category,
                      relevance_score, offer_type, discount_pct, expires_at, status
               FROM personalized_offers
               WHERE customer_id = %s AND status = 'active'
               ORDER BY relevance_score DESC
               LIMIT 10""",
            (customer_id,),
        )
        cols = [desc[0] for desc in cur.description]
        offers = []
        for row in cur.fetchall():
            offer = dict(zip(cols, row))
            for k, v in offer.items():
                if hasattr(v, "isoformat"):
                    offer[k] = v.isoformat()
            offers.append(offer)
        return {"offers": offers}


@app.get("/api/customer/{customer_id}/activity")
def get_customer_activity(customer_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT session_id, current_category, top_interest_score,
                      products_viewed, cart_items, device_type, last_activity
               FROM active_sessions
               WHERE customer_id = %s
               ORDER BY last_activity DESC
               LIMIT 5""",
            (customer_id,),
        )
        cols = [desc[0] for desc in cur.description]
        sessions = []
        for row in cur.fetchall():
            session = dict(zip(cols, row))
            for k, v in session.items():
                if hasattr(v, "isoformat"):
                    session[k] = v.isoformat()
            sessions.append(session)
        return {"sessions": sessions}


@app.post("/api/customer/{customer_id}/recommend")
async def get_recommendations(customer_id: str, req: ChatRequest):
    if not MODEL_SERVING_URL:
        raise HTTPException(status_code=503, detail="Style Assistant not configured")

    payload = {
        "messages": [{"role": "user", "content": req.message}],
        "customer_id": customer_id,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            MODEL_SERVING_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Style Assistant error")
        return resp.json()


@app.get("/api/customers/search")
def search_customers(q: str = "", limit: int = 20):
    with get_db() as conn:
        cur = conn.cursor()
        if q:
            cur.execute(
                """SELECT customer_id, first_name, last_name, loyalty_tier,
                          points_balance, lifetime_value, segment, churn_risk_level
                   FROM loyalty_status
                   WHERE customer_id ILIKE %s
                      OR first_name ILIKE %s
                      OR last_name ILIKE %s
                   ORDER BY lifetime_value DESC
                   LIMIT %s""",
                (f"%{q}%", f"%{q}%", f"%{q}%", limit),
            )
        else:
            cur.execute(
                """SELECT customer_id, first_name, last_name, loyalty_tier,
                          points_balance, lifetime_value, segment, churn_risk_level
                   FROM loyalty_status
                   ORDER BY lifetime_value DESC
                   LIMIT %s""",
                (limit,),
            )
        cols = [desc[0] for desc in cur.description]
        return {"customers": [dict(zip(cols, row)) for row in cur.fetchall()]}


@app.get("/api/dashboard/stats")
def get_dashboard_stats():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total_customers,
                COUNT(*) FILTER (WHERE segment = 'High-Value Active') AS high_value_active,
                COUNT(*) FILTER (WHERE churn_risk_level = 'High') AS high_churn_risk,
                AVG(lifetime_value)::numeric(10,2) AS avg_ltv,
                AVG(points_balance)::integer AS avg_points
            FROM loyalty_status
        """)
        row = cur.fetchone()
        cols = [desc[0] for desc in cur.description]
        stats = dict(zip(cols, row))

        cur.execute("SELECT COUNT(*) FROM personalized_offers WHERE status = 'active'")
        stats["active_offers"] = cur.fetchone()[0]

        return stats
