"""
Loyalty Engine - Customer Portal API
FastAPI backend serving personalized offers, loyalty status, and Style Assistant.
Connects to Lakebase via OAuth token generated from Databricks workspace.
"""

import os
import time
import json
import psycopg2
import requests as req_lib
from contextlib import contextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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

# Configuration
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "ep-snowy-bird-d2ok41nc.database.us-east-1.cloud.databricks.com")
LAKEBASE_PORT = os.environ.get("LAKEBASE_PORT", "5432")
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "projects/loyalty-engine/branches/production/endpoints/primary")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://fevm-tko27-filipe.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MODEL_SERVING_URL = os.environ.get("MODEL_SERVING_URL", "")

# Token cache
_token_cache = {"token": None, "expires_at": 0, "username": None}


def _get_lakebase_creds():
    """Get or refresh Lakebase OAuth credentials."""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"], _token_cache["username"]

    token = DATABRICKS_TOKEN
    if not token:
        token = os.environ.get("DATABRICKS_TOKEN", "")

    # Generate Lakebase credential
    resp = req_lib.post(
        f"{DATABRICKS_HOST}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {token}"},
        json={"endpoint": LAKEBASE_ENDPOINT},
        timeout=10,
    )
    resp.raise_for_status()
    cred = resp.json()

    # Get username
    if not _token_cache["username"]:
        user_resp = req_lib.get(
            f"{DATABRICKS_HOST}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        _token_cache["username"] = user_resp.json()["userName"]

    _token_cache["token"] = cred["token"]
    _token_cache["expires_at"] = now + 3500  # ~1 hour
    return _token_cache["token"], _token_cache["username"]


@contextmanager
def get_db():
    lb_token, username = _get_lakebase_creds()
    conn = psycopg2.connect(
        host=LAKEBASE_HOST, port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB, user=username, password=lb_token,
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


# --- API Routes ---

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
        raise HTTPException(status_code=503, detail="Style Assistant not configured yet")

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


# --- Static files (React frontend) ---
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")
