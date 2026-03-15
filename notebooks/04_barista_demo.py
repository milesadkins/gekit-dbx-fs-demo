# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Feature Store Demo — Barista Order Verification
# MAGIC
# MAGIC **Scenario**: A customer walks up to the counter. The barista scans their loyalty card
# MAGIC (passes `customer_id` to the system). The system instantly:
# MAGIC 1. Looks up pre-computed features from the **Synced Tables** in Lakebase (sub-second)
# MAGIC 2. Returns the **pre-generated barista greeting** — no LLM call at serving time
# MAGIC
# MAGIC The barista sees: *"Hey Sarah! Vanilla Sweet Cream Cold Brew with oat milk and an extra shot, grande today?"*
# MAGIC
# MAGIC All the LLM work happened during feature engineering. This notebook is pure lookup.

# COMMAND ----------

CATALOG = "madkins2_catalog"
SCHEMA = "gekit"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Lookup
# MAGIC
# MAGIC Queries the synced tables (Lakebase-backed) for a given customer.
# MAGIC Sub-second point lookups — everything is pre-computed.

# COMMAND ----------

import json


def get_customer_features(customer_id: str) -> dict:
    """Look up all pre-computed features from the synced tables."""
    drink_row = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.customer_drink_features_synced
        WHERE customer_id = '{customer_id}'
    """).first()

    profile_row = spark.sql(f"""
        SELECT profile_summary, barista_greeting
        FROM {CATALOG}.{SCHEMA}.customer_profile_features_synced_v2
        WHERE customer_id = '{customer_id}'
    """).first()

    if not drink_row:
        return {}

    features = drink_row.asDict()
    if profile_row:
        features["profile_summary"] = profile_row["profile_summary"]
        features["barista_greeting"] = profile_row["barista_greeting"]

    return features


# Quick test
features = get_customer_features("C001")
print(json.dumps({k: str(v)[:100] for k, v in features.items()}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Demo: Walk-up Order Verification
# MAGIC
# MAGIC Simulating customers scanning their loyalty cards at the counter.
# MAGIC Every piece of data below — including the barista greeting — is a **pre-computed feature**.
# MAGIC No LLM calls happen here. This is pure lookup.

# COMMAND ----------

demo_customers = ["C001", "C003", "C005", "C008", "C002", "C004"]

for cid in demo_customers:
    features = get_customer_features(cid)

    print(f"{'='*70}")
    print(f"  LOYALTY SCAN: {cid}")
    print(f"  Tier: {features.get('loyalty_tier', '?')} | Visits/week: {features.get('orders_per_week', '?')}")
    print(f"  Predicted order: {features.get('top_drink_name', '?')} ({features.get('preferred_size', '?')})")
    print(f"{'='*70}")
    print(f"\n  BARISTA SAYS: {features.get('barista_greeting', 'Hi! What can I get started for you?')}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Profile Deep Dive
# MAGIC
# MAGIC The profile summary is also pre-computed — useful for barista training or the mobile app.

# COMMAND ----------

for cid in ["C001", "C003", "C008"]:
    features = get_customer_features(cid)
    print(f"{'='*70}")
    print(f"  {cid} — {features.get('top_drink_name', '?')}")
    print(f"{'='*70}")
    print(f"  {features.get('profile_summary', 'N/A')}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Under the Hood: Synced Table Status

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

for suffix in ["customer_drink_features_synced", "customer_profile_features_synced_v2"]:
    st_name = f"{CATALOG}.{SCHEMA}.{suffix}"
    resp = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{st_name}")
    state = resp.get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
    print(f"{suffix}: {state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive: Try Any Customer
# MAGIC
# MAGIC Change the customer ID below to test different personas (C001-C010).

# COMMAND ----------

CUSTOMER_ID = "C006"

features = get_customer_features(CUSTOMER_ID)

print(f"Customer: {CUSTOMER_ID}")
print(f"\nBarista greeting: {features.get('barista_greeting', 'N/A')}")
print(f"\nFull profile: {features.get('profile_summary', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐      ┌───────────────────────────────┐      ┌───────────────────────┐
# MAGIC │    Source Tables     │      │      Feature Tables           │      │    Synced Tables       │
# MAGIC │    (Unity Catalog)   │─────▶│      (FE Client + UC)        │─────▶│    (Lakebase Postgres) │
# MAGIC │                      │      │                               │      │                        │
# MAGIC │  customers           │      │  customer_drink_features      │      │  drink_features_synced │
# MAGIC │  drinks              │      │    (structured aggregates)    │      │                        │
# MAGIC │  stores              │      │                               │      │  profile_features_     │
# MAGIC │  transactions        │      │  customer_profile_features    │      │    synced              │
# MAGIC │  barista_notes       │      │    • profile_summary  ◄─ LLM │      │                        │
# MAGIC │                      │      │    • barista_greeting ◄─ LLM │      │                        │
# MAGIC └─────────────────────┘      └───────────────────────────────┘      └───────────┬───────────┘
# MAGIC                                                                                  │
# MAGIC                                         ┌───────────────────────────────────────┘
# MAGIC                                         ▼
# MAGIC                                ┌───────────────────────┐
# MAGIC                                │  Barista POS / App     │
# MAGIC                                │                        │
# MAGIC                                │  Input: customer_id    │
# MAGIC                                │  Output:               │
# MAGIC                                │    • barista_greeting  │  ◄── Pre-computed, no LLM at serve time
# MAGIC                                │    • profile_summary   │
# MAGIC                                │    • drink features    │
# MAGIC                                │                        │
# MAGIC                                │  Latency: sub-second   │
# MAGIC                                └───────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key insight**: Both `profile_summary` and `barista_greeting` are pre-computed
# MAGIC during feature engineering using `ai_query`. At serving time, it's a pure key-value
# MAGIC lookup — no LLM inference, no latency spike, no token costs per request.
