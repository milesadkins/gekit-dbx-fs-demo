# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Feature Store Demo — Online Serving Setup
# MAGIC
# MAGIC Sets up the **online serving** layer using **Synced Tables** backed by Lakebase:
# MAGIC 1. Enables **Change Data Feed** on feature tables
# MAGIC 2. Creates **Synced Tables** — low-latency, auto-synced copies in Lakebase (Postgres)
# MAGIC 3. Validates the synced tables are online and queryable
# MAGIC
# MAGIC Once synced tables are live, they can be queried directly (sub-second point lookups)
# MAGIC or exposed via a Feature Serving endpoint for REST API access.

# COMMAND ----------

CATALOG = "madkins2_catalog"
SCHEMA = "gekit"
LAKEBASE_INSTANCE = "agent-memory-miles"
LAKEBASE_DB = "gekit_serving"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enable Change Data Feed
# MAGIC
# MAGIC Synced Tables require CDF on the source Delta tables to support incremental sync.

# COMMAND ----------

for table in ["customer_drink_features", "customer_profile_features"]:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SCHEMA}.{table}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"  CDF enabled on {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Synced Tables
# MAGIC
# MAGIC Synced Tables are the replacement for Online Tables. They sync feature table data
# MAGIC to a Lakebase (Postgres) instance for sub-second point lookups by primary key.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

synced_table_configs = [
    {
        "name": f"{CATALOG}.{SCHEMA}.customer_drink_features_synced",
        "source": f"{CATALOG}.{SCHEMA}.customer_drink_features",
    },
    {
        "name": f"{CATALOG}.{SCHEMA}.customer_profile_features_synced",
        "source": f"{CATALOG}.{SCHEMA}.customer_profile_features",
    },
]

for st in synced_table_configs:
    print(f"Creating synced table: {st['name']}")
    try:
        w.api_client.do("POST", "/api/2.0/database/synced_tables", body={
            "name": st["name"],
            "database_instance_name": LAKEBASE_INSTANCE,
            "logical_database_name": LAKEBASE_DB,
            "spec": {
                "source_table_full_name": st["source"],
                "primary_key_columns": ["customer_id"],
                "scheduling_policy": "TRIGGERED",
            },
        })
        print(f"  Created: {st['name']}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Already exists: {st['name']}")
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for sync to complete

# COMMAND ----------

for st in synced_table_configs:
    print(f"Checking {st['name']}...")
    for i in range(30):
        resp = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{st['name']}")
        state = resp.get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
        print(f"  [{i+1}/30] {state}")
        if "ONLINE" in state:
            break
        time.sleep(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate — Query the Synced Tables
# MAGIC
# MAGIC The synced tables are FOREIGN tables in Unity Catalog backed by Lakebase Postgres.
# MAGIC They support standard SQL queries with sub-second latency for point lookups.

# COMMAND ----------

# Query drink features
display(spark.sql(f"""
    SELECT customer_id, top_drink_name, preferred_size, orders_per_week, loyalty_tier, milk_preference
    FROM {CATALOG}.{SCHEMA}.customer_drink_features_synced
    ORDER BY customer_id
"""))

# COMMAND ----------

# Query profile features
display(spark.sql(f"""
    SELECT customer_id, profile_summary
    FROM {CATALOG}.{SCHEMA}.customer_profile_features_synced
    ORDER BY customer_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Optional) Feature Serving Endpoint
# MAGIC
# MAGIC For production use, you can expose these features via a REST endpoint using a FeatureSpec.
# MAGIC This gives you a managed HTTP API with auth, scaling, and monitoring.
# MAGIC
# MAGIC ```python
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
# MAGIC
# MAGIC fe = FeatureEngineeringClient()
# MAGIC
# MAGIC fe.create_feature_spec(
# MAGIC     name=f"{CATALOG}.{SCHEMA}.starbucks_order_spec",
# MAGIC     features=[
# MAGIC         FeatureLookup(table_name=f"{CATALOG}.{SCHEMA}.customer_drink_features", lookup_key=["customer_id"]),
# MAGIC         FeatureLookup(table_name=f"{CATALOG}.{SCHEMA}.customer_profile_features", lookup_key=["customer_id"]),
# MAGIC     ],
# MAGIC )
# MAGIC
# MAGIC # Then create endpoint via UI or SDK — takes ~5-10 min to provision
# MAGIC ```
