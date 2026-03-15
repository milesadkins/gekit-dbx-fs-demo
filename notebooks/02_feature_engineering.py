# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Feature Store Demo — Feature Engineering
# MAGIC
# MAGIC Creates **Feature Tables** in Unity Catalog that power online serving:
# MAGIC 1. **customer_drink_features** — aggregated ordering patterns per customer
# MAGIC 2. **customer_profile_features** — LLM-generated customer profile summary (uses `ai_query`)
# MAGIC
# MAGIC Both are registered as Feature Tables with `FeatureEngineeringClient` so they can be
# MAGIC published to Online Tables for real-time lookups.

# COMMAND ----------

CATALOG = "madkins2_catalog"
SCHEMA = "gekit"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customer Drink Features
# MAGIC
# MAGIC Structured features derived from transaction history — the quantitative side of "knowing your customer."

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

transactions = spark.table("transactions")
drinks = spark.table("drinks")
customers = spark.table("customers")

# Join transactions with drink metadata
txn_enriched = transactions.join(drinks, "drink_id")

# Aggregate per customer
customer_drink_features = (
    txn_enriched
    .groupBy("customer_id")
    .agg(
        # Volume & recency
        F.count("*").alias("total_orders"),
        F.round(F.count("*") / F.lit(28), 1).alias("orders_per_week"),  # ~28 weeks of data
        F.max("order_timestamp").alias("last_order_ts"),
        F.round(F.avg("amount_usd"), 2).alias("avg_order_amount"),

        # Top drink (mode)
        F.mode("drink_id").alias("top_drink_id"),
        F.mode("name").alias("top_drink_name"),

        # Category distribution
        F.round(
            F.sum(F.when(F.col("category") == "espresso", 1).otherwise(0)) / F.count("*"), 2
        ).alias("pct_espresso"),
        F.round(
            F.sum(F.when(F.col("category") == "cold_brew", 1).otherwise(0)) / F.count("*"), 2
        ).alias("pct_cold_brew"),
        F.round(
            F.sum(F.when(F.col("category") == "tea", 1).otherwise(0)) / F.count("*"), 2
        ).alias("pct_tea"),
        F.round(
            F.sum(F.when(F.col("category") == "refresher", 1).otherwise(0)) / F.count("*"), 2
        ).alias("pct_refresher"),

        # Preferences
        F.round(F.avg("caffeine_mg"), 0).alias("avg_caffeine_mg"),
        F.round(F.avg("calories_tall"), 0).alias("avg_calories"),
        F.mode("size").alias("preferred_size"),

        # Customization patterns — collect distinct customizations
        F.concat_ws(
            " | ",
            F.collect_set("customizations")
        ).alias("customization_history"),

        # Distinct drinks tried
        F.countDistinct("drink_id").alias("unique_drinks_tried"),
    )
)

# Join with customer attributes
customer_drink_features = (
    customer_drink_features
    .join(
        customers.select("customer_id", "loyalty_tier", "milk_preference", "prefers_less_sugar", "prefers_decaf", "typical_visit_time", "home_store_id"),
        "customer_id"
    )
)

display(customer_drink_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as Feature Table

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Drop and recreate to ensure clean state
spark.sql("DROP TABLE IF EXISTS customer_drink_features")

fe.create_table(
    name=f"{CATALOG}.{SCHEMA}.customer_drink_features",
    primary_keys=["customer_id"],
    df=customer_drink_features,
    description="Aggregated drink ordering patterns per customer — powers online order prediction.",
)

print("customer_drink_features written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Profile Features (LLM-generated)
# MAGIC
# MAGIC Uses `ai_query()` to generate a rich text profile for each customer by combining:
# MAGIC - Their structured ordering features
# MAGIC - Barista notes (tribal knowledge)
# MAGIC - Drink characteristics of their favorites
# MAGIC
# MAGIC This mirrors the Gekit `view.llm()` pattern from the reference architecture — pre-computing
# MAGIC an LLM-generated profile as a feature, so downstream serving doesn't need to call the LLM at query time.

# COMMAND ----------

# First, aggregate barista notes per customer
barista_notes = spark.table("barista_notes")

customer_notes = (
    barista_notes
    .groupBy("customer_id")
    .agg(
        F.concat_ws(
            "\n- ",
            F.collect_list(
                F.concat(F.lit("["), F.col("note_date").cast("string"), F.lit("] "), F.col("note"))
            )
        ).alias("barista_notes_text")
    )
)

# Build LLM input: combine structured features + notes + top drink info
profile_input = (
    customer_drink_features
    .join(customer_notes, "customer_id", "left")
    .join(
        drinks.select(
            F.col("drink_id"),
            F.col("name").alias("fav_drink_name"),
            F.col("flavor_profile").alias("fav_drink_flavor"),
            F.col("description").alias("fav_drink_description"),
            F.col("base_type").alias("fav_drink_base"),
        ),
        customer_drink_features["top_drink_id"] == drinks["drink_id"],
        "left"
    )
    .fillna({"barista_notes_text": "No barista notes available."})
)

display(profile_input.select("customer_id", "top_drink_name", "barista_notes_text").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Profiles with `ai_query`

# COMMAND ----------

profile_input.createOrReplaceTempView("profile_input")

customer_profiles_df = spark.sql("""
  SELECT
    customer_id,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT(
        'You are a Starbucks barista AI. Generate a concise customer profile (3-4 sentences) that would help a barista know exactly what this regular wants before they even order. Be specific and actionable.\n\n',
        'CUSTOMER: ', customer_id, ' (', loyalty_tier, ' member)\n',
        'MILK: ', milk_preference,
        CASE WHEN prefers_less_sugar THEN ' | LESS SUGAR' ELSE '' END,
        CASE WHEN prefers_decaf THEN ' | DECAF' ELSE '' END, '\n',
        'VISITS: ', typical_visit_time, ' regular, ~', CAST(orders_per_week AS STRING), ' orders/week\n',
        'FAVORITE: ', top_drink_name, ' (', CAST(ROUND(total_orders * 0.4, 0) AS INT), '+ times) — ', fav_drink_flavor, '\n',
        'ALSO ORDERS: ', CAST(unique_drinks_tried AS STRING), ' different drinks tried\n',
        'PREFERRED SIZE: ', preferred_size, '\n',
        'CUSTOMIZATIONS SEEN: ', customization_history, '\n',
        'AVG CAFFEINE: ', CAST(avg_caffeine_mg AS STRING), 'mg | AVG CALORIES: ', CAST(avg_calories AS STRING), '\n\n',
        'BARISTA NOTES:\n- ', barista_notes_text, '\n\n',
        'Write the profile as if briefing a new barista about this regular. Focus on: (1) their go-to order with exact customizations, (2) any known preferences or pet peeves, (3) personality/vibe that helps the interaction.'
      )
    ) AS profile_summary,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT(
        'You are a friendly Starbucks barista. Generate a SHORT, natural greeting for a regular customer who just scanned their loyalty card. The greeting should:\n',
        '1. Use their name if known\n',
        '2. Predict their most likely order with specific customizations\n',
        '3. Ask for confirmation in a casual, friendly way\n',
        '4. Reference something personal if relevant (time of day, recent changes, etc.)\n\n',
        'CUSTOMER: ', customer_id, ' (', loyalty_tier, ' member)\n',
        'TOP DRINK: ', top_drink_name, ' (', preferred_size, ')\n',
        'MILK: ', milk_preference,
        CASE WHEN prefers_less_sugar THEN ' | less sugar' ELSE '' END,
        CASE WHEN prefers_decaf THEN ' | decaf' ELSE '' END, '\n',
        'VISITS: ', typical_visit_time, ' regular, ~', CAST(orders_per_week AS STRING), '/week\n',
        'CUSTOMIZATIONS: ', customization_history, '\n',
        'BARISTA NOTES:\n- ', barista_notes_text, '\n\n',
        'Generate ONLY the greeting — 2-3 sentences max. Sound human, not robotic. No quotation marks.'
      )
    ) AS barista_greeting,
    current_timestamp() AS profile_generated_at
  FROM profile_input
""")

display(customer_profiles_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Profile Feature Table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS customer_profile_features")

fe.create_table(
    name=f"{CATALOG}.{SCHEMA}.customer_profile_features",
    primary_keys=["customer_id"],
    df=customer_profiles_df,
    description="LLM-generated customer profiles summarizing ordering patterns, preferences, and barista tribal knowledge.",
)

print("customer_profile_features written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Feature Tables

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_name, table_type
  FROM {CATALOG}.information_schema.tables
  WHERE table_schema = '{SCHEMA}'
  ORDER BY table_name
"""))
