# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Feature Store Demo — Data Setup
# MAGIC
# MAGIC Creates synthetic data in `madkins2_catalog.gekit` that models a Starbucks-like ordering system:
# MAGIC - **customers** — loyalty members with preferences
# MAGIC - **drinks** — menu catalog with flavor/nutrition characteristics
# MAGIC - **stores** — store locations
# MAGIC - **transactions** — historical orders with customizations
# MAGIC - **barista_notes** — free-text notes baristas leave about regulars

# COMMAND ----------

CATALOG = "madkins2_catalog"
SCHEMA = "gekit"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drinks Catalog

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

drinks_data = [
    # (drink_id, name, category, base, size_options, calories_tall, caffeine_mg, is_seasonal, flavor_profile, description)
    ("D001", "Pike Place Roast", "brewed_coffee", "coffee", "tall,grande,venti", 235, 235, False, "bold,roasted,smooth", "Our everyday go-to, a smooth and balanced cup."),
    ("D002", "Caffè Americano", "espresso", "espresso", "tall,grande,venti", 15, 225, False, "bold,rich,clean", "Espresso shots topped with hot water for a rich bold flavor."),
    ("D003", "Caramel Macchiato", "espresso", "espresso", "tall,grande,venti", 250, 150, False, "sweet,caramel,vanilla", "Freshly steamed milk with vanilla, espresso and caramel drizzle."),
    ("D004", "Vanilla Sweet Cream Cold Brew", "cold_brew", "cold_brew", "tall,grande,venti,trenta", 200, 185, False, "sweet,vanilla,smooth", "Slow-steeped cold brew topped with vanilla sweet cream."),
    ("D005", "Iced Matcha Latte", "tea", "matcha", "tall,grande,venti", 200, 80, False, "earthy,sweet,creamy", "Smooth and creamy matcha sweetened with milk."),
    ("D006", "Pumpkin Spice Latte", "espresso", "espresso", "tall,grande,venti", 380, 150, True, "sweet,spiced,pumpkin", "The OG fall classic with pumpkin and warm spices."),
    ("D007", "Strawberry Açaí Refresher", "refresher", "juice", "tall,grande,venti,trenta", 90, 45, False, "fruity,sweet,tart", "Sweet strawberry and açaí with green coffee extract."),
    ("D008", "Chai Tea Latte", "tea", "chai", "tall,grande,venti", 240, 50, False, "spiced,sweet,warm", "Black tea infused with cinnamon, clove and other spices."),
    ("D009", "White Chocolate Mocha", "espresso", "espresso", "tall,grande,venti", 430, 150, False, "sweet,creamy,chocolate", "Espresso with white chocolate sauce and steamed milk."),
    ("D010", "Nitro Cold Brew", "cold_brew", "cold_brew", "tall,grande", 5, 280, False, "bold,smooth,creamy", "Velvety smooth cold brew infused with nitrogen."),
    ("D011", "Dragon Drink", "refresher", "coconut_milk", "tall,grande,venti,trenta", 130, 45, False, "fruity,tropical,sweet", "Mango dragonfruit refresher with creamy coconut milk."),
    ("D012", "Flat White", "espresso", "espresso", "tall,grande,venti", 170, 195, False, "bold,smooth,velvety", "Ristretto shots with whole milk microfoam."),
    ("D013", "Iced Brown Sugar Oat Milk Shaken Espresso", "espresso", "espresso", "tall,grande,venti", 120, 255, False, "sweet,cinnamon,oat", "Shaken espresso with brown sugar and oat milk."),
    ("D014", "Matcha Crème Frappuccino", "frappuccino", "matcha", "tall,grande,venti", 420, 70, False, "sweet,earthy,creamy", "Matcha blended with milk and ice, topped with whip."),
    ("D015", "Hot Chocolate", "chocolate", "chocolate", "tall,grande,venti", 370, 25, False, "sweet,chocolate,rich", "Steamed milk with mocha sauce and whipped cream."),
]

drinks_schema = StructType([
    StructField("drink_id", StringType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("base_type", StringType()),
    StructField("size_options", StringType()),
    StructField("calories_tall", IntegerType()),
    StructField("caffeine_mg", IntegerType()),
    StructField("is_seasonal", BooleanType()),
    StructField("flavor_profile", StringType()),
    StructField("description", StringType()),
])

drinks_df = spark.createDataFrame(drinks_data, schema=drinks_schema)
drinks_df.write.mode("overwrite").saveAsTable("drinks")
display(drinks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stores

# COMMAND ----------

stores_data = [
    ("S01", "Pike Place Original", "Seattle", "WA", 47.6097, -122.3425),
    ("S02", "Capitol Hill Reserve", "Seattle", "WA", 47.6205, -122.3210),
    ("S03", "Bellevue Square", "Bellevue", "WA", 47.6153, -122.2040),
    ("S04", "U-District", "Seattle", "WA", 47.6588, -122.3130),
    ("S05", "Fremont", "Seattle", "WA", 47.6510, -122.3500),
]

stores_schema = StructType([
    StructField("store_id", StringType()),
    StructField("store_name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])

stores_df = spark.createDataFrame(stores_data, schema=stores_schema)
stores_df.write.mode("overwrite").saveAsTable("stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

customers_data = [
    ("C001", "Sarah Chen", "gold", "oat_milk", True, False, "morning", "S01"),
    ("C002", "Mike Rodriguez", "green", "whole_milk", False, True, "afternoon", "S02"),
    ("C003", "Priya Patel", "gold", "almond_milk", True, False, "morning", "S01"),
    ("C004", "James Wilson", "basic", "whole_milk", False, False, "evening", "S03"),
    ("C005", "Emily Tanaka", "gold", "oat_milk", False, False, "morning", "S04"),
    ("C006", "David Kim", "green", "coconut_milk", True, True, "afternoon", "S02"),
    ("C007", "Lisa Thompson", "basic", "whole_milk", False, False, "morning", "S05"),
    ("C008", "Alex Rivera", "gold", "oat_milk", True, False, "morning", "S01"),
    ("C009", "Rachel Green", "green", "almond_milk", False, True, "afternoon", "S03"),
    ("C010", "Tom Bradley", "basic", "whole_milk", False, False, "evening", "S04"),
]

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("loyalty_tier", StringType()),
    StructField("milk_preference", StringType()),
    StructField("prefers_less_sugar", BooleanType()),
    StructField("prefers_decaf", BooleanType()),
    StructField("typical_visit_time", StringType()),
    StructField("home_store_id", StringType()),
])

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df.write.mode("overwrite").saveAsTable("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions
# MAGIC
# MAGIC 6 months of order history. Each customer has patterns — favorite drinks, typical customizations, visit cadence.

# COMMAND ----------

import random
from datetime import datetime, timedelta

random.seed(42)

# Define ordering patterns per customer (customer_id -> weighted drink preferences + customizations)
customer_patterns = {
    "C001": {
        "drinks": [("D004", 0.40), ("D013", 0.30), ("D010", 0.15), ("D002", 0.15)],
        "customizations": ["oat milk", "extra shot", "light ice", "no syrup"],
        "freq_per_week": 5,
    },
    "C002": {
        "drinks": [("D006", 0.35), ("D003", 0.30), ("D009", 0.20), ("D015", 0.15)],
        "customizations": ["whole milk", "extra whip", "extra caramel", "venti"],
        "freq_per_week": 3,
    },
    "C003": {
        "drinks": [("D005", 0.45), ("D008", 0.25), ("D013", 0.20), ("D007", 0.10)],
        "customizations": ["almond milk", "half sweet", "no whip", "light ice"],
        "freq_per_week": 6,
    },
    "C004": {
        "drinks": [("D001", 0.50), ("D002", 0.30), ("D010", 0.20)],
        "customizations": ["black", "no room", "grande"],
        "freq_per_week": 2,
    },
    "C005": {
        "drinks": [("D012", 0.40), ("D004", 0.30), ("D002", 0.20), ("D010", 0.10)],
        "customizations": ["oat milk", "extra shot", "no sugar", "tall"],
        "freq_per_week": 5,
    },
    "C006": {
        "drinks": [("D011", 0.35), ("D007", 0.30), ("D005", 0.20), ("D014", 0.15)],
        "customizations": ["coconut milk", "light ice", "no sweetener", "grande"],
        "freq_per_week": 4,
    },
    "C007": {
        "drinks": [("D003", 0.40), ("D006", 0.30), ("D009", 0.20), ("D015", 0.10)],
        "customizations": ["whole milk", "extra pump vanilla", "whipped cream"],
        "freq_per_week": 2,
    },
    "C008": {
        "drinks": [("D013", 0.50), ("D004", 0.25), ("D010", 0.15), ("D012", 0.10)],
        "customizations": ["oat milk", "half sweet", "extra shot", "no whip"],
        "freq_per_week": 6,
    },
    "C009": {
        "drinks": [("D008", 0.35), ("D005", 0.30), ("D007", 0.20), ("D014", 0.15)],
        "customizations": ["almond milk", "decaf", "half sweet", "no whip"],
        "freq_per_week": 3,
    },
    "C010": {
        "drinks": [("D001", 0.60), ("D002", 0.25), ("D015", 0.15)],
        "customizations": ["black", "no room", "venti", "extra hot"],
        "freq_per_week": 1,
    },
}

# Map customers to home stores
customer_stores = {row.customer_id: row.home_store_id for row in customers_df.collect()}

transactions = []
txn_id = 1
start_date = datetime(2025, 9, 1)
end_date = datetime(2026, 3, 14)
days = (end_date - start_date).days

for cust_id, pattern in customer_patterns.items():
    drink_ids = [d[0] for d in pattern["drinks"]]
    drink_weights = [d[1] for d in pattern["drinks"]]
    store_id = customer_stores[cust_id]
    total_orders = int(pattern["freq_per_week"] * days / 7)

    for _ in range(total_orders):
        drink = random.choices(drink_ids, weights=drink_weights, k=1)[0]
        day_offset = random.randint(0, days)
        order_date = start_date + timedelta(days=day_offset)
        # Time of day based on customer pattern
        hour_map = {"morning": random.randint(6, 9), "afternoon": random.randint(12, 15), "evening": random.randint(17, 20)}
        hour = hour_map[pattern.get("typical_visit_time", "morning") if "typical_visit_time" not in pattern else list(hour_map.keys())[0]]
        # Look up customer's typical_visit_time
        cust_row = [c for c in customers_data if c[0] == cust_id][0]
        hour = hour_map[cust_row[6]]
        minute = random.randint(0, 59)
        order_ts = order_date.replace(hour=hour, minute=minute)

        # Pick 1-3 customizations
        n_cust = random.randint(1, min(3, len(pattern["customizations"])))
        custs = ", ".join(random.sample(pattern["customizations"], n_cust))
        size = random.choice(["tall", "grande", "venti"])

        transactions.append((
            f"T{txn_id:05d}",
            cust_id,
            drink,
            store_id,
            order_ts.isoformat(),
            size,
            custs,
            round(random.uniform(4.50, 7.50), 2),
        ))
        txn_id += 1

txn_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("drink_id", StringType()),
    StructField("store_id", StringType()),
    StructField("order_timestamp", StringType()),
    StructField("size", StringType()),
    StructField("customizations", StringType()),
    StructField("amount_usd", DoubleType()),
])

txn_df = (
    spark.createDataFrame(transactions, schema=txn_schema)
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .orderBy("order_timestamp")
)
txn_df.write.mode("overwrite").saveAsTable("transactions")
print(f"Created {txn_df.count()} transactions")
display(txn_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Barista Notes
# MAGIC Free-text observations baristas log about regulars — the kind of tribal knowledge that makes a great customer experience.

# COMMAND ----------

barista_notes_data = [
    ("C001", "S01", "2025-11-15", "Sarah always mobile orders her VSCCB with oat milk before her 7:30 train. If we're out of vanilla she'll take caramel instead."),
    ("C001", "S01", "2026-01-08", "Noticed Sarah switched to the brown sugar oat shaken — she said she's cutting back on sweetness. Still wants the extra shot."),
    ("C001", "S01", "2026-02-20", "Sarah brought a coworker today — recommended the nitro to them. She's basically our brand ambassador lol."),
    ("C002", "S02", "2025-10-22", "Mike always wants extra whip, don't forget the caramel drizzle on top AND inside the cup. He will send it back."),
    ("C002", "S02", "2025-12-01", "PSL season is Mike's favorite — he orders it even when he comes in for pastries only. Venti every time."),
    ("C002", "S02", "2026-02-14", "Mike brought valentines drinks for his whole office — 8 PSLs. Good tipper."),
    ("C003", "S01", "2025-09-30", "Priya is super specific: iced matcha, almond milk, only 2 scoops matcha, half the normal sweetener. Write it on the cup clearly."),
    ("C003", "S01", "2025-12-15", "Priya asked if we can do a chai with matcha — she calls it a 'dirty chai matcha'. We made it work, she loved it."),
    ("C003", "S01", "2026-03-01", "Priya always asks for the cup to be filled to the very top. Light ice = more drink for her."),
    ("C005", "S04", "2025-11-10", "Emily is a flat white purist — oat milk only, gets annoyed if we sub 2%. She can taste the difference."),
    ("C005", "S04", "2026-01-20", "Emily started doing a flat white with an extra shot on Mon/Wed — says those are her hard meeting days."),
    ("C006", "S02", "2025-10-05", "David is our plant-based guy — coconut milk everything. He'll ask about ingredients if it's something new on the menu."),
    ("C006", "S02", "2026-02-28", "David tried the new lavender oat latte and wasn't into it. Stick with his usual dragon drink or refreshers."),
    ("C008", "S01", "2025-11-25", "Alex is here every single morning. Brown sugar oat shaken, half sweet, extra shot. He has it memorized and so should we."),
    ("C008", "S01", "2026-01-15", "Alex asked about our blonde roast options — might be branching out. Still orders the shaken espresso 95% of the time though."),
    ("C008", "S01", "2026-03-10", "Alex said he's training for a marathon — switched to tall from grande to cut calories but still needs the caffeine."),
]

notes_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("store_id", StringType()),
    StructField("note_date", StringType()),
    StructField("note", StringType()),
])

notes_df = (
    spark.createDataFrame(barista_notes_data, schema=notes_schema)
    .withColumn("note_date", F.to_date("note_date"))
)
notes_df.write.mode("overwrite").saveAsTable("barista_notes")
display(notes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

for table in ["customers", "drinks", "stores", "transactions", "barista_notes"]:
    count = spark.table(table).count()
    print(f"  {CATALOG}.{SCHEMA}.{table}: {count} rows")
