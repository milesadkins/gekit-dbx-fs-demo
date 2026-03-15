# Starbucks Order Prediction — Databricks Feature Store Demo

A demo of Databricks Feature Store with online serving that mimics a Starbucks use case. When a customer scans their loyalty card, the system instantly looks up pre-computed features — including an LLM-generated barista greeting — from Lakebase-backed synced tables. Zero LLM calls at serving time.

```
"Hey Sarah, I see you're in a bit early today, hope you're getting a good start
 before that 7:30 train. I'm going to take a guess you're in the mood for your
 usual vanilla sweet cream cold brew with oat milk, extra shot, and less sugar
 — would that be right?"
```

## Relationship to the Reference Architecture

This demo is a Databricks-native reimplementation of the Gekit reference files (`view.py`, `agent.py`, `mcp_server.py`). Those files demonstrate a DoorDash grocery replacement agent where:

- `view.py` builds a materialized view by joining multiple Postgres tables (orders, items, conversations) and calling an LLM to generate a customer profile
- `mcp_server.py` exposes that materialized view + database lookups as MCP tools for real-time access
- `agent.py` is an AI agent that retrieves profiles + item details via MCP, then generates a replacement recommendation

This demo maps each component to Databricks-native equivalents:

| Gekit Reference | Databricks Equivalent | What It Does |
|---|---|---|
| `view.py` — Postgres connectors + joins + `view.llm()` | `02_feature_engineering` — Spark joins + `ai_query()` | Aggregates multi-table data and generates LLM profiles as pre-computed features |
| `view.py` — `MaterializationConfig` + refresh loop | Feature Tables + Synced Tables (Lakebase) | Materializes features and auto-syncs to a low-latency serving layer |
| `mcp_server.py` — `get_customer_profile` tool | Synced table point lookup via SQL | Sub-second retrieval of pre-computed features by primary key |
| `mcp_server.py` — `get_order`, `get_item` tools | Source tables in Unity Catalog | Structured data accessible via SQL (no custom server needed) |
| `agent.py` — OpenAI agent + MCP tools + streaming | `04_barista_demo` — pure feature lookup | **Key difference**: the greeting is pre-computed as a feature, so no agent/LLM call is needed at serving time |

The fundamental architectural shift: Gekit pre-computes the **profile** but generates the **action** (replacement recommendation) at serving time via an agent. This demo pre-computes **both** the profile and the action (barista greeting) during feature engineering, making serving a pure lookup with zero LLM latency.

## Use Case

**Problem**: Starbucks baristas build relationships with regulars over time — they learn names, go-to orders, quirks, and preferences. But this "tribal knowledge" lives in people's heads. When a barista is new, transferred, or just busy, that personalized experience disappears.

**Solution**: Combine structured transaction data with unstructured barista notes, use `ai_query` to pre-compute both a rich customer profile and a ready-to-use barista greeting as features, then serve them in real-time through Lakebase-backed synced tables. When a customer walks up, the system:

1. **Looks up pre-computed features** from the synced tables (sub-second latency)
2. **Returns the barista greeting** — already generated, no LLM call needed
3. **Delivers a personalized experience** with zero serving-time inference cost

The key insight: **all** LLM work happens at feature engineering time, not at serving time. Both `profile_summary` and `barista_greeting` are pre-computed features stored in the online serving layer.

## Architecture

```
┌─────────────────────┐      ┌───────────────────────────────┐      ┌───────────────────────┐
│    Source Tables     │      │      Feature Tables           │      │    Synced Tables       │
│    (Unity Catalog)   │─────▶│      (FE Client + UC)        │─────▶│    (Lakebase Postgres) │
│                      │      │                               │      │                        │
│  customers           │      │  customer_drink_features      │      │  drink_features_synced │
│  drinks              │      │    (structured aggregates)    │      │                        │
│  stores              │      │                               │      │  profile_features_     │
│  transactions        │      │  customer_profile_features    │      │    synced              │
│  barista_notes       │      │    • profile_summary  ◄─ LLM │      │                        │
│                      │      │    • barista_greeting ◄─ LLM │      │                        │
└─────────────────────┘      └───────────────────────────────┘      └───────────┬───────────┘
                                                                                 │
                                        ┌────────────────────────────────────────┘
                                        ▼
                               ┌───────────────────────┐
                               │  Barista POS / App     │
                               │                        │
                               │  Input: customer_id    │
                               │  Output:               │
                               │    • barista_greeting  │  ◄── Pre-computed, zero LLM at serve time
                               │    • profile_summary   │
                               │    • drink features    │
                               │                        │
                               │  Latency: sub-second   │
                               └───────────────────────┘
```

### Databricks Components Used

| Component | Role |
|---|---|
| **Unity Catalog** | Governance layer for all tables, feature tables, and synced tables |
| **Feature Engineering Client** | Registers feature tables with primary keys for online serving |
| **AI Functions (`ai_query`)** | Pre-computes both `profile_summary` and `barista_greeting` during feature engineering using Llama 3.3 70B |
| **Synced Tables (Lakebase)** | Low-latency, auto-synced serving layer backed by Postgres — the replacement for Online Tables |
| **Lakebase** | Managed Postgres instance that hosts the synced tables for sub-second point lookups |
| **Foundation Model API** | Hosts `databricks-meta-llama-3-3-70b-instruct` used by `ai_query` at feature engineering time |

## Data Model

All tables live in `madkins2_catalog.gekit`.

### Source Tables

#### `customers` — 10 rows
Loyalty program members with distinct personas.

| Column | Type | Description |
|---|---|---|
| `customer_id` | string | Primary key (C001–C010) |
| `name` | string | Customer name |
| `loyalty_tier` | string | gold / green / basic |
| `milk_preference` | string | oat_milk / whole_milk / almond_milk / coconut_milk |
| `prefers_less_sugar` | boolean | Dietary preference flag |
| `prefers_decaf` | boolean | Dietary preference flag |
| `typical_visit_time` | string | morning / afternoon / evening |
| `home_store_id` | string | FK to stores |

#### `drinks` — 15 rows
Menu catalog with real Starbucks drinks and rich characteristics.

| Column | Type | Description |
|---|---|---|
| `drink_id` | string | Primary key (D001–D015) |
| `name` | string | e.g., "Vanilla Sweet Cream Cold Brew" |
| `category` | string | espresso / cold_brew / tea / refresher / frappuccino / chocolate |
| `base_type` | string | coffee / espresso / cold_brew / matcha / chai / juice / coconut_milk / chocolate |
| `size_options` | string | Comma-separated available sizes |
| `calories_tall` | int | Calorie count for tall size |
| `caffeine_mg` | int | Caffeine content in mg |
| `is_seasonal` | boolean | Whether the drink is seasonal (only PSL) |
| `flavor_profile` | string | Comma-separated flavor tags (e.g., "bold,smooth,creamy") |
| `description` | string | Menu description |

#### `stores` — 5 rows
Seattle-area Starbucks locations.

| Column | Type | Description |
|---|---|---|
| `store_id` | string | Primary key (S01–S05) |
| `store_name` | string | e.g., "Pike Place Original" |
| `city` | string | City name |
| `state` | string | State abbreviation |
| `latitude` | double | GPS coordinate |
| `longitude` | double | GPS coordinate |

#### `transactions` — ~1,000+ rows
Six months of order history (Sep 2025 – Mar 2026) with realistic per-customer patterns.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | string | Primary key (T00001–T0XXXX) |
| `customer_id` | string | FK to customers |
| `drink_id` | string | FK to drinks |
| `store_id` | string | FK to stores |
| `order_timestamp` | timestamp | When the order was placed |
| `size` | string | tall / grande / venti |
| `customizations` | string | Free text (e.g., "oat milk, extra shot, light ice") |
| `amount_usd` | double | Order total |

Each customer has a weighted drink distribution and visit frequency:

| Customer | Persona | Top Drink | Freq/week |
|---|---|---|---|
| C001 Sarah Chen | Oat milk loyalist, cutting sweetness | Vanilla Sweet Cream Cold Brew | 5 |
| C002 Mike Rodriguez | Sweet tooth, extra whip everything | Pumpkin Spice Latte | 3 |
| C003 Priya Patel | Ultra-specific, half-sweet matcha | Iced Matcha Latte | 6 |
| C004 James Wilson | Black coffee purist | Pike Place Roast | 2 |
| C005 Emily Tanaka | Flat white purist, oat milk only | Flat White | 5 |
| C006 David Kim | Plant-based, refresher lover | Dragon Drink | 4 |
| C007 Lisa Thompson | Classic sweet drinks | Caramel Macchiato | 2 |
| C008 Alex Rivera | Daily creature of habit | Brown Sugar Oat Shaken Espresso | 6 |
| C009 Rachel Green | Decaf tea drinker | Chai Tea Latte | 3 |
| C010 Tom Bradley | Infrequent, simple black coffee | Pike Place Roast | 1 |

#### `barista_notes` — 16 rows
Free-text observations that baristas log about regulars — the tribal knowledge that doesn't exist in structured data.

| Column | Type | Description |
|---|---|---|
| `customer_id` | string | FK to customers |
| `store_id` | string | FK to stores |
| `note_date` | date | When the note was written |
| `note` | string | Free-text observation |

Example notes:
- *"Sarah always mobile orders her VSCCB with oat milk before her 7:30 train. If we're out of vanilla she'll take caramel instead."*
- *"Mike always wants extra whip, don't forget the caramel drizzle on top AND inside the cup. He will send it back."*
- *"Priya is super specific: iced matcha, almond milk, only 2 scoops matcha, half the normal sweetener. Write it on the cup clearly."*

### Feature Tables

#### `customer_drink_features` — 10 rows
Aggregated ordering patterns per customer. Primary key: `customer_id`.

| Feature | Type | Source |
|---|---|---|
| `total_orders` | int | count of transactions |
| `orders_per_week` | float | total_orders / 28 weeks |
| `last_order_ts` | timestamp | most recent transaction |
| `avg_order_amount` | float | mean of amount_usd |
| `top_drink_id` | string | mode of drink_id |
| `top_drink_name` | string | mode of drink name |
| `preferred_size` | string | mode of size |
| `customization_history` | string | pipe-delimited distinct customization strings |
| `pct_espresso` | float | share of espresso category orders |
| `pct_cold_brew` | float | share of cold_brew category orders |
| `pct_tea` | float | share of tea category orders |
| `pct_refresher` | float | share of refresher category orders |
| `avg_caffeine_mg` | float | mean caffeine across orders |
| `avg_calories` | float | mean calories across orders |
| `unique_drinks_tried` | int | count distinct drink_id |
| `loyalty_tier` | string | from customers table |
| `milk_preference` | string | from customers table |
| `prefers_less_sugar` | boolean | from customers table |
| `prefers_decaf` | boolean | from customers table |
| `typical_visit_time` | string | from customers table |
| `home_store_id` | string | from customers table |

#### `customer_profile_features` — 10 rows
LLM-generated customer profiles and barista greetings. Primary key: `customer_id`.

| Feature | Type | Source |
|---|---|---|
| `profile_summary` | string | Generated by `ai_query` — 3-4 sentence profile combining structured features, drink metadata, and barista notes |
| `barista_greeting` | string | Generated by `ai_query` — ready-to-use 2-3 sentence barista greeting with predicted order and personalization |
| `profile_generated_at` | timestamp | When both LLM features were computed |

Both `profile_summary` and `barista_greeting` are generated in a single `ai_query` SQL pass during feature engineering. The profile is a detailed internal brief; the greeting is the customer-facing output.

### How Structured Features Feed the LLM Prompts

The `customer_profile_features` table is built in `02_feature_engineering` by constructing rich text prompts from multiple data sources and passing them to `ai_query`. Here's the data flow.

#### Step 1: Build the Input DataFrame

Three sources are joined into a single `profile_input` DataFrame keyed on `customer_id`:

| Source | Join | Columns Used |
|---|---|---|
| `customer_drink_features` (feature table) | base table | All 21 features — totals, percentages, preferences, customization history |
| `barista_notes` (source table) | LEFT join on `customer_id` | Aggregated via `collect_list` → `concat_ws` into a single `barista_notes_text` string with date-stamped entries |
| `drinks` (source table) | LEFT join on `top_drink_id = drink_id` | `fav_drink_name`, `fav_drink_flavor`, `fav_drink_description`, `fav_drink_base` |

Customers without barista notes get the default: `"No barista notes available."`.

#### Step 2: Two `ai_query` Calls in One SQL Pass

The SQL `SELECT` generates both features simultaneously using two `ai_query` calls with different prompts:

**Profile Summary Prompt** — detailed internal brief:
```
You are a Starbucks barista AI. Generate a concise customer profile (3-4 sentences)...

CUSTOMER: {customer_id} ({loyalty_tier} member)
MILK: {milk_preference} [ | LESS SUGAR] [ | DECAF]
VISITS: {typical_visit_time} regular, ~{orders_per_week} orders/week
FAVORITE: {top_drink_name} ({total_orders * 0.4}+ times) — {fav_drink_flavor}
ALSO ORDERS: {unique_drinks_tried} different drinks tried
PREFERRED SIZE: {preferred_size}
CUSTOMIZATIONS SEEN: {customization_history}
AVG CAFFEINE: {avg_caffeine_mg}mg | AVG CALORIES: {avg_calories}

BARISTA NOTES:
- {barista_notes_text}
```

**Barista Greeting Prompt** — customer-facing output:
```
You are a friendly Starbucks barista. Generate a SHORT, natural greeting...

CUSTOMER: {customer_id} ({loyalty_tier} member)
TOP DRINK: {top_drink_name} ({preferred_size})
MILK: {milk_preference} [ | less sugar] [ | decaf]
VISITS: {typical_visit_time} regular, ~{orders_per_week}/week
CUSTOMIZATIONS: {customization_history}
BARISTA NOTES:
- {barista_notes_text}
```

#### Step 3: What Gets Stored (Example — C001 Sarah Chen)

**`profile_summary`**:
> *Sarah is a gold-tier morning regular who comes in almost every day before her 7:30 train. Her go-to is a Vanilla Sweet Cream Cold Brew with oat milk and an extra shot, but she's been shifting toward the Brown Sugar Oat Shaken Espresso to cut sweetness. She's particular about oat milk (no substitutions) and prefers light ice. Friendly and outgoing — she recommends drinks to friends and is basically a store ambassador.*

**`barista_greeting`**:
> *Hey Sarah, I see you're in a bit early today, hope you're getting a good start before that 7:30 train. I'm going to take a guess you're in the mood for your usual vanilla sweet cream cold brew with oat milk, extra shot, and less sugar — would that be right?*

Both are stored as features and synced to Lakebase. At serving time, notebook 04 does a pure SQL lookup — no LLM calls.

### Synced Tables (Online Serving Layer)

| Synced Table | Source Feature Table | Lakebase Instance |
|---|---|---|
| `customer_drink_features_synced` | `customer_drink_features` | `agent-memory-miles` |
| `customer_profile_features_synced_v2` | `customer_profile_features` | `agent-memory-miles` |

Both use **triggered scheduling** and sync to the `gekit_serving` logical database in the `agent-memory-miles` Lakebase instance. They appear as `FOREIGN` tables in Unity Catalog and support standard SQL queries with sub-second latency for point lookups.

Synced Tables are the Databricks replacement for Online Tables — they sync Delta table data to a managed Postgres (Lakebase) instance, providing the same low-latency point lookups with the added benefit of Lakebase's managed infrastructure.

## Notebooks

Run in order on any Databricks serverless environment.

| # | Notebook | What It Does | Runtime |
|---|---|---|---|
| 01 | `01_setup_data` | Creates all 5 source tables with synthetic data | ~1 min |
| 02 | `02_feature_engineering` | Builds both feature tables; uses `ai_query` for profiles AND greetings | ~2-3 min |
| 03 | `03_online_serving` | Enables CDF, creates synced tables in Lakebase, validates they're online | ~1-2 min |
| 04 | `04_barista_demo` | Live demo — pure feature lookups, zero LLM calls at serving time | ~1 min |

**Dependencies**: Notebooks 01 and 02 require `databricks-feature-engineering` as an environment dependency. Notebooks 03 and 04 run on vanilla serverless.

## Workspace

- **Workspace**: `fevm-madkins2` (`https://fevm-madkins2.cloud.databricks.com`)
- **Catalog**: `madkins2_catalog`
- **Schema**: `gekit`
- **Notebook path**: `/Users/miles.adkins@databricks.com/gekit-feature-store-demo/`
- **Lakebase instance**: `agent-memory-miles`
- **Lakebase database**: `gekit_serving`
- **Foundation model**: `databricks-meta-llama-3-3-70b-instruct`

## Key Demo Talking Points

1. **Zero LLM at serving time**: Both `profile_summary` and `barista_greeting` are pre-computed as features using `ai_query`. At serving time, it's a pure key-value lookup from Lakebase — no inference, no latency spike, no per-request token costs.

2. **Structured + Unstructured → LLM Feature**: The LLM prompt is built from transaction aggregates (quantitative), barista notes (qualitative free-text), and drink metadata (catalog attributes). The LLM synthesizes these into actionable features that would be impossible to derive with SQL alone.

3. **Synced Tables auto-refresh**: When new transactions land or barista notes are added, re-running the feature engineering pipeline and triggering a sync propagates changes to the serving layer automatically.

4. **Unity Catalog governance**: Every table, feature table, and synced table is governed under UC — lineage, access control, and audit trail included. The LLM-generated features are first-class UC assets.

5. **Lakebase as the serving layer**: Synced Tables backed by managed Postgres provide the same sub-second point lookups as the deprecated Online Tables, with the added benefit of being queryable via standard SQL and Postgres clients.

6. **Maps to the Gekit pattern**: This is the same architecture as the DoorDash grocery replacement reference (`view.py` → `mcp_server.py` → `agent.py`), but reimplemented with Databricks-native components. The key improvement: the greeting is pre-computed as a feature rather than generated at serving time by an agent.
