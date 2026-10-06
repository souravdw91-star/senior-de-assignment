# Product & Platform Engineering Design Note

**Author:** Senior Data Engineer Candidate  
**Assessment:** Senior Data Engineer Take-Home — Product & Platform Engineering  
**Target Platform:** Databricks Lakehouse, PySpark, Delta Lake, dbt Core  

---

## Executive Summary

This document outlines the architectural rationale, platform assumptions, operational readiness, and product design choices for the payment transaction ingestion and transformation platform. Built on the Databricks Lakehouse architecture using PySpark, Delta Lake, and dbt Core, the platform ingests REST API transaction payloads, enforces schema and data quality rules via a quarantine dead-letter pattern, models daily account-level metrics, and tracks high-watermark state incrementally.

---

## 1. Challenging Core Assumptions Before Productionization

Before scaling this pipeline to production, several foundational assumptions in the initial specification must be challenged and refined:

### A. Natural Key Definition & Duplicate Ambiguity
* **Current Assumption:** Transactions with identical attributes across all fields except `transaction_id` represent duplicate real-world events.
* **Challenge:** In high-volume payment processing, legitimate identical transactions can occur (e.g., a customer subscribing twice or tapping twice within seconds for identical amounts at the same merchant). Treating all matching fields as duplicates without a tight temporal window (e.g., within 5 seconds) risks quarantining valid business transactions.
* **Production Recommendation:** Establish explicit event identifiers from payment gateways (e.g., `idempotency_key` or `payment_intent_id`) at the API source. If unavailable, combine natural keys with a strict time-bucket window.

### B. Multi-Currency Aggregation Without Exchange Rates
* **Current Assumption:** Summing `amount` directly across debit/credit transactions for an account regardless of currency, while outputting a list of `currencies`.
* **Challenge:** Aggregating $100 USD and ¥100 JPY into a single `total_debit_amount` yields a mathematically invalid metric (`100 + 100 = 200` unitless sum), misrepresenting financial position to analysts.
* **Production Recommendation:** Introduce a curated daily FX rate reference table (e.g., `dim_currency_rates`) and convert all transactions to a standardized base currency (e.g., USD or EUR) at the Gold layer while preserving original transaction amounts in reporting marts.

### C. Source API Reliability & Rate Limits
* **Current Assumption:** Sequential REST API pagination over HTTP will scale predictably.
* **Challenge:** Synchronous, single-threaded pagination is a bottleneck for millions of records. Rate limits (HTTP 429) under sudden load can cause pipeline timeouts or worker starvation.
* **Production Recommendation:** Transition high-volume ingestion to an asynchronous connector framework with rate-limiter tokens, or migrate source ingestion to webhook events / CDC streams landed in cloud object storage (S3/ADLS) prior to Spark processing.

### D. Late-Arriving Data & Source Mutability
* **Current Assumption:** Source transaction dates arrive in near-chronological order and historical records never change.
* **Challenge:** Backdated refunds, dispute reversals, or offline terminal syncing result in late-arriving events older than the current high-watermark, which simple `gte.` API filters would permanently miss.
* **Production Recommendation:** Implement a configurable lookback window (e.g., watermark minus 3 days) combined with idempotent Delta Lake `MERGE INTO` operations.

---

## 2. Production Observability, Monitoring & Alerting Strategy

To maintain high data SLA guarantees and operational transparency, the production pipeline implements multi-layered observability across infrastructure, data quality, and business logic.

```
                  +-----------------------------------+
                  |   REST API Source / Webhook       |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------+-----------------+
                  |   PySpark Ingestion Engine        |
                  |   - Retry & Exponential Backoff   |
                  +--------+----------------+---------+
                           |                |
           Valid Records   |                | Defective Records
                           v                v
      +--------------------+----+      +----+-----------------------+
      |  Bronze Delta Table     |      | Quarantine Delta Table    |
      |  (Raw Ledger + Metadata)|      | (Dead-Letter + Reasons)   |
      +--------------------+----+      +----+-----------------------+
                           |                |
                           |                v
                           |      +---------+-----------------------+
                           |      | Operational Alerting Engine     |
                           |      | - PagerDuty / Slack Triggers    |
                           |      +---------------------------------+
                           v
      +--------------------+----+
      |  dbt Transformation     |
      |  (Gold Marts Layer)     |
      +--------------------+----+
                           |
                           v
      +--------------------+----+
      |  Daily Account Summary  |
      |  (Product Data Mart)    |
      +-------------------------+
```

### Key Metrics & Alerts

| Metric Category | Specific Indicator | Warning Threshold | Critical Alert Threshold | Target Action |
|---|---|---|---|---|
| **Pipeline Reliability** | Job Execution Duration | > 15 mins | > 30 mins / Timeout | Alert DE On-Call; check API latency |
| **API Health** | HTTP 429/5xx Error Count | > 5 retries / run | > 15 retries / Failure | Trigger API vendor incident response |
| **Data Quality** | Quarantine Ratio (`Quarantined / Total`) | > 2% of payload | > 5% of payload | Page Data Engineer; inspect upstream schema drift |
| **Freshness SLA** | Watermark Lag (`Now - Watermark`) | > 6 hours | > 24 hours | Escalate pipeline delay to downstream stakeholders |
| **Volume Anomalies** | Row Count Variance vs 7-day Moving Avg | ± 30% deviation | ± 50% deviation | Trigger automated data pause to prevent downstream skew |
| **Integrity Checks** | Primary Key Uniqueness Test Failures | Any duplicate (1+) | Any duplicate (1+) | Halt dbt compilation; investigate aggregation logic |

---

## 3. Designing a Reusable Multi-API Ingestion Framework

To extend this solution from 1 API to 10+ payment and financial APIs without duplicating code, we abstract the pipeline into a **metadata-driven ingestion engine**.

### Framework Architecture

1. **API Configuration Schema (`config/apis.yaml`):**
   ```yaml
   api_name: stripe_transactions
   endpoint_url: "https://api.stripe.com/v1/charges"
   auth_type: bearer_token
   pagination:
     type: offset_or_cursor
     limit_param: limit
     offset_param: starting_after
   watermark:
     field: created
     strategy: timestamp_lookback
     lookback_hours: 6
   pydantic_schema: "schemas.stripe.StripeChargeModel"
   target_bronze_table: "workspace.bronze.stripe_charges"
   target_quarantine_table: "workspace.quarantine.stripe_charges"
   ```

2. **Core Components:**
   * **Generic Connector Engine:** PySpark driver executing standardized paginated HTTP calls with token bucket rate-limiting and retry backoff.
   * **Dynamic Schema Validation Engine:** Accepts a target Pydantic model dynamically imported from the configuration, outputting standardized valid and quarantined `Row` objects.
   * **Unified Medallion Storage Handler:** Standardized Delta Lake write adapter handling ingestion metadata tagging (`ingestion_timestamp`, `source_api`, `schema_version`).

---

## 4. Building a Trustworthy Data Product for Analysts & Product Managers

A data product is trustworthy only when consumers can rely on its correctness, completeness, timeliness, and semantic clarity.

### Strategy for `daily_account_summary`:

1. **Enforce Strict Grain & Uniqueness:**
   * Primary key: `(account_id, transaction_date)`. Verified via dbt test `unique_combination_of_columns`. Zero duplicate risk.
2. **Deterministic Idempotency:**
   * Standardized SQL transformation using `CREATE OR REPLACE TABLE` (or dbt `materialized='table'`). Rerunning historical dates guarantees identical results without row duplication or ghost records.
3. **Data Quality SLA Badging:**
   * Expose a metadata column (`updated_at`) and link table catalog tags in Unity Catalog showing automated test pass/fail status from the latest run.
4. **Transparent Business Logic:**
   * Deterministic tie-breaking for `top_category` spend using explicit ordering (`ORDER BY SUM(amount) DESC, merchant_category ASC`).

---

## 5. Exposing Lineage, Ownership, Documentation, and Quality Status

To turn dark data into an open, self-serve asset, metadata must be surfaced directly in user workflows:

* **Unity Catalog Integration:**
  * Tag tables with ownership metadata (`Owner: Data Platform Team`, `SLA: Tier-1 Daily 06:00 UTC`).
  * Publish column-level descriptions directly inside the Databricks Metastore via dbt docs sync (`dbt-databricks`).
* **Automated Data Lineage:**
  * Utilize Unity Catalog automated column-level lineage to map data dependencies from `REST API -> Bronze Delta -> Quarantine / Gold Daily Summary`.
* **Data Quality Visibility:**
  * Expose quarantine metrics in a public Databricks SQL Dashboard accessible by analysts, showing daily valid vs. quarantined volume trends and top error reasons.

---

## 6. Architectural Trade-offs & Time-Limit Rationale

Due to the 3-4 hour duration constraint, explicit pragmatic trade-offs were made:

| Feature / Area | Time-Constrained Implementation | Full Production Design | Rationale for Choice |
|---|---|---|---|
| **Deduplication Engine** | In-memory Python `set()` checking natural key per batch. | Delta Lake `MERGE INTO` with stateful deduplication table. | Simple, zero extra cluster overhead, sufficient for batch sizes under assessment test limits. |
| **Watermark Storage** | JSON file stored in Unity Catalog Volume. | Databricks Control Database / Key-Value State Store. | Fast file I/O using native Databricks Volume paths without requiring external database setups. |
| **Currency Handling** | Comma-separated string list of distinct currencies (`currencies`). | Multi-currency conversion via daily FX rate dim table to standardized base currency. | Avoided assuming inaccurate FX conversion rates while preserving full currency visibility for analysts. |
| **Parallelism** | Single-threaded paginated HTTP fetch in driver Python process. | Distributed PySpark `mapPartitions` or asynchronous `aiohttp` parallel fetch. | Prevents API rate-limit threshold breaching (HTTP 429) during initial assessment evaluation. |

---

## 7. AI Tool Usage Disclosure & Verification Methodology

### AI Assistance Breakdown
* **AI Tool Used:** Antigravity AI Code Assistant / LLM Pair Programmer.
* **Tasks Assisted:**
  1. Drafted initial Pydantic schema validation models (`TransactionModel`).
  2. Generated template dbt `schema.yml` configuration with `dbt_utils` test specifications.
  3. Formatted markdown documentation structure for project reports.

### Independent Verification & Accountability
* **Schema Validation Audit:** Tested Pydantic validation edge cases against synthetic invalid records (e.g. negative amounts, invalid status strings, malformed categories).
* **Idempotency & Watermark Run Testing:** Executed multi-pass execution testing in `incremental_ingest.ipynb`, verifying that Run 2 executed cleanly without duplicating records or corrupting the persisted high-watermark JSON payload.
* **SQL Logic Review:** Verified CTE window function logic (`ROW_NUMBER() OVER (...)`) for `top_category` ranking to confirm correct aggregation behavior on tie-breaking spend scenarios.
