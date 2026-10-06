# Senior Data Engineer Take-Home Assessment — Product & Platform Data Pipeline

An end-to-end production-minded data ingestion and transformation pipeline implemented on **Databricks Lakehouse**, **PySpark**, **Delta Lake**, and **dbt Core**.

---

## 📋 Table of Contents
1. [Project Overview & Architecture](#-project-overview--architecture)
2. [Repository Structure](#-repository-structure)
3. [Environment & Setup Instructions](#-environment--setup-instructions)
4. [Technology Rationale](#-technology-rationale)
5. [Validation & Data Quality Strategy](#-validation--data-quality-strategy)
6. [Incremental Ingestion & Watermark Strategy](#-incremental-ingestion--watermark-strategy)
7. [Testing & Quality Assurance](#-testing--quality-assurance)
8. [Gap Analysis & Technical Pointers](#-gap-analysis--technical-pointers)
9. [Product & Platform Design Note Summary](#-product--platform-design-note-summary)
10. [AI Tool Usage Disclosure](#-ai-tool-usage-disclosure)

---

## 🏗️ Project Overview & Architecture

This solution ingests financial payment transactions from a REST API, validates records against strict schema and enum rules, routes defective and duplicate records to a quarantine dead-letter table, persists valid transactions in a raw/bronze Delta table, models an idempotent daily account summary data mart, and tracks state incrementally using watermark timestamps.

```
┌─────────────────────────┐
│   REST API Source       │ (Supabase REST API: /transactions)
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ PySpark Ingestion Engine│ (Pydantic Validation & Natural Key Deduplication)
└──────┬────────────┬─────┘
       │            │
  Valid│            │Defective / Duplicate
       ▼            ▼
┌──────────────┐  ┌─────────────────────────┐
│ Bronze Table │  │ Quarantine Table        │
│ (Raw Ledger) │  │ (Dead-Letter + Reasons) │
└──────┬───────┘  └─────────────────────────┘
       │
       ▼
┌─────────────────────────┐
│ dbt Core / Spark SQL    │ (Idempotent Transformation Mart)
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────┐
│ Daily Account Summary   │ (Gold Data Product Table)
└─────────────────────────┘
```

---

## 📁 Repository Structure

```
senior-de-assignment/
├── README.md                           # Main repository documentation & run instructions
├── docs/
│   ├── product_platform_note.md        # Comprehensive Product & Platform Engineering Design Note
│   └── requirements/                   # Original assessment PDF prompt
├── ingestion/
│   ├── ingest_transactions.ipynb       # Core ingestion pipeline with Pydantic validation & retry logic
│   └── incremental_ingest.ipynb       # Two-pass orchestration demonstrating high-watermark persistence
├── dbt_project/                        # Production dbt Core transformation project
│   ├── dbt_project.yml                 # dbt project definition
│   ├── packages.yml                    # External dbt packages (dbt_utils)
│   └── models/
│       ├── sources.yml                 # Source definition for main_dev.bronze_transactions
│       └── marts/
│           ├── daily_account_summary.sql # Gold layer dbt transformation model
│           └── schema.yml              # Column documentation & dbt_utils validation tests
├── sql/
│   └── daily_account_summary.ipynb     # Interactive Databricks SQL notebook for table creation
├── tests/
│   ├── run_validation_tests.ipynb      # Automated PySpark / SQL quality assertion checks
│   └── generate_submission_outputs.ipynb # Generator producing submission CSV/JSON artifacts
└── outputs/                            # Evaluated submission artifacts
    ├── daily_account_summary_sample.csv # Sample output (100 rows)
    ├── quarantine_sample.csv           # Sample dead-letter output showing error reasons
    ├── watermark_run1.json             # Watermark state after initial run
    └── watermark_run2.json             # Watermark state after incremental run
```

---

## 🚀 Environment & Setup Instructions

### Prerequisites
* Databricks Community Edition (or Standard/Enterprise workspace with Unity Catalog).
* Python 3.10+ runtime with PySpark and `pydantic`.
* dbt Core with `dbt-databricks` or `dbt-spark` connector (optional, if running dbt CLI directly).

### Configuration & Credentials
Set environment variables or enter workspace widget values:
* `ASSESSMENT_API_BASE_URL`: `https://****.supabase.co/rest/v1`
* `ASSESSMENT_API_KEY`: `sb_publishable_******************G`
* `ASSESSMENT_AUTH_TOKEN`: `sb_publishable_******************G`

### Step-by-Step Pipeline Execution (Databricks Platform)

1. **Import Workspace Repository:**
   * Import the project folder into your Databricks workspace under `/Workspace/Users/<your_email>/senior-de-assignment/`.

2. **Execute Raw Ingestion Pipeline:**
   * Open and run `ingestion/ingest_transactions.ipynb`.
   * This fetches records, performs validation and deduplication, and creates Delta tables `main_dev.bronze_transactions` and `main_dev.quarantine_transactions`.

3. **Verify Incremental Watermarking:**
   * Open and run `ingestion/incremental_ingest.ipynb`.
   * This executes Run 1 (initial sync) and Run 2 (incremental verification) and saves state to Unity Catalog volumes `/Volumes/workspace/default/ingestion_state/watermark.json`.

4. **Run Transformation Layer:**
   * **Via Databricks Notebook:** Run `sql/daily_account_summary.ipynb`.
   * **Via dbt CLI:**
     ```bash
     cd dbt_project
     dbt deps
     dbt run --select daily_account_summary
     dbt test --select daily_account_summary
     ```

5. **Execute Validation & Export Submission Artifacts:**
   * Run `tests/run_validation_tests.ipynb` to verify data quality assertions.
   * Run `tests/generate_submission_outputs.ipynb` to refresh CSV/JSON files in the `outputs/` folder.

---

## 🛠️ Technology Rationale

* **PySpark + Delta Lake:** Provides ACID transactions, schema enforcement, append-only performance, and time-travel capabilities needed for raw medallion lakehouse storage.
* **Pydantic (`BaseModel`):** Enables robust, pythonic schema validation at the ingestion boundary with clean exception catching for dead-letter routing.
* **dbt Core:** Industry standard for modular SQL modeling, line-of-sight data documentation, version-controlled transformations, and testing.
* **Unity Catalog Volumes:** Reliable, cloud-agnostic storage for high-watermark JSON metadata state.

---

## 🛡️ Validation & Data Quality Strategy

### 1. Schema Validation (Pydantic Model)
Each record returned by the REST API is validated against `TransactionModel`:
* `amount`: Must be strictly greater than 0 (`> 0`).
* `currency`: Enum check against valid codes (`USD`, `EUR`, `GBP`, `CHF`, `JPY`, `AUD`, `CAD`).
* `transaction_type`: Lowercase only (`debit` or `credit`).
* `merchant_name`: Non-empty and not whitespace-only.
* `merchant_category`: Valid category enum (`e-commerce`, `travel`, `food_and_beverage`, `groceries`, `electronics`, `retail`, `entertainment`, `health`, `transportation`, `home_and_garden`, `payroll`, `transfer`).
* `status`: Lowercase only (`completed`, `pending`, `failed`, `reversed`).
* `country_code`: 2-character uppercase string.

### 2. Dead-Letter Quarantine Routing
Invalid records failing Pydantic validation are caught cleanly (without crashing the pipeline) and routed to `main_dev.quarantine_transactions` with metadata:
* `error_reason`: Full exception string detailing exact field rule violations.
* `ingestion_timestamp`: UTC execution timestamp.

### 3. Duplicate Detection Strategy
* **Natural Key:** Combination of all fields except `transaction_id`: `(account_id, transaction_date, amount, currency, transaction_type, merchant_name, merchant_category, status, country_code)`.
* **Action Taken:** Duplicates are flagged and routed directly to the quarantine table with `error_reason = "Duplicate transaction record detected based on structural natural key validation rules."` to prevent double-counting in downstream financial marts.

---

## 🔄 Incremental Ingestion & Watermark Strategy

1. **High-Watermark Persistence:**
   * High-watermark tracks `max_transaction_date` from successfully processed valid transactions.
   * State is persisted as JSON at `/Volumes/workspace/default/ingestion_state/watermark.json`.
2. **Native API Filtering:**
   * Subsequent runs request `GET /transactions?transaction_date=gte.<watermark>&order=transaction_date.asc`.
3. **Execution Behavior:**
   * **First Run:** Watermark defaults to `2024-01-01T00:00:00Z` and ingests all historical data.
   * **Incremental Run:** Pipeline resumes from saved watermark, requesting only newer/equal records.
   * **No-New-Data Behavior:** Handles empty API payload responses gracefully without mutating watermark state.
   * **Late-Arriving Data Strategy:** For production, a 3-day lookback window (`watermark - 3 days`) combined with Delta `MERGE INTO` is recommended to capture backdated refunds or offline terminal syncs.

---

## 🧪 Testing & Quality Assurance

1. **dbt Column & Metric Tests (`dbt_project/models/marts/schema.yml`):**
   * Primary Key Uniqueness: `dbt_utils.unique_combination_of_columns` on `(account_id, transaction_date)`.
   * Non-Null Assertions: On `account_id`, `transaction_date`, `total_debit_amount`, `total_credit_amount`, `net_amount`, `transaction_count`, `top_category`, `updated_at`.
   * Expression Checks: `dbt_utils.expression_is_true` checking `total_debit_amount >= 0`, `total_credit_amount >= 0`, `transaction_count > 0`.
   * Enum Accepted Values: Validation on `top_category`.

2. **Automated Notebook Assertions (`tests/run_validation_tests.ipynb`):**
   * Verifies non-null primary keys.
   * Enforces zero row explosion on composite key grain.
   * Asserts non-negative financial debit aggregates.

---

## 🔍 Gap Analysis & Technical Pointers

Based on an audit of the implemented pipeline against the assessment requirements, the following key pointers highlight areas where the code can be improved for full production readiness:

1. **Strict ISO 8601 UTC Date Validation (Critical):**
   * *Issue:* `TransactionModel` in Pydantic treats `transaction_date` as a generic `str` without strict ISO 8601 parsing (`YYYY-MM-DDTHH:MM:SSZ`) or calendar date validation.
   * *Impact:* Invalid dates such as `2024-11-31T14:22:00Z` (November 31st does not exist) passed through Pydantic validation into the watermark calculation, advancing the watermark to an invalid date.
   * *Recommendation:* Add `@field_validator('transaction_date')` using `datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")` or `datetime.fromisoformat` to quarantine invalid date strings.

2. **ISO 3166-1 Alpha-2 Country Code Lookup:**
   * *Issue:* The country code validator only checks `len(v) == 2 and v.isupper()`.
   * *Impact:* Violates requirement "Format alone is not sufficient" by accepting non-existent 2-letter codes like `"XX"`.
   * *Recommendation:* Validate against an explicit ISO 3166-1 alpha-2 country set or `pycountry` lookup.

3. **Incremental Boundary Deduplication Across Runs (`gte.` vs `gt.`):**
   * *Issue:* The API filter uses `gte.<watermark>`, returning boundary records from previous runs. Since `natural_keys_seen` is tracked in-memory per single run, re-fetched boundary records could be appended again to Delta tables on subsequent runs.
   * *Recommendation:* Replace append mode with Delta Lake `MERGE INTO target USING source ON target.natural_key = source.natural_key` or use `gt.<watermark>` filtering.

4. **Hardcoded User Workspace Paths:**
   * *Issue:* Notebooks contain hardcoded paths like `/Users/sourav.dw91@gmail.com/...`.
   * *Impact:* Hinders portability across different user workspaces or automated CI/CD job clusters.
   * *Recommendation:* Use dynamic relative path resolution via Databricks workspace utilities.

5. **Multi-Currency Aggregation:**
   * *Issue:* Financial amounts are summed across multiple currencies into `total_debit_amount` / `total_credit_amount`.
   * *Recommendation:* Introduce daily FX exchange rate conversion tables to compute metrics in a standardized base currency (USD/EUR).

---

## 📝 Product & Platform Design Note Summary

A detailed design note is provided in `docs/product_platform_note.md`, covering:
1. **Challenging Core Assumptions:** Duplicate key definition, multi-currency metrics, rate limits, late-arriving data.
2. **Production Observability & Monitoring:** SLA tracking, error rate alerting, freshness monitoring, volume anomaly detection.
3. **Reusable Multi-API Platform Design:** Metadata-driven config schemas (`config/apis.yaml`), generic connector engines, dynamic Pydantic schema validation.
4. **Data Product Trustworthiness:** Guaranteed primary key uniqueness, deterministic idempotency, SLA badging in Unity Catalog.
5. **Lineage & Metadata Exposure:** Unity Catalog column-level lineage, dbt docs integration, public data quality dashboards.
6. **Architectural Trade-offs:** Detailed rationale for batch vs stream choices, in-memory deduplication, and file-based state stores.
7. **AI Tool Usage & Independent Verification:** Accountability details and validation methodology.

---

## 🤖 AI Tool Usage Disclosure

* **AI Tools Used:** Antigravity AI Code Assistant / LLM Pair Programmer.
* **Usage Context:** AI tools were used for generating boilerplate code structure for Pydantic models, dbt YAML schema files, and structuring markdown documentation.
* **Verification Process:** All logic was empirically tested in Databricks Community Edition notebook runs, verified with automated unit tests in `run_validation_tests.ipynb`, and manually audited against original PDF requirements.
