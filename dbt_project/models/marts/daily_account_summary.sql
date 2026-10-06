{{ config(
    materialized='table',
    file_format='delta'
) }}

-- CTE 1: Filter for completed and valid transactions only
WITH filtered_transactions AS (
    SELECT 
        account_id,
        -- Safely extract and truncate the ISO 8601 string to a clean UTC Date object
        CAST(TO_TIMESTAMP(transaction_date) AS DATE) AS transaction_date,
        -- Ensure precise numeric metrics calculations by casting string amount to Decimal
        CAST(amount AS DECIMAL(18, 4)) AS amount_numeric,
        transaction_type,
        merchant_name,
        merchant_category,
        currency
    FROM {{ source('main_dev', 'bronze_transactions') }}
    -- Enforce Task 2 Requirements: isolate completed records and implicitly bypass quarantine
    WHERE status = 'completed'
),

-- CTE 2: Aggregate standard metrics grouped by Account and Day
daily_base_metrics AS (
    SELECT
        account_id,
        transaction_date,
        COALESCE(SUM(CASE WHEN transaction_type = 'debit' THEN amount_numeric ELSE 0 END), 0) AS total_debit_amount,
        COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount_numeric ELSE 0 END), 0) AS total_credit_amount,
        COUNT(1) AS transaction_count,
        COUNT(DISTINCT merchant_name) AS distinct_merchants,
        -- Collect distinctive currency strings into an ordered, comma-separated format
        ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(COLLECT_LIST(currency))), ', ') AS currencies
    FROM filtered_transactions
    GROUP BY account_id, transaction_date
),

-- CTE 3: Compute total volume per category to figure out the "top_category"
category_spend_ranking AS (
    SELECT 
        account_id,
        transaction_date,
        merchant_category,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_date 
            ORDER BY SUM(amount_numeric) DESC, merchant_category ASC
        ) AS category_rank
    FROM filtered_transactions
    GROUP BY account_id, transaction_date, merchant_category
),

-- CTE 4: Filter out the absolute highest spend category for each block window
top_category_filtered AS (
    SELECT 
        account_id,
        transaction_date,
        merchant_category AS top_category
    FROM category_spend_ranking
    WHERE category_rank = 1
)

-- Final Query Assemble Stage: Combine aggregates with the top performing categories
SELECT
    m.account_id,
    m.transaction_date,
    CAST(m.total_debit_amount AS DECIMAL(18, 2)) AS total_debit_amount,
    CAST(m.total_credit_amount AS DECIMAL(18, 2)) AS total_credit_amount,
    -- Net Amount Calculation: Credit values minus Debit values
    CAST((m.total_credit_amount - m.total_debit_amount) AS DECIMAL(18, 2)) AS net_amount,
    m.transaction_count,
    m.distinct_merchants,
    t.top_category,
    m.currencies,
    -- Standard lineage field displaying compilation run metadata checkpoints
    CURRENT_TIMESTAMP() AS updated_at
FROM daily_base_metrics m
LEFT JOIN top_category_filtered t 
    ON m.account_id = t.account_id 
    AND m.transaction_date = t.transaction_date
