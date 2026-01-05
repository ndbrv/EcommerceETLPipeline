{{
    config(
        materialized='table',
        tags=['transform', 'transactions']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

transformed AS (
    SELECT
        -- IDs
        t.source_transaction_id,
        t.source_order_id,
        
        -- Enrich with order info
        o.source_customer_id,
        o.order_date,
        o.order_status,
        o.total_amount AS order_total_amount,
        
        -- Transaction Details
        t.transaction_type,
        t.transaction_status,
        t.transaction_date,
        t.transaction_amount,
        
        -- Payment Details
        t.payment_method,
        t.payment_processor,
        t.processor_transaction_id,
        t.card_last_four,
        
        -- Transaction Status Categories
        CASE 
            WHEN t.transaction_status = 'approved' THEN 'Successful'
            WHEN t.transaction_status IN ('processing', 'pending') THEN 'In Progress'
            WHEN t.transaction_status IN ('failed', 'declined') THEN 'Failed'
            ELSE 'Unknown'
        END AS transaction_status_category,
        
        CASE 
            WHEN t.transaction_status = 'approved' THEN TRUE
            ELSE FALSE
        END AS is_successful,
        
        CASE 
            WHEN t.transaction_status IN ('failed', 'declined') THEN TRUE
            ELSE FALSE
        END AS is_failed,
        
        -- Transaction Type Flags
        CASE WHEN t.transaction_type = 'payment' THEN TRUE ELSE FALSE END AS is_payment,
        CASE WHEN t.transaction_type = 'refund' THEN TRUE ELSE FALSE END AS is_refund,
        
        -- Amount with direction (negative for refunds)
        CASE 
            WHEN t.transaction_type = 'refund' THEN -1 * ABS(t.transaction_amount)
            ELSE t.transaction_amount
        END AS signed_amount,
        
        -- Time between order and transaction
        DATEDIFF(minute, o.order_date, t.transaction_date) AS minutes_from_order_to_transaction,
        DATEDIFF(hour, o.order_date, t.transaction_date) AS hours_from_order_to_transaction,
        DATEDIFF(day, o.order_date, t.transaction_date) AS days_from_order_to_transaction,
        
        CASE 
            WHEN DATEDIFF(minute, o.order_date, t.transaction_date) <= 5 THEN 'Immediate'
            WHEN DATEDIFF(minute, o.order_date, t.transaction_date) <= 60 THEN 'Fast'
            WHEN DATEDIFF(hour, o.order_date, t.transaction_date) <= 24 THEN 'Same Day'
            WHEN DATEDIFF(day, o.order_date, t.transaction_date) <= 7 THEN 'Within Week'
            ELSE 'Delayed'
        END AS transaction_timing,
        
        -- Extract date parts
        DATE_TRUNC('month', t.transaction_date) AS transaction_month,
        DATE_TRUNC('week', t.transaction_date) AS transaction_week,
        DAYOFWEEK(t.transaction_date) AS day_of_week,
        HOUR(t.transaction_date) AS hour_of_day,
        
        -- Transaction size category
        CASE 
            WHEN t.transaction_amount < 50 THEN 'Small'
            WHEN t.transaction_amount < 100 THEN 'Medium'
            WHEN t.transaction_amount < 500 THEN 'Large'
            ELSE 'Extra Large'
        END AS transaction_size,
        
        -- Metadata
        t.generated_at,
        t.loaded_at,
        t.batch_id,
        t.source_system
        
    FROM transactions t
    LEFT JOIN orders o ON t.source_order_id = o.source_order_id
)

SELECT * FROM transformed

