{{
    config(
        materialized='table',
        tags=['transform', 'orders']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

order_item_aggregates AS (
    SELECT
        source_order_id,
        COUNT(*) AS item_count,
        SUM(quantity) AS total_items_quantity,
        SUM(total_price) AS calculated_order_total,
        AVG(unit_price) AS avg_item_price,
        MAX(unit_price) AS max_item_price,
        MIN(unit_price) AS min_item_price
    FROM order_items
    GROUP BY source_order_id
),

transaction_aggregates AS (
    SELECT
        source_order_id,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN transaction_type = 'payment' THEN transaction_amount ELSE 0 END) AS total_payments,
        SUM(CASE WHEN transaction_type = 'refund' THEN transaction_amount ELSE 0 END) AS total_refunds,
        SUM(CASE 
            WHEN transaction_type = 'payment' THEN transaction_amount 
            WHEN transaction_type = 'refund' THEN -1 * transaction_amount 
            ELSE 0 
        END) AS net_transaction_amount,
        COUNT(CASE WHEN transaction_type = 'payment' THEN 1 END) AS payment_count,
        COUNT(CASE WHEN transaction_type = 'refund' THEN 1 END) AS refund_count,
        COUNT(CASE WHEN transaction_status = 'approved' THEN 1 END) AS approved_transaction_count,
        COUNT(CASE WHEN transaction_status = 'failed' THEN 1 END) AS failed_transaction_count,
        COUNT(CASE WHEN transaction_status = 'declined' THEN 1 END) AS declined_transaction_count,
        COUNT(CASE WHEN transaction_status = 'pending' THEN 1 END) AS pending_transaction_count,
        COUNT(CASE WHEN transaction_status = 'processing' THEN 1 END) AS processing_transaction_count
    FROM transactions
    GROUP BY source_order_id
),

transformed AS (
    SELECT
        -- IDs
        o.source_order_id,
        o.source_customer_id,
        
        -- Date/Time
        o.order_date,
        
        -- Order Status
        o.order_status,
        
        CASE 
            WHEN o.order_status IN ('completed', 'delivered') THEN 'Fulfilled'
            WHEN o.order_status IN ('processing', 'shipped') THEN 'In Progress'
            WHEN o.order_status IN ('pending', 'payment_pending') THEN 'Awaiting Action'
            WHEN o.order_status IN ('cancelled', 'refunded') THEN 'Terminated'
            ELSE 'Unknown'
        END AS order_status_category,
        
        CASE 
            WHEN o.order_status IN ('completed', 'delivered') THEN TRUE
            ELSE FALSE
        END AS is_fulfilled,
        
        CASE 
            WHEN o.order_status IN ('cancelled', 'refunded') THEN TRUE
            ELSE FALSE
        END AS is_terminated,
        
        -- Financial Amounts
        o.total_amount,
        oia.calculated_order_total,
        
        -- Variance check between reported and calculated totals
        ABS(o.total_amount - COALESCE(oia.calculated_order_total, 0)) AS amount_variance,
        
        CASE 
            WHEN ABS(o.total_amount - COALESCE(oia.calculated_order_total, 0)) > 0.01 THEN TRUE
            ELSE FALSE
        END AS has_amount_discrepancy,
        
        -- Order Item Details
        COALESCE(oia.item_count, 0) AS item_count,
        COALESCE(oia.total_items_quantity, 0) AS total_items_quantity,
        COALESCE(oia.avg_item_price, 0) AS avg_item_price,
        COALESCE(oia.max_item_price, 0) AS max_item_price,
        COALESCE(oia.min_item_price, 0) AS min_item_price,
        
        -- Order Size Categories
        CASE 
            WHEN o.total_amount < 50 THEN 'Small'
            WHEN o.total_amount < 100 THEN 'Medium'
            WHEN o.total_amount < 500 THEN 'Large'
            ELSE 'Extra Large'
        END AS order_size,
        
        CASE 
            WHEN COALESCE(oia.item_count, 0) = 1 THEN 'Single Item'
            WHEN COALESCE(oia.item_count, 0) <= 3 THEN 'Few Items'
            WHEN COALESCE(oia.item_count, 0) <= 10 THEN 'Multiple Items'
            ELSE 'Bulk Order'
        END AS order_complexity,
        
        -- Transaction Aggregates
        COALESCE(ta.transaction_count, 0) AS transaction_count,
        COALESCE(ta.total_payments, 0) AS total_payments,
        COALESCE(ta.total_refunds, 0) AS total_refunds,
        COALESCE(ta.net_transaction_amount, 0) AS net_transaction_amount,
        COALESCE(ta.payment_count, 0) AS payment_count,
        COALESCE(ta.refund_count, 0) AS refund_count,
        COALESCE(ta.approved_transaction_count, 0) AS approved_transaction_count,
        COALESCE(ta.failed_transaction_count, 0) AS failed_transaction_count,
        COALESCE(ta.declined_transaction_count, 0) AS declined_transaction_count,
        COALESCE(ta.pending_transaction_count, 0) AS pending_transaction_count,
        COALESCE(ta.processing_transaction_count, 0) AS processing_transaction_count,
        
        -- Transaction Flags
        CASE 
            WHEN COALESCE(ta.transaction_count, 0) > 1 THEN TRUE
            ELSE FALSE
        END AS has_multiple_transactions,
        
        CASE 
            WHEN COALESCE(ta.refund_count, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_refunds,
        
        CASE 
            WHEN COALESCE(ta.failed_transaction_count, 0) > 0 
                OR COALESCE(ta.declined_transaction_count, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_failed_transactions,
        
        -- Payment Status Category
        CASE 
            WHEN COALESCE(ta.approved_transaction_count, 0) > 0 THEN 'Paid'
            WHEN COALESCE(ta.pending_transaction_count, 0) > 0 
                OR COALESCE(ta.processing_transaction_count, 0) > 0 THEN 'Payment Pending'
            WHEN COALESCE(ta.failed_transaction_count, 0) > 0 
                OR COALESCE(ta.declined_transaction_count, 0) > 0 THEN 'Payment Failed'
            ELSE 'No Payment'
        END AS payment_status,
        
        -- Payment & Shipping
        o.payment_method,
        o.shipping_address,
        
        -- Time-based attributes
        DATEDIFF(day, o.order_date, CURRENT_DATE()) AS days_since_order,
        DATEDIFF(hour, o.order_date, CURRENT_DATE()) AS hours_since_order,
        
        -- Extract date parts for analysis
        DATE_TRUNC('month', o.order_date) AS order_month,
        DATE_TRUNC('week', o.order_date) AS order_week,
        DAYOFWEEK(o.order_date) AS day_of_week,
        HOUR(o.order_date) AS hour_of_day,
        
        CASE 
            WHEN DAYOFWEEK(o.order_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS weekday_weekend,
        
        -- Metadata
        o.generated_at,
        o.loaded_at,
        o.batch_id,
        o.source_system
        
    FROM orders o
    LEFT JOIN order_item_aggregates oia ON o.source_order_id = oia.source_order_id
    LEFT JOIN transaction_aggregates ta ON o.source_order_id = ta.source_order_id
)

SELECT * FROM transformed

