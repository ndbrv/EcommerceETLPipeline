{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'payments']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('transform_transactions') }}
),

orders AS (
    SELECT
        source_order_id,
        payment_method,
        order_date,
        total_amount
    FROM {{ ref('transform_orders') }}
),

payment_method_metrics AS (
    SELECT
        o.payment_method,
        
        -- Transaction counts
        COUNT(DISTINCT t.source_transaction_id) AS total_transactions,
        COUNT(DISTINCT CASE WHEN t.is_successful THEN t.source_transaction_id END) AS successful_transactions,
        COUNT(DISTINCT CASE WHEN t.is_failed THEN t.source_transaction_id END) AS failed_transactions,
        COUNT(DISTINCT CASE WHEN t.is_payment THEN t.source_transaction_id END) AS payment_transactions,
        COUNT(DISTINCT CASE WHEN t.is_refund THEN t.source_transaction_id END) AS refund_transactions,
        
        -- Order counts
        COUNT(DISTINCT o.source_order_id) AS total_orders,
        
        -- Transaction amounts
        SUM(CASE WHEN t.is_payment AND t.is_successful THEN t.transaction_amount ELSE 0 END) AS total_payment_amount,
        SUM(CASE WHEN t.is_refund AND t.is_successful THEN t.transaction_amount ELSE 0 END) AS total_refund_amount,
        SUM(CASE WHEN t.is_successful THEN t.signed_amount ELSE 0 END) AS net_amount,
        
        -- Average metrics
        AVG(CASE WHEN t.is_payment THEN t.transaction_amount END) AS avg_payment_amount,
        AVG(CASE WHEN t.is_refund THEN t.transaction_amount END) AS avg_refund_amount,
        
        -- Order amounts
        SUM(o.total_amount) AS total_order_value,
        AVG(o.total_amount) AS avg_order_value,
        
        -- Timing metrics
        AVG(CASE WHEN t.is_payment THEN t.minutes_from_order_to_transaction END) AS avg_minutes_to_payment,
        
        -- Success rates
        ROUND(COUNT(CASE WHEN t.is_successful THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS overall_success_rate,
        ROUND(COUNT(CASE WHEN t.is_payment AND t.is_successful THEN 1 END)::DECIMAL / 
              NULLIF(COUNT(CASE WHEN t.is_payment THEN 1 END), 0) * 100, 2) AS payment_success_rate,
        
        -- Refund rate
        ROUND(COUNT(CASE WHEN t.is_refund THEN 1 END)::DECIMAL / 
              NULLIF(COUNT(CASE WHEN t.is_payment THEN 1 END), 0) * 100, 2) AS refund_rate
        
    FROM transactions t
    JOIN orders o ON t.source_order_id = o.source_order_id
    GROUP BY o.payment_method
),

processor_metrics AS (
    SELECT
        payment_processor,
        
        -- Transaction counts
        COUNT(DISTINCT source_transaction_id) AS total_transactions,
        COUNT(DISTINCT CASE WHEN is_successful THEN source_transaction_id END) AS successful_transactions,
        COUNT(DISTINCT CASE WHEN is_failed THEN source_transaction_id END) AS failed_transactions,
        
        -- Transaction amounts
        SUM(CASE WHEN is_successful THEN transaction_amount ELSE 0 END) AS total_processed_amount,
        AVG(CASE WHEN is_successful THEN transaction_amount END) AS avg_transaction_amount,
        
        -- Success rate
        ROUND(COUNT(CASE WHEN is_successful THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS success_rate,
        
        -- Timing
        AVG(minutes_from_order_to_transaction) AS avg_processing_time_minutes
        
    FROM transactions
    WHERE payment_processor IS NOT NULL
    GROUP BY payment_processor
),

final AS (
    SELECT
        -- Payment method
        pm.payment_method,
        
        -- Transaction volumes
        pm.total_transactions,
        pm.successful_transactions,
        pm.failed_transactions,
        pm.payment_transactions,
        pm.refund_transactions,
        pm.total_orders,
        
        -- Transaction amounts
        pm.total_payment_amount,
        pm.total_refund_amount,
        pm.net_amount,
        pm.avg_payment_amount,
        pm.avg_refund_amount,
        
        -- Order metrics
        pm.total_order_value,
        pm.avg_order_value,
        
        -- Timing
        pm.avg_minutes_to_payment,
        
        -- Success rates
        pm.overall_success_rate,
        pm.payment_success_rate,
        pm.refund_rate,
        
        -- Performance indicators
        CASE
            WHEN pm.payment_success_rate >= 95 THEN 'Excellent'
            WHEN pm.payment_success_rate >= 90 THEN 'Good'
            WHEN pm.payment_success_rate >= 80 THEN 'Fair'
            ELSE 'Poor'
        END AS performance_rating,
        
        CASE
            WHEN pm.refund_rate < 2 THEN 'Low Refunds'
            WHEN pm.refund_rate < 5 THEN 'Normal Refunds'
            WHEN pm.refund_rate < 10 THEN 'High Refunds'
            ELSE 'Very High Refunds'
        END AS refund_level,
        
        -- Flags
        CASE WHEN pm.payment_success_rate < 90 THEN TRUE ELSE FALSE END AS needs_attention,
        CASE WHEN pm.refund_rate > 5 THEN TRUE ELSE FALSE END AS high_refund_rate,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM payment_method_metrics pm
)

SELECT * FROM final
ORDER BY total_payment_amount DESC

