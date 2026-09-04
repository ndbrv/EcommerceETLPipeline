{{
    config(
        materialized='table',
        tags=['curated', 'fact', 'orders']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

order_items AS (
    SELECT
        source_order_id,
        COUNT(DISTINCT source_product_id) AS distinct_products,
        COUNT(DISTINCT category) AS distinct_categories,
        COUNT(DISTINCT brand) AS distinct_brands,
        LISTAGG(DISTINCT category, ', ') WITHIN GROUP (ORDER BY category) AS categories_list,
        LISTAGG(DISTINCT brand, ', ') WITHIN GROUP (ORDER BY brand) AS brands_list
    FROM {{ ref('transform_order_items') }}
    GROUP BY source_order_id
),

transactions AS (
    SELECT
        source_order_id,
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        COUNT(DISTINCT source_transaction_id) AS transaction_count,
        SUM(CASE WHEN is_payment THEN 1 ELSE 0 END) AS payment_count,
        SUM(CASE WHEN is_refund THEN 1 ELSE 0 END) AS refund_count,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_transaction_count,
        SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) AS failed_transaction_count
    FROM {{ ref('transform_transactions') }}
    GROUP BY source_order_id
),

final AS (
    SELECT
        -- Order identifiers
        o.source_order_id,
        o.source_customer_id,
        
        -- Customer attributes (denormalized for easier querying)
        c.full_name AS customer_name,
        c.email AS customer_email,
        c.city AS customer_city,
        c.state AS customer_state,
        c.customer_segment,
        c.customer_value_segment,
        c.recency_segment,
        c.total_orders AS customer_total_orders,
        c.lifetime_value AS customer_lifetime_value,
        
        -- Order details
        o.order_date,
        o.order_status,
        o.order_status_category,
        o.total_amount,
        o.payment_method,
        o.shipping_address,
        
        -- Order flags
        o.is_fulfilled,
        o.is_terminated,
        o.has_amount_discrepancy,
        
        -- Order metrics
        o.item_count,
        o.total_items_quantity,
        o.order_size,
        o.order_complexity,
        
        -- Product diversity
        COALESCE(oi.distinct_products, 0) AS distinct_products,
        COALESCE(oi.distinct_categories, 0) AS distinct_categories,
        COALESCE(oi.distinct_brands, 0) AS distinct_brands,
        oi.categories_list,
        oi.brands_list,
        
        -- Transaction metrics
        COALESCE(t.transaction_count, 0) AS transaction_count,
        COALESCE(t.payment_count, 0) AS payment_count,
        COALESCE(t.refund_count, 0) AS refund_count,
        COALESCE(t.successful_transaction_count, 0) AS successful_transaction_count,
        COALESCE(t.failed_transaction_count, 0) AS failed_transaction_count,
        t.first_transaction_date,
        t.last_transaction_date,
        
        -- Transaction aggregates from transform_orders
        o.transaction_count,
        o.total_payments,
        o.total_refunds,
        o.net_transaction_amount,
        o.has_refunds,
        o.has_failed_transactions,
        
        -- Time dimensions
        o.days_since_order,
        o.hours_since_order,
        o.order_recency,
        o.order_month,
        o.order_week,
        o.day_of_week,
        o.hour_of_day,
        o.weekday_weekend,
        
        -- Business flags
        CASE WHEN t.refund_count > 0 THEN TRUE ELSE FALSE END AS has_any_refunds,
        CASE WHEN t.failed_transaction_count > 0 THEN TRUE ELSE FALSE END AS has_any_failures,
        CASE WHEN o.total_amount > c.avg_order_value * 2 THEN TRUE ELSE FALSE END AS is_high_value_for_customer,
        CASE WHEN o.order_date = c.first_order_date THEN TRUE ELSE FALSE END AS is_first_order,
        
        -- Metadata
        o.generated_at,
        o.loaded_at,
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM orders o
    LEFT JOIN customers c ON o.source_customer_id = c.source_customer_id
    LEFT JOIN order_items oi ON o.source_order_id = oi.source_order_id
    LEFT JOIN transactions t ON o.source_order_id = t.source_order_id
)

SELECT * FROM final

