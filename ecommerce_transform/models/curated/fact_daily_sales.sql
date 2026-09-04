{{
    config(
        materialized='table',
        tags=['curated', 'fact', 'sales', 'daily']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

transactions AS (
    SELECT * FROM {{ ref('transform_transactions') }}
),

order_items AS (
    SELECT * FROM {{ ref('transform_order_items') }}
),

daily_order_metrics AS (
    SELECT
        DATE(order_date) AS order_date,
        
        -- Order Counts
        COUNT(DISTINCT source_order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_fulfilled THEN source_order_id END) AS fulfilled_orders,
        COUNT(DISTINCT CASE WHEN is_terminated THEN source_order_id END) AS cancelled_orders,
        COUNT(DISTINCT CASE WHEN order_status IN ('pending', 'payment_pending') THEN source_order_id END) AS pending_orders,
        
        -- Customer Counts
        COUNT(DISTINCT source_customer_id) AS unique_customers,
        
        -- Revenue Metrics
        SUM(total_amount) AS gross_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(total_amount) AS min_order_value,
        MAX(total_amount) AS max_order_value,
        
        -- Item Metrics
        SUM(item_count) AS total_items,
        SUM(total_items_quantity) AS total_units,
        AVG(item_count) AS avg_items_per_order,
        
        -- Order Size Distribution
        COUNT(DISTINCT CASE WHEN order_size = 'Small' THEN source_order_id END) AS small_orders,
        COUNT(DISTINCT CASE WHEN order_size = 'Medium' THEN source_order_id END) AS medium_orders,
        COUNT(DISTINCT CASE WHEN order_size = 'Large' THEN source_order_id END) AS large_orders,
        COUNT(DISTINCT CASE WHEN order_size = 'Extra Large' THEN source_order_id END) AS extra_large_orders,
        
        -- Payment Method Distribution
        COUNT(DISTINCT CASE WHEN payment_method = 'credit_card' THEN source_order_id END) AS credit_card_orders,
        COUNT(DISTINCT CASE WHEN payment_method = 'paypal' THEN source_order_id END) AS paypal_orders,
        COUNT(DISTINCT CASE WHEN payment_method = 'debit_card' THEN source_order_id END) AS debit_card_orders,
        
        -- Time Segments
        COUNT(DISTINCT CASE WHEN weekday_weekend = 'Weekend' THEN source_order_id END) AS weekend_orders,
        COUNT(DISTINCT CASE WHEN weekday_weekend = 'Weekday' THEN source_order_id END) AS weekday_orders
        
    FROM orders
    GROUP BY DATE(order_date)
),

daily_transaction_metrics AS (
    SELECT
        DATE(transaction_date) AS transaction_date,
        
        -- Transaction Counts
        COUNT(DISTINCT source_transaction_id) AS total_transactions,
        COUNT(DISTINCT CASE WHEN is_successful THEN source_transaction_id END) AS successful_transactions,
        COUNT(DISTINCT CASE WHEN is_failed THEN source_transaction_id END) AS failed_transactions,
        COUNT(DISTINCT CASE WHEN is_payment THEN source_transaction_id END) AS payment_transactions,
        COUNT(DISTINCT CASE WHEN is_refund THEN source_transaction_id END) AS refund_transactions,
        
        -- Transaction Amounts
        SUM(CASE WHEN is_payment AND is_successful THEN transaction_amount ELSE 0 END) AS total_payments,
        SUM(CASE WHEN is_refund AND is_successful THEN transaction_amount ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN is_successful THEN signed_amount ELSE 0 END) AS net_transaction_amount,
        
        -- Success Rates
        ROUND(COUNT(CASE WHEN is_successful THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS transaction_success_rate
        
    FROM transactions
    GROUP BY DATE(transaction_date)
),

daily_product_metrics AS (
    SELECT
        DATE(order_date) AS order_date,
        
        -- Product Metrics
        COUNT(DISTINCT source_product_id) AS unique_products_sold,
        COUNT(DISTINCT category) AS unique_categories,
        COUNT(DISTINCT brand) AS unique_brands,
        
        -- Revenue by Product
        SUM(item_revenue) AS product_revenue,
        AVG(unit_price) AS avg_product_price
        
    FROM order_items
    GROUP BY DATE(order_date)
),

final AS (
    SELECT
        -- Date Dimension
        om.order_date,
        DAYOFWEEK(om.order_date) AS day_of_week,
        DAYNAME(om.order_date) AS day_name,
        WEEK(om.order_date) AS week_of_year,
        MONTH(om.order_date) AS month,
        QUARTER(om.order_date) AS quarter,
        YEAR(om.order_date) AS year,
        CASE WHEN DAYOFWEEK(om.order_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        
        -- Order Metrics
        om.total_orders,
        om.fulfilled_orders,
        om.cancelled_orders,
        om.pending_orders,
        om.unique_customers,
        
        -- Revenue Metrics
        om.gross_revenue,
        COALESCE(tm.net_transaction_amount, om.gross_revenue) AS net_revenue,
        om.avg_order_value,
        om.min_order_value,
        om.max_order_value,
        
        -- Item Metrics
        om.total_items,
        om.total_units,
        om.avg_items_per_order,
        
        -- Order Size Distribution
        om.small_orders,
        om.medium_orders,
        om.large_orders,
        om.extra_large_orders,
        
        -- Payment Methods
        om.credit_card_orders,
        om.paypal_orders,
        om.debit_card_orders,
        
        -- Time Distribution
        om.weekend_orders,
        om.weekday_orders,
        
        -- Transaction Metrics
        COALESCE(tm.total_transactions, 0) AS total_transactions,
        COALESCE(tm.successful_transactions, 0) AS successful_transactions,
        COALESCE(tm.failed_transactions, 0) AS failed_transactions,
        COALESCE(tm.payment_transactions, 0) AS payment_transactions,
        COALESCE(tm.refund_transactions, 0) AS refund_transactions,
        COALESCE(tm.total_payments, 0) AS total_payments,
        COALESCE(tm.total_refunds, 0) AS total_refunds,
        COALESCE(tm.transaction_success_rate, 0) AS transaction_success_rate,
        
        -- Product Metrics
        COALESCE(pm.unique_products_sold, 0) AS unique_products_sold,
        COALESCE(pm.unique_categories, 0) AS unique_categories,
        COALESCE(pm.unique_brands, 0) AS unique_brands,
        COALESCE(pm.avg_product_price, 0) AS avg_product_price,
        
        -- KPIs
        ROUND(om.gross_revenue / NULLIF(om.unique_customers, 0), 2) AS revenue_per_customer,
        ROUND(om.fulfilled_orders::DECIMAL / NULLIF(om.total_orders, 0) * 100, 2) AS order_fulfillment_rate,
        ROUND(om.cancelled_orders::DECIMAL / NULLIF(om.total_orders, 0) * 100, 2) AS order_cancellation_rate,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM daily_order_metrics om
    LEFT JOIN daily_transaction_metrics tm ON om.order_date = tm.transaction_date
    LEFT JOIN daily_product_metrics pm ON om.order_date = pm.order_date
)

SELECT * FROM final
ORDER BY order_date DESC



