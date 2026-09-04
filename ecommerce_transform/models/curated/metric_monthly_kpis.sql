{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'kpis', 'monthly']
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

monthly_order_metrics AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        
        -- Order counts
        COUNT(DISTINCT source_order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_fulfilled THEN source_order_id END) AS fulfilled_orders,
        COUNT(DISTINCT CASE WHEN is_terminated THEN source_order_id END) AS cancelled_orders,
        
        -- Customer metrics
        COUNT(DISTINCT source_customer_id) AS unique_customers,
        COUNT(DISTINCT source_order_id)::DECIMAL / NULLIF(COUNT(DISTINCT source_customer_id), 0) AS orders_per_customer,
        
        -- Revenue metrics
        SUM(total_amount) AS gross_revenue,
        AVG(total_amount) AS avg_order_value,
        MEDIAN(total_amount) AS median_order_value,
        
        -- Item metrics
        SUM(item_count) AS total_items,
        SUM(total_items_quantity) AS total_units,
        AVG(item_count) AS avg_items_per_order,
        
        -- Performance rates
        ROUND(COUNT(DISTINCT CASE WHEN is_fulfilled THEN source_order_id END)::DECIMAL / 
              NULLIF(COUNT(DISTINCT source_order_id), 0) * 100, 2) AS fulfillment_rate,
        ROUND(COUNT(DISTINCT CASE WHEN is_terminated THEN source_order_id END)::DECIMAL / 
              NULLIF(COUNT(DISTINCT source_order_id), 0) * 100, 2) AS cancellation_rate
        
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
),

monthly_transaction_metrics AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        
        -- Transaction counts
        COUNT(DISTINCT source_transaction_id) AS total_transactions,
        COUNT(DISTINCT CASE WHEN is_successful THEN source_transaction_id END) AS successful_transactions,
        COUNT(DISTINCT CASE WHEN is_payment THEN source_transaction_id END) AS payment_transactions,
        COUNT(DISTINCT CASE WHEN is_refund THEN source_transaction_id END) AS refund_transactions,
        
        -- Transaction amounts
        SUM(CASE WHEN is_payment AND is_successful THEN transaction_amount ELSE 0 END) AS total_payments,
        SUM(CASE WHEN is_refund AND is_successful THEN transaction_amount ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN is_successful THEN signed_amount ELSE 0 END) AS net_transaction_amount,
        
        -- Success rate
        ROUND(COUNT(CASE WHEN is_successful THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS transaction_success_rate
        
    FROM transactions
    GROUP BY DATE_TRUNC('month', transaction_date)
),

monthly_product_metrics AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        
        -- Product diversity
        COUNT(DISTINCT source_product_id) AS unique_products_sold,
        COUNT(DISTINCT category) AS unique_categories,
        COUNT(DISTINCT brand) AS unique_brands,
        
        -- Product revenue
        SUM(item_revenue) AS product_revenue,
        AVG(unit_price) AS avg_product_price
        
    FROM order_items
    GROUP BY DATE_TRUNC('month', order_date)
),

monthly_with_prior AS (
    SELECT
        om.month,
        
        -- Order metrics
        om.total_orders,
        om.fulfilled_orders,
        om.cancelled_orders,
        om.unique_customers,
        om.orders_per_customer,
        
        -- Revenue metrics
        om.gross_revenue,
        COALESCE(tm.net_transaction_amount, om.gross_revenue) AS net_revenue,
        om.avg_order_value,
        om.median_order_value,
        
        -- Item metrics
        om.total_items,
        om.total_units,
        om.avg_items_per_order,
        
        -- Performance rates
        om.fulfillment_rate,
        om.cancellation_rate,
        COALESCE(tm.transaction_success_rate, 0) AS transaction_success_rate,
        
        -- Transaction metrics
        COALESCE(tm.total_transactions, 0) AS total_transactions,
        COALESCE(tm.successful_transactions, 0) AS successful_transactions,
        COALESCE(tm.payment_transactions, 0) AS payment_transactions,
        COALESCE(tm.refund_transactions, 0) AS refund_transactions,
        COALESCE(tm.total_payments, 0) AS total_payments,
        COALESCE(tm.total_refunds, 0) AS total_refunds,
        
        -- Product metrics
        COALESCE(pm.unique_products_sold, 0) AS unique_products_sold,
        COALESCE(pm.unique_categories, 0) AS unique_categories,
        COALESCE(pm.unique_brands, 0) AS unique_brands,
        COALESCE(pm.avg_product_price, 0) AS avg_product_price,
        
        -- Derived KPIs
        ROUND(om.gross_revenue / NULLIF(om.unique_customers, 0), 2) AS revenue_per_customer,
        ROUND(om.gross_revenue / NULLIF(om.total_orders, 0), 2) AS revenue_per_order,
        
        -- Prior month values for MoM calculations
        LAG(om.total_orders) OVER (ORDER BY om.month) AS prior_month_orders,
        LAG(om.unique_customers) OVER (ORDER BY om.month) AS prior_month_customers,
        LAG(om.gross_revenue) OVER (ORDER BY om.month) AS prior_month_revenue,
        LAG(om.avg_order_value) OVER (ORDER BY om.month) AS prior_month_aov,
        
        -- Prior year values for YoY calculations
        LAG(om.total_orders, 12) OVER (ORDER BY om.month) AS prior_year_orders,
        LAG(om.unique_customers, 12) OVER (ORDER BY om.month) AS prior_year_customers,
        LAG(om.gross_revenue, 12) OVER (ORDER BY om.month) AS prior_year_revenue
        
    FROM monthly_order_metrics om
    LEFT JOIN monthly_transaction_metrics tm ON om.month = tm.month
    LEFT JOIN monthly_product_metrics pm ON om.month = pm.month
),

final AS (
    SELECT
        -- Date attributes
        month,
        YEAR(month) AS year,
        MONTH(month) AS month_number,
        MONTHNAME(month) AS month_name,
        QUARTER(month) AS quarter,
        
        -- Order metrics
        total_orders,
        fulfilled_orders,
        cancelled_orders,
        unique_customers,
        orders_per_customer,
        
        -- Revenue metrics
        gross_revenue,
        net_revenue,
        avg_order_value,
        median_order_value,
        
        -- Item metrics
        total_items,
        total_units,
        avg_items_per_order,
        
        -- Performance rates
        fulfillment_rate,
        cancellation_rate,
        transaction_success_rate,
        
        -- Transaction metrics
        total_transactions,
        successful_transactions,
        payment_transactions,
        refund_transactions,
        total_payments,
        total_refunds,
        
        -- Product metrics
        unique_products_sold,
        unique_categories,
        unique_brands,
        avg_product_price,
        
        -- KPIs
        revenue_per_customer,
        revenue_per_order,
        
        -- Month over Month (MoM) Growth
        total_orders - prior_month_orders AS mom_orders_change,
        ROUND((total_orders::DECIMAL / NULLIF(prior_month_orders, 0) - 1) * 100, 2) AS mom_orders_growth_pct,
        
        unique_customers - prior_month_customers AS mom_customers_change,
        ROUND((unique_customers::DECIMAL / NULLIF(prior_month_customers, 0) - 1) * 100, 2) AS mom_customers_growth_pct,
        
        gross_revenue - prior_month_revenue AS mom_revenue_change,
        ROUND((gross_revenue / NULLIF(prior_month_revenue, 0) - 1) * 100, 2) AS mom_revenue_growth_pct,
        
        avg_order_value - prior_month_aov AS mom_aov_change,
        ROUND((avg_order_value / NULLIF(prior_month_aov, 0) - 1) * 100, 2) AS mom_aov_growth_pct,
        
        -- Year over Year (YoY) Growth
        total_orders - prior_year_orders AS yoy_orders_change,
        ROUND((total_orders::DECIMAL / NULLIF(prior_year_orders, 0) - 1) * 100, 2) AS yoy_orders_growth_pct,
        
        unique_customers - prior_year_customers AS yoy_customers_change,
        ROUND((unique_customers::DECIMAL / NULLIF(prior_year_customers, 0) - 1) * 100, 2) AS yoy_customers_growth_pct,
        
        gross_revenue - prior_year_revenue AS yoy_revenue_change,
        ROUND((gross_revenue / NULLIF(prior_year_revenue, 0) - 1) * 100, 2) AS yoy_revenue_growth_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM monthly_with_prior
)

SELECT * FROM final
ORDER BY month DESC

