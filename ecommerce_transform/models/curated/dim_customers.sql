{{
    config(
        materialized='table',
        tags=['curated', 'dimension', 'customers']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('transform_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

transactions AS (
    SELECT * FROM {{ ref('transform_transactions') }}
),

customer_order_metrics AS (
    SELECT
        source_customer_id,
        COUNT(DISTINCT source_order_id) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        DATEDIFF(day, MIN(order_date), MAX(order_date)) AS customer_lifespan_days,
        SUM(CASE WHEN is_fulfilled THEN 1 ELSE 0 END) AS completed_orders,
        SUM(CASE WHEN is_terminated THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(item_count) AS total_items_purchased,
        COUNT(DISTINCT order_month) AS active_months
    FROM orders
    GROUP BY source_customer_id
),

customer_transaction_metrics AS (
    SELECT
        o.source_customer_id,
        SUM(CASE WHEN t.is_refund THEN t.transaction_amount ELSE 0 END) AS total_refunds,
        COUNT(CASE WHEN t.is_refund THEN 1 END) AS refund_count,
        SUM(t.signed_amount) AS net_revenue
    FROM transactions t
    JOIN orders o ON t.source_order_id = o.source_order_id
    WHERE t.is_successful
    GROUP BY o.source_customer_id
),

final AS (
    SELECT
        -- Customer Identifiers
        c.source_customer_id,
        
        -- Personal Information
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.gender,
        c.date_of_birth,
        c.age,
        c.age_group,
        
        -- Location
        c.street_address,
        c.city,
        c.state,
        c.zip_code,
        c.country,
        
        -- Account Status
        c.registration_date,
        c.last_login,
        c.is_active,
        c.email_verified,
        c.phone_verified,
        c.verification_status,
        c.is_fully_verified,
        c.days_since_registration,
        c.months_since_registration,
        c.customer_tenure,
        c.days_since_last_login,
        c.activity_status,
        
        -- Preferences
        c.marketing_opt_in,
        c.preferred_contact_method,
        c.customer_segment,
        
        -- Order Metrics
        COALESCE(om.total_orders, 0) AS total_orders,
        COALESCE(om.lifetime_value, 0) AS lifetime_value,
        COALESCE(om.avg_order_value, 0) AS avg_order_value,
        om.first_order_date,
        om.last_order_date,
        COALESCE(om.customer_lifespan_days, 0) AS customer_lifespan_days,
        COALESCE(om.completed_orders, 0) AS completed_orders,
        COALESCE(om.cancelled_orders, 0) AS cancelled_orders,
        COALESCE(om.total_items_purchased, 0) AS total_items_purchased,
        COALESCE(om.active_months, 0) AS active_months,
        
        -- Transaction Metrics
        COALESCE(tm.total_refunds, 0) AS total_refunds,
        COALESCE(tm.refund_count, 0) AS refund_count,
        COALESCE(tm.net_revenue, 0) AS net_revenue,
        
        -- Customer Value Segments
        CASE
            WHEN om.total_orders IS NULL THEN 'No Orders'
            WHEN om.total_orders = 1 THEN 'One-Time'
            WHEN om.total_orders BETWEEN 2 AND 5 THEN 'Occasional'
            WHEN om.total_orders BETWEEN 6 AND 10 THEN 'Regular'
            ELSE 'VIP'
        END AS customer_order_segment,
        
        CASE
            WHEN om.lifetime_value IS NULL THEN 'No Value'
            WHEN om.lifetime_value < 100 THEN 'Low Value'
            WHEN om.lifetime_value < 500 THEN 'Medium Value'
            WHEN om.lifetime_value < 1000 THEN 'High Value'
            ELSE 'Premium Value'
        END AS customer_value_segment,
        
        -- Recency
        DATEDIFF(day, om.last_order_date, CURRENT_DATE()) AS days_since_last_order,
        
        CASE
            WHEN om.last_order_date IS NULL THEN 'Never Ordered'
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 30 THEN 'Active'
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 90 THEN 'At Risk'
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 180 THEN 'Churning'
            ELSE 'Churned'
        END AS recency_segment,
        
        -- RFM Score Components (1-5 scale)
        CASE
            WHEN om.last_order_date IS NULL THEN 1
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 30 THEN 5
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 90 THEN 4
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 180 THEN 3
            WHEN DATEDIFF(day, om.last_order_date, CURRENT_DATE()) <= 365 THEN 2
            ELSE 1
        END AS recency_score,
        
        CASE
            WHEN om.total_orders IS NULL OR om.total_orders = 0 THEN 1
            WHEN om.total_orders = 1 THEN 2
            WHEN om.total_orders BETWEEN 2 AND 5 THEN 3
            WHEN om.total_orders BETWEEN 6 AND 10 THEN 4
            ELSE 5
        END AS frequency_score,
        
        CASE
            WHEN om.lifetime_value IS NULL OR om.lifetime_value = 0 THEN 1
            WHEN om.lifetime_value < 100 THEN 2
            WHEN om.lifetime_value < 500 THEN 3
            WHEN om.lifetime_value < 1000 THEN 4
            ELSE 5
        END AS monetary_score,
        
        -- Flags
        CASE WHEN om.first_order_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_purchased,
        CASE WHEN tm.refund_count > 0 THEN TRUE ELSE FALSE END AS has_refunds,
        CASE WHEN om.cancelled_orders > 0 THEN TRUE ELSE FALSE END AS has_cancellations,
        
        -- Metadata
        c.generated_at,
        c.loaded_at,
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM customers c
    LEFT JOIN customer_order_metrics om ON c.source_customer_id = om.source_customer_id
    LEFT JOIN customer_transaction_metrics tm ON c.source_customer_id = tm.source_customer_id
)

SELECT * FROM final



