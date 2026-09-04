{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'cohorts']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

customers AS (
    SELECT
        source_customer_id,
        registration_date,
        DATE_TRUNC('month', registration_date) AS cohort_month
    FROM {{ ref('transform_customers') }}
),

customer_first_order AS (
    SELECT
        source_customer_id,
        MIN(order_date) AS first_order_date,
        DATE_TRUNC('month', MIN(order_date)) AS first_order_month
    FROM orders
    GROUP BY source_customer_id
),

cohort_orders AS (
    SELECT
        c.cohort_month,
        cfo.first_order_month,
        DATE_TRUNC('month', o.order_date) AS order_month,
        DATEDIFF(month, cfo.first_order_month, o.order_date) AS months_since_first_order,
        COUNT(DISTINCT o.source_customer_id) AS customers_in_period,
        COUNT(DISTINCT o.source_order_id) AS orders_in_period,
        SUM(o.total_amount) AS revenue_in_period,
        AVG(o.total_amount) AS avg_order_value_in_period
    FROM orders o
    JOIN customers c ON o.source_customer_id = c.source_customer_id
    JOIN customer_first_order cfo ON o.source_customer_id = cfo.source_customer_id
    WHERE o.is_fulfilled
    GROUP BY c.cohort_month, cfo.first_order_month, DATE_TRUNC('month', o.order_date), 
             DATEDIFF(month, cfo.first_order_month, o.order_date)
),

cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT source_customer_id) AS cohort_customers
    FROM customers
    WHERE source_customer_id IN (SELECT source_customer_id FROM customer_first_order)
    GROUP BY cohort_month
),

final AS (
    SELECT
        -- Cohort Identifiers
        co.cohort_month AS registration_cohort_month,
        co.first_order_month,
        co.order_month,
        co.months_since_first_order,
        
        -- Cohort Size
        cs.cohort_customers AS cohort_size,
        
        -- Period Metrics
        co.customers_in_period AS active_customers,
        co.orders_in_period,
        co.revenue_in_period,
        co.avg_order_value_in_period,
        
        -- Retention & Performance
        ROUND(co.customers_in_period::DECIMAL / NULLIF(cs.cohort_customers, 0) * 100, 2) AS retention_rate,
        ROUND(co.orders_in_period::DECIMAL / NULLIF(co.customers_in_period, 0), 2) AS orders_per_active_customer,
        ROUND(co.revenue_in_period / NULLIF(co.customers_in_period, 0), 2) AS revenue_per_active_customer,
        ROUND(co.revenue_in_period / NULLIF(cs.cohort_customers, 0), 2) AS revenue_per_cohort_customer,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM cohort_orders co
    JOIN cohort_size cs ON co.cohort_month = cs.cohort_month
)

SELECT * FROM final
ORDER BY registration_cohort_month, months_since_first_order



