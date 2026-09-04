{{
    config(
        materialized='table',
        tags=['curated', 'fact', 'order_items']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('transform_order_items') }}
),

orders AS (
    SELECT 
        source_order_id,
        source_customer_id,
        order_date,
        order_status,
        total_amount,
        payment_method,
        is_fulfilled,
        is_terminated
    FROM {{ ref('transform_orders') }}
),

customers AS (
    SELECT
        source_customer_id,
        full_name,
        email,
        customer_segment,
        customer_value_segment,
        city,
        state
    FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT
        source_product_id,
        product_name,
        brand,
        category,
        current_price,
        current_stock_quantity,
        is_available,
        revenue_performance,
        popularity_segment
    FROM {{ ref('dim_products') }}
),

final AS (
    SELECT
        -- Line item identifiers
        oi.source_order_item_id,
        oi.source_order_id,
        oi.source_product_id,
        o.source_customer_id,
        
        -- Customer attributes (denormalized)
        c.full_name AS customer_name,
        c.email AS customer_email,
        c.customer_segment,
        c.customer_value_segment,
        c.city AS customer_city,
        c.state AS customer_state,
        
        -- Order attributes (denormalized)
        o.order_date,
        o.order_status,
        o.total_amount AS order_total_amount,
        o.payment_method,
        o.is_fulfilled AS order_is_fulfilled,
        o.is_terminated AS order_is_terminated,
        
        -- Product attributes at time of order
        oi.product_name,
        oi.category,
        oi.brand,
        
        -- Current product attributes
        p.current_price,
        p.current_stock_quantity,
        p.is_available AS product_is_available,
        p.revenue_performance,
        p.popularity_segment,
        
        -- Line item details
        oi.quantity,
        oi.unit_price,
        oi.total_price,
        
        -- Pricing analysis
        oi.calculated_unit_price,
        oi.discount_vs_current_price,
        oi.discount_percentage_vs_current,
        oi.was_discounted,
        
        -- Revenue
        oi.item_revenue,
        oi.percent_of_order_total,
        
        -- Classifications
        oi.item_importance,
        oi.quantity_category,
        oi.price_tier,
        
        -- Flags
        oi.exceeds_minimum_order,
        CASE WHEN oi.unit_price < p.current_price THEN TRUE ELSE FALSE END AS purchased_below_current_price,
        CASE WHEN oi.percent_of_order_total > 50 THEN TRUE ELSE FALSE END AS is_primary_item,
        
        -- Metadata
        oi.generated_at,
        oi.loaded_at,
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM order_items oi
    LEFT JOIN orders o ON oi.source_order_id = o.source_order_id
    LEFT JOIN customers c ON o.source_customer_id = c.source_customer_id
    LEFT JOIN products p ON oi.source_product_id = p.source_product_id
)

SELECT * FROM final

