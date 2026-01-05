{{
    config(
        materialized='table',
        tags=['transform', 'order_items']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('transform_products') }}
),

orders AS (
    SELECT * FROM {{ ref('transform_orders') }}
),

transformed AS (
    SELECT
        -- IDs
        oi.source_order_item_id,
        oi.source_order_id,
        oi.source_product_id,
        
        -- Enrich with order info
        o.source_customer_id,
        o.order_date,
        o.order_status,
        o.order_status_category,
        o.total_amount AS order_total_amount,
        
        -- Enrich with product info
        p.product_name,
        p.brand,
        p.category,
        p.price AS current_product_price,
        
        -- Item Details
        oi.quantity,
        oi.unit_price,
        oi.total_price,
        
        -- Price Analysis
        oi.total_price / NULLIF(oi.quantity, 0) AS calculated_unit_price,
        
        -- Compare to current price (identify historical discounts)
        p.price - oi.unit_price AS discount_vs_current_price,
        
        CASE 
            WHEN p.price > oi.unit_price THEN 
                ROUND(((p.price - oi.unit_price) / NULLIF(p.price, 0)) * 100, 2)
            ELSE 0
        END AS discount_percentage_vs_current,
        
        CASE 
            WHEN oi.unit_price < p.price THEN TRUE
            ELSE FALSE
        END AS was_discounted,
        
        -- Revenue metrics
        oi.total_price AS item_revenue,
        
        -- Order contribution
        ROUND((oi.total_price / NULLIF(o.total_amount, 0)) * 100, 2) AS percent_of_order_total,
         
        -- Quantity categories
        CASE 
            WHEN oi.quantity = 1 THEN 'Single'
            WHEN oi.quantity <= 3 THEN 'Few'
            WHEN oi.quantity <= 10 THEN 'Multiple'
            ELSE 'Bulk'
        END AS quantity_category,
        
        -- Price range
        CASE 
            WHEN oi.unit_price < 20 THEN 'Budget'
            WHEN oi.unit_price < 50 THEN 'Economy'
            WHEN oi.unit_price < 100 THEN 'Mid-Range'
            WHEN oi.unit_price < 500 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier,
        
        -- Flags
        CASE 
            WHEN oi.quantity > p.minimum_order_quantity THEN TRUE
            ELSE FALSE
        END AS exceeds_minimum_order,
        
        -- Time-based attributes
        DATE_TRUNC('month', o.order_date) AS order_month,
        DATE_TRUNC('week', o.order_date) AS order_week,
        
        -- Metadata
        oi.generated_at,
        oi.loaded_at,
        oi.batch_id,
        oi.source_system
        
    FROM order_items oi
    LEFT JOIN products p ON oi.source_product_id = p.source_product_id
    LEFT JOIN orders o ON oi.source_order_id = o.source_order_id
)

SELECT * FROM transformed

