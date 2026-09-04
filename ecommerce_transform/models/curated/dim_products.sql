{{
    config(
        materialized='table',
        tags=['curated', 'dimension', 'products']
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('transform_products') }}
),

order_items AS (
    SELECT * FROM {{ ref('transform_order_items') }}
),

product_sales_metrics AS (
    SELECT
        source_product_id,
        COUNT(DISTINCT source_order_id) AS times_ordered,
        COUNT(DISTINCT source_customer_id) AS unique_customers,
        SUM(quantity) AS total_quantity_sold,
        SUM(item_revenue) AS total_revenue,
        AVG(unit_price) AS avg_selling_price,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(CASE WHEN was_discounted THEN 1 END) AS discounted_sales_count
    FROM order_items
    GROUP BY source_product_id
),

final AS (
    SELECT
        -- Product Identifiers
        p.source_product_id,
        
        -- Product Information
        p.product_name,
        p.brand,
        p.category,
        p.description,
        
        -- Pricing
        p.price AS current_price,
        p.discount_percentage AS current_discount_percentage,
        p.discounted_price AS current_discounted_price,
        p.discount_amount AS current_discount_amount,
        p.is_on_sale,
        p.discount_category,
        p.price_category,
        
        -- Inventory
        p.stock_quantity AS current_stock_quantity,
        p.availability_status AS current_availability_status,
        p.minimum_order_quantity,
        p.stock_level,
        p.is_available,
        
        -- Product Quality
        p.rating AS current_rating,
        p.rating_category,
        
        -- Physical Attributes
        p.weight,
        p.width,
        p.height,
        p.depth,
        
        -- Policies
        p.warranty_info,
        p.shipping_info,
        p.return_policy,
        
        -- Identifiers
        p.barcode,
        p.qr_code,
        
        -- Media
        p.thumbnail_url,
        
        -- Sales Performance Metrics
        COALESCE(sm.times_ordered, 0) AS times_ordered,
        COALESCE(sm.unique_customers, 0) AS unique_customers,
        COALESCE(sm.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(sm.total_revenue, 0) AS total_revenue,
        COALESCE(sm.avg_selling_price, 0) AS avg_selling_price,
        sm.first_order_date,
        sm.last_order_date,
        COALESCE(sm.discounted_sales_count, 0) AS discounted_sales_count,
        
        -- Performance Indicators
        CASE
            WHEN sm.total_revenue IS NULL THEN 'Never Sold'
            WHEN sm.total_revenue < 100 THEN 'Low Performer'
            WHEN sm.total_revenue < 1000 THEN 'Average Performer'
            WHEN sm.total_revenue < 5000 THEN 'Strong Performer'
            ELSE 'Top Performer'
        END AS revenue_performance,
        
        CASE
            WHEN sm.times_ordered IS NULL THEN 'Never Ordered'
            WHEN sm.times_ordered < 5 THEN 'Rarely Ordered'
            WHEN sm.times_ordered < 20 THEN 'Occasionally Ordered'
            WHEN sm.times_ordered < 50 THEN 'Frequently Ordered'
            ELSE 'Best Seller'
        END AS popularity_segment,
        
        -- Recency
        CASE
            WHEN sm.last_order_date IS NULL THEN NULL
            ELSE DATEDIFF(day, sm.last_order_date, CURRENT_DATE())
        END AS days_since_last_sale,
        
        CASE
            WHEN sm.last_order_date IS NULL THEN 'Never Sold'
            WHEN DATEDIFF(day, sm.last_order_date, CURRENT_DATE()) <= 7 THEN 'Selling This Week'
            WHEN DATEDIFF(day, sm.last_order_date, CURRENT_DATE()) <= 30 THEN 'Sold Recently'
            WHEN DATEDIFF(day, sm.last_order_date, CURRENT_DATE()) <= 90 THEN 'Stale'
            ELSE 'Dead Stock'
        END AS sales_recency_segment,
        
        -- Stock Health
        CASE
            WHEN p.stock_quantity = 0 THEN 'Out of Stock - Urgent'
            WHEN p.stock_quantity > 0 AND sm.total_quantity_sold > 0 
                AND p.stock_quantity < (sm.total_quantity_sold * 0.1) THEN 'Low Stock - Reorder Soon'
            WHEN p.stock_quantity > 100 AND sm.times_ordered < 5 THEN 'Overstocked - Slow Moving'
            WHEN p.stock_quantity > 0 THEN 'Healthy Stock'
            ELSE 'Unknown'
        END AS stock_health,
        
        -- Flags
        CASE WHEN sm.times_ordered > 0 THEN TRUE ELSE FALSE END AS has_sales,
        CASE WHEN p.stock_quantity = 0 THEN TRUE ELSE FALSE END AS is_out_of_stock,
        CASE WHEN p.stock_quantity > 0 AND p.availability_status = 'In Stock' THEN TRUE ELSE FALSE END AS is_buyable,
        
        -- Metadata
        p.generated_at,
        p.loaded_at,
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM products p
    LEFT JOIN product_sales_metrics sm ON p.source_product_id = sm.source_product_id
)

SELECT * FROM final



