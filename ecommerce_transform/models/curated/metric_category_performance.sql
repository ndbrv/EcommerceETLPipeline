{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'categories']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('transform_order_items') }}
),

products AS (
    SELECT
        source_product_id,
        category,
        current_price,
        current_stock_quantity,
        is_available
    FROM {{ ref('dim_products') }}
),

category_sales AS (
    SELECT
        oi.category,
        
        -- Sales volume
        COUNT(DISTINCT oi.source_order_id) AS total_orders,
        COUNT(DISTINCT oi.customer_id) AS unique_customers,
        COUNT(DISTINCT oi.source_product_id) AS products_in_category,
        SUM(oi.quantity) AS total_units_sold,
        
        -- Revenue
        SUM(oi.item_revenue) AS total_revenue,
        AVG(oi.item_revenue) AS avg_revenue_per_line,
        SUM(oi.total_price) AS gross_revenue,
        
        -- Pricing
        AVG(oi.unit_price) AS avg_selling_price,
        MIN(oi.unit_price) AS min_price,
        MAX(oi.unit_price) AS max_price,
        
        -- Order characteristics
        AVG(oi.quantity) AS avg_quantity_per_order,
        AVG(oi.percent_of_order_total) AS avg_basket_share,
        
        -- Discount analysis
        COUNT(CASE WHEN oi.was_discounted THEN 1 END) AS discounted_sales,
        AVG(CASE WHEN oi.was_discounted THEN oi.discount_percentage_vs_current ELSE 0 END) AS avg_discount_percentage,
        
        -- Recency
        MIN(oi.order_date) AS first_sale_date,
        MAX(oi.order_date) AS last_sale_date,
        DATEDIFF(day, MAX(oi.order_date), CURRENT_DATE()) AS days_since_last_sale
        
    FROM order_items oi
    GROUP BY oi.category
),

category_inventory AS (
    SELECT
        category,
        COUNT(DISTINCT source_product_id) AS total_products,
        SUM(current_stock_quantity) AS total_stock,
        AVG(current_stock_quantity) AS avg_stock_per_product,
        COUNT(CASE WHEN is_available THEN 1 END) AS available_products,
        COUNT(CASE WHEN current_stock_quantity = 0 THEN 1 END) AS out_of_stock_products
    FROM products
    GROUP BY category
),

category_totals AS (
    SELECT
        SUM(total_revenue) AS total_market_revenue,
        SUM(total_units_sold) AS total_market_units
    FROM category_sales
),

final AS (
    SELECT
        -- Category identifier
        cs.category,
        
        -- Sales volume
        cs.total_orders,
        cs.unique_customers,
        cs.products_in_category,
        cs.total_units_sold,
        
        -- Revenue metrics
        cs.total_revenue,
        cs.avg_revenue_per_line,
        cs.gross_revenue,
        
        -- Pricing metrics
        cs.avg_selling_price,
        cs.min_price,
        cs.max_price,
        cs.max_price - cs.min_price AS price_range,
        
        -- Order characteristics
        cs.avg_quantity_per_order,
        cs.avg_basket_share,
        
        -- Discount metrics
        cs.discounted_sales,
        ROUND(cs.discounted_sales::DECIMAL / NULLIF(cs.total_orders, 0) * 100, 2) AS discount_rate,
        cs.avg_discount_percentage,
        
        -- Inventory metrics
        ci.total_products,
        ci.total_stock,
        ci.avg_stock_per_product,
        ci.available_products,
        ci.out_of_stock_products,
        ROUND(ci.available_products::DECIMAL / NULLIF(ci.total_products, 0) * 100, 2) AS availability_rate,
        
        -- Market share
        ROUND(cs.total_revenue / NULLIF(ct.total_market_revenue, 0) * 100, 2) AS revenue_market_share,
        ROUND(cs.total_units_sold::DECIMAL / NULLIF(ct.total_market_units, 0) * 100, 2) AS unit_market_share,
        
        -- Per product performance
        ROUND(cs.total_revenue / NULLIF(cs.products_in_category, 0), 2) AS revenue_per_product,
        ROUND(cs.total_units_sold::DECIMAL / NULLIF(cs.products_in_category, 0), 2) AS units_per_product,
        ROUND(cs.total_orders::DECIMAL / NULLIF(cs.products_in_category, 0), 2) AS orders_per_product,
        
        -- Recency
        cs.first_sale_date,
        cs.last_sale_date,
        cs.days_since_last_sale,
        DATEDIFF(day, cs.first_sale_date, cs.last_sale_date) AS category_lifespan_days,
        
        -- Performance ratings
        CASE
            WHEN cs.total_revenue > ct.total_market_revenue * 0.15 THEN 'Top Category'
            WHEN cs.total_revenue > ct.total_market_revenue * 0.10 THEN 'Strong Category'
            WHEN cs.total_revenue > ct.total_market_revenue * 0.05 THEN 'Average Category'
            ELSE 'Small Category'
        END AS category_size,
        
        CASE
            WHEN cs.days_since_last_sale <= 7 THEN 'Active'
            WHEN cs.days_since_last_sale <= 30 THEN 'Recent'
            WHEN cs.days_since_last_sale <= 90 THEN 'Declining'
            ELSE 'Dormant'
        END AS activity_status,
        
        -- Inventory health
        CASE
            WHEN ci.out_of_stock_products::DECIMAL / NULLIF(ci.total_products, 0) > 0.3 THEN 'Critical'
            WHEN ci.out_of_stock_products::DECIMAL / NULLIF(ci.total_products, 0) > 0.1 THEN 'Needs Attention'
            ELSE 'Healthy'
        END AS inventory_health,
        
        -- Flags
        CASE WHEN cs.total_revenue > ct.total_market_revenue * 0.15 THEN TRUE ELSE FALSE END AS is_top_category,
        CASE WHEN cs.days_since_last_sale > 90 THEN TRUE ELSE FALSE END AS is_declining,
        CASE WHEN ci.out_of_stock_products > ci.total_products * 0.3 THEN TRUE ELSE FALSE END AS has_stock_issues,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM category_sales cs
    LEFT JOIN category_inventory ci ON cs.category = ci.category
    CROSS JOIN category_totals ct
)

SELECT * FROM final
ORDER BY total_revenue DESC

