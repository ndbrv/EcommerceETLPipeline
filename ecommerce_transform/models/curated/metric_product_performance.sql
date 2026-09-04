{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'products']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('transform_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('transform_products') }}
),

product_metrics AS (
    SELECT
        oi.source_product_id,
        oi.product_name,
        oi.category,
        oi.brand,
        
        -- Sales Volume
        COUNT(DISTINCT oi.source_order_id) AS total_orders,
        COUNT(DISTINCT oi.customer_id) AS unique_customers,
        SUM(oi.quantity) AS total_units_sold,
        COUNT(DISTINCT oi.source_order_item_id) AS total_order_lines,
        
        -- Revenue Metrics
        SUM(oi.item_revenue) AS total_revenue,
        AVG(oi.item_revenue) AS avg_revenue_per_line,
        SUM(oi.total_price) AS gross_revenue,
        
        -- Pricing Metrics
        AVG(oi.unit_price) AS avg_selling_price,
        MIN(oi.unit_price) AS min_selling_price,
        MAX(oi.unit_price) AS max_selling_price,
        
        -- Discount Analysis
        COUNT(CASE WHEN oi.was_discounted THEN 1 END) AS discounted_sales,
        AVG(CASE WHEN oi.was_discounted THEN oi.discount_percentage_vs_current ELSE 0 END) AS avg_discount_percentage,
        
        -- Basket Analysis
        AVG(oi.percent_of_order_total) AS avg_basket_share,
        COUNT(DISTINCT CASE WHEN oi.item_importance = 'Primary' THEN oi.source_order_id END) AS primary_item_orders,
        
        -- Recency
        MIN(oi.order_date) AS first_sale_date,
        MAX(oi.order_date) AS last_sale_date,
        DATEDIFF(day, MAX(oi.order_date), CURRENT_DATE()) AS days_since_last_sale,
        
        -- Quantity Distribution
        AVG(oi.quantity) AS avg_quantity_per_order,
        MAX(oi.quantity) AS max_quantity_ordered
        
    FROM order_items oi
    GROUP BY oi.source_product_id, oi.product_name, oi.category, oi.brand
),

category_benchmarks AS (
    SELECT
        category,
        AVG(total_revenue) AS category_avg_revenue,
        AVG(total_units_sold) AS category_avg_units,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) AS category_median_revenue
    FROM product_metrics
    GROUP BY category
),

final AS (
    SELECT
        -- Product Identifiers
        pm.source_product_id,
        pm.product_name,
        pm.category,
        pm.brand,
        
        -- Current Product Status
        p.current_price,
        p.current_stock_quantity,
        p.current_rating,
        p.is_available,
        p.is_on_sale,
        
        -- Sales Volume
        pm.total_orders,
        pm.unique_customers,
        pm.total_units_sold,
        pm.total_order_lines,
        
        -- Revenue Metrics
        pm.total_revenue,
        pm.avg_revenue_per_line,
        pm.gross_revenue,
        
        -- Pricing Metrics
        pm.avg_selling_price,
        pm.min_selling_price,
        pm.max_selling_price,
        p.current_price - pm.avg_selling_price AS price_variance_from_avg,
        
        -- Discount Analysis
        pm.discounted_sales,
        ROUND(pm.discounted_sales::DECIMAL / NULLIF(pm.total_order_lines, 0) * 100, 2) AS discount_rate,
        pm.avg_discount_percentage,
        
        -- Basket Analysis
        pm.avg_basket_share,
        pm.primary_item_orders,
        ROUND(pm.primary_item_orders::DECIMAL / NULLIF(pm.total_orders, 0) * 100, 2) AS primary_item_rate,
        
        -- Recency
        pm.first_sale_date,
        pm.last_sale_date,
        pm.days_since_last_sale,
        DATEDIFF(day, pm.first_sale_date, pm.last_sale_date) AS product_selling_lifespan_days,
        
        -- Quantity Metrics
        pm.avg_quantity_per_order,
        pm.max_quantity_ordered,
        
        -- Performance vs Category
        pm.total_revenue - cb.category_avg_revenue AS revenue_vs_category_avg,
        ROUND((pm.total_revenue / NULLIF(cb.category_avg_revenue, 0) - 1) * 100, 2) AS revenue_vs_category_avg_pct,
        pm.total_units_sold - cb.category_avg_units AS units_vs_category_avg,
        
        -- Performance Ratings
        CASE
            WHEN pm.total_revenue >= cb.category_median_revenue * 2 THEN 'Top Performer'
            WHEN pm.total_revenue >= cb.category_median_revenue THEN 'Above Average'
            WHEN pm.total_revenue >= cb.category_median_revenue * 0.5 THEN 'Average'
            ELSE 'Below Average'
        END AS performance_rating,
        
        CASE
            WHEN pm.days_since_last_sale <= 7 THEN 'Hot'
            WHEN pm.days_since_last_sale <= 30 THEN 'Active'
            WHEN pm.days_since_last_sale <= 90 THEN 'Cooling'
            ELSE 'Cold'
        END AS sales_temperature,
        
        -- Stock Health Indicators
        CASE
            WHEN p.current_stock_quantity = 0 THEN 'Out of Stock'
            WHEN p.current_stock_quantity > 0 AND pm.total_units_sold > 0 
                AND p.current_stock_quantity::DECIMAL / pm.total_units_sold < 0.1 THEN 'Low Stock'
            WHEN p.current_stock_quantity > 100 AND pm.total_orders < 5 THEN 'Overstocked'
            ELSE 'Healthy'
        END AS inventory_health,
        
        -- Flags
        CASE WHEN pm.days_since_last_sale > 90 THEN TRUE ELSE FALSE END AS is_stale,
        CASE WHEN p.current_stock_quantity = 0 THEN TRUE ELSE FALSE END AS needs_restock,
        CASE WHEN pm.total_revenue > cb.category_median_revenue THEN TRUE ELSE FALSE END AS is_top_performer,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM product_metrics pm
    LEFT JOIN category_benchmarks cb ON pm.category = cb.category
    LEFT JOIN {{ ref('dim_products') }} p ON pm.source_product_id = p.source_product_id
)

SELECT * FROM final
ORDER BY total_revenue DESC



