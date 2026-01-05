{{
    config(
        materialized='table',
        tags=['transform', 'products']
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

transformed AS (
    SELECT
        -- IDs
        source_product_id,
        
        -- Product Info
        product_name,
        brand,
        category,
        description,
        
        -- Pricing (from staging)
        price,
        discount_percentage,
        
        -- BUSINESS LOGIC: Calculate discounted price
        ROUND(price * (1 - discount_percentage / 100.0), 2) AS discounted_price,
        ROUND(price * (discount_percentage / 100.0), 2) AS discount_amount,
        
        -- Inventory
        stock_quantity,
        availability_status,
        minimum_order_quantity,
        
        -- Ratings
        rating,
        
        
        CASE 
            WHEN rating >= 4.5 THEN 'Excellent'
            WHEN rating >= 4.0 THEN 'Very Good'
            WHEN rating >= 3.0 THEN 'Good'
            WHEN rating >= 2.0 THEN 'Fair'
            ELSE 'Poor'
        END AS rating_category,
        
        
        CASE 
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_quantity <= 10 THEN 'Low Stock'
            WHEN stock_quantity <= 50 THEN 'Medium Stock'
            ELSE 'High Stock'
        END AS stock_level,
        
        
        CASE 
            WHEN discounted_price < 20 THEN 'Budget'
            WHEN discounted_price < 50 THEN 'Economy'
            WHEN discounted_price < 100 THEN 'Mid-Range'
            WHEN discounted_price < 500 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_category,
        
        -- BUSINESS LOGIC: Sale flags
        CASE 
            WHEN discount_percentage > 0 THEN TRUE 
            ELSE FALSE 
        END AS is_on_sale,
        
        CASE 
            WHEN discount_percentage >= 50 THEN 'Massive'
            WHEN discount_percentage >= 20 THEN 'Large'
            WHEN discount_percentage >= 10 THEN 'Moderate'
            WHEN discount_percentage > 0 THEN 'Small'
            ELSE 'No Discount'
        END AS discount_category,
        
        -- BUSINESS LOGIC: Availability flags
        CASE 
            WHEN stock_quantity > 0 AND availability_status = 'In Stock' THEN TRUE
            ELSE FALSE
        END AS is_available,
        
        -- Physical dimensions
        weight,
        width,
        height,
        depth,
        
        -- Product details
        warranty_info,
        shipping_info,
        return_policy,
        
        -- Identifiers
        barcode,
        qr_code,
        
        -- Media
        thumbnail_url,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source_system
        
    FROM products
)

SELECT * FROM transformed