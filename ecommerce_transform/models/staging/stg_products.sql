{{
    config(
        materialized='table',
        tags=['staging', 'products']
    )
}}

WITH raw AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        -- Primary Key
        product_id,
        
        -- Product Information
        product_name,
        brand,
        category,
        description,
        
        -- Pricing (just type conversion, no calculations)
        price::DECIMAL(10,2) AS price,
        discount_percentage::DECIMAL(10,2) AS discount_percentage,
        
        -- Inventory
        stock AS stock_quantity,
        availability_status,
        minimum_order_qty AS minimum_order_quantity,
        
        -- Ratings
        rating::DECIMAL(3,2) AS rating,
        
        -- Physical Dimensions
        weight::DECIMAL(10,2) AS weight,
        width::DECIMAL(10,2) AS width,
        height::DECIMAL(10,2) AS height,
        depth::DECIMAL(10,2) AS depth,
        
        -- Product Details
        warranty_info,
        shipping_info,
        return_policy,
        CASE 
            WHEN return_policy IS NULL THEN NULL
            WHEN UPPER(return_policy) LIKE '%NO RETURN%' THEN 0
            ELSE CAST(REGEXP_REPLACE(return_policy, '[^0-9]', '') AS INT)
        END AS return_policy_days_clean, 
        
        -- Identifiers
        barcode,
        qr_code,
        
        -- Media
        thumbnail AS thumbnail_url,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source AS source_system
        
    FROM raw
    WHERE product_id IS NOT NULL
      AND product_name IS NOT NULL
)

SELECT * FROM cleaned