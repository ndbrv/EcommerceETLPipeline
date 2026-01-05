{{
    config(
        materialized='table',
        tags=['staging', 'order_items']
    )
}}

WITH raw AS (
    SELECT * FROM {{ source('raw', 'raw_order_items') }}
),

cleaned AS (
    SELECT
        -- Primary Keys
        id,
        source_order_item_id,
        source_order_id,
        source_product_id,
        
        -- Item Details
        quantity,
        
        -- Financial Amounts (convert FLOAT to DECIMAL)
        unit_price::DECIMAL(10,2) AS unit_price,
        total_price::DECIMAL(10,2) AS total_price,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source AS source_system
        
    FROM raw
)

SELECT * FROM cleaned

