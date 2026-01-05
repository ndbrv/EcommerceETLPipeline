{{
    config(
        materialized='table',
        tags=['staging', 'orders']
    )
}}

WITH raw AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

cleaned AS (
    SELECT
        -- Primary Keys
        id,
        source_order_id,
        source_customer_id,
        
        -- Date/Time
        order_date,
        
        -- Order Status
        order_status,
        
        -- Financial Amount (convert FLOAT to DECIMAL)
        total_amount::DECIMAL(10,2) AS total_amount,
        
        -- Payment & Shipping
        payment_method,
        shipping_address,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source AS source_system
        
    FROM raw
)

SELECT * FROM cleaned