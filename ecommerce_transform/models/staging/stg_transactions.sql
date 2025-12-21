{{
    config(
        materialized='table',
        tags=['staging', 'transactions']
    )
}}

WITH raw AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        -- Primary Keys
        transaction_id,
        order_id,
        customer_id,
        
        -- Date/Time
        order_date,
        DATE(order_date) AS order_date_key,  -- For joining to dim_date later
        
        -- Order Status
        order_status,
        
        -- Financial Amounts (convert FLOAT to DECIMAL)
        subtotal::DECIMAL(10,2) AS subtotal_amount,
        tax::DECIMAL(10,2) AS tax_amount,
        shipping_cost::DECIMAL(10,2) AS shipping_amount,
        total_amount::DECIMAL(10,2) AS total_amount,
        
        -- Order Details
        num_items AS item_count,
        items_detail,
        
        -- Payment & Shipping Methods
        payment_method,
        shipping_method,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source AS source_system
        
    FROM raw
    WHERE transaction_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_date IS NOT NULL
      AND total_amount >= 0  -- Basic data quality check
)

SELECT * FROM cleaned