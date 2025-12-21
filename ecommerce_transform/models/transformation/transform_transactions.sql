{{
    config(
        materialized='table',
        tags=['intermediate', 'transactions']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

enriched AS (
    SELECT
        -- IDs
        transaction_id,
        order_id,
        customer_id,
        
        -- Date/Time
        order_date,
        order_date_key,
        
        -- BUSINESS LOGIC: Date/Time extractions
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        EXTRACT(DAY FROM order_date) AS order_day,
        EXTRACT(HOUR FROM order_date) AS order_hour,
        EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week_num,
        DAYNAME(order_date) AS day_name,
        MONTHNAME(order_date) AS month_name,
        QUARTER(order_date) AS quarter,
        
        -- BUSINESS LOGIC: Time of day categories
        CASE 
            WHEN EXTRACT(HOUR FROM order_date) BETWEEN 0 AND 5 THEN 'Night'
            WHEN EXTRACT(HOUR FROM order_date) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM order_date) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM order_date) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Late Night'
        END AS time_of_day,
        
        -- BUSINESS LOGIC: Weekend/weekday
        CASE 
            WHEN DAYOFWEEK(order_date) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN DAYOFWEEK(order_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        
        -- Order Status
        order_status,
        
        -- BUSINESS LOGIC: Status categories
        CASE 
            WHEN order_status IN ('Completed', 'Delivered') THEN TRUE
            ELSE FALSE
        END AS is_completed,
        
        CASE 
            WHEN order_status IN ('Cancelled', 'Returned') THEN TRUE
            ELSE FALSE
        END AS is_cancelled,
        
        CASE 
            WHEN order_status IN ('Completed', 'Delivered') THEN 'Successful'
            WHEN order_status IN ('Cancelled', 'Returned') THEN 'Unsuccessful'
            ELSE 'In Progress'
        END AS order_outcome,
        
        -- Financial Amounts
        subtotal_amount,
        tax_amount,
        shipping_amount,
        total_amount,
        
        -- BUSINESS LOGIC: Financial calculations
        ROUND((tax_amount / NULLIF(subtotal_amount, 0)) * 100, 2) AS effective_tax_rate,
        ROUND((shipping_amount / NULLIF(subtotal_amount, 0)) * 100, 2) AS shipping_percentage,
        subtotal_amount + tax_amount + shipping_amount AS calculated_total,
        
        -- BUSINESS LOGIC: Verify totals match
        ABS(total_amount - (subtotal_amount + tax_amount + shipping_amount)) < 0.01 AS is_total_accurate,
        
        -- Order Details
        item_count,
        items_detail,
        
        -- BUSINESS LOGIC: Average item price
        ROUND(subtotal_amount / NULLIF(item_count, 0), 2) AS avg_item_price,
        
        -- BUSINESS LOGIC: Order value categories
        CASE 
            WHEN total_amount < 25 THEN 'Micro'
            WHEN total_amount < 50 THEN 'Small'
            WHEN total_amount < 100 THEN 'Medium'
            WHEN total_amount < 250 THEN 'Large'
            WHEN total_amount < 500 THEN 'Very Large'
            ELSE 'Premium'
        END AS order_value_tier,
        
        -- BUSINESS LOGIC: Order size categories
        CASE 
            WHEN item_count = 1 THEN 'Single Item'
            WHEN item_count BETWEEN 2 AND 3 THEN 'Few Items'
            WHEN item_count BETWEEN 4 AND 7 THEN 'Multiple Items'
            WHEN item_count BETWEEN 8 AND 15 THEN 'Many Items'
            ELSE 'Bulk Order'
        END AS order_size_category,
        
        -- BUSINESS LOGIC: Free shipping flag
        CASE 
            WHEN shipping_amount = 0 THEN TRUE
            ELSE FALSE
        END AS is_free_shipping,
        
        -- Payment & Shipping
        payment_method,
        shipping_method,
        
        -- BUSINESS LOGIC: Payment type categories
        CASE 
            WHEN payment_method IN ('Credit Card', 'Debit Card') THEN 'Card'
            WHEN payment_method IN ('PayPal', 'Apple Pay', 'Google Pay') THEN 'Digital Wallet'
            WHEN payment_method = 'Bank Transfer' THEN 'Bank Transfer'
            ELSE 'Other'
        END AS payment_type,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source_system
        
    FROM transactions
)

SELECT * FROM enriched