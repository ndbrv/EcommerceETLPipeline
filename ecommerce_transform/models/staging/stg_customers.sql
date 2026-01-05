{{
    config(
        materialized='table',
        tags=['staging', 'customers']
    )
}}

WITH raw AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
),

cleaned AS (
    SELECT
        -- Primary Key
        id,
        source_customer_id,
        
        -- Personal Information
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,  -- Simple concatenation OK
        email,
        phone, --To do later clean up phone number
        date_of_birth,
        gender,
        
        -- Address
        street_address,
        city,
        state,
        zip_code,
        country,
        
        -- Account Information
        registration_date,
        last_login,
        is_active,
        email_verified,
        phone_verified,
        
        -- Preferences
        marketing_opt_in,
        preferred_contact_method,
        customer_segment,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source AS source_system
        
    FROM raw
)

SELECT * FROM cleaned