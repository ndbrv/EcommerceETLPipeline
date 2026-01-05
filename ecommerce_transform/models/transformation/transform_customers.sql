{{
    config(
        materialized='table',
        tags=['transform', 'customers']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

transformed AS (
    SELECT
        -- IDs
        id,
        source_customer_id,
        
        -- Personal Info
        first_name,
        last_name,
        full_name,
        email,
        phone,
        date_of_birth,
        gender,
        
        
        DATEDIFF(year, date_of_birth, CURRENT_DATE()) AS age,
        
        
        CASE 
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Under 18'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 55 THEN '45-54'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 65 THEN '55-64'
            ELSE '65+'
        END AS age_group,
        
        -- Address
        street_address,
        city,
        state,
        zip_code,
        country,
        
        -- Account Info
        registration_date,
        last_login,
        is_active,
        email_verified,
        phone_verified,
        
        DATEDIFF(day, registration_date, CURRENT_DATE()) AS days_since_registration,
        DATEDIFF(month, registration_date, CURRENT_DATE()) AS months_since_registration,
        DATEDIFF(year, registration_date, CURRENT_DATE()) AS years_since_registration,
        
        CASE 
            WHEN DATEDIFF(day, registration_date, CURRENT_DATE()) < 30 THEN 'New'
            WHEN DATEDIFF(day, registration_date, CURRENT_DATE()) < 90 THEN 'Recent'
            WHEN DATEDIFF(year, registration_date, CURRENT_DATE()) < 1 THEN 'Active'
            WHEN DATEDIFF(year, registration_date, CURRENT_DATE()) < 3 THEN 'Established'
            ELSE 'Veteran'
        END AS customer_tenure,
        
        DATEDIFF(day, last_login, CURRENT_DATE()) AS days_since_last_login,
        
        CASE 
            WHEN last_login IS NULL THEN 'Never Logged In'
            WHEN DATEDIFF(day, last_login, CURRENT_DATE()) <= 7 THEN 'Highly Active'
            WHEN DATEDIFF(day, last_login, CURRENT_DATE()) <= 30 THEN 'Active'
            WHEN DATEDIFF(day, last_login, CURRENT_DATE()) <= 90 THEN 'Occasional'
            WHEN DATEDIFF(day, last_login, CURRENT_DATE()) <= 180 THEN 'Dormant'
            ELSE 'Inactive'
        END AS activity_status,
        
        CASE 
            WHEN email_verified AND phone_verified THEN 'Fully Verified'
            WHEN email_verified OR phone_verified THEN 'Partially Verified'
            ELSE 'Unverified'
        END AS verification_status,
        
        CASE 
            WHEN email_verified AND phone_verified THEN TRUE
            ELSE FALSE
        END AS is_fully_verified,
        
        -- Preferences
        marketing_opt_in,
        preferred_contact_method,
        customer_segment,
        
        -- Metadata
        generated_at,
        loaded_at,
        batch_id,
        source_system
        
    FROM customers
)

SELECT * FROM transformed