{{
    config(
        materialized='table',
        tags=['curated', 'dimension', 'geography']
    )
}}

WITH customer_locations AS (
    SELECT DISTINCT
        city,
        state,
        zip_code,
        country
    FROM {{ ref('transform_customers') }}
    WHERE city IS NOT NULL
),

geography_enriched AS (
    SELECT
        -- Location identifiers
        ROW_NUMBER() OVER (ORDER BY country, state, city, zip_code) AS geography_key,
        
        -- Address components
        city,
        state,
        zip_code,
        country,
        
        -- Concatenated full location
        city || ', ' || state || ' ' || zip_code AS city_state_zip,
        city || ', ' || state AS city_state,
        state || ', ' || country AS state_country,
        
        -- Regional groupings (US-centric, adjust as needed)
        CASE
            WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
            WHEN state IN ('OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
            WHEN state IN ('DE', 'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA', 'OK', 'TX') THEN 'South'
            WHEN state IN ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV', 'WA', 'OR', 'CA', 'AK', 'HI') THEN 'West'
            ELSE 'Other'
        END AS us_region,
        
        -- Metro area flags (examples - expand based on your needs)
        CASE
            WHEN city IN ('New York', 'Brooklyn', 'Queens', 'Bronx', 'Manhattan') THEN 'New York Metro'
            WHEN city IN ('Los Angeles', 'Long Beach', 'Anaheim', 'Santa Ana') THEN 'Los Angeles Metro'
            WHEN city IN ('Chicago', 'Naperville', 'Joliet') THEN 'Chicago Metro'
            WHEN city IN ('Houston', 'The Woodlands', 'Sugar Land') THEN 'Houston Metro'
            WHEN city IN ('Phoenix', 'Mesa', 'Scottsdale') THEN 'Phoenix Metro'
            WHEN city IN ('Philadelphia', 'Camden', 'Wilmington') THEN 'Philadelphia Metro'
            WHEN city IN ('San Antonio') THEN 'San Antonio Metro'
            WHEN city IN ('San Diego', 'Carlsbad') THEN 'San Diego Metro'
            WHEN city IN ('Dallas', 'Fort Worth', 'Arlington') THEN 'Dallas Metro'
            WHEN city IN ('San Jose', 'Sunnyvale', 'Santa Clara') THEN 'San Jose Metro'
            WHEN city IN ('Austin', 'Round Rock') THEN 'Austin Metro'
            WHEN city IN ('Jacksonville') THEN 'Jacksonville Metro'
            WHEN city IN ('San Francisco', 'Oakland', 'Hayward') THEN 'San Francisco Metro'
            WHEN city IN ('Columbus') THEN 'Columbus Metro'
            WHEN city IN ('Indianapolis', 'Carmel', 'Anderson') THEN 'Indianapolis Metro'
            WHEN city IN ('Seattle', 'Tacoma', 'Bellevue') THEN 'Seattle Metro'
            WHEN city IN ('Denver', 'Aurora', 'Lakewood') THEN 'Denver Metro'
            WHEN city IN ('Boston', 'Cambridge', 'Newton') THEN 'Boston Metro'
            WHEN city IN ('Atlanta', 'Sandy Springs', 'Roswell') THEN 'Atlanta Metro'
            ELSE 'Other'
        END AS metro_area,
        
        -- Population density proxy (based on metro area)
        CASE
            WHEN city IN ('New York', 'Brooklyn', 'Queens', 'Bronx', 'Manhattan', 
                         'Los Angeles', 'Chicago', 'San Francisco', 'Philadelphia', 'Boston') THEN 'High Density'
            WHEN city IN ('Houston', 'Phoenix', 'Dallas', 'San Antonio', 'San Diego', 
                         'San Jose', 'Austin', 'Seattle', 'Denver', 'Atlanta') THEN 'Medium Density'
            ELSE 'Low Density'
        END AS population_density,
        
        -- Time zone (US-centric)
        CASE
            WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 'DC', 
                          'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'OH', 'IN', 'MI', 'KY', 'TN') THEN 'Eastern'
            WHEN state IN ('AL', 'AR', 'IL', 'IA', 'KS', 'LA', 'MN', 'MS', 'MO', 'NE', 'ND', 'OK', 
                          'SD', 'TX', 'WI') THEN 'Central'
            WHEN state IN ('AZ', 'CO', 'ID', 'MT', 'NM', 'UT', 'WY') THEN 'Mountain'
            WHEN state IN ('CA', 'NV', 'OR', 'WA') THEN 'Pacific'
            WHEN state = 'AK' THEN 'Alaska'
            WHEN state = 'HI' THEN 'Hawaii'
            ELSE 'Other'
        END AS time_zone
        
    FROM customer_locations
)

SELECT * FROM geography_enriched

