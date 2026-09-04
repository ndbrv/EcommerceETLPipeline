{{
    config(
        materialized='table',
        tags=['curated', 'dimension', 'dates']
    )
}}

WITH date_spine AS (
    -- Generate dates for the last 3 years and next 1 year
    SELECT
        DATEADD(day, SEQ4(), DATEADD(year, -3, DATE_TRUNC('year', CURRENT_DATE()))) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1461)) -- 4 years of dates
),

date_attributes AS (
    SELECT
        date_day,
        
        -- Basic date parts
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        WEEK(date_day) AS week_of_year,
        DAY(date_day) AS day_of_month,
        DAYOFYEAR(date_day) AS day_of_year,
        DAYOFWEEK(date_day) AS day_of_week,
        
        -- Named parts
        MONTHNAME(date_day) AS month_name,
        DAYNAME(date_day) AS day_name,
        
        -- Quarter strings
        'Q' || QUARTER(date_day) || ' ' || YEAR(date_day) AS quarter_name,
        
        -- Month strings
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'MMMM YYYY') AS month_year_name,
        
        -- Week strings
        TO_CHAR(date_day, 'YYYY-"W"IW') AS year_week,
        
        -- First/Last day indicators
        date_day = DATE_TRUNC('month', date_day) AS is_first_day_of_month,
        date_day = LAST_DAY(date_day) AS is_last_day_of_month,
        date_day = DATE_TRUNC('quarter', date_day) AS is_first_day_of_quarter,
        date_day = LAST_DAY(DATE_TRUNC('quarter', date_day)) AS is_last_day_of_quarter,
        date_day = DATE_TRUNC('year', date_day) AS is_first_day_of_year,
        date_day = LAST_DAY(DATE_TRUNC('year', date_day)) AS is_last_day_of_year,
        
        -- Weekday/Weekend
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        DAYOFWEEK(date_day) IN (0, 6) AS is_weekend,
        
        -- Relative date flags
        date_day = CURRENT_DATE() AS is_today,
        date_day = DATEADD(day, -1, CURRENT_DATE()) AS is_yesterday,
        date_day >= DATE_TRUNC('week', CURRENT_DATE()) AS is_current_week,
        date_day >= DATE_TRUNC('month', CURRENT_DATE()) AS is_current_month,
        date_day >= DATE_TRUNC('quarter', CURRENT_DATE()) AS is_current_quarter,
        date_day >= DATE_TRUNC('year', CURRENT_DATE()) AS is_current_year,
        
        -- Days from today
        DATEDIFF(day, date_day, CURRENT_DATE()) AS days_from_today,
        
        -- Previous period dates
        DATEADD(day, -1, date_day) AS prior_day,
        DATEADD(week, -1, date_day) AS prior_week_same_day,
        DATEADD(month, -1, date_day) AS prior_month_same_day,
        DATEADD(year, -1, date_day) AS prior_year_same_day,
        
        -- First day of periods
        DATE_TRUNC('week', date_day) AS first_day_of_week,
        DATE_TRUNC('month', date_day) AS first_day_of_month,
        DATE_TRUNC('quarter', date_day) AS first_day_of_quarter,
        DATE_TRUNC('year', date_day) AS first_day_of_year,
        
        -- Fiscal periods (assuming fiscal year starts in January, adjust if needed)
        YEAR(date_day) AS fiscal_year,
        QUARTER(date_day) AS fiscal_quarter,
        
        -- ISO Week
        TO_CHAR(date_day, 'IYYY') AS iso_year,
        TO_CHAR(date_day, 'IW') AS iso_week
        
    FROM date_spine
)

SELECT * FROM date_attributes

