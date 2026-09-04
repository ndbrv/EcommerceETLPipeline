{{
    config(
        materialized='table',
        tags=['curated', 'metric', 'affinity', 'basket_analysis']
    )
}}

WITH order_items AS (
    SELECT
        source_order_id,
        source_product_id,
        product_name,
        category,
        brand,
        item_revenue
    FROM {{ ref('transform_order_items') }}
),

-- Self-join to find product pairs in the same order
product_pairs AS (
    SELECT
        a.source_product_id AS product_a_id,
        a.product_name AS product_a_name,
        a.category AS product_a_category,
        a.brand AS product_a_brand,
        b.source_product_id AS product_b_id,
        b.product_name AS product_b_name,
        b.category AS product_b_category,
        b.brand AS product_b_brand,
        a.source_order_id
    FROM order_items a
    JOIN order_items b 
        ON a.source_order_id = b.source_order_id
        AND a.source_product_id < b.source_product_id  -- Avoid duplicates and self-pairs
),

-- Count how often each pair appears together
pair_counts AS (
    SELECT
        product_a_id,
        product_a_name,
        product_a_category,
        product_a_brand,
        product_b_id,
        product_b_name,
        product_b_category,
        product_b_brand,
        COUNT(DISTINCT source_order_id) AS times_purchased_together
    FROM product_pairs
    GROUP BY 
        product_a_id, product_a_name, product_a_category, product_a_brand,
        product_b_id, product_b_name, product_b_category, product_b_brand
    HAVING COUNT(DISTINCT source_order_id) >= 3  -- Filter early
),

-- Individual product purchase counts
product_totals AS (
    SELECT
        source_product_id,
        COUNT(DISTINCT source_order_id) AS total_orders
    FROM order_items
    GROUP BY source_product_id
),

-- Calculate affinity metrics
final AS (
    SELECT
        -- Product A
        pc.product_a_id,
        pc.product_a_name,
        pc.product_a_category,
        pc.product_a_brand,
        
        -- Product B
        pc.product_b_id,
        pc.product_b_name,
        pc.product_b_category,
        pc.product_b_brand,
        
        -- Co-occurrence metrics
        pc.times_purchased_together,
        pta.total_orders AS product_a_total_orders,
        ptb.total_orders AS product_b_total_orders,
        
        -- Affinity scores
        -- Support: % of orders containing both products
        ROUND(pc.times_purchased_together::DECIMAL / 
              NULLIF((SELECT COUNT(DISTINCT source_order_id) FROM order_items), 0) * 100, 4) AS support_pct,
        
        -- Confidence A->B: % of A orders that also contain B
        ROUND(pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0) * 100, 2) AS confidence_a_to_b_pct,
        
        -- Confidence B->A: % of B orders that also contain A
        ROUND(pc.times_purchased_together::DECIMAL / NULLIF(ptb.total_orders, 0) * 100, 2) AS confidence_b_to_a_pct,
        
        -- Lift: How much more likely to buy together vs. independently
        ROUND(
            (pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0)) /
            NULLIF((ptb.total_orders::DECIMAL / (SELECT COUNT(DISTINCT source_order_id) FROM order_items)), 0),
            2
        ) AS lift,
        
        -- Cross-category flag
        CASE WHEN pc.product_a_category != pc.product_b_category THEN TRUE ELSE FALSE END AS is_cross_category,
        
        -- Cross-brand flag
        CASE WHEN pc.product_a_brand != pc.product_b_brand THEN TRUE ELSE FALSE END AS is_cross_brand,
        
        -- Affinity strength
        CASE
            WHEN ROUND(pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0) * 100, 2) >= 50 THEN 'Very Strong'
            WHEN ROUND(pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0) * 100, 2) >= 30 THEN 'Strong'
            WHEN ROUND(pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0) * 100, 2) >= 15 THEN 'Moderate'
            ELSE 'Weak'
        END AS affinity_strength,
        
        -- Recommendation priority
        CASE
            WHEN pc.times_purchased_together >= 10 
                AND ROUND(pc.times_purchased_together::DECIMAL / NULLIF(pta.total_orders, 0) * 100, 2) >= 20
                THEN TRUE
            ELSE FALSE
        END AS recommend_for_bundling,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS last_updated_at
        
    FROM pair_counts pc
    LEFT JOIN product_totals pta ON pc.product_a_id = pta.source_product_id
    LEFT JOIN product_totals ptb ON pc.product_b_id = ptb.source_product_id
)

SELECT * FROM final
ORDER BY times_purchased_together DESC, confidence_a_to_b_pct DESC

