{{ config(
    materialized='table',
    tags=['core', 'betting', 'props'],
    unique_key=['prop_market_key']
) }}

WITH stg_props AS (
    SELECT DISTINCT
        market -- This is the raw market name like "Player Points"
    FROM {{ ref('stg__player_props') }}
),

derived_cleaned_market AS (
    SELECT
        market as market_original,
        CASE
            WHEN LOWER(market) LIKE '%points%' AND LOWER(market) LIKE '%assists%' AND LOWER(market) LIKE '%rebounds%' THEN 'PTS+AST+REB'
            WHEN LOWER(market) LIKE '%points%' AND LOWER(market) LIKE '%assists%' THEN 'PTS+AST'
            WHEN LOWER(market) LIKE '%points%' AND LOWER(market) LIKE '%rebounds%' THEN 'PTS+REB'
            WHEN LOWER(market) LIKE '%rebounds%' AND LOWER(market) LIKE '%assists%' THEN 'REB+AST'
            WHEN LOWER(market) LIKE '%points%' THEN 'PTS'
            WHEN LOWER(market) LIKE '%rebounds%' THEN 'REB'
            WHEN LOWER(market) LIKE '%assists%' THEN 'AST'
            WHEN LOWER(market) LIKE '%three pointers%' OR LOWER(market) LIKE '%threes made%' OR LOWER(market) LIKE '%fg3m%' THEN 'FG3M'
            WHEN LOWER(market) LIKE '%blocks%' THEN 'BLK'
            WHEN LOWER(market) LIKE '%steals%' THEN 'STL'
            WHEN LOWER(market) LIKE '%double-double%' THEN 'DBL_DBL'
            WHEN LOWER(market) LIKE '%triple-double%' THEN 'TRP_DBL'
            WHEN LOWER(market) LIKE '%first basket%' THEN 'FIRST_BASKET'
            -- Add more specific mappings as needed based on your raw `market` values
            ELSE REPLACE(UPPER(market), ' ', '_') -- Fallback, attempt to create a slug-like version
        END AS market_cleaned
    FROM stg_props
),

categorized AS (
    SELECT
        market_cleaned,
        market_original,
        
        -- Base prop category
        CASE
            WHEN market_cleaned IN ('PTS', 'FG3M') THEN 'Scoring'
            WHEN market_cleaned IN ('REB', 'AST', 'BLK', 'STL') THEN 'Performance'
            WHEN market_cleaned IN ('PTS+AST', 'PTS+REB', 'REB+AST', 'PTS+AST+REB') THEN 'Combined'
            WHEN market_cleaned IN ('DBL_DBL', 'TRP_DBL') THEN 'Achievement'
            WHEN market_cleaned IN ('FIRST_BASKET') THEN 'First Basket'
            ELSE 'Other'
        END AS prop_category,
        
        -- Detailed stat type
        CASE
            WHEN market_cleaned = 'PTS' THEN 'Points'
            WHEN market_cleaned = 'REB' THEN 'Rebounds'
            WHEN market_cleaned = 'AST' THEN 'Assists'
            WHEN market_cleaned = 'BLK' THEN 'Blocks'
            WHEN market_cleaned = 'STL' THEN 'Steals'
            WHEN market_cleaned = 'FG3M' THEN 'Three Pointers Made'
            WHEN market_cleaned = 'PTS+AST' THEN 'Points + Assists'
            WHEN market_cleaned = 'PTS+REB' THEN 'Points + Rebounds'
            WHEN market_cleaned = 'REB+AST' THEN 'Rebounds + Assists'
            WHEN market_cleaned = 'PTS+AST+REB' THEN 'Points + Assists + Rebounds'
            WHEN market_cleaned = 'DBL_DBL' THEN 'Double-Double'
            WHEN market_cleaned = 'TRP_DBL' THEN 'Triple-Double'
            WHEN market_cleaned = 'FIRST_BASKET' THEN 'First Basket Scorer'
            ELSE market_cleaned -- Use the cleaned version if no specific override
        END AS stat_type,
        
        -- Is it a combined stat?
        CASE
            WHEN market_cleaned IN ('PTS+AST', 'PTS+REB', 'REB+AST', 'PTS+AST+REB') THEN TRUE
            ELSE FALSE
        END AS is_combined,
        
        -- Typical market availability
        CASE
            WHEN market_cleaned IN ('PTS', 'REB', 'AST') THEN 'High'
            WHEN market_cleaned IN ('PTS+AST', 'PTS+REB', 'FG3M') THEN 'Medium'
            WHEN market_cleaned IN ('BLK', 'STL', 'REB+AST', 'PTS+AST+REB', 'DBL_DBL', 'TRP_DBL', 'FIRST_BASKET') THEN 'Low'
            ELSE 'Variable'
        END AS market_availability,
        
        -- Proposition format
        CASE
            WHEN market_cleaned IN ('DBL_DBL', 'TRP_DBL', 'FIRST_BASKET') THEN 'Yes/No'
            ELSE 'Over/Under'
        END AS prop_format,
        
        -- Statistical dimensions
        CASE
            WHEN market_cleaned IN ('PTS', 'FG3M', 'PTS+AST', 'PTS+REB', 'PTS+AST+REB') THEN TRUE
            ELSE FALSE
        END AS involves_scoring,
        
        CASE 
            WHEN market_cleaned IN ('REB', 'REB+AST', 'PTS+REB', 'PTS+AST+REB') THEN TRUE
            ELSE FALSE
        END AS involves_rebounds,
        
        CASE
            WHEN market_cleaned IN ('AST', 'PTS+AST', 'REB+AST', 'PTS+AST+REB') THEN TRUE
            ELSE FALSE 
        END AS involves_assists,
        
        CASE
            WHEN market_cleaned IN ('BLK') THEN TRUE
            ELSE FALSE
        END AS involves_blocks,
        
        CASE
            WHEN market_cleaned IN ('STL') THEN TRUE
            ELSE FALSE
        END AS involves_steals,
        
        -- Metadata
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM derived_cleaned_market -- Changed source CTE
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['market_cleaned']) }} AS prop_market_key,
        market_cleaned,
        stat_type,
        market_original,
        prop_category,
        is_combined,
        market_availability,
        prop_format,
        involves_scoring,
        involves_rebounds,
        involves_assists, 
        involves_blocks,
        involves_steals,
        valid_from,
        valid_to,
        is_current
    FROM categorized
)

SELECT * FROM final
