{{ config(
    materialized='table',
    tags=['core', 'betting', 'props'],
    unique_key=['prop_market_key']
) }}

WITH props_source AS (
    SELECT DISTINCT
        market_cleaned,
        max_market AS market_original
    FROM {{ ref('int__player_props_normalized') }}
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
            WHEN market_cleaned = 'FG3M' THEN 'Three Pointers'
            WHEN market_cleaned = 'PTS+AST' THEN 'Points + Assists'
            WHEN market_cleaned = 'PTS+REB' THEN 'Points + Rebounds'
            WHEN market_cleaned = 'REB+AST' THEN 'Rebounds + Assists'
            WHEN market_cleaned = 'PTS+AST+REB' THEN 'Points + Assists + Rebounds'
            WHEN market_cleaned = 'DBL_DBL' THEN 'Double-Double'
            WHEN market_cleaned = 'TRP_DBL' THEN 'Triple-Double'
            WHEN market_cleaned = 'FIRST_BASKET' THEN 'First Basket'
            ELSE market_cleaned
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
    FROM props_source
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
