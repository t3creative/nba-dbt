{{ config(
    materialized='table',
    tags=['core', 'betting', 'sportsbooks'],
    unique_key=['sportsbook_key']
) }}

WITH sportsbooks_source AS (
    SELECT DISTINCT
        sportsbook
    FROM {{ ref('stg__player_props') }}
),

enriched AS (
    SELECT
        sportsbook,
        -- Add standard classifications for sportsbooks
        CASE
            WHEN LOWER(sportsbook) LIKE '%fanduel%' THEN 'FanDuel'
            WHEN LOWER(sportsbook) LIKE '%draftkings%' THEN 'DraftKings'
            WHEN LOWER(sportsbook) LIKE '%caesars%' THEN 'Caesars'
            WHEN LOWER(sportsbook) LIKE '%bet365%' THEN 'Bet365'
            WHEN LOWER(sportsbook) LIKE '%betmgm%' THEN 'BetMGM'
            WHEN LOWER(sportsbook) LIKE '%pointsbet%' THEN 'PointsBet'
            WHEN LOWER(sportsbook) LIKE '%barstool%' THEN 'Barstool'
            WHEN LOWER(sportsbook) LIKE '%wynnbet%' THEN 'WynnBET'
            WHEN LOWER(sportsbook) LIKE '%unibet%' THEN 'Unibet'
            WHEN LOWER(sportsbook) LIKE '%bet_rivers%' OR LOWER(sportsbook) LIKE '%betrivers%' THEN 'BetRivers'
            WHEN LOWER(sportsbook) LIKE '%twinspires%' THEN 'TwinSpires'
            WHEN LOWER(sportsbook) LIKE '%bally%' THEN 'Bally Bet'
            WHEN LOWER(sportsbook) LIKE '%fox%' THEN 'FOX Bet'
            ELSE sportsbook
        END AS sportsbook_name,
        
        -- Parent company classifications
        CASE
            WHEN LOWER(sportsbook) LIKE '%fanduel%' THEN 'Flutter Entertainment'
            WHEN LOWER(sportsbook) LIKE '%draftkings%' THEN 'DraftKings Inc.'
            WHEN LOWER(sportsbook) LIKE '%caesars%' THEN 'Caesars Entertainment'
            WHEN LOWER(sportsbook) LIKE '%bet365%' THEN 'Bet365 Group Ltd'
            WHEN LOWER(sportsbook) LIKE '%betmgm%' THEN 'Entain/MGM Resorts'
            WHEN LOWER(sportsbook) LIKE '%pointsbet%' THEN 'PointsBet Holdings'
            WHEN LOWER(sportsbook) LIKE '%barstool%' THEN 'Penn National Gaming'
            WHEN LOWER(sportsbook) LIKE '%wynnbet%' THEN 'Wynn Resorts'
            WHEN LOWER(sportsbook) LIKE '%unibet%' THEN 'Kindred Group'
            WHEN LOWER(sportsbook) LIKE '%bet_rivers%' OR LOWER(sportsbook) LIKE '%betrivers%' THEN 'Rush Street Interactive'
            WHEN LOWER(sportsbook) LIKE '%twinspires%' THEN 'Churchill Downs Inc.'
            WHEN LOWER(sportsbook) LIKE '%bally%' THEN 'Bally''s Corporation'
            WHEN LOWER(sportsbook) LIKE '%fox%' THEN 'Flutter Entertainment'
            ELSE 'Independent'
        END AS parent_company,
        
        -- Book type classification
        CASE
            WHEN LOWER(sportsbook) LIKE '%betmgm%' OR 
                 LOWER(sportsbook) LIKE '%caesars%' OR
                 LOWER(sportsbook) LIKE '%wynnbet%' OR
                 LOWER(sportsbook) LIKE '%bally%' THEN 'Casino-Based'
            WHEN LOWER(sportsbook) LIKE '%fanduel%' OR 
                 LOWER(sportsbook) LIKE '%draftkings%' THEN 'DFS-Origin'
            WHEN LOWER(sportsbook) LIKE '%bet365%' OR
                 LOWER(sportsbook) LIKE '%unibet%' THEN 'European-Origin'
            ELSE 'Other'
        END AS book_type,
        
        -- Metadata
        current_timestamp AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current
    FROM sportsbooks_source
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['sportsbook']) }} AS sportsbook_key,
        sportsbook AS sportsbook_code,
        sportsbook_name,
        parent_company,
        book_type,
        LOWER(REGEXP_REPLACE(sportsbook, '[^a-zA-Z0-9]', '_')) AS sportsbook_slug,
        valid_from,
        valid_to,
        is_current
    FROM enriched
)

SELECT * FROM final
