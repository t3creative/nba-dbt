{{ config(materialized='table',
    tags=['core', 'seasons'],
    unique_key=['season_year', 'season_type']
) }}

WITH season_source AS (
    SELECT DISTINCT
        season_year,
        CASE 
            -- Simplify season types to just three categories
            WHEN game_subtype = '' THEN 'Regular Season'
            WHEN game_subtype IN ('in-season', 'in-season-knockout', 'Global Games') THEN 'Regular Season'
            ELSE 'Regular Season' -- Default to Regular Season for any other types
        END AS season_type
    FROM {{ ref('stg__schedules') }}
),

-- Add in the missing season types (Preseason and Playoffs) that might not be in the source
complete_seasons AS (
    SELECT DISTINCT
        season_year,
        season_type
    FROM season_source
    
    UNION
    
    -- Add Preseason
    SELECT 
        season_year,
        'Preseason' AS season_type
    FROM (SELECT DISTINCT season_year FROM season_source) s
    
    UNION
    
    -- Add Playoffs
    SELECT 
        season_year,
        'Playoffs' AS season_type
    FROM (SELECT DISTINCT season_year FROM season_source) s
),

-- Extract start and end years from the season_year field
season_years AS (
    SELECT
        season_year,
        season_type,
        -- Extract the starting year (first 4 characters)
        SUBSTRING(season_year, 1, 4)::integer AS start_year,
        -- Extract the ending year (create full year from the last 2 characters)
        CASE 
            WHEN LENGTH(season_year) >= 7 THEN
                ('20' || SUBSTRING(season_year, 6, 2))::integer
            ELSE
                SUBSTRING(season_year, 1, 4)::integer + 1
        END AS end_year
    FROM complete_seasons
),

-- Calculate the most recent regular season for determining current season
latest_season AS (
    SELECT MAX(start_year) AS max_start_year
    FROM season_years
    WHERE season_type = 'Regular Season'
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['season_year', 'season_type']) }} AS season_key,
        season_year,
        season_type,
        CASE 
            WHEN season_type = 'Regular Season' THEN 1
            WHEN season_type = 'Preseason' THEN 0
            WHEN season_type = 'Playoffs' THEN 2
            ELSE 99
        END AS season_type_sort,
        start_year,
        end_year,
        -- Handle special cases for COVID-affected seasons
        CASE 
            -- 2019-20 season (suspended in March, resumed in bubble)
            WHEN season_year = '2019-20' AND season_type = 'Regular Season' THEN 
                TO_DATE('2019-10-22', 'YYYY-MM-DD')
            -- 2020-21 season (started late in December)
            WHEN season_year = '2020-21' AND season_type = 'Regular Season' THEN 
                TO_DATE('2020-12-22', 'YYYY-MM-DD')
            -- Preseason typically starts in September
            WHEN season_type = 'Preseason' THEN
                TO_DATE(start_year || '-09-15', 'YYYY-MM-DD')
            -- Playoffs typically start in April
            WHEN season_type = 'Playoffs' THEN
                TO_DATE(end_year || '-04-15', 'YYYY-MM-DD')
            -- Normal regular seasons
            WHEN season_type = 'Regular Season' AND start_year BETWEEN 1946 AND 2100 THEN 
                TO_DATE(start_year || '-10-01', 'YYYY-MM-DD')
            ELSE NULL
        END AS start_date,
        CASE 
            -- 2019-20 season (suspended in March, resumed in bubble)
            WHEN season_year = '2019-20' AND season_type = 'Regular Season' THEN 
                TO_DATE('2020-08-14', 'YYYY-MM-DD')
            -- 2019-20 playoffs (bubble playoffs)
            WHEN season_year = '2019-20' AND season_type = 'Playoffs' THEN 
                TO_DATE('2020-10-11', 'YYYY-MM-DD')
            -- 2020-21 season (compressed schedule)
            WHEN season_year = '2020-21' AND season_type = 'Regular Season' THEN 
                TO_DATE('2021-05-16', 'YYYY-MM-DD')
            -- Preseason typically ends in October
            WHEN season_type = 'Preseason' THEN
                TO_DATE(start_year || '-10-15', 'YYYY-MM-DD')
            -- Playoffs typically end in June
            WHEN season_type = 'Playoffs' THEN
                TO_DATE(end_year || '-06-30', 'YYYY-MM-DD')
            -- Normal regular seasons
            WHEN season_type = 'Regular Season' AND end_year BETWEEN 1947 AND 2101 THEN 
                TO_DATE(end_year || '-04-14', 'YYYY-MM-DD')
            ELSE NULL
        END AS end_date,
        current_timestamp AS valid_from,
        NULL AS valid_to,
        -- Only the latest season for each season type is current
        CASE
            WHEN start_year = (SELECT max_start_year FROM latest_season) THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM 
        season_years
)

SELECT * FROM final
