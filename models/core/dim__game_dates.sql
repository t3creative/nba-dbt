{{ config(
    materialized='table',
    tags=['core', 'dates'],
    unique_key=['date_day']
) }}

-- Get distinct game dates from schedules
WITH game_dates AS (
    SELECT DISTINCT
        game_date AS date_day
    FROM {{ ref('stg__schedules') }}
),

-- Generate date attributes
date_attributes AS (
    SELECT
        date_day,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        CASE EXTRACT(DOW FROM date_day)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week_name,
        EXTRACT(MONTH FROM date_day) AS month_of_year,
        CASE EXTRACT(MONTH FROM date_day)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        EXTRACT(YEAR FROM date_day) AS year_number,
        EXTRACT(QUARTER FROM date_day) AS quarter_of_year,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        -- NBA Season Logic
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 10 THEN EXTRACT(YEAR FROM date_day)::TEXT
            WHEN EXTRACT(MONTH FROM date_day) < 10 THEN (EXTRACT(YEAR FROM date_day) - 1)::TEXT
        END AS nba_season,
        -- Day type classification
        CASE
            WHEN EXTRACT(DOW FROM date_day) = 5 THEN 'Friday'
            WHEN EXTRACT(DOW FROM date_day) = 6 THEN 'Saturday'
            WHEN EXTRACT(DOW FROM date_day) = 0 THEN 'Sunday'
            ELSE 'Weekday'
        END AS day_type
    FROM game_dates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    date_day,
    day_of_week,
    day_of_week_name,
    month_of_year,
    month_name,
    year_number,
    quarter_of_year,
    is_weekend,
    year_month,
    nba_season,
    day_type,
    -- Season segments (rough approximation)
    CASE
        WHEN month_of_year BETWEEN 10 AND 12 THEN 'Early Season'
        WHEN month_of_year BETWEEN 1 AND 2 THEN 'Mid Season'
        WHEN month_of_year = 3 THEN 'Late Season'
        WHEN month_of_year = 4 THEN 'End of Season'
        WHEN month_of_year BETWEEN 5 AND 6 THEN 'Playoffs'
        ELSE 'Off Season'
    END AS season_segment
FROM date_attributes
ORDER BY date_day 