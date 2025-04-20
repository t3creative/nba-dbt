{{
    config(
        schema='staging',
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nba_api', 'league_dash_player_bio_stats') }}
),

-- Extract first and last name from player_name
name_extraction as (
    select
        *,
        -- Handle special cases with periods and multiple spaces
        case 
            when player_name like '%.%' then 
                split_part(player_name, ' ', 1)
            else
                split_part(player_name, ' ', 1)
        end as first_name,
        case 
            when player_name like '%.%' then 
                regexp_replace(player_name, '^[^.]+[.] ', '')
            else
                regexp_replace(player_name, '^[^ ]+ ', '')
        end as last_name
    from source
),

-- Add row number for each player
player_with_row_number as (
    select
        *,
        row_number() over (partition by player_id order by season desc) as row_num
    from name_extraction
),

-- Get the latest data for each player (keeping only row_num = 1)
latest_player_data as (
    select
        player_id,
        first_name,
        last_name,
        player_name as full_name,
        -- Position is not available in this table, set to NULL
        NULL as position,
        -- Height information
        case 
            when player_height = '' then NULL
            else player_height 
        end as height,
        player_height_inches as height_inches,
        case 
            when player_weight = '' then NULL
            when player_weight ~ '^[0-9]+(\.[0-9]+)?$' then cast(player_weight as decimal(5,2))
            else NULL
        end as weight,
        -- Birth date is not available in this table, set to NULL
        NULL as birth_date,
        case 
            when draft_year = '' then NULL
            when draft_year = 'Undrafted' then NULL
            when draft_year ~ '^[0-9]+$' then cast(draft_year as integer) 
            else NULL
        end as draft_year,
        case 
            when draft_round = '' then NULL
            when draft_round = 'Undrafted' then NULL
            when draft_round ~ '^[0-9]+$' then cast(draft_round as integer)
            else NULL
        end as draft_round,
        case 
            when draft_number = '' then NULL
            when draft_number = 'Undrafted' then NULL
            when draft_number ~ '^[0-9]+$' then cast(draft_number as integer)
            else NULL
        end as draft_number,
        case 
            when college = '' then NULL
            else college
        end as college,
        case 
            when country = '' then NULL
            else country
        end as country,
        -- Current season to determine if a player is active
        current_date as valid_from,
        NULL as valid_to,
        -- We'll consider all players in the dataset as active
        true as active
    from player_with_row_number
    where row_num = 1
)

select
    player_id,
    first_name,
    last_name,
    full_name,
    position,
    height,
    height_inches,
    weight,
    birth_date,
    draft_year,
    draft_round,
    draft_number,
    college,
    country,
    active,
    valid_from,
    valid_to
from latest_player_data
