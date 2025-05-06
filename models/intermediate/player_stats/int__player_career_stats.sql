{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']}
        ]
    )
}}

with 
-- Get source game logs for authoritative game dates
game_logs as (
    select distinct
        game_id,
        game_date,
        season_id
    from {{ ref('stg__game_logs_league') }}
),

-- Get player box scores
source_data as (
    select 
        pbs.player_game_key,
        pbs.player_id,
        pbs.game_id,
        pbs.team_id,
        pbs.first_name,
        pbs.family_name,
        pbs.pts,
        pbs.min,
        pbs.fga,
        pbs.fta,
        gl.game_date,
        gl.season_id,
        pbs.season_year
    from {{ ref('stg__player_traditional_bxsc') }} pbs
    inner join game_logs gl on pbs.game_id = gl.game_id

    -- Filter based on the starting year extracted from pbs.season_year
    where cast(substring(pbs.season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}

    -- Incremental logic
    {% if is_incremental() %}
    and gl.game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- For incremental models, get previous career stats to use as starting point
{% if is_incremental() %}
previous_career_stats as (
    select
        player_id,
        max(career_total_pts) as prev_career_total_pts,
        max(career_games_to_date) as prev_career_games,
        max(career_high_pts_to_date) as prev_career_high_pts
    from {{ this }}
    group by player_id
),
{% endif %}

-- Calculate current period stats
current_period_stats as (
    select
        player_game_key,
        player_id,
        first_name || ' ' || family_name as player_name,
        game_id,
        game_date,
        season_id,
        season_year,
        pts,
        min,
        fga,
        fta,
        
        -- Precompute the row numbers for efficient ordering
        row_number() over (partition by player_id order by game_date, game_id) as game_seq
    from source_data
)

select
    cps.player_game_key,
    cps.player_id,
    cps.player_name,
    cps.game_id,
    cps.game_date,
    cps.season_id,
    cps.season_year,
    cps.pts,
    cps.min,
    cps.fga,
    cps.fta,
    
    -- Career stats - optimized for incremental processing
    {% if is_incremental() %}
        -- Add current period points to previous career totals
        cps.pts + coalesce(pcs.prev_career_total_pts, 0) as career_total_pts,
        cps.game_seq + coalesce(pcs.prev_career_games, 0) as career_games_to_date,
        (cps.pts + coalesce(pcs.prev_career_total_pts, 0)) / 
            nullif((cps.game_seq + coalesce(pcs.prev_career_games, 0)), 0) as career_ppg_to_date,
        greatest(cps.pts, coalesce(pcs.prev_career_high_pts, 0)) as career_high_pts_to_date,
        case 
            when cps.pts > coalesce(pcs.prev_career_high_pts, 0) then 1 
            else 0 
        end as is_new_career_high
    {% else %}
        -- Initial full load uses window functions
        {{ career_stat('cps.player_id', 'cps.game_date', 'cps.pts', 'sum') }} as career_total_pts,
        {{ career_stat('cps.player_id', 'cps.game_date', '1', 'count') }} as career_games_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date', 'cps.pts', 'avg') }} as career_ppg_to_date,
        {{ career_stat('cps.player_id', 'cps.game_date', 'cps.pts', 'max') }} as career_high_pts_to_date,
        case when cps.pts = {{ career_stat('cps.player_id', 'cps.game_date', 'cps.pts', 'max') }}
             and cps.pts > coalesce(
                max(cps.pts) over (
                    partition by cps.player_id 
                    order by cps.game_date
                    rows between unbounded preceding and 1 preceding
                ), 0)
             then 1 else 0 
        end as is_new_career_high
    {% endif %}
from current_period_stats cps
{% if is_incremental() %}
left join previous_career_stats pcs
    on cps.player_id = pcs.player_id
{% endif %}
