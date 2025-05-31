{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['player_id', 'season_id']}
        ]
    )
}}

with 
-- Get source game logs for authoritative game dates
game_logs as (
    select distinct
        game_id,
        game_date,
        season_id -- Using season_id for season partitioning
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
        gl.season_id, -- Carry season_id through
        pbs.season_year
    from {{ ref('int__player_bxsc_traditional') }} pbs
    inner join game_logs gl on pbs.game_id = gl.game_id


    -- Incremental logic
    {% if is_incremental() %}
    and gl.game_date > (select coalesce(max(game_date), '1900-01-01') from {{ this }}) -- Safety coalesce
    {% endif %}
),

-- For incremental models, get previous season stats to use as starting point
{% if is_incremental() %}
previous_season_stats as (
    select
        player_id,
        season_id, -- Add season_id to group by
        max(season_total_pts) as prev_season_total_pts,
        max(season_games_to_date) as prev_season_games,
        max(season_high_pts_to_date) as prev_season_high_pts
    from {{ this }}
    group by player_id, season_id
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
        season_id, -- Ensure season_id is here
        season_year,
        pts,
        min,
        fga,
        fta,
        
        -- Precompute the row numbers for efficient ordering within each season
        row_number() over (partition by player_id, season_id order by game_date, game_id) as game_seq_in_season
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
    cps.game_seq_in_season as season_games_to_date, -- game_seq_in_season directly gives games to date for the season

    -- Season stats - optimized for incremental processing
    {% if is_incremental() %}
        -- Add current period points to previous season totals
        cps.pts + coalesce(pss.prev_season_total_pts, 0) as season_total_pts,
        -- season_games_to_date is already cps.game_seq_in_season when joined with previous data for same season
        -- but if it's a new season for the player in this batch, pss.prev_season_games would be NULL.
        -- The game_seq_in_season will be correct.
        
        (cps.pts + coalesce(pss.prev_season_total_pts, 0)) / 
            nullif(cps.game_seq_in_season, 0) as season_ppg_to_date, -- Use game_seq_in_season
        greatest(cps.pts, coalesce(pss.prev_season_high_pts, 0)) as season_high_pts_to_date,
        case 
            when cps.pts > coalesce(pss.prev_season_high_pts, 0) then 1 
            else 0 
        end as is_new_season_high
    {% else %}
        -- Initial full load uses window functions, partitioned by season
        sum(cps.pts) over (partition by cps.player_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_total_pts,
        -- season_games_to_date is game_seq_in_season
        avg(cps.pts) over (partition by cps.player_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_ppg_to_date,
        max(cps.pts) over (partition by cps.player_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_high_pts_to_date,
        case 
            when cps.pts = max(cps.pts) over (partition by cps.player_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row)
            and cps.pts > coalesce(
                max(cps.pts) over (
                    partition by cps.player_id, cps.season_id 
                    order by cps.game_date, cps.game_id
                    rows between unbounded preceding and 1 preceding
                ), 0)
            then 1 
            else 0 
        end as is_new_season_high
    {% endif %}
from current_period_stats cps
{% if is_incremental() %}
left join previous_season_stats pss
    on cps.player_id = pss.player_id and cps.season_id = pss.season_id -- Join on player_id and season_id
{% endif %}
order by cps.player_id, cps.game_date, cps.game_id -- Added for deterministic output if needed
