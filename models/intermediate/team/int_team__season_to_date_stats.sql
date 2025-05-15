{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='team_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['team_game_key'], 'unique': True},
            {'columns': ['game_id', 'team_id']},
            {'columns': ['team_id', 'season_id']}
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

-- Get team box scores
source_data as (
    select 
        tbs.team_game_key,
        tbs.team_id,
        tbs.game_id,
        tbs.pts,
        -- Determine if the game was a win. Assuming 'PLUS_MINUS' > 0 indicates a win.
        -- Adjust this logic if your boxscore model has a direct 'is_win' field or uses a different margin indicator.
        case when tbs.plus_minus > 0 then 1 else 0 end as is_win,
        gl.game_date,
        gl.season_id,
        tbs.season_year
    from {{ ref('int_team__combined_boxscore') }} tbs -- Using a combined team boxscore model
    inner join game_logs gl on tbs.game_id = gl.game_id

    -- Filter based on the starting year extracted from tbs.season_year
    where cast(substring(tbs.season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}

    -- Incremental logic
    {% if is_incremental() %}
    and gl.game_date > (select coalesce(max(game_date), '1900-01-01') from {{ this }}) -- Safety coalesce
    {% endif %}
),

-- For incremental models, get previous season stats to use as starting point
{% if is_incremental() %}
previous_season_stats as (
    select
        team_id,
        season_id, -- Add season_id to group by
        max(season_total_pts) as prev_season_total_pts,
        max(season_wins_to_date) as prev_season_wins,
        max(season_losses_to_date) as prev_season_losses,
        max(season_games_to_date) as prev_season_games
    from {{ this }}
    group by team_id, season_id
),
{% endif %}

-- Calculate current period stats
current_period_stats as (
    select
        team_game_key,
        team_id,
        game_id,
        game_date,
        season_id,
        season_year,
        pts,
        is_win,
        
        -- Precompute the row numbers for efficient ordering within each season
        row_number() over (partition by team_id, season_id order by game_date, game_id) as game_seq_in_season
    from source_data
)

select
    cps.team_game_key,
    cps.team_id,
    cps.game_id,
    cps.game_date,
    cps.season_id,
    cps.season_year,
    cps.pts,
    cps.is_win,
    cps.game_seq_in_season as season_games_to_date,

    -- Season stats - optimized for incremental processing
    {% if is_incremental() %}
        cps.pts + coalesce(pss.prev_season_total_pts, 0) as season_total_pts,
        cps.is_win + coalesce(pss.prev_season_wins, 0) as season_wins_to_date,
        (case when cps.is_win = 0 then 1 else 0 end) + coalesce(pss.prev_season_losses, 0) as season_losses_to_date,
        
        case 
            when cps.game_seq_in_season = 0 then null -- or 0.0 if you prefer
            else (cps.is_win + coalesce(pss.prev_season_wins, 0))::numeric / cps.game_seq_in_season 
        end as season_win_pct_to_date

    {% else %}
        -- Initial full load uses window functions, partitioned by season
        sum(cps.pts) over (partition by cps.team_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_total_pts,
        sum(cps.is_win) over (partition by cps.team_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_wins_to_date,
        sum(case when cps.is_win = 0 then 1 else 0 end) over (partition by cps.team_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row) as season_losses_to_date,
        
        case 
            when cps.game_seq_in_season = 0 then null -- or 0.0 if you prefer
            else (sum(cps.is_win) over (partition by cps.team_id, cps.season_id order by cps.game_date, cps.game_id rows between unbounded preceding and current row))::numeric / cps.game_seq_in_season 
        end as season_win_pct_to_date
        
    {% endif %}
from current_period_stats cps
{% if is_incremental() %}
left join previous_season_stats pss
    on cps.team_id = pss.team_id and cps.season_id = pss.season_id -- Join on team_id and season_id
{% endif %}
order by cps.team_id, cps.game_date, cps.game_id -- Added for deterministic output if needed
