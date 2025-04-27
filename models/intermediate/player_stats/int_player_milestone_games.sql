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

with source_data as (
    select * from {{ ref('int_player_career_stats') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}
    {% if is_incremental() %}
    -- Add standard incremental filter using AND
    and game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

milestone_flags as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_id,
        pts,
        -- Scoring threshold flags using macro
        {{ calculate_milestone_flags('pts') }}
    from source_data
),

{% if is_incremental() %}
-- Get previous milestone counts for incremental processing
previous_milestones as (
    select
        player_id,
        max(thirty_plus_games_career) as prev_thirty_plus_games,
        max(forty_plus_games_career) as prev_forty_plus_games
    from {{ this }}
    group by player_id
),

-- Previous milestone dates need special handling
previous_milestone_dates as (
    select
        player_id,
        max(latest_30_plus_date) as prev_latest_30_plus_date,
        max(latest_40_plus_date) as prev_latest_40_plus_date
    from {{ this }}
    group by player_id
),
{% endif %}

-- Season aggregations
season_aggs as (
    select
        player_id,
        season_id,
        sum(is_30_plus_game) as thirty_plus_games_season,
        sum(is_40_plus_game) as forty_plus_games_season
    from milestone_flags
    group by player_id, season_id
),

-- Latest milestone dates for current batch only
current_milestone_dates as (
    select
        player_id,
        max(case when is_30_plus_game = 1 then game_date end) as current_latest_30_plus_date,
        max(case when is_40_plus_game = 1 then game_date end) as current_latest_40_plus_date
    from milestone_flags
    group by player_id
),

-- Pre-calculate the base values for incremental processing
{% if is_incremental() %}
base_values as (
    select
        mf.player_game_key,
        mf.player_id,
        mf.game_id,
        mf.game_date,
        mf.season_id,
        mf.pts,
        mf.is_30_plus_game,
        mf.is_40_plus_game,
        coalesce(pm.prev_thirty_plus_games, 0) as base_thirty_plus_games,
        coalesce(pm.prev_forty_plus_games, 0) as base_forty_plus_games
    from milestone_flags mf
    left join previous_milestones pm
        on mf.player_id = pm.player_id
)
{% else %}
base_values as (
    select
        player_game_key,
        player_id,
        game_id,
        game_date,
        season_id,
        pts,
        is_30_plus_game,
        is_40_plus_game,
        0 as base_thirty_plus_games,
        0 as base_forty_plus_games
    from milestone_flags
)
{% endif %}

select
    bv.player_game_key,
    bv.player_id,
    bv.game_id,
    bv.game_date,
    bv.season_id,
    bv.pts,
    bv.is_30_plus_game,
    bv.is_40_plus_game,
    
    -- Career counts of previous 30+ and 40+ point games (not including current game)
    bv.base_thirty_plus_games + coalesce(sum(bv.is_30_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as thirty_plus_games_career,
    
    bv.base_forty_plus_games + coalesce(sum(bv.is_40_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as forty_plus_games_career,
    
    -- Season totals
    sa.thirty_plus_games_season,
    sa.forty_plus_games_season,
    
    -- Previous season totals (using lag)
    lag(sa.thirty_plus_games_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as thirty_plus_games_prev_season,
    
    lag(sa.forty_plus_games_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as forty_plus_games_prev_season,
    
    -- Latest milestone dates - optimized for incremental processing
    {% if is_incremental() %}
        -- Use the most recent date between previous stored date and current batch
        case 
            when cmd.current_latest_30_plus_date is not null and pmd.prev_latest_30_plus_date is not null 
            then greatest(cmd.current_latest_30_plus_date, pmd.prev_latest_30_plus_date)
            else coalesce(cmd.current_latest_30_plus_date, pmd.prev_latest_30_plus_date)
        end as latest_30_plus_date,
        
        case 
            when cmd.current_latest_40_plus_date is not null and pmd.prev_latest_40_plus_date is not null 
            then greatest(cmd.current_latest_40_plus_date, pmd.prev_latest_40_plus_date)
            else coalesce(cmd.current_latest_40_plus_date, pmd.prev_latest_40_plus_date)
        end as latest_40_plus_date
    {% else %}
        cmd.current_latest_30_plus_date as latest_30_plus_date,
        cmd.current_latest_40_plus_date as latest_40_plus_date
    {% endif %}
from base_values bv
left join season_aggs sa 
    on bv.player_id = sa.player_id and bv.season_id = sa.season_id
left join current_milestone_dates cmd
    on bv.player_id = cmd.player_id
{% if is_incremental() %}
left join previous_milestone_dates pmd
    on bv.player_id = pmd.player_id
{% endif %}
