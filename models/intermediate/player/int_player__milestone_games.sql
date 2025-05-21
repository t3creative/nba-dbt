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
    select * from {{ ref('int_player__career_to_date_stats') }}
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
        season_year,
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

-- Season aggregations for previous season totals - REMAINS THE SAME for `_prev_season` lag
season_aggs_for_lag as (
    select
        player_id,
        season_id,
        sum(is_30_plus_game) as total_30_plus_games_in_season, -- Renamed for clarity
        sum(is_40_plus_game) as total_40_plus_games_in_season  -- Renamed for clarity
    from milestone_flags
    group by player_id, season_id
),

-- Latest milestone dates from PREVIOUS games within the current batch (for incremental part)
-- This CTE is no longer the primary source for latest_date in the final select for current game,
-- but helps in correctly determining the running max when combining with {{this}}
current_batch_prior_milestone_dates as (
    select
        player_id,
        game_date,
        game_id,
        max(case when is_30_plus_game = 1 then game_date end) over (
            partition by player_id 
            order by game_date, game_id 
            rows between unbounded preceding and 1 preceding
        ) as batch_latest_30_plus_date_prior,
        max(case when is_40_plus_game = 1 then game_date end) over (
            partition by player_id 
            order by game_date, game_id 
            rows between unbounded preceding and 1 preceding
        ) as batch_latest_40_plus_date_prior
    from milestone_flags
),

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
        mf.season_year,
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
        season_year,
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
    bv.season_year,
    bv.pts,
    bv.is_30_plus_game,
    bv.is_40_plus_game,
    
    -- Career counts of 30+ and 40+ point games PRIOR to current game
    bv.base_thirty_plus_games + coalesce(sum(bv.is_30_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as thirty_plus_games_career_prior,
    
    bv.base_forty_plus_games + coalesce(sum(bv.is_40_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as forty_plus_games_career_prior,
    
    -- Season totals of 30+ and 40+ games PRIOR to current game in THIS season
    coalesce(sum(bv.is_30_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as thirty_plus_games_this_season_prior,

    coalesce(sum(bv.is_40_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as forty_plus_games_this_season_prior,
    
    -- Previous season totals (using lag on full season aggregates)
    lag(sa_lag.total_30_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id -- season_id must be comparable/sortable
    ) as thirty_plus_games_prev_season,
    
    lag(sa_lag.total_40_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as forty_plus_games_prev_season,
    
    -- Latest milestone dates PRIOR to current game
    {% if is_incremental() %}
        greatest(
            max(case when bv.is_30_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_30_plus_date
        ) as latest_30_plus_date_prior,
        
        greatest(
            max(case when bv.is_40_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_40_plus_date
        ) as latest_40_plus_date_prior
    {% else %}
        max(case when bv.is_30_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_30_plus_date_prior,
        
        max(case when bv.is_40_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_40_plus_date_prior
    {% endif %}
from base_values bv
-- Join for previous season's totals
left join season_aggs_for_lag sa_lag
    on bv.player_id = sa_lag.player_id and bv.season_id = sa_lag.season_id
{% if is_incremental() %}
left join previous_milestone_dates pmd
    on bv.player_id = pmd.player_id
{% endif %}
