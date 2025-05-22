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
        -- Scoring threshold flags using macro (assuming it handles 30/40)
        {{ calculate_milestone_flags('pts') }},
        -- Add flags for higher scoring thresholds
        case when pts >= 50 then 1 else 0 end as is_50_plus_game,
        case when pts >= 60 then 1 else 0 end as is_60_plus_game,
        case when pts >= 70 then 1 else 0 end as is_70_plus_game,
        case when pts >= 80 then 1 else 0 end as is_80_plus_game
    from source_data
),

{% if is_incremental() %}
-- Get previous milestone counts for incremental processing
previous_milestones as (
    select
        player_id,
        max(pts_thirty_plus_games_career) as prev_30_plus_games,
        max(pts_forty_plus_games_career) as prev_40_plus_games,
        -- Add previous counts for new thresholds
        max(pts_fifty_plus_games_career) as prev_50_plus_games,
        max(pts_sixty_plus_games_career) as prev_60_plus_games,
        max(pts_seventy_plus_games_career) as prev_70_plus_games,
        max(pts_eighty_plus_games_career) as prev_80_plus_games
    from {{ this }}
    group by player_id
),

-- Previous milestone dates need special handling
previous_milestone_dates as (
    select
        player_id,
        max(latest_30_plus_date) as prev_latest_30_plus_date,
        max(latest_40_plus_date) as prev_latest_40_plus_date,
        -- Add previous dates for new thresholds
        max(latest_50_plus_date) as prev_latest_50_plus_date,
        max(latest_60_plus_date) as prev_latest_60_plus_date,
        max(latest_70_plus_date) as prev_latest_70_plus_date,
        max(latest_80_plus_date) as prev_latest_80_plus_date
    from {{ this }}
    group by player_id
),
{% endif %}

-- Season aggregations for previous season totals (lag) and current season totals (window function)
season_aggs_for_lag as (
    select
        player_id,
        season_id,
        sum(is_30_plus_game) as total_30_plus_games_in_season,
        sum(is_40_plus_game) as total_40_plus_games_in_season,
        -- Add sums for new thresholds
        sum(is_50_plus_game) as total_50_plus_games_in_season,
        sum(is_60_plus_game) as total_60_plus_games_in_season,
        sum(is_70_plus_game) as total_70_plus_games_in_season,
        sum(is_80_plus_game) as total_80_plus_games_in_season
    from milestone_flags
    group by player_id, season_id
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
        mf.is_50_plus_game, -- Add new flags
        mf.is_60_plus_game,
        mf.is_70_plus_game,
        mf.is_80_plus_game,
        coalesce(pm.prev_30_plus_games, 0) as base_30_plus_games,
        coalesce(pm.prev_40_plus_games, 0) as base_40_plus_games,
        -- Add base values for new thresholds
        coalesce(pm.prev_50_plus_games, 0) as base_50_plus_games,
        coalesce(pm.prev_60_plus_games, 0) as base_60_plus_games,
        coalesce(pm.prev_70_plus_games, 0) as base_70_plus_games,
        coalesce(pm.prev_80_plus_games, 0) as base_80_plus_games
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
        is_50_plus_game, -- Add new flags
        is_60_plus_game,
        is_70_plus_game,
        is_80_plus_game,
        0 as base_30_plus_games,
        0 as base_40_plus_games,
        -- Add base values for new thresholds
        0 as base_50_plus_games,
        0 as base_60_plus_games,
        0 as base_70_plus_games,
        0 as base_80_plus_games
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
    bv.is_30_plus_game as is_30_plus_pt_game,
    bv.is_40_plus_game as is_40_plus_pt_game,
    bv.is_50_plus_game as is_50_plus_pt_game,
    bv.is_60_plus_game as is_60_plus_pt_game,
    bv.is_70_plus_game as is_70_plus_pt_game,
    bv.is_80_plus_game as is_80_plus_pt_game,
    
    -- Career counts of 30+, 40+, 50+, 60+, 70+, 80+ point games PRIOR to current game
    bv.base_30_plus_games + coalesce(sum(bv.is_30_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_30_plus_pt_games,
    
    bv.base_40_plus_games + coalesce(sum(bv.is_40_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_40_plus_pt_games,

    -- Add career counts for new thresholds
    bv.base_50_plus_games + coalesce(sum(bv.is_50_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_50_plus_pt_games,

    bv.base_60_plus_games + coalesce(sum(bv.is_60_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_60_plus_pt_games,

    bv.base_70_plus_games + coalesce(sum(bv.is_70_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_70_plus_pt_games,

    bv.base_80_plus_games + coalesce(sum(bv.is_80_plus_game) over (
        partition by bv.player_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as career_80_plus_pt_games,
    
    -- Season totals of 30+, 40+, 50+, 60+, 70+, 80+ games PRIOR to current game in THIS season
    coalesce(sum(bv.is_30_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as thirty_plus_pt_games_this_season,

    coalesce(sum(bv.is_40_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as forty_plus_pt_games_this_season,

    -- Add season counts for new thresholds
    coalesce(sum(bv.is_50_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as fifty_plus_pt_games_this_season,

    coalesce(sum(bv.is_60_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as sixty_plus_pt_games_this_season,

    coalesce(sum(bv.is_70_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as seventy_plus_pt_games_this_season,

    coalesce(sum(bv.is_80_plus_game) over (
        partition by bv.player_id, bv.season_id 
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
    ), 0) as eighty_plus_pt_games_this_season,
    
    -- Previous season totals (using lag on full season aggregates)
    lag(sa_lag.total_30_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id -- season_id must be comparable/sortable
    ) as thirty_plus_pt_games_last_season,
    
    lag(sa_lag.total_40_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as forty_plus_pt_games_last_season,

    -- Add previous season counts for new thresholds
    lag(sa_lag.total_50_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as fifty_plus_pt_games_last_season,

    lag(sa_lag.total_60_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as sixty_plus_pt_games_last_season,

    lag(sa_lag.total_70_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as seventy_plus_pt_games_last_season,

    lag(sa_lag.total_80_plus_games_in_season, 1, 0) over (
        partition by bv.player_id order by bv.season_id
    ) as eighty_plus_pt_games_last_season,
    
    -- Latest milestone dates PRIOR to current game
    {% if is_incremental() %}
        greatest(
            max(case when bv.is_30_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_30_plus_date
        ) as latest_30_plus_pt_game_date,
        
        greatest(
            max(case when bv.is_40_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_40_plus_date
        ) as latest_40_plus_pt_game_date,

        -- Add latest dates for new thresholds
        greatest(
            max(case when bv.is_50_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_50_plus_date
        ) as latest_50_plus_pt_game_date,

        greatest(
            max(case when bv.is_60_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_60_plus_date
        ) as latest_60_plus_pt_game_date,

        greatest(
            max(case when bv.is_70_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_70_plus_date
        ) as latest_70_plus_pt_game_date,

        greatest(
            max(case when bv.is_80_plus_game = 1 then bv.game_date end) over (
                partition by bv.player_id 
                order by bv.game_date, bv.game_id
                rows between unbounded preceding and 1 preceding
            ),
            pmd.prev_latest_80_plus_date
        ) as latest_80_plus_pt_game_date
    {% else %}
        max(case when bv.is_30_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_30_plus_pt_game_date,
        
        max(case when bv.is_40_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_40_plus_pt_game_date,

        -- Add latest dates for new thresholds
        max(case when bv.is_50_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_50_plus_pt_game_date,

        max(case when bv.is_60_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_60_plus_pt_game_date,

        max(case when bv.is_70_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_70_plus_pt_game_date,

        max(case when bv.is_80_plus_game = 1 then bv.game_date end) over (
            partition by bv.player_id 
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
        ) as latest_80_plus_pt_game_date
    {% endif %}
from base_values bv
-- Join for previous season's totals
left join season_aggs_for_lag sa_lag
    on bv.player_id = sa_lag.player_id and bv.season_id = sa_lag.season_id
{% if is_incremental() %}
left join previous_milestone_dates pmd
    on bv.player_id = pmd.player_id
{% endif %}
