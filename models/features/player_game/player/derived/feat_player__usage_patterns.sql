{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_game_key'], 'unique': True},
            {'columns': ['player_id', 'game_date', 'game_id']}
        ]
    )
}}

with source_data as (
    select 
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_id,
        season_year,
        pts,
        min,
        fga,
        fta
    from {{ ref('int_player__career_to_date_stats') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }} 
    {% if is_incremental() %}
    -- Add standard incremental filter using AND
    and game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

usage_data as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_id,
        pts,
        min,
        fga,
        fta,
        {{ usage_rate('fga', 'fta', 'min') }} as game_usage_rate
    from source_data
    where min > 0
)

select
    ud.player_game_key,
    ud.player_id,
    ud.player_name,
    ud.game_id,
    ud.game_date,
    ud.season_id,

    -- Average usage in games PRIOR to the current game, based on point thresholds
    avg(case when ud.pts >= 30 then ud.game_usage_rate end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as avg_usage_in_30_plus_career_prior,

    avg(case when ud.pts >= 40 then ud.game_usage_rate end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as avg_usage_in_40_plus_career_prior,

    -- Average usage in games PRIOR to the current game, by scoring range
    avg(case when ud.pts between 0 and 9 then ud.game_usage_rate end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as avg_usage_0_to_9_pts_prior,

    avg(case when ud.pts between 10 and 19 then ud.game_usage_rate end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as avg_usage_10_to_19_pts_prior,

    avg(case when ud.pts between 20 and 29 then ud.game_usage_rate end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as avg_usage_20_to_29_pts_prior,

    -- Count of games PRIOR to the current game in each scoring range
    count(case when ud.pts >= 30 then 1 end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as count_30_plus_games_prior,

    count(case when ud.pts >= 40 then 1 end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as count_40_plus_games_prior,
    
    count(case when ud.pts between 0 and 9 then 1 end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as count_0_to_9_pt_games_prior,

    count(case when ud.pts between 10 and 19 then 1 end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as count_10_to_19_pt_games_prior,

    count(case when ud.pts between 20 and 29 then 1 end) over (
        partition by ud.player_id 
        order by ud.game_date, ud.game_id 
        rows between unbounded preceding and 1 preceding
    ) as count_20_to_29_pt_games_prior,

    -- Total games with usage calculation PRIOR to current game
    count(ud.game_usage_rate) over (
        partition by ud.player_id
        order by ud.game_date, ud.game_id
        rows between unbounded preceding and 1 preceding
    ) as total_games_with_usage_prior

from usage_data ud
-- No GROUP BY needed here as we are using window functions for all aggregations 