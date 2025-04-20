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
    select *
    from {{ ref('int_player_career_stats') }}
    {{ incremental_date_filter('game_date') }}
),

{% if is_incremental() %}
-- Get previous usage stats for incremental processing
previous_usage_stats as (
    select
        player_id,
        max(avg_usage_in_30_plus_career) as prev_avg_usage_in_30_plus_career,
        max(avg_usage_in_40_plus_career) as prev_avg_usage_in_40_plus_career,
        max(count_30_plus_games) as prev_count_30_plus_games,
        max(count_40_plus_games) as prev_count_40_plus_games,
        max(total_games_with_usage) as prev_total_games_with_usage
    from {{ this }}
    group by player_id
),
{% endif %}

-- Prepare data for usage calculation
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

    -- Usage stats for high-scoring games using macro
    avg(case when ud.pts >= 30 then ud.game_usage_rate end) as avg_usage_in_30_plus_career,
    avg(case when ud.pts >= 40 then ud.game_usage_rate end) as avg_usage_in_40_plus_career,

    -- Usage stats by scoring range using macro
    avg(case when ud.pts between 0 and 9 then ud.game_usage_rate end) as avg_usage_0_to_9_pts,
    avg(case when ud.pts between 10 and 19 then ud.game_usage_rate end) as avg_usage_10_to_19_pts,
    avg(case when ud.pts between 20 and 29 then ud.game_usage_rate end) as avg_usage_20_to_29_pts,

    -- Count of games in each scoring range
    count(case when ud.pts >= 30 then 1 end) as count_30_plus_games,
    count(case when ud.pts >= 40 then 1 end) as count_40_plus_games,
    count(case when ud.pts between 0 and 9 then 1 end) as count_0_to_9_pt_games,
    count(case when ud.pts between 10 and 19 then 1 end) as count_10_to_19_pt_games,
    count(case when ud.pts between 20 and 29 then 1 end) as count_20_to_29_pt_games,

    -- Total games with usage calculation
    count(*) as total_games_with_usage
from usage_data ud
group by 
    ud.player_game_key, 
    ud.player_id,
    ud.player_name, 
    ud.game_id, 
    ud.game_date,
    ud.season_id 