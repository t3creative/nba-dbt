{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'defensive'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with defensive_boxscore as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_name,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Core Defensive Metrics (Worth rolling calculations)
        pts_allowed,            -- Points allowed per matchup
        matchup_fg_pct,         -- Opponent FG% when guarded
        matchup_fg3_pct,        -- Opponent 3P% when guarded  
        tov_forced,             -- Turnovers forced (active defense)
        
        -- Defensive Activity/Engagement
        partial_poss,           -- Defensive possessions (workload)
        def_switches,           -- Switching frequency (scheme involvement)

        -- Season-specific game sequence for progressive rolling logic
        ROW_NUMBER() OVER (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
        ) as season_game_sequence,

        updated_at

    from {{ ref('int_player__combined_boxscore') }}
),

progressive_rolling_features as (
    select 
        -- Keep all original columns
        player_game_key,
        game_id,
        player_name,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        -- Generate defensive rolling features (9 features × 6 stats = 54 features)
        {{ generate_stat_features('pts_allowed') }},
        {{ generate_stat_features('matchup_fg_pct') }},
        {{ generate_stat_features('matchup_fg3_pct') }},
        {{ generate_stat_features('tov_forced') }},
        {{ generate_stat_features('partial_poss') }},
        {{ generate_stat_features('def_switches') }}

    from defensive_boxscore
)

select *
from progressive_rolling_features