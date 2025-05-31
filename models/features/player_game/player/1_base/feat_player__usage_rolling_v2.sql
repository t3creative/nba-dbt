{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'usage'],
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

with usage_boxscore as (
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

        -- Core Usage Stats (High predictive value for rolling)
        pct_of_team_pts,        -- Scoring responsibility
        pct_of_team_fga,        -- Shot volume share
        pct_of_team_fgm,        -- Shot make share
        pct_of_team_ast,        -- Playmaking responsibility
        pct_of_team_reb,        -- Total rebounding share
        pct_of_team_tov,        -- Turnover burden
        
        -- Specialized Usage (Position-specific insights)
        pct_of_team_fg3a,       -- 3PT volume responsibility
        pct_of_team_fta,        -- Free throw generation
        pct_of_team_stl,        -- Defensive activity share

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

        -- Generate usage rolling features (9 features × 9 stats = 81 features)
        {{ generate_stat_features('pct_of_team_pts') }},
        {{ generate_stat_features('pct_of_team_fga') }},
        {{ generate_stat_features('pct_of_team_fgm') }},
        {{ generate_stat_features('pct_of_team_ast') }},
        {{ generate_stat_features('pct_of_team_reb') }},
        {{ generate_stat_features('pct_of_team_tov') }},
        {{ generate_stat_features('pct_of_team_fg3a') }},
        {{ generate_stat_features('pct_of_team_fta') }},
        {{ generate_stat_features('pct_of_team_stl') }}

    from usage_boxscore
)

select *
from progressive_rolling_features