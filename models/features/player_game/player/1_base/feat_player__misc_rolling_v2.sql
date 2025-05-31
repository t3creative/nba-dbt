{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'misc'],
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

with misc_boxscore as (
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

        -- Hustle & Aggression Metrics (Individual skill-based)
        fouls_drawn,            -- Contact generation/aggression
        second_chance_pts,      -- Offensive rebounding follow-through
        
        -- Opponent Impact While On Court (Defensive value)
        opp_pts_off_tov_while_on,    -- Transition defense
        opp_pts_in_paint_while_on,   -- Interior defense impact

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
        fouls_drawn,
        second_chance_pts,
        opp_pts_off_tov_while_on,
        opp_pts_in_paint_while_on,
        season_game_sequence,
        updated_at,

        -- Generate misc rolling features (9 features × 4 stats = 36 features)
        {{ generate_stat_features('fouls_drawn') }},
        {{ generate_stat_features('second_chance_pts') }},
        {{ generate_stat_features('opp_pts_off_tov_while_on') }},
        {{ generate_stat_features('opp_pts_in_paint_while_on') }}

    from misc_boxscore
)

select *
from progressive_rolling_features