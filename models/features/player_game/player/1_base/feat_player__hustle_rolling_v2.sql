{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'hustle'],
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

with hustle_boxscore as (
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

        -- Elite Hustle Metrics (Pure winning plays)
        deflections,            -- Active hands, defensive disruption
        charges_drawn,          -- Sacrifice body, taking contact
        screen_ast,             -- Selfless offense, setting up teammates
        tot_loose_balls_rec,    -- 50/50 balls, hustle plays
        
        -- Boxing Out Excellence (Positioning and effort)
        def_box_outs,           -- Defensive rebounding fundamentals
        box_out_team_reb,       -- Team-first rebounding (assists team rebound)

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

        -- Generate hustle rolling features (9 features × 6 stats = 54 features)
        {{ generate_stat_features('deflections') }},
        {{ generate_stat_features('charges_drawn') }},
        {{ generate_stat_features('screen_ast') }},
        {{ generate_stat_features('tot_loose_balls_rec') }},
        {{ generate_stat_features('def_box_outs') }},
        {{ generate_stat_features('box_out_team_reb') }}

    from hustle_boxscore
)

select *
from progressive_rolling_features