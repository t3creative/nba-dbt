{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'scoring'],
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

with scoring_boxscore as (
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

        -- Shot Selection Profile (Modern NBA critical metrics)
        pct_fga_3pt,            -- 3PT rate (shot selection evolution)
        pct_pts_3pt,            -- 3PT scoring dependency 
        pct_pts_2pt,            -- 2PT scoring dependency (ADDED)
        pct_pts_in_paint,       -- Interior scoring ability
        pct_pts_midrange_2pt,   -- Mid-range dependency (efficiency killer)
        
        -- Scoring Method Diversity 
        pct_pts_fastbreak,      -- Transition scoring
        pct_pts_ft,             -- Free throw generation/dependency
        pct_pts_off_tov,        -- Opportunistic scoring
        
        -- Creation Ability (Key differentiator)
        pct_unassisted_fgm,     -- Self-creation rate (overall)
        pct_unassisted_3pt,     -- Perimeter creation ability
        pct_assisted_fgm,       -- Assisted shots rate (ADDED)

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

        -- Generate scoring profile rolling features (9 features × 11 stats = 99 features)
        {{ generate_stat_features('pct_fga_3pt') }},
        {{ generate_stat_features('pct_pts_3pt') }},
        {{ generate_stat_features('pct_pts_2pt') }},
        {{ generate_stat_features('pct_pts_in_paint') }},
        {{ generate_stat_features('pct_pts_midrange_2pt') }},
        {{ generate_stat_features('pct_pts_fastbreak') }},
        {{ generate_stat_features('pct_pts_ft') }},
        {{ generate_stat_features('pct_pts_off_tov') }},
        {{ generate_stat_features('pct_unassisted_fgm') }},
        {{ generate_stat_features('pct_unassisted_3pt') }},
        {{ generate_stat_features('pct_assisted_fgm') }}

    from scoring_boxscore
)

select *
from progressive_rolling_features