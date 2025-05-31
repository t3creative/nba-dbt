{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'tracking'],
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

with tracking_boxscore as (
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

        -- Physical Activity & Effort Metrics
        distance,               -- Total distance traveled (effort/conditioning)
        speed,                  -- Average speed (athleticism/pace)
        touches,                -- Ball handling responsibility
        passes,                 -- Passing volume (playmaking workload)
        
        -- Shot Quality & Pressure
        cont_fgm,
        cont_fga,
        cont_fg_pct,            -- Contested shot efficiency (skill under pressure)
        uncont_fgm,
        uncont_fga,
        uncont_fg_pct,          -- Open shot efficiency (baseline skill)
        
        -- Specialized Defensive Impact
        def_at_rim_fgm,
        def_at_rim_fga,
        def_at_rim_fg_pct,      -- Rim protection effectiveness
        
        -- Rebounding Hustle & Positioning
        def_reb_chances,        -- Defensive rebounding opportunities (positioning)
        
        -- Advanced Playmaking
        secondary_ast,          -- Hockey assists (floor vision)

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

        -- Generate tracking rolling features (9 features × 9 stats = 81 features)
        {{ generate_stat_features('distance') }},
        {{ generate_stat_features('speed') }},
        {{ generate_stat_features('touches') }},
        {{ generate_stat_features('passes') }},
        {{ generate_stat_features('cont_fgm') }},
        {{ generate_stat_features('cont_fga') }},
        {{ generate_stat_features('cont_fg_pct') }},
        {{ generate_stat_features('uncont_fgm') }},
        {{ generate_stat_features('uncont_fga') }},
        {{ generate_stat_features('uncont_fg_pct') }},
        {{ generate_stat_features('def_at_rim_fgm') }},
        {{ generate_stat_features('def_at_rim_fga') }},
        {{ generate_stat_features('def_at_rim_fg_pct') }},
        {{ generate_stat_features('def_reb_chances') }},
        {{ generate_stat_features('secondary_ast') }}

    from tracking_boxscore
)

select *
from progressive_rolling_features