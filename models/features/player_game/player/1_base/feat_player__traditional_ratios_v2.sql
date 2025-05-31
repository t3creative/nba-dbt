{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'traditional'],
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

with player_boxscore_data as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        player_name,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Raw stats for progressive ratio calculations
        min,
        pts, reb, ast, stl, blk, tov, pf,
        fgm, fga, fg3m, fg3a, ftm, fta,
        off_reb, def_reb,
        possessions, -- per-100 possession context
        
        -- Season-specific game sequence for progressive ratio logic
        ROW_NUMBER() OVER (
            PARTITION BY player_id, season_year
            ORDER BY game_date, game_id
        ) as season_game_sequence,

        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

progressive_ratio_features as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        player_name,
        team_id,
        opponent_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        -- Core Production Efficiency (Per-36 focus for NBA context)
        {{ generate_ratio_features('pts', 'min', [3, 5, 10], ['per_36']) }},
        {{ generate_ratio_features('reb', 'min', [3, 5, 10], ['per_36']) }},
        {{ generate_ratio_features('ast', 'min', [3, 5, 10], ['per_36']) }},
        
        -- Defensive Activity Rates
        {{ generate_ratio_features('stl', 'min', [3, 5, 10], ['per_36']) }},
        {{ generate_ratio_features('blk', 'min', [3, 5, 10], ['per_36']) }},
        
        -- Negative Impact Metrics
        {{ generate_ratio_features('tov', 'min', [3, 5, 10], ['per_36']) }},
        {{ generate_ratio_features('pf', 'min', [3, 5, 10], ['per_36']) }},

        -- Shooting Efficiency Evolution (Critical for NBA prediction)
        {{ generate_efficiency_features('fg', [3, 5, 10]) }},
        {{ generate_efficiency_features('fg3', [3, 5, 10]) }},
        {{ generate_efficiency_features('ft', [3, 5, 10]) }},

        -- Advanced Efficiency Ratios
        {% for window in [3, 5, 10] %}
        {{ predictive_ast_to_tov_ratio('ast', 'tov', window) }} as ast_to_tov_l{{ window }},
        {% endfor %}
        
        -- Per-100 Possession Context (Modern NBA Analytics)
        {% for window in [3, 5, 10] %}
        {{ predictive_per_100_ratio('pts', 'possessions', window) }} as pts_per_100_l{{ window }},
        {{ predictive_per_100_ratio('ast', 'possessions', window) }} as ast_per_100_l{{ window }},
        {{ predictive_per_100_ratio('tov', 'possessions', window) }} as tov_per_100_l{{ window }},
        {% endfor %}

        -- Rebounding Efficiency Splits
        {% for window in [3, 5, 10] %}
        {{ predictive_per_36_ratio('off_reb', 'min', window) }} as off_reb_per_36_l{{ window }},
        {{ predictive_per_36_ratio('def_reb', 'min', window) }} as def_reb_per_36_l{{ window }},
        {% endfor %}

        -- Shot Volume Trends (Usage indicators)
        {% for window in [3, 5, 10] %}
        {{ predictive_per_36_ratio('fga', 'min', window) }} as fga_per_36_l{{ window }},
        {{ predictive_per_36_ratio('fg3a', 'min', window) }} as fg3a_per_36_l{{ window }},
        {{ predictive_per_36_ratio('fta', 'min', window) }} as fta_per_36_l{{ window }},
        {% endfor %}

        -- Comprehensive PRA (Points + Rebounds + Assists) Efficiency
        {% for window in [3, 5, 10] %}
        {{ predictive_per_36_ratio('pts + reb + ast', 'min', window) }} as pra_per_36_l{{ window }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from player_boxscore_data
)

select *
from progressive_ratio_features