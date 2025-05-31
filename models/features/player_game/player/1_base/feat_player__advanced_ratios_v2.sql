{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'advanced'],
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['player_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

with player_advanced_data as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Advanced Ratings
        off_rating,
        def_rating,
        net_rating,
        est_off_rating,
        est_def_rating,
        est_net_rating,

        -- Usage and Pace
        usage_pct,
        possessions,
        pace,
        est_pace,

        -- Traditional Stats for Extended Ratios
        pts, ast, reb, fgm, fga, fg3m, fg3a, fta, tov, touches, min,

        -- Game sequence for ratio computation
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

advanced_ratio_features as (
    select
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,
        season_game_sequence,
        updated_at,

        -- Advanced Per-100 Possession Ratios
        {% for window in [3, 5, 10] %}
        {{ predictive_per_100_ratio('off_rating', 'possessions', window) }} as off_rating_per_100_l{{ window }},
        {{ predictive_per_100_ratio('def_rating', 'possessions', window) }} as def_rating_per_100_l{{ window }},
        {{ predictive_per_100_ratio('net_rating', 'possessions', window) }} as net_rating_per_100_l{{ window }},
        {{ predictive_per_100_ratio('est_off_rating', 'possessions', window) }} as est_off_rating_per_100_l{{ window }},
        {{ predictive_per_100_ratio('est_def_rating', 'possessions', window) }} as est_def_rating_per_100_l{{ window }},
        {{ predictive_per_100_ratio('est_net_rating', 'possessions', window) }} as est_net_rating_per_100_l{{ window }},
        {% endfor %}

        -- Efficiency Ratios: Points per usage percentage
        {% for window in [3, 5, 10] %}
        {{ predictive_ratio_window('pts', 'usage_pct', window) }} as pts_per_usage_pct_l{{ window }},
        {% endfor %}

        -- Possession Efficiency Metrics
        {% for stat in ['pts', 'ast', 'reb', 'fgm', 'fga', 'fg3m', 'fg3a'] %}
        {% for window in [3, 5, 10] %}
        {{ predictive_per_100_ratio(stat, 'possessions', window) }} as {{ stat }}_per_100_possessions_l{{ window }},
        {% endfor %}
        {% endfor %}

        -- Touch Efficiency Metrics
        {% for window in [3, 5, 10] %}
        {{ predictive_ratio_window('pts', 'touches', window) }} as pts_per_touch_l{{ window }},
        {{ predictive_ratio_window('ast', 'touches', window, 'per_100', 100) }} as ast_per_100_touches_l{{ window }},
        {{ predictive_ratio_window('tov', 'touches', window, 'per_100', 100) }} as tov_per_100_touches_l{{ window }},
        {{ predictive_per_minute_ratio('fga + (0.44 * fta)', 'min', window) }} as tsa_per_min_l{{ window }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from player_advanced_data
)

select *
from advanced_ratio_features
