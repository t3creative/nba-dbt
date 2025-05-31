{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'scoring'],
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

with player_scoring_data as (
    select
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        min,
        pct_fga_2pt, pct_fga_3pt,
        pct_pts_2pt, pct_pts_midrange_2pt, pct_pts_3pt,
        pct_pts_fastbreak, pct_pts_ft, pct_pts_off_tov, pct_pts_in_paint,
        pct_assisted_2pt, pct_unassisted_2pt,
        pct_assisted_3pt, pct_unassisted_3pt,
        pct_assisted_fgm, pct_unassisted_fgm,
        touches, fga,

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

scoring_ratio_features as (
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

        -- Efficiency by Shot Type
        {% for window in [3, 5, 10] %}
        ({{ predictive_per_minute_ratio('pct_pts_3pt', 'pct_fga_3pt', window) }}) as pct_pts_to_fga_3pt_l{{ window }},
        {% endfor %}

        -- Creation Rate
        {% for window in [3, 5, 10] %}
        {{ predictive_ratio_window('pct_unassisted_fgm', 'touches', window) }} as pct_unassisted_fgm_per_touch_l{{ window }},
        {% endfor %}

        -- Shot Quality
        {% for window in [3, 5, 10] %}
        {{ predictive_ratio_window('pct_pts_in_paint', 'fga', window) }} as pct_pts_in_paint_per_fga_l{{ window }},
        {% endfor %}

        -- Distribution Efficiency
        {% for col in ['pct_pts_fastbreak', 'pct_pts_off_tov', 'pct_pts_ft'] %}
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio(col, 'min', window) }} as {{ col }}_per_min_l{{ window }},
        {% endfor %}
        {% endfor %}

        -- Assisted Scoring Profile
        {% for col in ['pct_assisted_2pt', 'pct_unassisted_2pt', 'pct_assisted_3pt', 'pct_unassisted_3pt'] %}
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio(col, 'min', window) }} as {{ col }}_per_min_l{{ window }},
        {% endfor %}
        {% endfor %}

        -- Shot Type Balance
        {% for window in [3, 5, 10] %}
        ({{ predictive_per_minute_ratio('pct_fga_3pt', 'min', window) }} - {{ predictive_per_minute_ratio('pct_fga_2pt', 'min', window) }})
        as pct_fga_3v2_diff_per_min_l{{ window }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from player_scoring_data
)

select *
from scoring_ratio_features
