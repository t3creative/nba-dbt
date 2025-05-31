{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'hustle', 'tracking'],
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

with player_activity_data as (
    select
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        min,
        possessions,
        deflections, tot_loose_balls_rec,
        distance, speed, touches,
        cont_fg_pct, uncont_fg_pct,
        cont_shots,
        screen_ast_pts,
        secondary_ast, ft_ast,
        def_at_rim_fg_pct,

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

hustle_tracking_features as (
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

        -- Hustle Efficiency
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('deflections', 'min', window) }} as deflections_per_min_l{{ window }},
        {{ predictive_per_100_ratio('tot_loose_balls_rec', 'possessions', window) }} as loose_balls_per_100_possessions_l{{ window }},
        {% endfor %}

        -- Activity Rate
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('distance', 'min', window) }} as distance_per_min_l{{ window }},
        {{ predictive_per_100_ratio('touches', 'possessions', window) }} as touches_per_100_possessions_l{{ window }},
        {% endfor %}

        -- Pressure Performance Trends
        {% for window in [3, 5, 10] %}
        ({{ predictive_per_minute_ratio('cont_fg_pct', 'min', window) }} - {{ predictive_per_minute_ratio('uncont_fg_pct', 'min', window) }})
        as fg_pressure_diff_l{{ window }},
        {% endfor %}

        -- Additional: Rim Protection
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('def_at_rim_fg_pct', 'min', window) }} as def_at_rim_fg_pct_per_min_l{{ window }},
        {% endfor %}

        -- Additional: Passing Value
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('secondary_ast + ft_ast', 'min', window) }} as value_passes_per_min_l{{ window }},
        {% endfor %}

        -- Additional: Defensive Engagement
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('cont_shots', 'min', window) }} as cont_shots_per_min_l{{ window }},
        {{ predictive_per_minute_ratio('screen_ast_pts', 'min', window) }} as screen_ast_pts_per_min_l{{ window }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from player_activity_data
)

select *
from hustle_tracking_features
