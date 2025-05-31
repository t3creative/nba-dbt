{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'usage'],
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

with player_usage_data as (
    select
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Team share stats
        min,
        pct_of_team_pts, pct_of_team_ast, pct_of_team_tov,
        pct_of_team_fgm, pct_of_team_fga, pct_of_team_fg3m, pct_of_team_fg3a,
        pct_of_team_ftm, pct_of_team_fta,
        pct_of_team_oreb, pct_of_team_dreb, pct_of_team_reb,
        pct_of_team_stl, pct_of_team_blk, pct_of_team_blk_allowed,
        pct_of_team_pf, pct_of_team_pfd,

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

usage_ratio_features as (
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

        -- Team Share Efficiency
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio('pct_of_team_pts', 'min', window) }} as pct_of_team_pts_per_min_l{{ window }},
        {% endfor %}

        -- Responsibility Load Composite per Minute
        {% for window in [3, 5, 10] %}
        (
            {{ predictive_per_minute_ratio('pct_of_team_pts + pct_of_team_ast + pct_of_team_reb + pct_of_team_fga', 'min', window) }}
        ) as team_load_index_per_min_l{{ window }},
        {% endfor %}

        -- Broad Team Share Metrics
        {% for col in [
            'pct_of_team_fgm', 'pct_of_team_fga', 'pct_of_team_fg3m', 'pct_of_team_fg3a',
            'pct_of_team_ftm', 'pct_of_team_fta',
            'pct_of_team_oreb', 'pct_of_team_dreb', 'pct_of_team_reb',
            'pct_of_team_ast', 'pct_of_team_tov',
            'pct_of_team_stl', 'pct_of_team_blk', 'pct_of_team_pf', 'pct_of_team_pfd',
            'pct_of_team_blk_allowed'
        ] %}
        {% for window in [3, 5, 10] %}
        {{ predictive_per_minute_ratio(col, 'min', window) }} as {{ col }}_per_min_l{{ window }},
        {% endfor %}
        {% endfor %}

        -- Trend: pct_of_team_ast vs pct_of_team_pts
        {% for window in [3, 5, 10] %}
        (
            {{ predictive_per_minute_ratio('pct_of_team_ast', 'min', window) }} -
            {{ predictive_per_minute_ratio('pct_of_team_pts', 'min', window) }}
        ) as role_delta_ast_vs_pts_l{{ window }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from player_usage_data
)

select *
from usage_ratio_features
