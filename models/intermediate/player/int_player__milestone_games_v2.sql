{{ config(
    schema='intermediate',
    materialized='incremental',
    unique_key='player_game_key',
    on_schema_change='sync_all_columns',
    indexes=[
        {'columns': ['player_game_key']},
        {'columns': ['game_id', 'player_id']}
    ]
) }}

/*
  Refactored to loop over all thresholds in one place.
  Add or remove values in the `thresholds` list below.
*/
{% set thresholds = [30, 40, 50, 60, 70, 80] %}
{% set words = {
    30: 'thirty', 40: 'forty', 50: 'fifty',
    60: 'sixty', 70: 'seventy', 80: 'eighty'
} %}

with source_data as (
    select * from {{ ref('int_player__career_to_date_stats_v1') }}
),

milestone_flags as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        game_date,
        season_id,
        season_year,
        pts,
        -- one-shot macro emits is_30_plus_game … is_80_plus_game
        {{ calculate_milestone_flags('pts', thresholds=thresholds) }}
    from source_data
),

{% if is_incremental() %}
previous_milestones as (
    select
        player_id,
        {% for t in thresholds -%}
        max(career_{{ t }}_plus_pt_games) as prev_{{ t }}_plus_games{{ "," if not loop.last }}
        {%- endfor %}
    from {{ this }}
    group by player_id
),

previous_milestone_dates as (
    select
        player_id,
        {% for t in thresholds -%}
        max(latest_{{ t }}_plus_pt_game_date) as prev_latest_{{ t }}_plus_date{{ "," if not loop.last }}
        {%- endfor %}
    from {{ this }}
    group by player_id
),
{% endif %}

season_aggs_for_lag as (
    select
        player_id,
        season_id,
        {% for t in thresholds -%}
        sum(is_{{ t }}_plus_game) as total_{{ t }}_plus_games_in_season{{ "," if not loop.last }}
        {%- endfor %}
    from milestone_flags
    group by player_id, season_id
),

{% if is_incremental() %}
base_values as (
    select
        mf.*,
        {% for t in thresholds -%}
        coalesce(pm.prev_{{ t }}_plus_games, 0) as base_{{ t }}_plus_games{{ "," if not loop.last }}
        {%- endfor %}
    from milestone_flags mf
    left join previous_milestones pm using (player_id)
)
{% else %}
base_values as (
    select
        *,
        {% for t in thresholds -%}
        0 as base_{{ t }}_plus_games{{ "," if not loop.last }}
        {%- endfor %}
    from milestone_flags
)
{% endif %}

select
    bv.player_game_key,
    bv.player_id,
    bv.game_id,
    bv.game_date,
    bv.season_id,
    bv.season_year,
    bv.pts,

    -- flag columns
    {% for t in thresholds -%}
    bv.is_{{ t }}_plus_game as is_{{ t }}_plus_pt_game{{ "," if not loop.last }}
    {%- endfor %},

    -- career counts before this game
    {% for t in thresholds -%}
    (
      bv.base_{{ t }}_plus_games
      + coalesce(
          sum(bv.is_{{ t }}_plus_game) over (
            partition by bv.player_id
            order by bv.game_date, bv.game_id
            rows between unbounded preceding and 1 preceding
          ), 0
        )
    ) as career_{{ t }}_plus_pt_games{{ "," if not loop.last }}
    {%- endfor %},

    -- season counts before this game
    {% for t in thresholds -%}
    coalesce(
      sum(bv.is_{{ t }}_plus_game) over (
        partition by bv.player_id, bv.season_id
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
      ), 0
    ) as {{ words[t] }}_plus_pt_games_this_season{{ "," if not loop.last }}
    {%- endfor %},

    -- last season totals
    {% for t in thresholds -%}
    lag(sa.total_{{ t }}_plus_games_in_season, 1, 0) over (
      partition by bv.player_id order by bv.season_id
    ) as {{ words[t] }}_plus_pt_games_last_season{{ "," if not loop.last }}
    {%- endfor %},

    -- latest milestone dates before this game
    {% if is_incremental() -%}
      {% for t in thresholds -%}
      greatest(
        max(case when bv.is_{{ t }}_plus_game = 1 then bv.game_date end) over (
          partition by bv.player_id
          order by bv.game_date, bv.game_id
          rows between unbounded preceding and 1 preceding
        ),
        pmd.prev_latest_{{ t }}_plus_date
      ) as latest_{{ t }}_plus_pt_game_date{{ "," if not loop.last }}
      {%- endfor %}
    {% else -%}
      {% for t in thresholds -%}
      max(case when bv.is_{{ t }}_plus_game = 1 then bv.game_date end) over (
        partition by bv.player_id
        order by bv.game_date, bv.game_id
        rows between unbounded preceding and 1 preceding
      ) as latest_{{ t }}_plus_pt_game_date{{ "," if not loop.last }}
      {%- endfor %}
    {% endif %}
from base_values bv
left join season_aggs_for_lag sa
  on bv.player_id = sa.player_id and bv.season_id = sa.season_id
{% if is_incremental() %}
left join previous_milestone_dates pmd using (player_id)
{% endif %}