{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'opponent', 'base', 'structure'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
) }}

with source_schedules as (
    select distinct
        game_id,
        season_year,
        game_date,
        home_team_id,
        away_team_id
    from {{ ref('stg__schedules') }}
    where home_team_id is not null 
      and away_team_id is not null
      and home_team_id != away_team_id
    {% if is_incremental() %}
      and game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

home_team_perspective as (
    select
        game_id,
        season_year,
        game_date,
        home_team_id as team_id,
        away_team_id as opponent_id,
        'HOME' as home_away,
        {{ dbt_utils.generate_surrogate_key(['game_id', 'home_team_id']) }} as team_game_key
    from source_schedules
),

away_team_perspective as (
    select
        game_id,
        season_year,
        game_date,
        away_team_id as team_id,
        home_team_id as opponent_id,
        'AWAY' as home_away,
        {{ dbt_utils.generate_surrogate_key(['game_id', 'away_team_id']) }} as team_game_key
    from source_schedules
),

final as (
    select 
        team_game_key,
        game_id,
        season_year,
        game_date,
        team_id,
        opponent_id,
        home_away,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from home_team_perspective
    
    union all
    
    select 
        team_game_key,
        game_id,
        season_year,
        game_date,
        team_id,
        opponent_id,
        home_away,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from away_team_perspective
)

select * from final