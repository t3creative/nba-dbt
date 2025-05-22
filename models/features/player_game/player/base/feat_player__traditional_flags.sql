{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'flags', 'traditional'],
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
        home_away,

        -- Traditional Boxscore Stats for Flags
        min,
        pts,
        reb,
        ast,
        stl,
        blk,
        tov,
        pf,
        fg3m,
        fga,
        fg_pct,
        fta,
        ft_pct,
        
        -- Advanced Stat for Efficiency Flags
        ts_pct, -- True Shooting Percentage

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

final as (
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

        -- Basic Participation & Context Flags
        case when min > 0 then TRUE else FALSE end as played_game,
        case when home_away = 'Home' then TRUE else FALSE end as is_home_game,

        -- Statistical Milestone Flags
        case
            when (
                (case when pts >= 10 then 1 else 0 end) +
                (case when reb >= 10 then 1 else 0 end) +
                (case when ast >= 10 then 1 else 0 end) +
                (case when stl >= 10 then 1 else 0 end) + -- Considered for double/triple double
                (case when blk >= 10 then 1 else 0 end)  -- Considered for double/triple double
            ) >= 2 then TRUE
            else FALSE
        end as is_double_double,
        case
            when (
                (case when pts >= 10 then 1 else 0 end) +
                (case when reb >= 10 then 1 else 0 end) +
                (case when ast >= 10 then 1 else 0 end) +
                (case when stl >= 10 then 1 else 0 end) +
                (case when blk >= 10 then 1 else 0 end)
            ) >= 3 then TRUE
            else FALSE
        end as is_triple_double,
        case when pts >= 20 then TRUE else FALSE end as pts_ge_20,
        case when pts >= 30 then TRUE else FALSE end as pts_ge_30,
        case when pts >= 40 then TRUE else FALSE end as pts_ge_40,
        case when pts >= 50 then TRUE else FALSE end as pts_ge_50,
        case when pts >= 60 then TRUE else FALSE end as pts_ge_60,
        case when pts >= 70 then TRUE else FALSE end as pts_ge_70,
        case when reb >= 10 then TRUE else FALSE end as reb_ge_10,
        case when reb >= 15 then TRUE else FALSE end as reb_ge_15,
        case when reb >= 20 then TRUE else FALSE end as reb_ge_20,
        case when reb >= 25 then TRUE else FALSE end as reb_ge_25,
        case when ast >= 10 then TRUE else FALSE end as ast_ge_10,
        case when ast >= 15 then TRUE else FALSE end as ast_ge_15,
        case when ast >= 20 then TRUE else FALSE end as ast_ge_20,
        case when stl >= 3 then TRUE else FALSE end as stl_ge_3,
        case when stl >= 5 then TRUE else FALSE end as stl_ge_5,
        case when blk >= 3 then TRUE else FALSE end as blk_ge_3,
        case when blk >= 5 then TRUE else FALSE end as blk_ge_5,
        case when fg3m >= 5 then TRUE else FALSE end as fg3m_ge_5,
        case when fg3m >= 8 then TRUE else FALSE end as fg3m_ge_8,
        case when fg3m >= 10 then TRUE else FALSE end as fg3m_ge_10,

        -- Performance & Efficiency Indicator Flags
        case when pf >= 6 then TRUE else FALSE end as fouled_out,
        case when tov >= 5 then TRUE else FALSE end as high_turnovers,
        case when pts >= 20 and ts_pct >= 0.600 then TRUE else FALSE end as efficient_scoring_night,
        case when fga >= 15 and fg_pct < 0.400 then TRUE else FALSE end as inefficient_volume_scorer,
        case when fta >= 4 and ft_pct = 1.0 then TRUE else FALSE end as perfect_ft_shooting_min_attempts,
        
        updated_at
    from player_boxscore_data
)

select *
from final
