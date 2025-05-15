{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    tags=['intermediate', 'core', 'boxscore'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['opponent_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with traditional as (
    select *
    from {{ ref('int__team_bxsc_traditional') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

advanced as (
    select *
    from {{ ref('stg__team_advanced_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

hustle as (
    select *
    from {{ ref('stg__team_hustle_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

misc as (
    select *
    from {{ ref('stg__team_misc_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

scoring as (
    select *
    from {{ ref('stg__team_scoring_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

final as (
    select
        -- Primary Key
        t.team_game_key,

        -- Identifiers & Metadata
        t.game_id,
        t.team_id,
        t.opponent_id,
        t.game_date,
        t.season_year,
        t.home_away,
        t.team_city,
        t.team_name,
        t.team_tricode,

        -- Traditional Boxscore Stats
        t.min,
        t.fgm,
        t.fga,
        t.fg_pct,
        t.fg3m,
        t.fg3a,
        t.fg3_pct,
        t.ftm,
        t.fta,
        t.ft_pct,
        t.off_reb,
        t.def_reb,
        t.reb,
        t.ast,
        t.stl,
        t.blk,
        t.tov,
        t.pf,
        t.pts,
        t.plus_minus,

        -- Advanced Boxscore Stats
        a.est_off_rating,
        a.off_rating,
        a.est_def_rating,
        a.def_rating,
        a.est_net_rating,
        a.net_rating,
        a.ast_pct,
        a.ast_to_tov_ratio,
        a.ast_ratio,
        a.off_reb_pct,
        a.def_reb_pct,
        a.reb_pct,
        a.est_team_tov_pct,
        a.tov_ratio,
        a.eff_fg_pct,
        a.ts_pct,
        a.usage_pct,
        a.est_usage_pct,
        a.est_pace,
        a.pace,
        a.pace_per_40,
        a.possessions,
        a.pie,

        -- Hustle Boxscore Stats
        h.cont_shots,
        h.cont_2pt,
        h.cont_3pt,
        h.deflections,
        h.charges_drawn,
        h.screen_ast,
        h.screen_ast_pts,
        h.off_loose_balls_rec,
        h.def_loose_balls_rec,
        h.tot_loose_balls_rec,
        h.off_box_outs,
        h.def_box_outs,
        h.box_out_team_reb,
        h.box_out_player_reb,
        h.tot_box_outs,

        -- Misc Boxscore Stats
        m.pts_off_tov,
        m.second_chance_pts,
        m.fastbreak_pts,
        m.pts_in_paint,
        m.opp_pts_off_tov,
        m.opp_second_chance_pts,
        m.opp_fastbreak_pts,
        m.opp_pts_in_paint,

        -- Scoring Boxscore Stats
        s.pct_fga_2pt,
        s.pct_fga_3pt,
        s.pct_pts_2pt,
        s.pct_pts_midrange_2pt,
        s.pct_pts_3pt,
        s.pct_pts_fastbreak,
        s.pct_pts_ft,
        s.pct_pts_off_tov,
        s.pct_pts_in_paint,
        s.pct_assisted_2pt,
        s.pct_unassisted_2pt,
        s.pct_assisted_3pt,
        s.pct_unassisted_3pt,
        s.pct_assisted_fgm,
        s.pct_unassisted_fgm,

        -- Per-Minute Normalized Stats
        case when t.min > 0 then t.pts / t.min else 0 end as pts_per_min,
        case when t.min > 0 then t.reb / t.min else 0 end as reb_per_min,
        case when t.min > 0 then t.ast / t.min else 0 end as ast_per_min,
        case when t.min > 0 then t.stl / t.min else 0 end as stl_per_min,
        case when t.min > 0 then t.blk / t.min else 0 end as blk_per_min,
        case when t.min > 0 then t.tov / t.min else 0 end as tov_per_min,
        case when t.min > 0 then (t.pts + t.reb + t.ast) / t.min else 0 end as pra_per_min,
        case when t.min > 0 then t.fgm / t.min else 0 end as fgm_per_min,
        case when t.min > 0 then t.fga / t.min else 0 end as fga_per_min,
        case when t.min > 0 then t.fg3m / t.min else 0 end as fg3m_per_min,
        case when t.min > 0 then t.fg3a / t.min else 0 end as fg3a_per_min,
        
        -- Per-100 Possessions Normalized Stats
        case when a.possessions > 0 then t.pts / a.possessions * 100 else 0 end as pts_per_100,
        case when a.possessions > 0 then t.reb / a.possessions * 100 else 0 end as reb_per_100,
        case when a.possessions > 0 then t.ast / a.possessions * 100 else 0 end as ast_per_100,
        case when a.possessions > 0 then t.stl / a.possessions * 100 else 0 end as stl_per_100,
        case when a.possessions > 0 then t.blk / a.possessions * 100 else 0 end as blk_per_100,
        case when a.possessions > 0 then t.tov / a.possessions * 100 else 0 end as tov_per_100,
        case when a.possessions > 0 then (t.pts + t.reb + t.ast) / a.possessions * 100 else 0 end as pra_per_100,
        case when a.possessions > 0 then t.fgm / a.possessions * 100 else 0 end as fgm_per_100,
        case when a.possessions > 0 then t.fga / a.possessions * 100 else 0 end as fga_per_100,
        case when a.possessions > 0 then t.fg3m / a.possessions * 100 else 0 end as fg3m_per_100,
        case when a.possessions > 0 then t.fg3a / a.possessions * 100 else 0 end as fg3a_per_100,
        
        -- Timestamps
        greatest(t.created_at, a.created_at, h.created_at, m.created_at, s.created_at) as created_at,
        greatest(t.updated_at, a.updated_at, h.updated_at, m.updated_at, s.updated_at) as updated_at

    from traditional t
    left join advanced a on t.team_game_key = a.team_game_key
    left join hustle h on t.team_game_key = h.team_game_key
    left join misc m on t.team_game_key = m.team_game_key
    left join scoring s on t.team_game_key = s.team_game_key
    where t.season_year >= '2017-18'
)

select *
from final 