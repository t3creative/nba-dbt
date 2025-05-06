{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    tags=['intermediate', 'core', 'boxscore'],
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

with traditional as (
    select *
    from {{ ref('int__player_traditional_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

advanced as (
    select *
    from {{ ref('stg__player_advanced_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

hustle as (
    select *
    from {{ ref('stg__player_hustle_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

misc as (
    select *
    from {{ ref('stg__player_misc_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

scoring as (
    select *
    from {{ ref('stg__player_scoring_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

tracking as (
    select *
    from {{ ref('stg__player_tracking_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

usage as (
    select *
    from {{ ref('stg__player_usage_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

defensive as (
    select *
    from {{ ref('stg__player_defensive_bxsc') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

player_bio as (
    select 
        player_id,
        position
    from {{ ref('stg__league_dash_player_bio') }}
),

final as (
    select
        -- Primary Key
        t.player_game_key,

        -- Identifiers & Metadata
        t.game_id,
        t.player_id,
        t.team_id,
        t.opponent_id,
        t.game_date,
        t.season_year,
        t.home_away,
        t.first_name,
        t.family_name,
        t.player_name,
        t.team_tricode,
        pb.position,

        -- Traditional Boxscore Stats
        t.min,
        t.pts,
        t.fgm,
        t.fga,
        t.fg_pct,
        t.fg3m,
        t.fg3a,
        t.fg3_pct,
        t.ftm,
        t.fta,
        t.ft_pct,
        t.reb,
        t.off_reb,
        t.def_reb,
        t.ast,
        t.stl,
        t.blk,
        t.tov,
        t.pf,
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
        m.opp_pts_off_tov_while_on,
        m.opp_second_chance_pts_while_on,
        m.opp_fastbreak_pts_while_on,
        m.opp_pts_in_paint_while_on,
        m.blk_against,
        m.fouls_drawn,

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

        -- Tracking Boxscore Stats
        tr.distance,
        tr.speed,
        tr.touches,
        tr.off_reb_chances,
        tr.def_reb_chances,
        tr.reb_chances,
        tr.cont_fgm,
        tr.cont_fga,
        tr.cont_fg_pct,
        tr.uncont_fgm,
        tr.uncont_fga,
        tr.uncont_fg_pct,
        tr.def_at_rim_fgm,
        tr.def_at_rim_fga,
        tr.def_at_rim_fg_pct,

        -- Usage Boxscore Stats
        u.pct_of_team_fgm,
        u.pct_of_team_fga,
        u.pct_of_team_fg3m,
        u.pct_of_team_fg3a,
        u.pct_of_team_ftm,
        u.pct_of_team_fta,
        u.pct_of_team_oreb,
        u.pct_of_team_dreb,
        u.pct_of_team_reb,
        u.pct_of_team_ast,
        u.pct_of_team_tov,
        u.pct_of_team_stl,
        u.pct_of_team_blk,
        u.pct_of_team_blk_allowed,
        u.pct_of_team_pf,
        u.pct_of_team_pfd,
        u.pct_of_team_pts,

        -- Defensive Boxscore Stats
        d.matchup_min,
        d.partial_poss,
        d.def_switches,
        d.pts_allowed,
        d.ast_allowed,
        d.tov_forced,
        d.matchup_fgm,
        d.matchup_fga,
        d.matchup_fg_pct,
        d.matchup_fg3m,
        d.matchup_fg3a,
        d.matchup_fg3_pct,

        -- Per-Minute Normalized Stats
        case when t.min > 0 then t.pts / t.min else 0 end as pts_per_min,
        case when t.min > 0 then (t.off_reb + t.def_reb) / t.min else 0 end as reb_per_min,
        case when t.min > 0 then t.ast / t.min else 0 end as ast_per_min,
        case when t.min > 0 then t.stl / t.min else 0 end as stl_per_min,
        case when t.min > 0 then t.blk / t.min else 0 end as blk_per_min,
        case when t.min > 0 then t.tov / t.min else 0 end as tov_per_min,
        case when t.min > 0 then (t.pts + t.off_reb + t.def_reb + t.ast) / t.min else 0 end as pra_per_min,
        
        -- Per-36 Normalized Stats
        case when t.min > 0 then t.pts / t.min * 36 else 0 end as pts_per_36,
        case when t.min > 0 then (t.off_reb + t.def_reb) / t.min * 36 else 0 end as reb_per_36,
        case when t.min > 0 then t.ast / t.min * 36 else 0 end as ast_per_36,
        case when t.min > 0 then t.stl / t.min * 36 else 0 end as stl_per_36,
        case when t.min > 0 then t.blk / t.min * 36 else 0 end as blk_per_36,
        case when t.min > 0 then t.tov / t.min * 36 else 0 end as tov_per_36,
        case when t.min > 0 then (t.pts + t.off_reb + t.def_reb + t.ast) / t.min * 36 else 0 end as pra_per_36,
        
        -- Per-100 Possessions Normalized Stats
        case when a.possessions > 0 and t.min > 0 then t.pts / a.possessions * 100 else 0 end as pts_per_100,
        case when a.possessions > 0 and t.min > 0 then (t.off_reb + t.def_reb) / a.possessions * 100 else 0 end as reb_per_100,
        case when a.possessions > 0 and t.min > 0 then t.ast / a.possessions * 100 else 0 end as ast_per_100,
        case when a.possessions > 0 and t.min > 0 then t.stl / a.possessions * 100 else 0 end as stl_per_100,
        case when a.possessions > 0 and t.min > 0 then t.blk / a.possessions * 100 else 0 end as blk_per_100,
        case when a.possessions > 0 and t.min > 0 then t.tov / a.possessions * 100 else 0 end as tov_per_100,
        case when a.possessions > 0 and t.min > 0 then (t.pts + t.off_reb + t.def_reb + t.ast) / a.possessions * 100 else 0 end as pra_per_100,

        -- Timestamps
        greatest(
            t.created_at, a.created_at, h.created_at, m.created_at,
            s.created_at, tr.created_at, u.created_at, d.created_at
        ) as created_at,
        greatest(
            t.updated_at, a.updated_at, h.updated_at, m.updated_at,
            s.updated_at, tr.updated_at, u.updated_at, d.updated_at
        ) as updated_at

    from traditional t
    left join advanced a on t.player_game_key = a.player_game_key
    left join hustle h on t.player_game_key = h.player_game_key
    left join misc m on t.player_game_key = m.player_game_key
    left join scoring s on t.player_game_key = s.player_game_key
    left join tracking tr on t.player_game_key = tr.player_game_key
    left join usage u on t.player_game_key = u.player_game_key
    left join defensive d on t.player_game_key = d.player_game_key
    left join player_bio pb on t.player_id = pb.player_id
    where t.season_year >= '2017-18'
)

select *
from final 