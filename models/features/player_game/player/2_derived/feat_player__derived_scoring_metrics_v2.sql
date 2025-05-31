{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'derived', 'scoring', 'composite'],
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

with base_data as (
    select
        player_game_key,
        player_id,
        player_name,
        game_id,
        team_id,
        opponent_id,
        game_date,
        season_year,
        updated_at
    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

rolling_traditional as (
    select * from {{ ref('feat_player__traditional_rolling_v2') }}
),

rolling_usage as (
    select * from {{ ref('feat_player__usage_rolling_v2') }}
),

rolling_scoring as (
    select * from {{ ref('feat_player__scoring_rolling_v2') }}
),

rolling_advanced as (
    select * from {{ ref('feat_player__advanced_rolling_v2') }}
),

rolling_misc as (
    select * from {{ ref('feat_player__misc_rolling_v2') }}
),

rolling_tracking as (
    select * from {{ ref('feat_player__tracking_rolling_v2') }}
),

rolling_defensive as (
    select * from {{ ref('feat_player__defensive_rolling_v2') }}
),

rolling_hustle as (
    select * from {{ ref('feat_player__hustle_rolling_v2') }}
),

derived_features as (
    select
        bd.player_game_key,
        bd.player_id,
        bd.player_name,
        bd.game_id,
        bd.team_id,
        bd.opponent_id,
        bd.game_date,
        bd.season_year,

        -- =================================================================
        -- 3-GAME ROLLING DERIVED FEATURES
        -- =================================================================
        
        -- Composite Scoring Efficiency (TS% weighted with eFG%)
        case
            when coalesce(rt.fga_avg_l3, 0) > 0 or coalesce(rt.fta_avg_l3, 0) > 0
            then (coalesce(ra.ts_pct_avg_l3, 0) * 0.6) + (coalesce(ra.eff_fg_pct_avg_l3, 0) * 0.4)
            else 0
        end as scoring_efficiency_composite_l3,

        -- Points per Touch Efficiency
        case
            when coalesce(rtr.touches_avg_l3, 0) > 0
            then coalesce(rt.pts_avg_l3, 0) / nullif(rtr.touches_avg_l3, 0)
            else 0
        end as points_per_touch_l3,

        -- Shot Creation Index (unassisted shots * team share)
        case
            when coalesce(rt.fga_avg_l3, 0) > 0
            then coalesce(rt.fga_avg_l3, 0) * coalesce(rs.pct_unassisted_fgm_avg_l3, 0) * coalesce(ru.pct_of_team_fga_avg_l3, 0)
            else 0
        end as shot_creation_index_l3,

        -- Defensive Attention Factor (contested shots impact)
        case
            when coalesce(rt.fga_avg_l3, 0) > 0
            then (coalesce(rtr.cont_fga_avg_l3, 0) / nullif(rt.fga_avg_l3, 0)) * 
                 (1 + coalesce(rtr.def_at_rim_fga_avg_l3, 0) / nullif(rt.fga_avg_l3, 0))
            else 0
        end as defensive_attention_factor_l3,

        -- Scoring Versatility (balance across 2PT, 3PT, FT) - FIXED
        1 - (
            abs(coalesce(rs.pct_pts_2pt_avg_l3, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_3pt_avg_l3, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_ft_avg_l3, 0) / 100.0 - 0.3333)
        ) as scoring_versatility_l3,

        -- Usage-Weighted True Shooting
        (coalesce(ra.ts_pct_avg_l3, 0)) * (coalesce(ra.usage_pct_avg_l3, 0)) as usage_weighted_ts_l3,

        -- Three-Point Value Index
        (coalesce(rs.pct_fga_3pt_avg_l3, 0)) * (coalesce(rt.fg3_pct_avg_l3, 0)) as three_point_value_index_l3,

        -- Paint Dominance Index - FIXED (derive 2PT attempt rate)
        (coalesce(rs.pct_pts_in_paint_avg_l3, 0)) * ((100.0 - coalesce(rs.pct_fga_3pt_avg_l3, 0))) as paint_dominance_index_l3,

        -- Assisted Shot Efficiency
        (coalesce(rs.pct_assisted_fgm_avg_l3, 0)) * (coalesce(ra.eff_fg_pct_avg_l3, 0)) as assisted_shot_efficiency_l3,

        -- Pace-Adjusted Scoring Rate
        case
            when coalesce(ra.pace_avg_l3, 0) > 0 and coalesce(rt.min_avg_l3, 0) > 0
            then (coalesce(rt.pts_avg_l3, 0) / nullif(rt.min_avg_l3, 0)) * (100.0 / nullif(ra.pace_avg_l3, 0))
            else 0
        end as pace_adjusted_scoring_rate_l3,

        -- Second Chance Conversion Rate
        case
            when coalesce(rt.off_reb_avg_l3, 0) > 0
            then coalesce(rm.second_chance_pts_avg_l3, 0) / nullif(rt.off_reb_avg_l3, 0)
            else 0
        end as second_chance_conversion_rate_l3,

        -- Contested vs Uncontested Shooting Differential
        coalesce(rtr.cont_fg_pct_avg_l3, 0) - coalesce(rtr.uncont_fg_pct_avg_l3, 0) as shooting_pressure_differential_l3,

        -- True Shooting Attempts per Minute
        case
            when coalesce(rt.min_avg_l3, 0) > 0
            then (coalesce(rt.fga_avg_l3, 0) + 0.44 * coalesce(rt.fta_avg_l3, 0)) / nullif(rt.min_avg_l3, 0)
            else 0
        end as tsa_per_minute_l3,

        -- Free Throw Generation Rate
        case
            when coalesce(rt.fga_avg_l3, 0) > 0
            then coalesce(rt.fta_avg_l3, 0) / nullif(rt.fga_avg_l3, 0)
            else 0
        end as ft_generation_rate_l3,

        -- Self-Created Scoring Rate
        case
            when coalesce(rt.min_avg_l3, 0) > 0 and coalesce(rt.fgm_avg_l3, 0) > 0
            then (coalesce(rs.pct_unassisted_fgm_avg_l3, 0) * coalesce(rt.fgm_avg_l3, 0)) / nullif(rt.min_avg_l3, 0)
            else 0
        end as self_created_scoring_rate_l3,

        -- Opportunistic Scoring (fastbreak + turnover points per minute)
        case
            when coalesce(rt.min_avg_l3, 0) > 0
            then (coalesce(rs.pct_pts_off_tov_avg_l3, 0) * coalesce(rt.pts_avg_l3, 0) + coalesce(rs.pct_pts_fastbreak_avg_l3, 0) * coalesce(rt.pts_avg_l3, 0)) / nullif(rt.min_avg_l3, 0)
            else 0
        end as opportunistic_scoring_rate_l3,

        -- Scoring vs Playmaking Balance
        case
            when coalesce(ru.pct_of_team_ast_avg_l3, 0) + 1e-6 > 0
            then coalesce(ra.usage_pct_avg_l3, 0) / (coalesce(ru.pct_of_team_ast_avg_l3, 0) + 1e-6)
            else 0
        end as scoring_playmaking_ratio_l3,

        -- =================================================================
        -- 5-GAME ROLLING DERIVED FEATURES
        -- =================================================================
        
        -- Composite Scoring Efficiency
        case
            when coalesce(rt.fga_avg_l5, 0) > 0 or coalesce(rt.fta_avg_l5, 0) > 0
            then (coalesce(ra.ts_pct_avg_l5, 0) * 0.6) + (coalesce(ra.eff_fg_pct_avg_l5, 0) * 0.4)
            else 0
        end as scoring_efficiency_composite_l5,

        -- Points per Touch Efficiency
        case
            when coalesce(rtr.touches_avg_l5, 0) > 0
            then coalesce(rt.pts_avg_l5, 0) / nullif(rtr.touches_avg_l5, 0)
            else 0
        end as points_per_touch_l5,

        -- Shot Creation Index
        case
            when coalesce(rt.fga_avg_l5, 0) > 0
            then coalesce(rt.fga_avg_l5, 0) * coalesce(rs.pct_unassisted_fgm_avg_l5, 0) * coalesce(ru.pct_of_team_fga_avg_l5, 0)
            else 0
        end as shot_creation_index_l5,

        -- Defensive Attention Factor
        case
            when coalesce(rt.fga_avg_l5, 0) > 0
            then (coalesce(rtr.cont_fga_avg_l5, 0) / nullif(rt.fga_avg_l5, 0)) * 
                 (1 + coalesce(rtr.def_at_rim_fga_avg_l5, 0) / nullif(rt.fga_avg_l5, 0))
            else 0
        end as defensive_attention_factor_l5,

        -- Scoring Versatility - FIXED
        1 - (
            abs(coalesce(rs.pct_pts_2pt_avg_l5, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_3pt_avg_l5, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_ft_avg_l5, 0) / 100.0 - 0.3333)
        ) as scoring_versatility_l5,

        -- Usage-Weighted True Shooting
        (coalesce(ra.ts_pct_avg_l5, 0)) * (coalesce(ra.usage_pct_avg_l5, 0)) as usage_weighted_ts_l5,

        -- Three-Point Value Index
        (coalesce(rs.pct_fga_3pt_avg_l5, 0)) * (coalesce(rt.fg3_pct_avg_l5, 0)) as three_point_value_index_l5,

        -- Paint Dominance Index - FIXED
        (coalesce(rs.pct_pts_in_paint_avg_l5, 0)) * ((100.0 - coalesce(rs.pct_fga_3pt_avg_l5, 0))) as paint_dominance_index_l5,

        -- Assisted Shot Efficiency
        (coalesce(rs.pct_assisted_fgm_avg_l5, 0)) * (coalesce(ra.eff_fg_pct_avg_l5, 0)) as assisted_shot_efficiency_l5,

        -- Pace-Adjusted Scoring Rate
        case
            when coalesce(ra.pace_avg_l5, 0) > 0 and coalesce(rt.min_avg_l5, 0) > 0
            then (coalesce(rt.pts_avg_l5, 0) / nullif(rt.min_avg_l5, 0)) * (100.0 / nullif(ra.pace_avg_l5, 0))
            else 0
        end as pace_adjusted_scoring_rate_l5,

        -- Second Chance Conversion Rate
        case
            when coalesce(rt.off_reb_avg_l5, 0) > 0
            then coalesce(rm.second_chance_pts_avg_l5, 0) / nullif(rt.off_reb_avg_l5, 0)
            else 0
        end as second_chance_conversion_rate_l5,

        -- Contested vs Uncontested Shooting Differential
        coalesce(rtr.cont_fg_pct_avg_l5, 0) - coalesce(rtr.uncont_fg_pct_avg_l5, 0) as shooting_pressure_differential_l5,

        -- True Shooting Attempts per Minute
        case
            when coalesce(rt.min_avg_l5, 0) > 0
            then (coalesce(rt.fga_avg_l5, 0) + 0.44 * coalesce(rt.fta_avg_l5, 0)) / nullif(rt.min_avg_l5, 0)
            else 0
        end as tsa_per_minute_l5,

        -- Free Throw Generation Rate
        case
            when coalesce(rt.fga_avg_l5, 0) > 0
            then coalesce(rt.fta_avg_l5, 0) / nullif(rt.fga_avg_l5, 0)
            else 0
        end as ft_generation_rate_l5,

        -- Self-Created Scoring Rate
        case
            when coalesce(rt.min_avg_l5, 0) > 0 and coalesce(rt.fgm_avg_l5, 0) > 0
            then (coalesce(rs.pct_unassisted_fgm_avg_l5, 0) / 100.0 * coalesce(rt.fgm_avg_l5, 0)) / nullif(rt.min_avg_l5, 0)
            else 0
        end as self_created_scoring_rate_l5,

        -- Opportunistic Scoring Rate - FIXED
        case
            when coalesce(rt.min_avg_l5, 0) > 0
            then (coalesce(rs.pct_pts_off_tov_avg_l5, 0) * coalesce(rt.pts_avg_l5, 0) + coalesce(rs.pct_pts_fastbreak_avg_l5, 0) * coalesce(rt.pts_avg_l5, 0)) / nullif(rt.min_avg_l5, 0)
            else 0
        end as opportunistic_scoring_rate_l5,

        -- Scoring vs Playmaking Balance
        case
            when coalesce(ru.pct_of_team_ast_avg_l5, 0) + 1e-6 > 0
            then coalesce(ra.usage_pct_avg_l5, 0) / (coalesce(ru.pct_of_team_ast_avg_l5, 0) + 1e-6)
            else 0
        end as scoring_playmaking_ratio_l5,

        -- =================================================================
        -- 10-GAME ROLLING DERIVED FEATURES
        -- =================================================================
        
        -- Composite Scoring Efficiency
        case
            when coalesce(rt.fga_avg_l10, 0) > 0 or coalesce(rt.fta_avg_l10, 0) > 0
            then (coalesce(ra.ts_pct_avg_l10, 0) * 0.6) + (coalesce(ra.eff_fg_pct_avg_l10, 0) * 0.4)
            else 0
        end as scoring_efficiency_composite_l10,

        -- Points per Touch Efficiency
        case
            when coalesce(rtr.touches_avg_l10, 0) > 0
            then coalesce(rt.pts_avg_l10, 0) / nullif(rtr.touches_avg_l10, 0)
            else 0
        end as points_per_touch_l10,

        -- Shot Creation Index
        case
            when coalesce(rt.fga_avg_l10, 0) > 0
            then coalesce(rt.fga_avg_l10, 0) * coalesce(rs.pct_unassisted_fgm_avg_l10, 0) * coalesce(ru.pct_of_team_fga_avg_l10, 0)
            else 0
        end as shot_creation_index_l10,

        -- Defensive Attention Factor
        case
            when coalesce(rt.fga_avg_l10, 0) > 0
            then (coalesce(rtr.cont_fga_avg_l10, 0) / nullif(rt.fga_avg_l10, 0)) * 
                 (1 + coalesce(rtr.def_at_rim_fga_avg_l10, 0) / nullif(rt.fga_avg_l10, 0))
            else 0
        end as defensive_attention_factor_l10,

        -- Scoring Versatility - FIXED
        1 - (
            abs(coalesce(rs.pct_pts_2pt_avg_l10, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_3pt_avg_l10, 0) / 100.0 - 0.3333) +
            abs(coalesce(rs.pct_pts_ft_avg_l10, 0) / 100.0 - 0.3333)
        ) as scoring_versatility_l10,

        -- Usage-Weighted True Shooting
        (coalesce(ra.ts_pct_avg_l10, 0)) * (coalesce(ra.usage_pct_avg_l10, 0)) as usage_weighted_ts_l10,

        -- Three-Point Value Index
        (coalesce(rs.pct_fga_3pt_avg_l10, 0)) * (coalesce(rt.fg3_pct_avg_l10, 0)) as three_point_value_index_l10,

        -- Paint Dominance Index - FIXED
        (coalesce(rs.pct_pts_in_paint_avg_l10, 0)) * ((100.0 - coalesce(rs.pct_fga_3pt_avg_l10, 0))) as paint_dominance_index_l10,

        -- Assisted Shot Efficiency
        (coalesce(rs.pct_assisted_fgm_avg_l10, 0)) * (coalesce(ra.eff_fg_pct_avg_l10, 0)) as assisted_shot_efficiency_l10,

        -- Pace-Adjusted Scoring Rate
        case
            when coalesce(ra.pace_avg_l10, 0) > 0 and coalesce(rt.min_avg_l10, 0) > 0
            then (coalesce(rt.pts_avg_l10, 0) / nullif(rt.min_avg_l10, 0)) * (100.0 / nullif(ra.pace_avg_l10, 0))
            else 0
        end as pace_adjusted_scoring_rate_l10,

        -- Second Chance Conversion Rate
        case
            when coalesce(rt.off_reb_avg_l10, 0) > 0
            then coalesce(rm.second_chance_pts_avg_l10, 0) / nullif(rt.off_reb_avg_l10, 0)
            else 0
        end as second_chance_conversion_rate_l10,

        -- Contested vs Uncontested Shooting Differential
        coalesce(rtr.cont_fg_pct_avg_l10, 0) - coalesce(rtr.uncont_fg_pct_avg_l10, 0) as shooting_pressure_differential_l10,

        -- True Shooting Attempts per Minute
        case
            when coalesce(rt.min_avg_l10, 0) > 0
            then (coalesce(rt.fga_avg_l10, 0) + 0.44 * coalesce(rt.fta_avg_l10, 0)) / nullif(rt.min_avg_l10, 0)
            else 0
        end as tsa_per_minute_l10,

        -- Free Throw Generation Rate
        case
            when coalesce(rt.fga_avg_l10, 0) > 0
            then coalesce(rt.fta_avg_l10, 0) / nullif(rt.fga_avg_l10, 0)
            else 0
        end as ft_generation_rate_l10,

        -- Self-Created Scoring Rate
        case
            when coalesce(rt.min_avg_l10, 0) > 0 and coalesce(rt.fgm_avg_l10, 0) > 0
            then (coalesce(rs.pct_unassisted_fgm_avg_l10, 0) * coalesce(rt.fgm_avg_l10, 0)) / nullif(rt.min_avg_l10, 0)
            else 0
        end as self_created_scoring_rate_l10,

        -- Opportunistic Scoring Rate - FIXED
        case
            when coalesce(rt.min_avg_l10, 0) > 0
            then (coalesce(rs.pct_pts_off_tov_avg_l10, 0) * coalesce(rt.pts_avg_l10, 0) + coalesce(rs.pct_pts_fastbreak_avg_l10, 0) * coalesce(rt.pts_avg_l10, 0)) / nullif(rt.min_avg_l10, 0)
            else 0
        end as opportunistic_scoring_rate_l10,

        -- Scoring vs Playmaking Balance
        case
            when coalesce(ru.pct_of_team_ast_avg_l10, 0) + 1e-6 > 0
            then coalesce(ra.usage_pct_avg_l10, 0) / (coalesce(ru.pct_of_team_ast_avg_l10, 0) + 1e-6)
            else 0
        end as scoring_playmaking_ratio_l10,

        -- =================================================================
        -- TREND FEATURES (SHORT vs LONG WINDOW COMPARISONS)
        -- =================================================================
        
        -- Scoring Efficiency Trend (3-game vs 10-game)
        case
            when coalesce(rt.fga_avg_l3, 0) > 0 or coalesce(rt.fta_avg_l3, 0) > 0
            then ((coalesce(ra.ts_pct_avg_l3, 0) * 0.6) + (coalesce(ra.eff_fg_pct_avg_l3, 0) * 0.4)) -
                 ((coalesce(ra.ts_pct_avg_l10, 0) * 0.6) + (coalesce(ra.eff_fg_pct_avg_l10, 0) * 0.4))
            else 0
        end as scoring_efficiency_trend_3v10,

        -- Points per Touch Trend
        case
            when coalesce(rtr.touches_avg_l3, 0) > 0 and coalesce(rtr.touches_avg_l10, 0) > 0
            then (coalesce(rt.pts_avg_l3, 0) / nullif(rtr.touches_avg_l3, 0)) -
                 (coalesce(rt.pts_avg_l10, 0) / nullif(rtr.touches_avg_l10, 0))
            else 0
        end as points_per_touch_trend_3v10,

        -- Usage-Weighted TS Trend
        (coalesce(ra.ts_pct_avg_l3, 0) * coalesce(ra.usage_pct_avg_l3, 0)) -
        (coalesce(ra.ts_pct_avg_l10, 0) * coalesce(ra.usage_pct_avg_l10, 0)) as usage_weighted_ts_trend_3v10,

        -- Shot Creation Trend
        case
            when coalesce(rt.fga_avg_l3, 0) > 0 and coalesce(rt.fga_avg_l10, 0) > 0
            then (coalesce(rt.fga_avg_l3, 0) * coalesce(rs.pct_unassisted_fgm_avg_l3, 0) * coalesce(ru.pct_of_team_fga_avg_l3, 0)) -
                 (coalesce(rt.fga_avg_l10, 0) * coalesce(rs.pct_unassisted_fgm_avg_l10, 0) * coalesce(ru.pct_of_team_fga_avg_l10, 0))
            else 0
        end as shot_creation_trend_3v10,

        -- =================================================================
        -- TIMESTAMPS
        -- =================================================================
        greatest(
            coalesce(bd.updated_at, '1900-01-01'::timestamp),
            coalesce(rt.updated_at, '1900-01-01'::timestamp),
            coalesce(ru.updated_at, '1900-01-01'::timestamp),
            coalesce(rs.updated_at, '1900-01-01'::timestamp),
            coalesce(ra.updated_at, '1900-01-01'::timestamp),
            coalesce(rm.updated_at, '1900-01-01'::timestamp),
            coalesce(rtr.updated_at, '1900-01-01'::timestamp),
            coalesce(rd.updated_at, '1900-01-01'::timestamp),
            coalesce(rh.updated_at, '1900-01-01'::timestamp)
        ) as updated_at

    from base_data bd
    left join rolling_traditional rt on bd.player_game_key = rt.player_game_key
    left join rolling_usage ru on bd.player_game_key = ru.player_game_key
    left join rolling_scoring rs on bd.player_game_key = rs.player_game_key
    left join rolling_advanced ra on bd.player_game_key = ra.player_game_key
    left join rolling_misc rm on bd.player_game_key = rm.player_game_key
    left join rolling_tracking rtr on bd.player_game_key = rtr.player_game_key
    left join rolling_defensive rd on bd.player_game_key = rd.player_game_key
    left join rolling_hustle rh on bd.player_game_key = rh.player_game_key
)

select * from derived_features