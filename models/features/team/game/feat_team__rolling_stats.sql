{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='merge',
    tags=['features', 'team', 'rolling', 'stats', 'performance'],
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

{% set rolling_window_games = var('rolling_window_games', 10) %}
{% set extended_lookback_days = rolling_window_games + 30 %}

with boxscores as (
    select
        -- Identifiers
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,
        opponent_id,

        -- Select ALL numeric stats from int_team__combined_boxscore
        -- Traditional
        min,
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
        fta,
        ft_pct,
        off_reb,
        def_reb,
        reb,
        ast,
        stl,
        blk,
        tov,
        pf,
        pts,
        plus_minus,

        -- Advanced
        est_off_rating,
        off_rating,
        est_def_rating,
        def_rating,
        est_net_rating,
        net_rating,
        ast_pct,
        ast_to_tov_ratio,
        ast_ratio,
        off_reb_pct,
        def_reb_pct,
        reb_pct,
        est_team_tov_pct,
        tov_ratio,
        eff_fg_pct,
        ts_pct,
        usage_pct,
        est_usage_pct,
        est_pace,
        pace,
        pace_per_40,
        possessions,
        pie,

        -- Hustle
        cont_shots,
        cont_2pt,
        cont_3pt,
        deflections,
        charges_drawn,
        screen_ast,
        screen_ast_pts,
        off_loose_balls_rec,
        def_loose_balls_rec,
        tot_loose_balls_rec,
        off_box_outs,
        def_box_outs,
        box_out_team_reb,
        box_out_player_reb,
        tot_box_outs,

        -- Misc
        pts_off_tov,
        second_chance_pts,
        fastbreak_pts,
        pts_in_paint,
        opp_pts_off_tov,
        opp_second_chance_pts,
        opp_fastbreak_pts,
        opp_pts_in_paint,

        -- Scoring
        pct_fga_2pt,
        pct_fga_3pt,
        pct_pts_2pt,
        pct_pts_midrange_2pt,
        pct_pts_3pt,
        pct_pts_fastbreak,
        pct_pts_ft,
        pct_pts_off_tov,
        pct_pts_in_paint,
        pct_assisted_2pt,
        pct_unassisted_2pt,
        pct_assisted_3pt,
        pct_unassisted_3pt,
        pct_assisted_fgm,
        pct_unassisted_fgm,

        -- Timestamps for Incremental
        updated_at

    from {{ ref('int_team__combined_boxscore') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= {{ var('training_start_season_year') }}
    {% if is_incremental() %}
    -- Look back N+1 days to ensure rolling calculations are correct near the incremental boundary
    and game_date >= (
        select max(game_date) - interval '{{ extended_lookback_days }} days'
        from {{ this }}
    )
    {% endif %}
),

team_performance_base as (
    select
        team_game_key,
        game_id,
        team_id,
        game_date as performance_date, -- Rename to avoid ambiguity
        season_year,
        -- Key metrics from feat_team__performance_metrics
        off_rating as perf_off_rating, -- Aliasing to avoid potential name collision
        def_rating as perf_def_rating,
        net_rating as perf_net_rating,
        pace as perf_pace,
        ts_pct as perf_ts_pct,
        ast_ratio as perf_ast_ratio,
        -- Assuming 'pts' for rolling L5 pts from feat_team__rolling_stats comes from the base performance model's points
        pts as perf_pts,
        -- Pass-through fields from feat_team__performance_metrics
        is_win,
        point_margin,
        game_num_in_season,
        last5_wins,
        last10_wins,
        team_strength_tier,
        team_form
    from {{ ref('feat_team__performance_metrics') }}
    {% if is_incremental() %}
    -- Ensure we fetch performance data corresponding to the boxscores date range
    where game_date >= (select coalesce(min(game_date), '1900-01-01') from boxscores) 
    and game_date <= (select coalesce(max(game_date), '2999-12-31') from boxscores)
    {% endif %}
),

joined_data as (
    select
        b.*, -- All fields from boxscores CTE
        tpb.perf_off_rating,
        tpb.perf_def_rating,
        tpb.perf_net_rating,
        tpb.perf_pace,
        tpb.perf_ts_pct,
        tpb.perf_ast_ratio,
        tpb.perf_pts,
        tpb.is_win,
        tpb.point_margin,
        tpb.game_num_in_season,
        tpb.last5_wins,
        tpb.last10_wins,
        tpb.team_strength_tier,
        tpb.team_form
    from boxscores b
    left join team_performance_base tpb
        on b.team_game_key = tpb.team_game_key
),

-- Create a separate CTE for window calculations to avoid ambiguity with game_date
calculations_base as (
    select 
        jd.*
    from joined_data jd
),

rolling_metrics_specific as (
    select
        cb.team_game_key,
        cb.game_id,
        cb.team_id,
        cb.season_year,
        cb.game_date,

        avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_off_rating_val,
        avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_def_rating_val,
        avg(cb.perf_pace) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_pace_val,
        avg(cb.perf_pts) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_pts_val,
        avg(cb.perf_ast_ratio) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_ast_ratio_val,
        avg(cb.perf_ts_pct) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) as team_l5_ts_pct_val,

        avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 1 preceding) as team_l10_off_rating_specific_val,
        avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 1 preceding) as team_l10_def_rating_specific_val,
        avg(cb.perf_pace) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 1 preceding) as team_l10_pace_specific_val,

        avg(cb.perf_off_rating) over (partition by cb.team_id, cb.season_year order by cb.game_date rows between unbounded preceding and 1 preceding) as team_season_off_rating_val,
        avg(cb.perf_def_rating) over (partition by cb.team_id, cb.season_year order by cb.game_date rows between unbounded preceding and 1 preceding) as team_season_def_rating_val,
        avg(cb.perf_pace) over (partition by cb.team_id, cb.season_year order by cb.game_date rows between unbounded preceding and 1 preceding) as team_season_pace_val,

        case
            when avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) >
                 avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 6 preceding) then 'IMPROVING'
            when avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) <
                 avg(cb.perf_off_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 6 preceding) then 'DECLINING'
            else 'STABLE'
        end as team_offense_trend,
        case
            when avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) <
                 avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 6 preceding) then 'IMPROVING'
            when avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 5 preceding and 1 preceding) >
                 avg(cb.perf_def_rating) over (partition by cb.team_id order by cb.game_date rows between 10 preceding and 6 preceding) then 'DECLINING'
            else 'STABLE'
        end as team_defense_trend
    from calculations_base cb
),

rolling_metrics_with_z_scores as (
    select
        rms.*,
        (rms.team_l10_off_rating_specific_val - avg(rms.team_l10_off_rating_specific_val) over (partition by rms.season_year)) /
            nullif(stddev(rms.team_l10_off_rating_specific_val) over (partition by rms.season_year), 0)
        as team_off_rating_z_score,
        (rms.team_l10_def_rating_specific_val - avg(rms.team_l10_def_rating_specific_val) over (partition by rms.season_year)) /
            nullif(stddev(rms.team_l10_def_rating_specific_val) over (partition by rms.season_year), 0)
        as team_def_rating_z_score,
        (rms.team_l10_pace_specific_val - avg(rms.team_l10_pace_specific_val) over (partition by rms.season_year)) /
            nullif(stddev(rms.team_l10_pace_specific_val) over (partition by rms.season_year), 0)
        as team_pace_z_score
    from rolling_metrics_specific rms
),

final as (
    select
        cb.*, -- Selects all fields from calculations_base (includes original boxscore stats, and pass-throughs like is_win)

        -- General rolling stats from original feat_team__rolling_stats.sql using macros
        -- Ensure all macros from the original file are present here, operating on columns from cb.*
        {{ calculate_rolling_avg('min', 'cb.team_id', 'cb.game_date') }} as min_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('min', 'cb.team_id', 'cb.game_date') }} as min_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('min', 'cb.team_id', 'cb.game_date', 1) }} as min_lag_1g,
        {{ calculate_rolling_avg('fgm', 'cb.team_id', 'cb.game_date') }} as fgm_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fgm', 'cb.team_id', 'cb.game_date') }} as fgm_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fgm', 'cb.team_id', 'cb.game_date', 1) }} as fgm_lag_1g,
        {{ calculate_rolling_avg('fga', 'cb.team_id', 'cb.game_date') }} as fga_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fga', 'cb.team_id', 'cb.game_date') }} as fga_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fga', 'cb.team_id', 'cb.game_date', 1) }} as fga_lag_1g,
        {{ calculate_rolling_avg('fg_pct', 'cb.team_id', 'cb.game_date') }} as fg_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fg_pct', 'cb.team_id', 'cb.game_date') }} as fg_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fg_pct', 'cb.team_id', 'cb.game_date', 1) }} as fg_pct_lag_1g,
        {{ calculate_rolling_avg('fg3m', 'cb.team_id', 'cb.game_date') }} as fg3m_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fg3m', 'cb.team_id', 'cb.game_date') }} as fg3m_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fg3m', 'cb.team_id', 'cb.game_date', 1) }} as fg3m_lag_1g,
        {{ calculate_rolling_avg('fg3a', 'cb.team_id', 'cb.game_date') }} as fg3a_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fg3a', 'cb.team_id', 'cb.game_date') }} as fg3a_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fg3a', 'cb.team_id', 'cb.game_date', 1) }} as fg3a_lag_1g,
        {{ calculate_rolling_avg('fg3_pct', 'cb.team_id', 'cb.game_date') }} as fg3_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fg3_pct', 'cb.team_id', 'cb.game_date') }} as fg3_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fg3_pct', 'cb.team_id', 'cb.game_date', 1) }} as fg3_pct_lag_1g,
        {{ calculate_rolling_avg('ftm', 'cb.team_id', 'cb.game_date') }} as ftm_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ftm', 'cb.team_id', 'cb.game_date') }} as ftm_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ftm', 'cb.team_id', 'cb.game_date', 1) }} as ftm_lag_1g,
        {{ calculate_rolling_avg('fta', 'cb.team_id', 'cb.game_date') }} as fta_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fta', 'cb.team_id', 'cb.game_date') }} as fta_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fta', 'cb.team_id', 'cb.game_date', 1) }} as fta_lag_1g,
        {{ calculate_rolling_avg('ft_pct', 'cb.team_id', 'cb.game_date') }} as ft_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ft_pct', 'cb.team_id', 'cb.game_date') }} as ft_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ft_pct', 'cb.team_id', 'cb.game_date', 1) }} as ft_pct_lag_1g,
        {{ calculate_rolling_avg('off_reb', 'cb.team_id', 'cb.game_date') }} as off_reb_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('off_reb', 'cb.team_id', 'cb.game_date') }} as off_reb_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('off_reb', 'cb.team_id', 'cb.game_date', 1) }} as off_reb_lag_1g,
        {{ calculate_rolling_avg('def_reb', 'cb.team_id', 'cb.game_date') }} as def_reb_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('def_reb', 'cb.team_id', 'cb.game_date') }} as def_reb_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('def_reb', 'cb.team_id', 'cb.game_date', 1) }} as def_reb_lag_1g,
        {{ calculate_rolling_avg('reb', 'cb.team_id', 'cb.game_date') }} as reb_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('reb', 'cb.team_id', 'cb.game_date') }} as reb_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('reb', 'cb.team_id', 'cb.game_date', 1) }} as reb_lag_1g,
        {{ calculate_rolling_avg('ast', 'cb.team_id', 'cb.game_date') }} as ast_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ast', 'cb.team_id', 'cb.game_date') }} as ast_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ast', 'cb.team_id', 'cb.game_date', 1) }} as ast_lag_1g,
        {{ calculate_rolling_avg('stl', 'cb.team_id', 'cb.game_date') }} as stl_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('stl', 'cb.team_id', 'cb.game_date') }} as stl_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('stl', 'cb.team_id', 'cb.game_date', 1) }} as stl_lag_1g,
        {{ calculate_rolling_avg('blk', 'cb.team_id', 'cb.game_date') }} as blk_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('blk', 'cb.team_id', 'cb.game_date') }} as blk_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('blk', 'cb.team_id', 'cb.game_date', 1) }} as blk_lag_1g,
        {{ calculate_rolling_avg('tov', 'cb.team_id', 'cb.game_date') }} as tov_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('tov', 'cb.team_id', 'cb.game_date') }} as tov_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('tov', 'cb.team_id', 'cb.game_date', 1) }} as tov_lag_1g,
        {{ calculate_rolling_avg('pf', 'cb.team_id', 'cb.game_date') }} as pf_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pf', 'cb.team_id', 'cb.game_date') }} as pf_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pf', 'cb.team_id', 'cb.game_date', 1) }} as pf_lag_1g,
        {{ calculate_rolling_avg('pts', 'cb.team_id', 'cb.game_date') }} as pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pts', 'cb.team_id', 'cb.game_date') }} as pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pts', 'cb.team_id', 'cb.game_date', 1) }} as pts_lag_1g,
        {{ calculate_rolling_avg('plus_minus', 'cb.team_id', 'cb.game_date') }} as plus_minus_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('plus_minus', 'cb.team_id', 'cb.game_date') }} as plus_minus_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('plus_minus', 'cb.team_id', 'cb.game_date', 1) }} as plus_minus_lag_1g,

        -- Advanced
        {{ calculate_rolling_avg('est_off_rating', 'cb.team_id', 'cb.game_date') }} as est_off_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_off_rating', 'cb.team_id', 'cb.game_date') }} as est_off_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_off_rating', 'cb.team_id', 'cb.game_date', 1) }} as est_off_rating_lag_1g,
        {{ calculate_rolling_avg('off_rating', 'cb.team_id', 'cb.game_date') }} as off_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('off_rating', 'cb.team_id', 'cb.game_date') }} as off_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('off_rating', 'cb.team_id', 'cb.game_date', 1) }} as off_rating_lag_1g,
        {{ calculate_rolling_avg('est_def_rating', 'cb.team_id', 'cb.game_date') }} as est_def_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_def_rating', 'cb.team_id', 'cb.game_date') }} as est_def_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_def_rating', 'cb.team_id', 'cb.game_date', 1) }} as est_def_rating_lag_1g,
        {{ calculate_rolling_avg('def_rating', 'cb.team_id', 'cb.game_date') }} as def_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('def_rating', 'cb.team_id', 'cb.game_date') }} as def_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('def_rating', 'cb.team_id', 'cb.game_date', 1) }} as def_rating_lag_1g,
        {{ calculate_rolling_avg('est_net_rating', 'cb.team_id', 'cb.game_date') }} as est_net_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_net_rating', 'cb.team_id', 'cb.game_date') }} as est_net_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_net_rating', 'cb.team_id', 'cb.game_date', 1) }} as est_net_rating_lag_1g,
        {{ calculate_rolling_avg('net_rating', 'cb.team_id', 'cb.game_date') }} as net_rating_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('net_rating', 'cb.team_id', 'cb.game_date') }} as net_rating_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('net_rating', 'cb.team_id', 'cb.game_date', 1) }} as net_rating_lag_1g,
        {{ calculate_rolling_avg('ast_pct', 'cb.team_id', 'cb.game_date') }} as ast_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ast_pct', 'cb.team_id', 'cb.game_date') }} as ast_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ast_pct', 'cb.team_id', 'cb.game_date', 1) }} as ast_pct_lag_1g,
        {{ calculate_rolling_avg('ast_to_tov_ratio', 'cb.team_id', 'cb.game_date') }} as ast_to_tov_ratio_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ast_to_tov_ratio', 'cb.team_id', 'cb.game_date') }} as ast_to_tov_ratio_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ast_to_tov_ratio', 'cb.team_id', 'cb.game_date', 1) }} as ast_to_tov_ratio_lag_1g,
        {{ calculate_rolling_avg('ast_ratio', 'cb.team_id', 'cb.game_date') }} as ast_ratio_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ast_ratio', 'cb.team_id', 'cb.game_date') }} as ast_ratio_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ast_ratio', 'cb.team_id', 'cb.game_date', 1) }} as ast_ratio_lag_1g,
        {{ calculate_rolling_avg('off_reb_pct', 'cb.team_id', 'cb.game_date') }} as off_reb_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('off_reb_pct', 'cb.team_id', 'cb.game_date') }} as off_reb_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('off_reb_pct', 'cb.team_id', 'cb.game_date', 1) }} as off_reb_pct_lag_1g,
        {{ calculate_rolling_avg('def_reb_pct', 'cb.team_id', 'cb.game_date') }} as def_reb_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('def_reb_pct', 'cb.team_id', 'cb.game_date') }} as def_reb_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('def_reb_pct', 'cb.team_id', 'cb.game_date', 1) }} as def_reb_pct_lag_1g,
        {{ calculate_rolling_avg('reb_pct', 'cb.team_id', 'cb.game_date') }} as reb_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('reb_pct', 'cb.team_id', 'cb.game_date') }} as reb_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('reb_pct', 'cb.team_id', 'cb.game_date', 1) }} as reb_pct_lag_1g,
        {{ calculate_rolling_avg('est_team_tov_pct', 'cb.team_id', 'cb.game_date') }} as est_team_tov_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_team_tov_pct', 'cb.team_id', 'cb.game_date') }} as est_team_tov_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_team_tov_pct', 'cb.team_id', 'cb.game_date', 1) }} as est_team_tov_pct_lag_1g,
        {{ calculate_rolling_avg('tov_ratio', 'cb.team_id', 'cb.game_date') }} as tov_ratio_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('tov_ratio', 'cb.team_id', 'cb.game_date') }} as tov_ratio_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('tov_ratio', 'cb.team_id', 'cb.game_date', 1) }} as tov_ratio_lag_1g,
        {{ calculate_rolling_avg('eff_fg_pct', 'cb.team_id', 'cb.game_date') }} as eff_fg_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('eff_fg_pct', 'cb.team_id', 'cb.game_date') }} as eff_fg_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('eff_fg_pct', 'cb.team_id', 'cb.game_date', 1) }} as eff_fg_pct_lag_1g,
        {{ calculate_rolling_avg('ts_pct', 'cb.team_id', 'cb.game_date') }} as ts_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('ts_pct', 'cb.team_id', 'cb.game_date') }} as ts_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('ts_pct', 'cb.team_id', 'cb.game_date', 1) }} as ts_pct_lag_1g,
        {{ calculate_rolling_avg('usage_pct', 'cb.team_id', 'cb.game_date') }} as usage_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('usage_pct', 'cb.team_id', 'cb.game_date') }} as usage_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('usage_pct', 'cb.team_id', 'cb.game_date', 1) }} as usage_pct_lag_1g,
        {{ calculate_rolling_avg('est_usage_pct', 'cb.team_id', 'cb.game_date') }} as est_usage_pct_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_usage_pct', 'cb.team_id', 'cb.game_date') }} as est_usage_pct_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_usage_pct', 'cb.team_id', 'cb.game_date', 1) }} as est_usage_pct_lag_1g,
        {{ calculate_rolling_avg('est_pace', 'cb.team_id', 'cb.game_date') }} as est_pace_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('est_pace', 'cb.team_id', 'cb.game_date') }} as est_pace_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('est_pace', 'cb.team_id', 'cb.game_date', 1) }} as est_pace_lag_1g,
        {{ calculate_rolling_avg('pace', 'cb.team_id', 'cb.game_date') }} as pace_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pace', 'cb.team_id', 'cb.game_date') }} as pace_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pace', 'cb.team_id', 'cb.game_date', 1) }} as pace_lag_1g,
        {{ calculate_rolling_avg('pace_per_40', 'cb.team_id', 'cb.game_date') }} as pace_per_40_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pace_per_40', 'cb.team_id', 'cb.game_date') }} as pace_per_40_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pace_per_40', 'cb.team_id', 'cb.game_date', 1) }} as pace_per_40_lag_1g,
        {{ calculate_rolling_avg('possessions', 'cb.team_id', 'cb.game_date') }} as possessions_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('possessions', 'cb.team_id', 'cb.game_date') }} as possessions_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('possessions', 'cb.team_id', 'cb.game_date', 1) }} as possessions_lag_1g,
        {{ calculate_rolling_avg('pie', 'cb.team_id', 'cb.game_date') }} as pie_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pie', 'cb.team_id', 'cb.game_date') }} as pie_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pie', 'cb.team_id', 'cb.game_date', 1) }} as pie_lag_1g,

        -- Hustle
        {{ calculate_rolling_avg('cont_shots', 'cb.team_id', 'cb.game_date') }} as cont_shots_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('cont_shots', 'cb.team_id', 'cb.game_date') }} as cont_shots_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('cont_shots', 'cb.team_id', 'cb.game_date', 1) }} as cont_shots_lag_1g,
        {{ calculate_rolling_avg('cont_2pt', 'cb.team_id', 'cb.game_date') }} as cont_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('cont_2pt', 'cb.team_id', 'cb.game_date') }} as cont_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('cont_2pt', 'cb.team_id', 'cb.game_date', 1) }} as cont_2pt_lag_1g,
        {{ calculate_rolling_avg('cont_3pt', 'cb.team_id', 'cb.game_date') }} as cont_3pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('cont_3pt', 'cb.team_id', 'cb.game_date') }} as cont_3pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('cont_3pt', 'cb.team_id', 'cb.game_date', 1) }} as cont_3pt_lag_1g,
        {{ calculate_rolling_avg('deflections', 'cb.team_id', 'cb.game_date') }} as deflections_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('deflections', 'cb.team_id', 'cb.game_date') }} as deflections_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('deflections', 'cb.team_id', 'cb.game_date', 1) }} as deflections_lag_1g,
        {{ calculate_rolling_avg('charges_drawn', 'cb.team_id', 'cb.game_date') }} as charges_drawn_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('charges_drawn', 'cb.team_id', 'cb.game_date') }} as charges_drawn_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('charges_drawn', 'cb.team_id', 'cb.game_date', 1) }} as charges_drawn_lag_1g,
        {{ calculate_rolling_avg('screen_ast', 'cb.team_id', 'cb.game_date') }} as screen_ast_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('screen_ast', 'cb.team_id', 'cb.game_date') }} as screen_ast_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('screen_ast', 'cb.team_id', 'cb.game_date', 1) }} as screen_ast_lag_1g,
        {{ calculate_rolling_avg('screen_ast_pts', 'cb.team_id', 'cb.game_date') }} as screen_ast_pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('screen_ast_pts', 'cb.team_id', 'cb.game_date') }} as screen_ast_pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('screen_ast_pts', 'cb.team_id', 'cb.game_date', 1) }} as screen_ast_pts_lag_1g,
        {{ calculate_rolling_avg('off_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as off_loose_balls_rec_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('off_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as off_loose_balls_rec_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('off_loose_balls_rec', 'cb.team_id', 'cb.game_date', 1) }} as off_loose_balls_rec_lag_1g,
        {{ calculate_rolling_avg('def_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as def_loose_balls_rec_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('def_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as def_loose_balls_rec_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('def_loose_balls_rec', 'cb.team_id', 'cb.game_date', 1) }} as def_loose_balls_rec_lag_1g,
        {{ calculate_rolling_avg('tot_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as tot_loose_balls_rec_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('tot_loose_balls_rec', 'cb.team_id', 'cb.game_date') }} as tot_loose_balls_rec_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('tot_loose_balls_rec', 'cb.team_id', 'cb.game_date', 1) }} as tot_loose_balls_rec_lag_1g,
        {{ calculate_rolling_avg('off_box_outs', 'cb.team_id', 'cb.game_date') }} as off_box_outs_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('off_box_outs', 'cb.team_id', 'cb.game_date') }} as off_box_outs_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('off_box_outs', 'cb.team_id', 'cb.game_date', 1) }} as off_box_outs_lag_1g,
        {{ calculate_rolling_avg('def_box_outs', 'cb.team_id', 'cb.game_date') }} as def_box_outs_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('def_box_outs', 'cb.team_id', 'cb.game_date') }} as def_box_outs_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('def_box_outs', 'cb.team_id', 'cb.game_date', 1) }} as def_box_outs_lag_1g,
        {{ calculate_rolling_avg('box_out_team_reb', 'cb.team_id', 'cb.game_date') }} as box_out_team_reb_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('box_out_team_reb', 'cb.team_id', 'cb.game_date') }} as box_out_team_reb_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('box_out_team_reb', 'cb.team_id', 'cb.game_date', 1) }} as box_out_team_reb_lag_1g,
        {{ calculate_rolling_avg('box_out_player_reb', 'cb.team_id', 'cb.game_date') }} as box_out_player_reb_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('box_out_player_reb', 'cb.team_id', 'cb.game_date') }} as box_out_player_reb_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('box_out_player_reb', 'cb.team_id', 'cb.game_date', 1) }} as box_out_player_reb_lag_1g,
        {{ calculate_rolling_avg('tot_box_outs', 'cb.team_id', 'cb.game_date') }} as tot_box_outs_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('tot_box_outs', 'cb.team_id', 'cb.game_date') }} as tot_box_outs_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('tot_box_outs', 'cb.team_id', 'cb.game_date', 1) }} as tot_box_outs_lag_1g,

        -- Misc
        {{ calculate_rolling_avg('pts_off_tov', 'cb.team_id', 'cb.game_date') }} as pts_off_tov_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pts_off_tov', 'cb.team_id', 'cb.game_date') }} as pts_off_tov_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pts_off_tov', 'cb.team_id', 'cb.game_date', 1) }} as pts_off_tov_lag_1g,
        {{ calculate_rolling_avg('second_chance_pts', 'cb.team_id', 'cb.game_date') }} as second_chance_pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('second_chance_pts', 'cb.team_id', 'cb.game_date') }} as second_chance_pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('second_chance_pts', 'cb.team_id', 'cb.game_date', 1) }} as second_chance_pts_lag_1g,
        {{ calculate_rolling_avg('fastbreak_pts', 'cb.team_id', 'cb.game_date') }} as fastbreak_pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('fastbreak_pts', 'cb.team_id', 'cb.game_date') }} as fastbreak_pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('fastbreak_pts', 'cb.team_id', 'cb.game_date', 1) }} as fastbreak_pts_lag_1g,
        {{ calculate_rolling_avg('pts_in_paint', 'cb.team_id', 'cb.game_date') }} as pts_in_paint_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pts_in_paint', 'cb.team_id', 'cb.game_date') }} as pts_in_paint_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pts_in_paint', 'cb.team_id', 'cb.game_date', 1) }} as pts_in_paint_lag_1g,
        {{ calculate_rolling_avg('opp_pts_off_tov', 'cb.team_id', 'cb.game_date') }} as opp_pts_off_tov_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('opp_pts_off_tov', 'cb.team_id', 'cb.game_date') }} as opp_pts_off_tov_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('opp_pts_off_tov', 'cb.team_id', 'cb.game_date', 1) }} as opp_pts_off_tov_lag_1g,
        {{ calculate_rolling_avg('opp_second_chance_pts', 'cb.team_id', 'cb.game_date') }} as opp_second_chance_pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('opp_second_chance_pts', 'cb.team_id', 'cb.game_date') }} as opp_second_chance_pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('opp_second_chance_pts', 'cb.team_id', 'cb.game_date', 1) }} as opp_second_chance_pts_lag_1g,
        {{ calculate_rolling_avg('opp_fastbreak_pts', 'cb.team_id', 'cb.game_date') }} as opp_fastbreak_pts_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('opp_fastbreak_pts', 'cb.team_id', 'cb.game_date') }} as opp_fastbreak_pts_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('opp_fastbreak_pts', 'cb.team_id', 'cb.game_date', 1) }} as opp_fastbreak_pts_lag_1g,
        {{ calculate_rolling_avg('opp_pts_in_paint', 'cb.team_id', 'cb.game_date') }} as opp_pts_in_paint_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('opp_pts_in_paint', 'cb.team_id', 'cb.game_date') }} as opp_pts_in_paint_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('opp_pts_in_paint', 'cb.team_id', 'cb.game_date', 1) }} as opp_pts_in_paint_lag_1g,

        -- Scoring
        {{ calculate_rolling_avg('pct_fga_2pt', 'cb.team_id', 'cb.game_date') }} as pct_fga_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_fga_2pt', 'cb.team_id', 'cb.game_date') }} as pct_fga_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_fga_2pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_fga_2pt_lag_1g,
        {{ calculate_rolling_avg('pct_fga_3pt', 'cb.team_id', 'cb.game_date') }} as pct_fga_3pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_fga_3pt', 'cb.team_id', 'cb.game_date') }} as pct_fga_3pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_fga_3pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_fga_3pt_lag_1g,
        {{ calculate_rolling_avg('pct_pts_2pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_2pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_2pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_2pt_lag_1g,
        {{ calculate_rolling_avg('pct_pts_midrange_2pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_midrange_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_midrange_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_midrange_2pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_midrange_2pt_lag_1g,
        {{ calculate_rolling_avg('pct_pts_3pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_3pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_3pt', 'cb.team_id', 'cb.game_date') }} as pct_pts_3pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_3pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_3pt_lag_1g,
        {{ calculate_rolling_avg('pct_pts_fastbreak', 'cb.team_id', 'cb.game_date') }} as pct_pts_fastbreak_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_fastbreak', 'cb.team_id', 'cb.game_date') }} as pct_pts_fastbreak_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_fastbreak', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_fastbreak_lag_1g,
        {{ calculate_rolling_avg('pct_pts_ft', 'cb.team_id', 'cb.game_date') }} as pct_pts_ft_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_ft', 'cb.team_id', 'cb.game_date') }} as pct_pts_ft_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_ft', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_ft_lag_1g,
        {{ calculate_rolling_avg('pct_pts_off_tov', 'cb.team_id', 'cb.game_date') }} as pct_pts_off_tov_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_off_tov', 'cb.team_id', 'cb.game_date') }} as pct_pts_off_tov_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_off_tov', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_off_tov_lag_1g,
        {{ calculate_rolling_avg('pct_pts_in_paint', 'cb.team_id', 'cb.game_date') }} as pct_pts_in_paint_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_pts_in_paint', 'cb.team_id', 'cb.game_date') }} as pct_pts_in_paint_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_pts_in_paint', 'cb.team_id', 'cb.game_date', 1) }} as pct_pts_in_paint_lag_1g,
        {{ calculate_rolling_avg('pct_assisted_2pt', 'cb.team_id', 'cb.game_date') }} as pct_assisted_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_assisted_2pt', 'cb.team_id', 'cb.game_date') }} as pct_assisted_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_assisted_2pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_assisted_2pt_lag_1g,
        {{ calculate_rolling_avg('pct_unassisted_2pt', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_2pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_unassisted_2pt', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_2pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_unassisted_2pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_unassisted_2pt_lag_1g,
        {{ calculate_rolling_avg('pct_assisted_3pt', 'cb.team_id', 'cb.game_date') }} as pct_assisted_3pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_assisted_3pt', 'cb.team_id', 'cb.game_date') }} as pct_assisted_3pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_assisted_3pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_assisted_3pt_lag_1g,
        {{ calculate_rolling_avg('pct_unassisted_3pt', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_3pt_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_unassisted_3pt', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_3pt_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_unassisted_3pt', 'cb.team_id', 'cb.game_date', 1) }} as pct_unassisted_3pt_lag_1g,
        {{ calculate_rolling_avg('pct_assisted_fgm', 'cb.team_id', 'cb.game_date') }} as pct_assisted_fgm_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_assisted_fgm', 'cb.team_id', 'cb.game_date') }} as pct_assisted_fgm_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_assisted_fgm', 'cb.team_id', 'cb.game_date', 1) }} as pct_assisted_fgm_lag_1g,
        {{ calculate_rolling_avg('pct_unassisted_fgm', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_fgm_roll_{{ rolling_window_games }}g_avg,
        {{ calculate_rolling_stddev('pct_unassisted_fgm', 'cb.team_id', 'cb.game_date') }} as pct_unassisted_fgm_roll_{{ rolling_window_games }}g_stddev,
        {{ calculate_lag('pct_unassisted_fgm', 'cb.team_id', 'cb.game_date', 1) }} as pct_unassisted_fgm_lag_1g,

        -- Specific rolling metrics from feat_team__rolling_stats logic (joined from rmz)
        coalesce(rmz.team_l5_off_rating_val, rmz.team_season_off_rating_val) as team_l5_off_rating,
        coalesce(rmz.team_l5_def_rating_val, rmz.team_season_def_rating_val) as team_l5_def_rating,
        coalesce(rmz.team_l5_pace_val, rmz.team_season_pace_val) as team_l5_pace,
        coalesce(rmz.team_l5_pts_val, 0) as team_l5_pts,
        coalesce(rmz.team_l5_ast_ratio_val, 0) as team_l5_ast_ratio,
        coalesce(rmz.team_l5_ts_pct_val, 0) as team_l5_ts_pct,

        coalesce(rmz.team_l10_off_rating_specific_val, rmz.team_season_off_rating_val) as team_l10_off_rating,
        coalesce(rmz.team_l10_def_rating_specific_val, rmz.team_season_def_rating_val) as team_l10_def_rating,
        coalesce(rmz.team_l10_pace_specific_val, rmz.team_season_pace_val) as team_l10_pace,

        coalesce(rmz.team_season_off_rating_val, 0) as team_season_off_rating,
        coalesce(rmz.team_season_def_rating_val, 0) as team_season_def_rating,
        coalesce(rmz.team_season_pace_val, 0) as team_season_pace,

        rmz.team_offense_trend,
        rmz.team_defense_trend,
        rmz.team_off_rating_z_score,
        rmz.team_def_rating_z_score,
        rmz.team_pace_z_score,

        -- jd.updated_at is already included via jd.* from boxscores
        current_timestamp as model_created_at,
        current_timestamp as model_updated_at

    from calculations_base cb
    left join rolling_metrics_with_z_scores rmz
        on cb.team_game_key = rmz.team_game_key
)

select * from final