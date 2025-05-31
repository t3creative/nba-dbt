{{ config(
    schema='features',
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'interior', 'deviations', 'scoring'],
    partition_by={
        "field": "game_date",
        "data_type": "date", 
        "granularity": "month"
    },
    indexes=[
        {'columns': ['player_game_key'], 'unique': True},
        {'columns': ['player_id', 'season_year']},
        {'columns': ['game_id']},
        {'columns': ['game_date']}
    ]
) }}

/*
Calculates comprehensive deviations in a player's recent interior scoring profile 
compared to their season-to-date baseline, ensuring strict temporal boundaries 
and zero data leakage for ML applications.

Interior scoring dimensions analyzed:
- Paint scoring reliance and efficiency  
- Rim attempt rate and finishing
- Mid-range vs paint balance
- Interior shot creation vs assisted scoring
- Second chance scoring efficiency
*/

with base_data as (
    select
        player_game_key,
        player_id, 
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        updated_at
    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

-- Get rolling scoring profile features
scoring_rolling as (
    select
        player_game_key,
        -- Interior Scoring Metrics (10-game rolling) - using available columns
        pct_pts_in_paint_avg_l10,
        pct_pts_midrange_2pt_avg_l10,
        -- Derive 2PT metrics from available 3PT data
        (100.0 - pct_fga_3pt_avg_l10) as pct_fga_2pt_avg_l10,  -- Derived: 2PT attempt rate
        pct_unassisted_fgm_avg_l10 as pct_unassisted_2pt_avg_l10,  -- Proxy for 2PT creation
        
        -- Short-term trends (3 and 5-game)
        pct_pts_in_paint_avg_l3,
        pct_pts_in_paint_avg_l5,
        pct_pts_midrange_2pt_avg_l3,
        pct_pts_midrange_2pt_avg_l5,
        
        updated_at
    from {{ ref('feat_player__scoring_rolling_v2') }}
),

-- Get traditional metrics for context
traditional_rolling as (
    select
        player_game_key,
        min_avg_l10,
        pts_avg_l10,
        fgm_avg_l10,
        fga_avg_l10,
        off_reb_avg_l10,
        
        -- Short-term context
        min_avg_l3,
        pts_avg_l3,
        off_reb_avg_l3,
        
        updated_at
    from {{ ref('feat_player__traditional_rolling_v2') }}
),

-- Get tracking data for rim statistics
tracking_rolling as (
    select
        player_game_key,
        def_at_rim_fg_pct_avg_l10,
        def_at_rim_fga_avg_l10,
        cont_fg_pct_avg_l10,
        uncont_fg_pct_avg_l10,
        
        -- Short-term tracking
        def_at_rim_fg_pct_avg_l3,
        def_at_rim_fga_avg_l3,
        
        updated_at
    from {{ ref('feat_player__tracking_rolling_v2') }}
),

-- Get miscellaneous stats for second chance scoring
misc_rolling as (
    select
        player_game_key,
        second_chance_pts_avg_l10,
        second_chance_pts_avg_l3,
        updated_at
    from {{ ref('feat_player__misc_rolling_v2') }}
),

-- Combine all rolling features
combined_rolling_features as (
    select
        bd.player_game_key,
        bd.player_id,
        bd.game_id,
        bd.team_id,
        bd.opponent_id,
        bd.season_year,
        bd.game_date,
        
        -- Scoring Profile
        coalesce(sr.pct_pts_in_paint_avg_l10, 0) as paint_scoring_pct_l10,
        coalesce(sr.pct_pts_midrange_2pt_avg_l10, 0) as midrange_scoring_pct_l10,
        -- Use derived 2PT metrics
        coalesce(sr.pct_pts_in_paint_avg_l10 + sr.pct_pts_midrange_2pt_avg_l10, 0) as twopt_scoring_pct_l10,
        coalesce(sr.pct_fga_2pt_avg_l10, 0) as twopt_attempt_pct_l10,
        -- Use overall unassisted rate as proxy for 2PT creation
        coalesce(sr.pct_unassisted_2pt_avg_l10, 0) as assisted_twopt_pct_l10,
        coalesce(sr.pct_unassisted_2pt_avg_l10, 0) as unassisted_twopt_pct_l10,
        
        -- Short-term trends
        coalesce(sr.pct_pts_in_paint_avg_l3, 0) as paint_scoring_pct_l3,
        coalesce(sr.pct_pts_in_paint_avg_l5, 0) as paint_scoring_pct_l5,
        coalesce(sr.pct_pts_midrange_2pt_avg_l3, 0) as midrange_scoring_pct_l3,
        coalesce(sr.pct_pts_midrange_2pt_avg_l5, 0) as midrange_scoring_pct_l5,
        
        -- Traditional Context
        coalesce(tr.min_avg_l10, 0) as min_avg_l10,
        coalesce(tr.pts_avg_l10, 0) as pts_avg_l10,
        coalesce(tr.fgm_avg_l10, 0) as fgm_avg_l10,
        coalesce(tr.fga_avg_l10, 0) as fga_avg_l10,
        coalesce(tr.off_reb_avg_l10, 0) as off_reb_avg_l10,
        coalesce(tr.min_avg_l3, 0) as min_avg_l3,
        coalesce(tr.off_reb_avg_l3, 0) as off_reb_avg_l3,
        
        -- Rim Statistics
        coalesce(tk.def_at_rim_fg_pct_avg_l10, 0) as rim_fg_pct_l10,
        coalesce(tk.def_at_rim_fga_avg_l10, 0) as rim_fga_l10,
        coalesce(tk.cont_fg_pct_avg_l10, 0) as contested_fg_pct_l10,
        coalesce(tk.uncont_fg_pct_avg_l10, 0) as uncontested_fg_pct_l10,
        coalesce(tk.def_at_rim_fg_pct_avg_l3, 0) as rim_fg_pct_l3,
        coalesce(tk.def_at_rim_fga_avg_l3, 0) as rim_fga_l3,
        
        -- Second Chance Context
        coalesce(mr.second_chance_pts_avg_l10, 0) as second_chance_pts_l10,
        coalesce(mr.second_chance_pts_avg_l3, 0) as second_chance_pts_l3,
        
        -- Calculate derived rates
        case 
            when coalesce(tr.min_avg_l10, 0) > 0 
            then coalesce(tk.def_at_rim_fga_avg_l10, 0) / tr.min_avg_l10 
            else 0 
        end as rim_attempt_rate_l10,
        
        case 
            when coalesce(tr.off_reb_avg_l10, 0) > 0 
            then coalesce(mr.second_chance_pts_avg_l10, 0) / tr.off_reb_avg_l10 
            else 0 
        end as second_chance_efficiency_l10,
        
        greatest(
            coalesce(bd.updated_at, '1900-01-01'::timestamp),
            coalesce(sr.updated_at, '1900-01-01'::timestamp),
            coalesce(tr.updated_at, '1900-01-01'::timestamp),
            coalesce(tk.updated_at, '1900-01-01'::timestamp),
            coalesce(mr.updated_at, '1900-01-01'::timestamp)
        ) as updated_at
        
    from base_data bd
    left join scoring_rolling sr on bd.player_game_key = sr.player_game_key
    left join traditional_rolling tr on bd.player_game_key = tr.player_game_key  
    left join tracking_rolling tk on bd.player_game_key = tk.player_game_key
    left join misc_rolling mr on bd.player_game_key = mr.player_game_key
    where coalesce(tr.min_avg_l10, 0) > 0  -- Filter out players with no meaningful minutes
),

-- Calculate season-to-date baselines (using PRIOR games only)
season_baselines as (
    select
        *,
        -- Paint Scoring Baseline (season-to-date prior to current game)
        avg(paint_scoring_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_paint_scoring_pct,
        
        -- Mid-range Baseline
        avg(midrange_scoring_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_midrange_pct,
        
        -- Two-Point Shot Profile Baseline
        avg(twopt_attempt_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_twopt_attempt_pct,
        
        -- Shot Creation Baseline
        avg(unassisted_twopt_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_unassisted_twopt_pct,
        
        -- Rim Statistics Baseline
        avg(rim_fg_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_rim_fg_pct,
        
        avg(rim_attempt_rate_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_rim_attempt_rate,
        
        -- Second Chance Baseline
        avg(second_chance_efficiency_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_baseline_second_chance_efficiency,
        
        -- Volatility Baselines (standard deviations)
        stddev(paint_scoring_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_paint_scoring_pct,
        
        stddev(rim_fg_pct_l10) over (
            partition by player_id, season_year 
            order by game_date 
            rows between unbounded preceding and 1 preceding
        ) as season_stddev_rim_fg_pct
        
    from combined_rolling_features
),

interior_scoring_deviations as (
    select
        -- Base Identifiers
        player_game_key,
        player_id,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        
        -- Context Metrics
        min_avg_l10,
        pts_avg_l10,
        fgm_avg_l10,
        
        -- =================================================================
        -- CURRENT INTERIOR SCORING PROFILE (10-GAME ROLLING)
        -- =================================================================
        paint_scoring_pct_l10 as current_paint_scoring_pct,
        midrange_scoring_pct_l10 as current_midrange_scoring_pct,
        twopt_attempt_pct_l10 as current_twopt_attempt_pct,
        unassisted_twopt_pct_l10 as current_unassisted_twopt_pct,
        rim_fg_pct_l10 as current_rim_fg_pct,
        rim_attempt_rate_l10 as current_rim_attempt_rate,
        second_chance_efficiency_l10 as current_second_chance_efficiency,
        
        -- =================================================================
        -- SEASON BASELINES (PRIOR GAMES ONLY)
        -- =================================================================
        coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10) as baseline_paint_scoring_pct,
        coalesce(season_baseline_midrange_pct, midrange_scoring_pct_l10) as baseline_midrange_pct,
        coalesce(season_baseline_twopt_attempt_pct, twopt_attempt_pct_l10) as baseline_twopt_attempt_pct,
        coalesce(season_baseline_unassisted_twopt_pct, unassisted_twopt_pct_l10) as baseline_unassisted_twopt_pct,
        coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10) as baseline_rim_fg_pct,
        coalesce(season_baseline_rim_attempt_rate, rim_attempt_rate_l10) as baseline_rim_attempt_rate,
        coalesce(season_baseline_second_chance_efficiency, second_chance_efficiency_l10) as baseline_second_chance_efficiency,
        
        -- =================================================================
        -- PRIMARY DEVIATION FEATURES
        -- =================================================================
        
        -- Paint Scoring Reliance Deviation
        paint_scoring_pct_l10 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10) as paint_scoring_reliance_deviation,
        
        -- Mid-Range Scoring Deviation  
        midrange_scoring_pct_l10 - coalesce(season_baseline_midrange_pct, midrange_scoring_pct_l10) as midrange_scoring_deviation,
        
        -- Interior vs Perimeter Balance Deviation
        (paint_scoring_pct_l10 + midrange_scoring_pct_l10) - 
        (coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10) + coalesce(season_baseline_midrange_pct, midrange_scoring_pct_l10)) as interior_balance_deviation,
        
        -- Shot Creation Deviation (Self-created 2PT shots)
        unassisted_twopt_pct_l10 - coalesce(season_baseline_unassisted_twopt_pct, unassisted_twopt_pct_l10) as shot_creation_deviation,
        
        -- Rim Finishing Efficiency Deviation
        rim_fg_pct_l10 - coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10) as rim_efficiency_deviation,
        
        -- Rim Attempt Rate Deviation
        rim_attempt_rate_l10 - coalesce(season_baseline_rim_attempt_rate, rim_attempt_rate_l10) as rim_attempt_rate_deviation,
        
        -- Second Chance Scoring Deviation
        second_chance_efficiency_l10 - coalesce(season_baseline_second_chance_efficiency, second_chance_efficiency_l10) as second_chance_efficiency_deviation,
        
        -- =================================================================
        -- TREND-BASED DEVIATIONS (SHORT vs LONG TERM)
        -- =================================================================
        
        -- Recent Paint Scoring Trend (3-game vs 10-game)
        paint_scoring_pct_l3 - paint_scoring_pct_l10 as paint_scoring_short_term_trend,
        
        -- Recent Mid-Range Trend
        midrange_scoring_pct_l3 - midrange_scoring_pct_l10 as midrange_short_term_trend,
        
        -- Recent vs Baseline Paint Trend
        paint_scoring_pct_l3 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10) as paint_vs_baseline_trend,
        
        -- Rim Performance Recent Trend
        rim_fg_pct_l3 - rim_fg_pct_l10 as rim_efficiency_short_term_trend,
        
        -- =================================================================
        -- STANDARDIZED DEVIATIONS (Z-SCORES)
        -- =================================================================
        
        -- Paint Scoring Z-Score
        case 
            when coalesce(season_stddev_paint_scoring_pct, 0) > 0 
            then (paint_scoring_pct_l10 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10)) / 
                 season_stddev_paint_scoring_pct
            else 0 
        end as paint_scoring_z_score,
        
        -- Rim Efficiency Z-Score
        case 
            when coalesce(season_stddev_rim_fg_pct, 0) > 0 
            then (rim_fg_pct_l10 - coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10)) / 
                 season_stddev_rim_fg_pct
            else 0 
        end as rim_efficiency_z_score,
        
        -- =================================================================
        -- COMPOSITE INTERIOR SCORING INDICES
        -- =================================================================
        
        -- Interior Dominance Index (paint + rim efficiency combined)
        (paint_scoring_pct_l10 * 0.6) + (rim_fg_pct_l10 * 0.4) as interior_dominance_index,
        
        -- Shot Profile Balance Score (1.0 = perfectly balanced interior game)
        1.0 - abs(paint_scoring_pct_l10 - midrange_scoring_pct_l10) as interior_shot_balance_score,
        
        -- Self-Creation Interior Index
        unassisted_twopt_pct_l10 * twopt_attempt_pct_l10 as interior_self_creation_index,
        
        -- =================================================================
        -- DEVIATION MAGNITUDE CATEGORIES
        -- =================================================================
        
        -- Paint Scoring Deviation Category
        case 
            when abs(paint_scoring_pct_l10 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10)) > 15 then 'MAJOR'
            when abs(paint_scoring_pct_l10 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10)) > 7 then 'MODERATE'
            when abs(paint_scoring_pct_l10 - coalesce(season_baseline_paint_scoring_pct, paint_scoring_pct_l10)) > 3 then 'MINOR'
            else 'NORMAL'
        end as paint_scoring_deviation_category,
        
        -- Rim Efficiency Deviation Category
        case 
            when abs(rim_fg_pct_l10 - coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10)) > 10 then 'MAJOR'
            when abs(rim_fg_pct_l10 - coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10)) > 5 then 'MODERATE'
            when abs(rim_fg_pct_l10 - coalesce(season_baseline_rim_fg_pct, rim_fg_pct_l10)) > 2 then 'MINOR'
            else 'NORMAL'
        end as rim_efficiency_deviation_category,
        
        updated_at
        
    from season_baselines
)

select * from interior_scoring_deviations