{{ config(
    schema='features',
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'team', 'shooting', 'pregame', 'ml_safe'],
    partition_by={
        "field": "game_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=['team_id', 'season_year'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']}
    ]
) }}

/*
ML-SAFE Team Shot Distribution Profile - PRE-GAME ONLY
Uses historical shooting patterns exclusively for prediction models.
No current game shot distribution data included.
*/

with base_games as (
    select
        team_game_key,
        game_id,
        team_id,
        opponent_id,
        season_year,
        game_date,
        home_away
    from {{ ref('feat_opp__game_opponents_v2') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

-- Get team scoring profile rolling metrics
team_scoring_rolling as (
    select * from {{ ref('feat_team__scoring_rolling_v2') }}
),

-- Get traditional metrics for context
team_traditional_rolling as (
    select * from {{ ref('feat_team__traditional_rolling_v2') }}
),

-- Calculate league averages for relative metrics (using historical data)
league_averages as (
    select
        season_year,
        game_date,
        
        -- Rolling league averages (excluding current game)
        avg(pct_fga_3pt_avg_l10) over (
            partition by season_year
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as league_avg_3pt_attempt_rate,
        
        avg(pct_pts_in_paint_avg_l10) over (
            partition by season_year
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as league_avg_paint_scoring_rate,
        
        avg(pct_pts_fastbreak_avg_l10) over (
            partition by season_year
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as league_avg_fastbreak_rate,
        
        avg(pct_assisted_fgm_avg_l10) over (
            partition by season_year
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as league_avg_assisted_rate
        
    from team_scoring_rolling
),

shot_distribution_profile as (
    select
        bg.team_game_key,
        bg.game_id,
        bg.team_id,
        bg.opponent_id,
        bg.season_year,
        bg.game_date,
        bg.home_away,
        
        -- =================================================================
        -- RECENT SHOT DISTRIBUTION (5-GAME ROLLING)
        -- =================================================================
        coalesce(tsr.pct_fga_2pt_avg_l5, 0) as recent_2pt_attempt_pct,
        coalesce(tsr.pct_fga_3pt_avg_l5, 0) as recent_3pt_attempt_pct,
        coalesce(tsr.pct_pts_2pt_avg_l5, 0) as recent_2pt_scoring_pct,
        coalesce(tsr.pct_pts_3pt_avg_l5, 0) as recent_3pt_scoring_pct,
        coalesce(tsr.pct_pts_midrange_2pt_avg_l5, 0) as recent_midrange_pct,
        coalesce(tsr.pct_pts_in_paint_avg_l5, 0) as recent_paint_scoring_pct,
        
        -- Shot Creation Profile
        coalesce(tsr.pct_assisted_2pt_avg_l5, 0) as recent_assisted_2pt_pct,
        coalesce(tsr.pct_unassisted_2pt_avg_l5, 0) as recent_unassisted_2pt_pct,
        coalesce(tsr.pct_assisted_3pt_avg_l5, 0) as recent_assisted_3pt_pct,
        coalesce(tsr.pct_unassisted_3pt_avg_l5, 0) as recent_unassisted_3pt_pct,
        coalesce(tsr.pct_assisted_fgm_avg_l5, 0) as recent_assisted_fgm_pct,
        coalesce(tsr.pct_unassisted_fgm_avg_l5, 0) as recent_unassisted_fgm_pct,
        
        -- Play Style Metrics
        coalesce(tsr.pct_pts_fastbreak_avg_l5, 0) as recent_fastbreak_pct,
        coalesce(tsr.pct_pts_off_tov_avg_l5, 0) as recent_pts_off_tov_pct,
        coalesce(tsr.pct_pts_ft_avg_l5, 0) as recent_ft_scoring_pct,
        
        -- =================================================================
        -- LONGER-TERM BASELINE (10-GAME ROLLING)
        -- =================================================================
        coalesce(tsr.pct_fga_3pt_avg_l10, 0) as baseline_3pt_attempt_pct,
        coalesce(tsr.pct_pts_in_paint_avg_l10, 0) as baseline_paint_scoring_pct,
        coalesce(tsr.pct_pts_fastbreak_avg_l10, 0) as baseline_fastbreak_pct,
        coalesce(tsr.pct_assisted_fgm_avg_l10, 0) as baseline_assisted_fgm_pct,
        coalesce(tsr.pct_pts_3pt_avg_l10, 0) as baseline_3pt_scoring_pct,
        coalesce(tsr.pct_pts_midrange_2pt_avg_l10, 0) as baseline_midrange_pct,
        
        -- =================================================================
        -- LEAGUE-RELATIVE METRICS (HISTORICAL COMPARISON)
        -- =================================================================
        
        -- Three-Point Tendency vs League
        case 
            when coalesce(la.league_avg_3pt_attempt_rate, 35) > 0
            then coalesce(tsr.pct_fga_3pt_avg_l10, 0) / la.league_avg_3pt_attempt_rate
            else 1.0
        end as rel_3pt_attempt_rate,
        
        -- Paint Scoring vs League
        case 
            when coalesce(la.league_avg_paint_scoring_rate, 45) > 0
            then coalesce(tsr.pct_pts_in_paint_avg_l10, 0) / la.league_avg_paint_scoring_rate
            else 1.0
        end as rel_paint_scoring_rate,
        
        -- Fastbreak Scoring vs League
        case 
            when coalesce(la.league_avg_fastbreak_rate, 12) > 0
            then coalesce(tsr.pct_pts_fastbreak_avg_l10, 0) / la.league_avg_fastbreak_rate
            else 1.0
        end as rel_fastbreak_rate,
        
        -- Ball Movement vs League
        case 
            when coalesce(la.league_avg_assisted_rate, 60) > 0
            then coalesce(tsr.pct_assisted_fgm_avg_l10, 0) / la.league_avg_assisted_rate
            else 1.0
        end as rel_assisted_rate,
        
        -- =================================================================
        -- SHOT DISTRIBUTION TRENDS (RECENT vs BASELINE)
        -- =================================================================
        coalesce(tsr.pct_fga_3pt_avg_l5, 0) - coalesce(tsr.pct_fga_3pt_avg_l10, 0) as three_point_attempt_trend,
        coalesce(tsr.pct_pts_in_paint_avg_l5, 0) - coalesce(tsr.pct_pts_in_paint_avg_l10, 0) as paint_scoring_trend,
        coalesce(tsr.pct_pts_fastbreak_avg_l5, 0) - coalesce(tsr.pct_pts_fastbreak_avg_l10, 0) as fastbreak_trend,
        coalesce(tsr.pct_assisted_fgm_avg_l5, 0) - coalesce(tsr.pct_assisted_fgm_avg_l10, 0) as ball_movement_trend,
        
        -- =================================================================
        -- SHOT PROFILE CONSISTENCY METRICS
        -- =================================================================
        coalesce(tsr.pct_fga_3pt_stddev_l5, 0) as three_point_attempt_volatility,
        coalesce(tsr.pct_pts_in_paint_stddev_l5, 0) as paint_scoring_volatility,
        coalesce(tsr.pct_pts_fastbreak_stddev_l5, 0) as fastbreak_volatility,
        
        -- =================================================================
        -- TEAM PLAYSTYLE CLASSIFICATION
        -- =================================================================
        
        -- Primary Playstyle (based on relative rates)
        case
            when (case when coalesce(la.league_avg_3pt_attempt_rate, 35) > 0
                       then coalesce(tsr.pct_fga_3pt_avg_l10, 0) / la.league_avg_3pt_attempt_rate
                       else 1.0 end) > 1.2 then 'THREE_POINT_HEAVY'
            when (case when coalesce(la.league_avg_paint_scoring_rate, 45) > 0
                       then coalesce(tsr.pct_pts_in_paint_avg_l10, 0) / la.league_avg_paint_scoring_rate
                       else 1.0 end) > 1.2 then 'PAINT_FOCUSED'
            when (case when coalesce(la.league_avg_fastbreak_rate, 12) > 0
                       then coalesce(tsr.pct_pts_fastbreak_avg_l10, 0) / la.league_avg_fastbreak_rate
                       else 1.0 end) > 1.2 then 'FAST_PACED'
            when (case when coalesce(la.league_avg_assisted_rate, 60) > 0
                       then coalesce(tsr.pct_assisted_fgm_avg_l10, 0) / la.league_avg_assisted_rate
                       else 1.0 end) > 1.2 then 'BALL_MOVEMENT'
            else 'BALANCED'
        end as team_playstyle_primary,
        
        -- Shot Creation Style
        case
            when coalesce(tsr.pct_unassisted_fgm_avg_l10, 0) > 45 then 'ISO_HEAVY'
            when coalesce(tsr.pct_assisted_fgm_avg_l10, 0) > 70 then 'TEAM_ORIENTED'
            else 'MIXED_CREATION'
        end as shot_creation_style,
        
        -- Interior vs Perimeter Balance
        case
            when coalesce(tsr.pct_pts_in_paint_avg_l10, 0) > 50 then 'INTERIOR_DOMINANT'
            when coalesce(tsr.pct_pts_3pt_avg_l10, 0) > 40 then 'PERIMETER_DOMINANT'
            else 'BALANCED_ATTACK'
        end as interior_perimeter_balance,
        
        -- =================================================================
        -- COMPOSITE SHOOTING INDICES
        -- =================================================================
        
        -- Modern Offense Index (3PT + Paint, avoiding mid-range)
        (coalesce(tsr.pct_pts_3pt_avg_l10, 0) + coalesce(tsr.pct_pts_in_paint_avg_l10, 0)) - 
        coalesce(tsr.pct_pts_midrange_2pt_avg_l10, 0) as modern_offense_index,
        
        -- Ball Movement Efficiency (assisted shots with good efficiency context)
        coalesce(tsr.pct_assisted_fgm_avg_l10, 0) * 
        (coalesce(ttr.fg_pct_avg_l10, 45) / 45.0) as ball_movement_efficiency_index,
        
        -- Pace and Space Index (3PT rate + fastbreak rate)
        (coalesce(tsr.pct_fga_3pt_avg_l10, 0) / 100.0) + 
        (coalesce(tsr.pct_pts_fastbreak_avg_l10, 0) / 100.0) as pace_space_index,
        
        -- =================================================================
        -- SHOT DISTRIBUTION STABILITY SCORE
        -- =================================================================
        
        -- Lower volatility = more predictable shot distribution
        100 - least(100, 
            coalesce(tsr.pct_fga_3pt_stddev_l5, 0) + 
            coalesce(tsr.pct_pts_in_paint_stddev_l5, 0) + 
            coalesce(tsr.pct_pts_fastbreak_stddev_l5, 0)
        ) as shot_distribution_stability_score,
        
        greatest(
            coalesce(tsr.updated_at, '1900-01-01'::timestamp),
            coalesce(ttr.updated_at, '1900-01-01'::timestamp)
        ) as updated_at
        
    from base_games bg
    left join team_scoring_rolling tsr on bg.team_game_key = tsr.team_game_key
    left join team_traditional_rolling ttr on bg.team_game_key = ttr.team_game_key
    left join league_averages la 
        on bg.season_year = la.season_year 
        and bg.game_date = la.game_date
),

deduped_shot_distribution_profile AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY team_game_key ORDER BY game_date DESC, updated_at DESC) AS row_num
        FROM shot_distribution_profile
    ) t
    WHERE row_num = 1
)

select * from deduped_shot_distribution_profile