{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    tags=['features', 'pga', 'team', 'base', 'rolling', 'scoring'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year']}
    ]
) }}

WITH scoring_boxscore AS (
    SELECT
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Scoring Stats from int_team__combined_boxscore
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

    FROM {{ ref('int_team__combined_boxscore') }}

    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= cast(substring('{{ var('training_start_season_year') }}' from 1 for 4) as integer)
    {% if is_incremental() %}
    -- Look back a fixed number of days to ensure rolling calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest rolling window (10 games).
    and game_date >= (
        select max(game_date) - interval '90 days'
        from {{ this }}
    )
    {% endif %}
),

final as (
    select
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Rolling Scoring Stats (Avg and StdDev for 3, 5, and 10 games)
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_fga_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_fga_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_fga_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_fga_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_fga_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_fga_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_fga_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_fga_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_fga_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_fga_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_fga_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_fga_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_fga_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_fga_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_fga_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_midrange_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_midrange_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_midrange_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_midrange_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_midrange_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_midrange_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_fastbreak_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_fastbreak_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_fastbreak_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_fastbreak_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_fastbreak_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_fastbreak_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_ft', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_ft_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_ft', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_ft_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_ft', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_ft_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_ft_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_ft_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_ft_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_off_tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_off_tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_off_tov_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_off_tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_off_tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_off_tov_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_in_paint_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_in_paint_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_in_paint_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_pts_in_paint_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_pts_in_paint_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_pts_in_paint_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'team_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'team_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'team_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_fgm_roll_10g_stddev,

        scoring_boxscore.updated_at

    from scoring_boxscore
)

select *
from final
