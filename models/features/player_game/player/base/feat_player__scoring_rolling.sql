{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'scoring'],
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

with scoring_boxscore as (
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

        -- Scoring Boxscore Stats
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

    from {{ ref('int_player__combined_boxscore') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= cast(substring('{{ var('training_start_season_year') }}' from 1 for 4) as integer)
    {% if is_incremental() %}
    -- Look back a fixed number of days to ensure rolling calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest rolling window.
    and game_date >= (
        select max(game_date) - interval '90 days'
        from {{ this }}
    )
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

        -- Scoring Boxscore Stats
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_fga_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_fga_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_fga_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_fga_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_fga_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_fga_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_fga_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_fga_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_fga_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_fga_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_fga_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_fga_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_fga_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_fga_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_fga_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_fga_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_fga_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_midrange_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_midrange_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_midrange_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_midrange_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_midrange_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_midrange_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_midrange_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_midrange_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_fastbreak_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_fastbreak_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_fastbreak', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_fastbreak_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_fastbreak_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_fastbreak_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_fastbreak', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_fastbreak_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_ft', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_ft_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_ft', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_ft_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_ft', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_ft_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_ft_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_ft_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_ft', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_ft_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_off_tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_off_tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_off_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_off_tov_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_off_tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_off_tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_off_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_off_tov_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_in_paint_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_in_paint_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_pts_in_paint', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_in_paint_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_pts_in_paint_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_pts_in_paint_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_pts_in_paint', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_pts_in_paint_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_assisted_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_assisted_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_assisted_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_assisted_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_assisted_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_unassisted_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_unassisted_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_unassisted_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_unassisted_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_unassisted_fgm_roll_10g_stddev,
        
        scoring_boxscore.updated_at

    from scoring_boxscore
)

select *
from final