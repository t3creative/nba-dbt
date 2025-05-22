{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'usage'],
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

with usage_boxscore as (
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

        -- Usage Boxscore Stats
        pct_of_team_fgm,
        pct_of_team_fga,
        pct_of_team_fg3m,
        pct_of_team_fg3a,
        pct_of_team_ftm,
        pct_of_team_fta,
        pct_of_team_oreb,
        pct_of_team_dreb,
        pct_of_team_reb,
        pct_of_team_ast,
        pct_of_team_tov,
        pct_of_team_stl,
        pct_of_team_blk,
        pct_of_team_blk_allowed,
        pct_of_team_pf,
        pct_of_team_pfd,
        pct_of_team_pts,

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

        -- Usage Boxscore Stats
        round(({{ calculate_rolling_avg('pct_of_team_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fg3m_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fg3m_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fg3m_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fg3m_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fg3m_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fg3m_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fg3a_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fg3a_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fg3a_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fg3a_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fg3a_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fg3a_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_ftm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_ftm_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_ftm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_ftm_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_ftm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_ftm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_ftm', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_ftm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_ftm', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_ftm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_ftm', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_ftm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_fta', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fta_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fta', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fta_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_fta', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fta_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_fta', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_fta_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fta', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_fta_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_fta', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_fta_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_oreb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_oreb_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_oreb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_oreb_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_oreb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_oreb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_oreb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_oreb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_oreb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_oreb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_oreb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_oreb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_dreb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_dreb_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_dreb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_dreb_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_dreb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_dreb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_dreb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_dreb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_dreb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_dreb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_dreb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_dreb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_ast_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_tov_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_tov_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_stl', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_stl_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_stl', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_stl_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_stl', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_stl_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_stl', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_stl_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_stl', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_stl_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_stl', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_stl_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_blk', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_blk_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_blk', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_blk_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_blk', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_blk_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_blk', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_blk_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_blk', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_blk_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_blk', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_blk_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_blk_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_blk_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_blk_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_blk_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_blk_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_blk_allowed_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_blk_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_blk_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_blk_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_blk_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_blk_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_blk_allowed_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_pf', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pf_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pf', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pf_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pf', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pf_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_pf', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pf_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pf', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pf_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pf', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pf_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_pfd', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pfd_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pfd', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pfd_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pfd', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pfd_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_pfd', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pfd_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pfd', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pfd_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pfd', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pfd_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pct_of_team_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('pct_of_team_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pct_of_team_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as pct_of_team_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as pct_of_team_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pct_of_team_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as pct_of_team_pts_roll_10g_stddev,
        usage_boxscore.updated_at

    from usage_boxscore
)

select *
from final 