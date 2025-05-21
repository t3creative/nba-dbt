{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'defensive'],
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

with defensive_boxscore as (
    select
        -- Identifiers & Metadata
        player_game_key,
        game_id,
        player_id,
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Defensive Boxscore Stats
        matchup_min,
        partial_poss,
        def_switches,
        pts_allowed,
        ast_allowed,
        tov_forced,
        matchup_fgm,
        matchup_fga,
        matchup_fg_pct,
        matchup_fg3m,
        matchup_fg3a,
        matchup_fg3_pct,

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
        team_id,
        opponent_id,
        game_date,
        season_year,

        -- Defensive Boxscore Stats
        round(({{ calculate_rolling_avg('matchup_min', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_min_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_min', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_min_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_min', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_min_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_min', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_min_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_min', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_min_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_min', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_min_roll_10g_stddev,

        round(({{ calculate_rolling_avg('partial_poss', 'player_id', 'game_date', 3) }})::numeric, 3) as partial_poss_roll_3g_avg,
        round(({{ calculate_rolling_avg('partial_poss', 'player_id', 'game_date', 5) }})::numeric, 3) as partial_poss_roll_5g_avg,
        round(({{ calculate_rolling_avg('partial_poss', 'player_id', 'game_date', 10) }})::numeric, 3) as partial_poss_roll_10g_avg,
        round(({{ calculate_rolling_stddev('partial_poss', 'player_id', 'game_date', 3) }})::numeric, 3) as partial_poss_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('partial_poss', 'player_id', 'game_date', 5) }})::numeric, 3) as partial_poss_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('partial_poss', 'player_id', 'game_date', 10) }})::numeric, 3) as partial_poss_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_switches', 'player_id', 'game_date', 3) }})::numeric, 3) as def_switches_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_switches', 'player_id', 'game_date', 5) }})::numeric, 3) as def_switches_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_switches', 'player_id', 'game_date', 10) }})::numeric, 3) as def_switches_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_switches', 'player_id', 'game_date', 3) }})::numeric, 3) as def_switches_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_switches', 'player_id', 'game_date', 5) }})::numeric, 3) as def_switches_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_switches', 'player_id', 'game_date', 10) }})::numeric, 3) as def_switches_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pts_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_allowed_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pts_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_allowed_roll_10g_stddev,

        round(({{ calculate_rolling_avg('ast_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('ast_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('ast_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_allowed_roll_10g_avg,
        round(({{ calculate_rolling_stddev('ast_allowed', 'player_id', 'game_date', 3) }})::numeric, 3) as ast_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('ast_allowed', 'player_id', 'game_date', 5) }})::numeric, 3) as ast_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('ast_allowed', 'player_id', 'game_date', 10) }})::numeric, 3) as ast_allowed_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tov_forced', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_forced_roll_3g_avg,
        round(({{ calculate_rolling_avg('tov_forced', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_forced_roll_5g_avg,
        round(({{ calculate_rolling_avg('tov_forced', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_forced_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tov_forced', 'player_id', 'game_date', 3) }})::numeric, 3) as tov_forced_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tov_forced', 'player_id', 'game_date', 5) }})::numeric, 3) as tov_forced_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tov_forced', 'player_id', 'game_date', 10) }})::numeric, 3) as tov_forced_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fgm_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fgm_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fgm_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fgm', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fgm_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fgm', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fgm_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fgm', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fgm_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fga_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fga_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fga_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fga', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fga_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fga', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fga_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fga', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fga_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fg_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg_pct_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3m_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3m_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3m_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fg3m', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3m_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3m', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3m_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3m', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3m_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3a_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3a_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3a_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fg3a', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3a_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3a', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3a_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3a', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3a_roll_10g_stddev,

        round(({{ calculate_rolling_avg('matchup_fg3_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3_pct_roll_3g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3_pct_roll_5g_avg,
        round(({{ calculate_rolling_avg('matchup_fg3_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3_pct_roll_10g_avg,
        round(({{ calculate_rolling_stddev('matchup_fg3_pct', 'player_id', 'game_date', 3) }})::numeric, 3) as matchup_fg3_pct_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3_pct', 'player_id', 'game_date', 5) }})::numeric, 3) as matchup_fg3_pct_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('matchup_fg3_pct', 'player_id', 'game_date', 10) }})::numeric, 3) as matchup_fg3_pct_roll_10g_stddev,
        defensive_boxscore.updated_at

    from defensive_boxscore
)

select *
from final 