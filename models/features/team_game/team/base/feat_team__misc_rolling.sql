{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'pga', 'team', 'base', 'rolling', 'misc'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['team_id', 'game_date']},
        {'columns': ['game_id']},
        {'columns': ['season_year']}
    ]
)
}}

WITH misc_boxscore AS (
    SELECT
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Misc Stats from int_team__combined_boxscore
        pts_off_tov,
        second_chance_pts,
        fastbreak_pts,
        pts_in_paint,
        opp_pts_off_tov,        -- Points team's defense allowed off opponent turnovers
        opp_second_chance_pts,  -- Points team's defense allowed on opponent second chances
        opp_fastbreak_pts,      -- Points team's defense allowed on opponent fastbreaks
        opp_pts_in_paint,       -- Points team's defense allowed in the paint by opponent

        -- Timestamps for Incremental
        updated_at

    FROM {{ ref('int_team__combined_boxscore') }}
    -- Filter based on the starting year extracted from season_year
    where cast(substring(season_year from 1 for 4) as integer) >= cast(substring('{{ var('training_start_season_year') }}' from 1 for 4) as integer)
    {% if is_incremental() %}
    -- Look back a fixed number of days to ensure rolling calculations are correct near the incremental boundary.
    -- This window should be large enough to capture the game history needed for the longest rolling window (10 games).
    -- Assuming a game is played every ~2 days on average, 30 days should be sufficient for 10 games.
    and game_date >= (
        select max(game_date) - interval '30 days'
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

        -- Rolling Misc Stats (Averages)
        round(({{ calculate_rolling_avg('pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as pts_off_tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as pts_off_tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as pts_off_tov_roll_10g_avg,
        round(({{ calculate_rolling_avg('second_chance_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as second_chance_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('second_chance_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as second_chance_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('second_chance_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as second_chance_pts_roll_10g_avg,
        round(({{ calculate_rolling_avg('fastbreak_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as fastbreak_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('fastbreak_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as fastbreak_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('fastbreak_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as fastbreak_pts_roll_10g_avg,
        round(({{ calculate_rolling_avg('pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as pts_in_paint_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as pts_in_paint_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as pts_in_paint_roll_10g_avg,
        round(({{ calculate_rolling_avg('opp_pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_10g_avg,
        round(({{ calculate_rolling_avg('opp_second_chance_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_second_chance_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_second_chance_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_10g_avg,
        round(({{ calculate_rolling_avg('opp_fastbreak_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_fastbreak_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_fastbreak_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_10g_avg,
        round(({{ calculate_rolling_avg('opp_pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_10g_avg,

        -- Rolling Misc Stats (Standard Deviations)
        round(({{ calculate_rolling_stddev('pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as pts_off_tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as pts_off_tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as pts_off_tov_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as second_chance_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as second_chance_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as second_chance_pts_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as fastbreak_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as fastbreak_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as fastbreak_pts_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as pts_in_paint_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as pts_in_paint_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as pts_in_paint_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_pts_off_tov_allowed_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_second_chance_pts_allowed_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_fastbreak_pts_allowed_roll_10g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint', 'team_id', 'game_date', 3) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint', 'team_id', 'game_date', 5) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint', 'team_id', 'game_date', 10) }})::numeric, 3) as opp_pts_in_paint_allowed_roll_10g_stddev,

        misc_boxscore.updated_at,
        CURRENT_TIMESTAMP AS created_at

    from misc_boxscore
)

select *
from final
