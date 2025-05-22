{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'misc'],
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

with misc_boxscore as (
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

        -- Misc Boxscore Stats
        pts_off_tov,
        second_chance_pts,
        fastbreak_pts,
        pts_in_paint,
        opp_pts_off_tov_while_on,
        opp_second_chance_pts_while_on,
        opp_fastbreak_pts_while_on,
        opp_pts_in_paint_while_on,
        blk_against,
        fouls_drawn,

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

        -- Misc Boxscore Stats
        round(({{ calculate_rolling_avg('pts_off_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_off_tov_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts_off_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_off_tov_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts_off_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_off_tov_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pts_off_tov', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_off_tov_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts_off_tov', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_off_tov_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts_off_tov', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_off_tov_roll_10g_stddev,

        round(({{ calculate_rolling_avg('second_chance_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as second_chance_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('second_chance_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as second_chance_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('second_chance_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as second_chance_pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as second_chance_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as second_chance_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('second_chance_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as second_chance_pts_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fastbreak_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as fastbreak_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('fastbreak_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as fastbreak_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('fastbreak_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as fastbreak_pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as fastbreak_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as fastbreak_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fastbreak_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as fastbreak_pts_roll_10g_stddev,

        round(({{ calculate_rolling_avg('pts_in_paint', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_in_paint_roll_3g_avg,
        round(({{ calculate_rolling_avg('pts_in_paint', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_in_paint_roll_5g_avg,
        round(({{ calculate_rolling_avg('pts_in_paint', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_in_paint_roll_10g_avg,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'player_id', 'game_date', 3) }})::numeric, 3) as pts_in_paint_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'player_id', 'game_date', 5) }})::numeric, 3) as pts_in_paint_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('pts_in_paint', 'player_id', 'game_date', 10) }})::numeric, 3) as pts_in_paint_roll_10g_stddev,

        round(({{ calculate_rolling_avg('opp_pts_off_tov_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_pts_off_tov_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_pts_off_tov_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_10g_avg,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_off_tov_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_pts_off_tov_while_on_roll_10g_stddev,

        round(({{ calculate_rolling_avg('opp_second_chance_pts_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_second_chance_pts_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_second_chance_pts_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_10g_avg,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_second_chance_pts_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_second_chance_pts_while_on_roll_10g_stddev,

        round(({{ calculate_rolling_avg('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_10g_avg,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_fastbreak_pts_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_fastbreak_pts_while_on_roll_10g_stddev,

        round(({{ calculate_rolling_avg('opp_pts_in_paint_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_3g_avg,
        round(({{ calculate_rolling_avg('opp_pts_in_paint_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_5g_avg,
        round(({{ calculate_rolling_avg('opp_pts_in_paint_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_10g_avg,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint_while_on', 'player_id', 'game_date', 3) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint_while_on', 'player_id', 'game_date', 5) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('opp_pts_in_paint_while_on', 'player_id', 'game_date', 10) }})::numeric, 3) as opp_pts_in_paint_while_on_roll_10g_stddev,

        round(({{ calculate_rolling_avg('blk_against', 'player_id', 'game_date', 3) }})::numeric, 3) as blk_against_roll_3g_avg,
        round(({{ calculate_rolling_avg('blk_against', 'player_id', 'game_date', 5) }})::numeric, 3) as blk_against_roll_5g_avg,
        round(({{ calculate_rolling_avg('blk_against', 'player_id', 'game_date', 10) }})::numeric, 3) as blk_against_roll_10g_avg,
        round(({{ calculate_rolling_stddev('blk_against', 'player_id', 'game_date', 3) }})::numeric, 3) as blk_against_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('blk_against', 'player_id', 'game_date', 5) }})::numeric, 3) as blk_against_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('blk_against', 'player_id', 'game_date', 10) }})::numeric, 3) as blk_against_roll_10g_stddev,

        round(({{ calculate_rolling_avg('fouls_drawn', 'player_id', 'game_date', 3) }})::numeric, 3) as fouls_drawn_roll_3g_avg,
        round(({{ calculate_rolling_avg('fouls_drawn', 'player_id', 'game_date', 5) }})::numeric, 3) as fouls_drawn_roll_5g_avg,
        round(({{ calculate_rolling_avg('fouls_drawn', 'player_id', 'game_date', 10) }})::numeric, 3) as fouls_drawn_roll_10g_avg,
        round(({{ calculate_rolling_stddev('fouls_drawn', 'player_id', 'game_date', 3) }})::numeric, 3) as fouls_drawn_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('fouls_drawn', 'player_id', 'game_date', 5) }})::numeric, 3) as fouls_drawn_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('fouls_drawn', 'player_id', 'game_date', 10) }})::numeric, 3) as fouls_drawn_roll_10g_stddev,
        misc_boxscore.updated_at

    from misc_boxscore
)

select *
from final 