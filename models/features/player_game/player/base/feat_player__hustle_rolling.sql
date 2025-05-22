{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'hustle'],
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

with hustle_boxscore as (
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

        -- Hustle Boxscore Stats
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
        box_out_player_reb,
        box_out_team_reb,
        tot_box_outs,

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

        -- Hustle Boxscore Stats
        round(({{ calculate_rolling_avg('cont_shots', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_shots_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_shots', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_shots_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_shots', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_shots_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_shots', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_shots_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_shots', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_shots_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_shots', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_shots_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_2pt', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_2pt', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_2pt', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_3pt', 'player_id', 'game_date', 3) }})::numeric, 3) as cont_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_3pt', 'player_id', 'game_date', 5) }})::numeric, 3) as cont_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_3pt', 'player_id', 'game_date', 10) }})::numeric, 3) as cont_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('deflections', 'player_id', 'game_date', 3) }})::numeric, 3) as deflections_roll_3g_avg,
        round(({{ calculate_rolling_avg('deflections', 'player_id', 'game_date', 5) }})::numeric, 3) as deflections_roll_5g_avg,
        round(({{ calculate_rolling_avg('deflections', 'player_id', 'game_date', 10) }})::numeric, 3) as deflections_roll_10g_avg,
        round(({{ calculate_rolling_stddev('deflections', 'player_id', 'game_date', 3) }})::numeric, 3) as deflections_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('deflections', 'player_id', 'game_date', 5) }})::numeric, 3) as deflections_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('deflections', 'player_id', 'game_date', 10) }})::numeric, 3) as deflections_roll_10g_stddev,

        round(({{ calculate_rolling_avg('charges_drawn', 'player_id', 'game_date', 3) }})::numeric, 3) as charges_drawn_roll_3g_avg,
        round(({{ calculate_rolling_avg('charges_drawn', 'player_id', 'game_date', 5) }})::numeric, 3) as charges_drawn_roll_5g_avg,
        round(({{ calculate_rolling_avg('charges_drawn', 'player_id', 'game_date', 10) }})::numeric, 3) as charges_drawn_roll_10g_avg,
        round(({{ calculate_rolling_stddev('charges_drawn', 'player_id', 'game_date', 3) }})::numeric, 3) as charges_drawn_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('charges_drawn', 'player_id', 'game_date', 5) }})::numeric, 3) as charges_drawn_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('charges_drawn', 'player_id', 'game_date', 10) }})::numeric, 3) as charges_drawn_roll_10g_stddev,

        round(({{ calculate_rolling_avg('screen_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as screen_ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('screen_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as screen_ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('screen_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as screen_ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('screen_ast', 'player_id', 'game_date', 3) }})::numeric, 3) as screen_ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast', 'player_id', 'game_date', 5) }})::numeric, 3) as screen_ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast', 'player_id', 'game_date', 10) }})::numeric, 3) as screen_ast_roll_10g_stddev,

        round(({{ calculate_rolling_avg('screen_ast_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as screen_ast_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('screen_ast_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as screen_ast_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('screen_ast_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as screen_ast_pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'player_id', 'game_date', 3) }})::numeric, 3) as screen_ast_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'player_id', 'game_date', 5) }})::numeric, 3) as screen_ast_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'player_id', 'game_date', 10) }})::numeric, 3) as screen_ast_pts_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as off_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as off_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as off_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as off_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as off_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as off_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as def_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as def_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as def_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as def_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as def_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as def_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as tot_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as tot_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as tot_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'player_id', 'game_date', 3) }})::numeric, 3) as tot_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'player_id', 'game_date', 5) }})::numeric, 3) as tot_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'player_id', 'game_date', 10) }})::numeric, 3) as tot_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as off_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as off_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as off_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as off_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as off_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as off_box_outs_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as def_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as def_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as def_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as def_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as def_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as def_box_outs_roll_10g_stddev,

        round(({{ calculate_rolling_avg('box_out_player_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as box_out_player_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('box_out_player_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as box_out_player_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('box_out_player_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as box_out_player_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as box_out_player_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as box_out_player_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as box_out_player_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('box_out_team_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as box_out_team_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('box_out_team_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as box_out_team_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('box_out_team_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as box_out_team_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'player_id', 'game_date', 3) }})::numeric, 3) as box_out_team_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'player_id', 'game_date', 5) }})::numeric, 3) as box_out_team_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'player_id', 'game_date', 10) }})::numeric, 3) as box_out_team_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tot_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as tot_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('tot_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as tot_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('tot_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as tot_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'player_id', 'game_date', 3) }})::numeric, 3) as tot_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'player_id', 'game_date', 5) }})::numeric, 3) as tot_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'player_id', 'game_date', 10) }})::numeric, 3) as tot_box_outs_roll_10g_stddev,
        
        hustle_boxscore.updated_at

    from hustle_boxscore
)

select *
from final 