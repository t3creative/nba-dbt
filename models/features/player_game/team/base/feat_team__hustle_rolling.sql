{{ config(
    materialized='incremental',
    unique_key='team_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'stats', 'rolling', 'hustle'],
    indexes=[
        {'columns': ['team_game_key'], 'unique': True},
        {'columns': ['game_id']},
        {'columns': ['team_id']},
        {'columns': ['game_date']},
        {'columns': ['season_year']}
    ]
)
}}

with hustle_boxscore as ( -- Renamed CTE
    select
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Hustle Stats from int_team__combined_boxscore
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

        -- Timestamps for Incremental
        updated_at -- Included updated_at from source

    FROM {{ ref('int_team__combined_boxscore') }}
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

final as ( -- Renamed CTE
    select
        -- Identifiers & Metadata
        team_game_key,
        game_id,
        team_id,
        game_date,
        season_year,

        -- Rolling Hustle Stats (Avg and StdDev for 3, 5, 10 games)
        round(({{ calculate_rolling_avg('cont_shots', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_shots_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_shots', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_shots_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_shots', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_shots_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_shots', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_shots_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_shots', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_shots_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_shots', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_shots_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_2pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_2pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_2pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_2pt', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_2pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_2pt', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_2pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_2pt', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_2pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('cont_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_3pt_roll_3g_avg,
        round(({{ calculate_rolling_avg('cont_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_3pt_roll_5g_avg,
        round(({{ calculate_rolling_avg('cont_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_3pt_roll_10g_avg,
        round(({{ calculate_rolling_stddev('cont_3pt', 'team_id', 'game_date', 3) }})::numeric, 3) as cont_3pt_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('cont_3pt', 'team_id', 'game_date', 5) }})::numeric, 3) as cont_3pt_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('cont_3pt', 'team_id', 'game_date', 10) }})::numeric, 3) as cont_3pt_roll_10g_stddev,

        round(({{ calculate_rolling_avg('deflections', 'team_id', 'game_date', 3) }})::numeric, 3) as deflections_roll_3g_avg,
        round(({{ calculate_rolling_avg('deflections', 'team_id', 'game_date', 5) }})::numeric, 3) as deflections_roll_5g_avg,
        round(({{ calculate_rolling_avg('deflections', 'team_id', 'game_date', 10) }})::numeric, 3) as deflections_roll_10g_avg,
        round(({{ calculate_rolling_stddev('deflections', 'team_id', 'game_date', 3) }})::numeric, 3) as deflections_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('deflections', 'team_id', 'game_date', 5) }})::numeric, 3) as deflections_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('deflections', 'team_id', 'game_date', 10) }})::numeric, 3) as deflections_roll_10g_stddev,

        round(({{ calculate_rolling_avg('charges_drawn', 'team_id', 'game_date', 3) }})::numeric, 3) as charges_drawn_roll_3g_avg,
        round(({{ calculate_rolling_avg('charges_drawn', 'team_id', 'game_date', 5) }})::numeric, 3) as charges_drawn_roll_5g_avg,
        round(({{ calculate_rolling_avg('charges_drawn', 'team_id', 'game_date', 10) }})::numeric, 3) as charges_drawn_roll_10g_avg,
        round(({{ calculate_rolling_stddev('charges_drawn', 'team_id', 'game_date', 3) }})::numeric, 3) as charges_drawn_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('charges_drawn', 'team_id', 'game_date', 5) }})::numeric, 3) as charges_drawn_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('charges_drawn', 'team_id', 'game_date', 10) }})::numeric, 3) as charges_drawn_roll_10g_stddev,

        round(({{ calculate_rolling_avg('screen_ast', 'team_id', 'game_date', 3) }})::numeric, 3) as screen_ast_roll_3g_avg,
        round(({{ calculate_rolling_avg('screen_ast', 'team_id', 'game_date', 5) }})::numeric, 3) as screen_ast_roll_5g_avg,
        round(({{ calculate_rolling_avg('screen_ast', 'team_id', 'game_date', 10) }})::numeric, 3) as screen_ast_roll_10g_avg,
        round(({{ calculate_rolling_stddev('screen_ast', 'team_id', 'game_date', 3) }})::numeric, 3) as screen_ast_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast', 'team_id', 'game_date', 5) }})::numeric, 3) as screen_ast_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast', 'team_id', 'game_date', 10) }})::numeric, 3) as screen_ast_roll_10g_stddev,

        round(({{ calculate_rolling_avg('screen_ast_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as screen_ast_pts_roll_3g_avg,
        round(({{ calculate_rolling_avg('screen_ast_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as screen_ast_pts_roll_5g_avg,
        round(({{ calculate_rolling_avg('screen_ast_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as screen_ast_pts_roll_10g_avg,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'team_id', 'game_date', 3) }})::numeric, 3) as screen_ast_pts_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'team_id', 'game_date', 5) }})::numeric, 3) as screen_ast_pts_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('screen_ast_pts', 'team_id', 'game_date', 10) }})::numeric, 3) as screen_ast_pts_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as off_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as off_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as off_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as off_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as off_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as off_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as def_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as def_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as def_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as def_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as def_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as def_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as tot_loose_balls_rec_roll_3g_avg,
        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as tot_loose_balls_rec_roll_5g_avg,
        round(({{ calculate_rolling_avg('tot_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as tot_loose_balls_rec_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'team_id', 'game_date', 3) }})::numeric, 3) as tot_loose_balls_rec_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'team_id', 'game_date', 5) }})::numeric, 3) as tot_loose_balls_rec_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tot_loose_balls_rec', 'team_id', 'game_date', 10) }})::numeric, 3) as tot_loose_balls_rec_roll_10g_stddev,

        round(({{ calculate_rolling_avg('off_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as off_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('off_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as off_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('off_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as off_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('off_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as off_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('off_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as off_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('off_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as off_box_outs_roll_10g_stddev,

        round(({{ calculate_rolling_avg('def_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as def_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('def_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as def_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('def_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as def_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('def_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as def_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('def_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as def_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('def_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as def_box_outs_roll_10g_stddev,

        round(({{ calculate_rolling_avg('box_out_team_reb', 'team_id', 'game_date', 3) }})::numeric, 3) as box_out_team_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('box_out_team_reb', 'team_id', 'game_date', 5) }})::numeric, 3) as box_out_team_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('box_out_team_reb', 'team_id', 'game_date', 10) }})::numeric, 3) as box_out_team_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'team_id', 'game_date', 3) }})::numeric, 3) as box_out_team_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'team_id', 'game_date', 5) }})::numeric, 3) as box_out_team_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('box_out_team_reb', 'team_id', 'game_date', 10) }})::numeric, 3) as box_out_team_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('box_out_player_reb', 'team_id', 'game_date', 3) }})::numeric, 3) as box_out_player_reb_roll_3g_avg,
        round(({{ calculate_rolling_avg('box_out_player_reb', 'team_id', 'game_date', 5) }})::numeric, 3) as box_out_player_reb_roll_5g_avg,
        round(({{ calculate_rolling_avg('box_out_player_reb', 'team_id', 'game_date', 10) }})::numeric, 3) as box_out_player_reb_roll_10g_avg,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'team_id', 'game_date', 3) }})::numeric, 3) as box_out_player_reb_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'team_id', 'game_date', 5) }})::numeric, 3) as box_out_player_reb_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('box_out_player_reb', 'team_id', 'game_date', 10) }})::numeric, 3) as box_out_player_reb_roll_10g_stddev,

        round(({{ calculate_rolling_avg('tot_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as tot_box_outs_roll_3g_avg,
        round(({{ calculate_rolling_avg('tot_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as tot_box_outs_roll_5g_avg,
        round(({{ calculate_rolling_avg('tot_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as tot_box_outs_roll_10g_avg,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'team_id', 'game_date', 3) }})::numeric, 3) as tot_box_outs_roll_3g_stddev,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'team_id', 'game_date', 5) }})::numeric, 3) as tot_box_outs_roll_5g_stddev,
        round(({{ calculate_rolling_stddev('tot_box_outs', 'team_id', 'game_date', 10) }})::numeric, 3) as tot_box_outs_roll_10g_stddev,

        hustle_boxscore.updated_at -- Select updated_at from the source CTE

    from hustle_boxscore -- Select from the first CTE
)

select * -- Select all columns from the final CTE
from final
