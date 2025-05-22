{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'hustle'],
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

with player_hustle_base_data as (
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

        -- Base counts for normalization
        min,
        possessions,

        -- Hustle Stats from int_player__combined_boxscore
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
        updated_at

    from {{ ref('int_player__combined_boxscore') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
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

        -- Per Minute Ratios
        case when min > 0 then round((cont_shots / min)::numeric, 3) else 0 end as cont_shots_per_min,
        case when min > 0 then round((cont_2pt / min)::numeric, 3) else 0 end as cont_2pt_per_min,
        case when min > 0 then round((cont_3pt / min)::numeric, 3) else 0 end as cont_3pt_per_min,
        case when min > 0 then round((deflections / min)::numeric, 3) else 0 end as deflections_per_min,
        case when min > 0 then round((charges_drawn / min)::numeric, 3) else 0 end as charges_drawn_per_min,
        case when min > 0 then round((screen_ast / min)::numeric, 3) else 0 end as screen_ast_per_min,
        case when min > 0 then round((screen_ast_pts / min)::numeric, 3) else 0 end as screen_ast_pts_per_min,
        case when min > 0 then round((off_loose_balls_rec / min)::numeric, 3) else 0 end as off_loose_balls_rec_per_min,
        case when min > 0 then round((def_loose_balls_rec / min)::numeric, 3) else 0 end as def_loose_balls_rec_per_min,
        case when min > 0 then round((tot_loose_balls_rec / min)::numeric, 3) else 0 end as tot_loose_balls_rec_per_min,
        case when min > 0 then round((off_box_outs / min)::numeric, 3) else 0 end as off_box_outs_per_min,
        case when min > 0 then round((def_box_outs / min)::numeric, 3) else 0 end as def_box_outs_per_min,
        case when min > 0 then round((box_out_team_reb / min)::numeric, 3) else 0 end as box_out_team_reb_per_min,
        case when min > 0 then round((box_out_player_reb / min)::numeric, 3) else 0 end as box_out_player_reb_per_min,
        case when min > 0 then round((tot_box_outs / min)::numeric, 3) else 0 end as tot_box_outs_per_min,

        -- Per 100 Possessions Ratios
        case when possessions > 0 and min > 0 then round(((cont_shots / possessions) * 100)::numeric, 3) else 0 end as cont_shots_per_100_poss,
        case when possessions > 0 and min > 0 then round(((cont_2pt / possessions) * 100)::numeric, 3) else 0 end as cont_2pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((cont_3pt / possessions) * 100)::numeric, 3) else 0 end as cont_3pt_per_100_poss,
        case when possessions > 0 and min > 0 then round(((deflections / possessions) * 100)::numeric, 3) else 0 end as deflections_per_100_poss,
        case when possessions > 0 and min > 0 then round(((charges_drawn / possessions) * 100)::numeric, 3) else 0 end as charges_drawn_per_100_poss,
        case when possessions > 0 and min > 0 then round(((screen_ast / possessions) * 100)::numeric, 3) else 0 end as screen_ast_per_100_poss,
        case when possessions > 0 and min > 0 then round(((screen_ast_pts / possessions) * 100)::numeric, 3) else 0 end as screen_ast_pts_per_100_poss,
        case when possessions > 0 and min > 0 then round(((off_loose_balls_rec / possessions) * 100)::numeric, 3) else 0 end as off_loose_balls_rec_per_100_poss,
        case when possessions > 0 and min > 0 then round(((def_loose_balls_rec / possessions) * 100)::numeric, 3) else 0 end as def_loose_balls_rec_per_100_poss,
        case when possessions > 0 and min > 0 then round(((tot_loose_balls_rec / possessions) * 100)::numeric, 3) else 0 end as tot_loose_balls_rec_per_100_poss,
        case when possessions > 0 and min > 0 then round(((off_box_outs / possessions) * 100)::numeric, 3) else 0 end as off_box_outs_per_100_poss,
        case when possessions > 0 and min > 0 then round(((def_box_outs / possessions) * 100)::numeric, 3) else 0 end as def_box_outs_per_100_poss,
        case when possessions > 0 and min > 0 then round(((box_out_team_reb / possessions) * 100)::numeric, 3) else 0 end as box_out_team_reb_per_100_poss,
        case when possessions > 0 and min > 0 then round(((box_out_player_reb / possessions) * 100)::numeric, 3) else 0 end as box_out_player_reb_per_100_poss,
        case when possessions > 0 and min > 0 then round(((tot_box_outs / possessions) * 100)::numeric, 3) else 0 end as tot_box_outs_per_100_poss,
        
        updated_at
    from player_hustle_base_data
)

select *
from final
