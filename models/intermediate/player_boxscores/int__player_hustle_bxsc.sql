{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_game_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        indexes=[
            {'columns': ['player_game_key']},
            {'columns': ['game_id', 'player_id']},
            {'columns': ['game_id', 'team_id', 'opponent_id']}
        ]
    )
}}

with box_scores as (
    select * from {{ ref('stg__player_hustle_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id 
        from {{ ref('int__game_opponents') }} 
        where game_date > (select max(game_date) from {{ this }}) 
    )
    {% endif %}
),

game_opponents as (
    select 
        game_id,
        team_id,
        opponent_id,
        game_date, 
        season_year,
        home_away
    from {{ ref('int__game_opponents') }}
),

team_tricodes as (
    select distinct
        team_id,
        team_tricode
    from {{ ref('stg__game_logs_league') }}
),

final as (
    select distinct on (bs.player_game_key)
        -- Identity and Context
        bs.player_game_key,
        gopp.season_year,
        bs.first_name,
        bs.family_name,
        concat(bs.first_name, ' ', bs.family_name) as player_name,
        tt.team_tricode,
        gopp.game_date,
        gopp.home_away,
        gopp.opponent_id,
        
        -- Hustle Stats - Contested Shots
        coalesce(bs.cont_shots, 0) as cont_shots,
        coalesce(bs.cont_2pt, 0) as cont_2pt,
        coalesce(bs.cont_3pt, 0) as cont_3pt,
        
        -- Hustle Stats - Active Plays
        coalesce(bs.deflections, 0) as deflections,
        coalesce(bs.charges_drawn, 0) as charges_drawn,
        coalesce(bs.screen_ast, 0) as screen_ast,
        coalesce(bs.screen_ast_pts, 0) as screen_ast_pts,
        
        -- Hustle Stats - Loose Balls
        coalesce(bs.off_loose_balls_rec, 0) as off_loose_balls_rec,
        coalesce(bs.def_loose_balls_rec, 0) as def_loose_balls_rec,
        coalesce(bs.tot_loose_balls_rec, 0) as tot_loose_balls_rec,
        
        -- Hustle Stats - Box Outs
        coalesce(bs.off_box_outs, 0) as off_box_outs,
        coalesce(bs.def_box_outs, 0) as def_box_outs,
        coalesce(bs.box_out_team_reb, 0) as box_out_team_reb,
        coalesce(bs.box_out_player_reb, 0) as box_out_player_reb,
        coalesce(bs.tot_box_outs, 0) as tot_box_outs,
        
        -- IDs and Metadata
        bs.game_id,
        bs.player_id,
        bs.team_id,
        bs.created_at,
        bs.updated_at
    from box_scores bs
    left join game_opponents gopp on bs.game_id = gopp.game_id and bs.team_id = gopp.team_id
    left join team_tricodes tt on bs.team_id = tt.team_id
    order by bs.player_game_key, gopp.game_date desc
)

select * from final