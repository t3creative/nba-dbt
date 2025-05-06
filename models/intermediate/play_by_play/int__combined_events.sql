{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='game_id_event_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "game_date",
            "data_type": "date",
            "granularity": "month"
        },
        indexes=[
            {'columns': ['game_id_event_key']},
            {'columns': ['game_id', 'event_number']},
            {'columns': ['game_id', 'period', 'game_clock_time']}
        ]
    )
}}

with v2_events as (
    select * from {{ ref('stg__playbyplay_v2_events') }}
    {% if is_incremental() %}
    where game_id::text > (select max(game_id) from {{ this }})
    {% endif %}
),

v3_events as (
    select * from {{ ref('stg__playbyplay_v3_events') }}
    {% if is_incremental() %}
    where game_id::text > (select max(game_id) from {{ this }})
    {% endif %}
),

game_dates as (
    select distinct
        game_id,
        game_date
    from {{ ref('int__game_opponents') }}
),

final as (
    select 
        -- Event identifiers
        concat(v2.game_id, '_', v2.event_num)::text as game_id_event_key,
        v2.game_id::text as game_id,
        v2.event_num::integer as event_number,
        v2.period::integer as period,
        'v2'::text as source_version,
        gd.game_date,
        
        -- Event details
        v2.event_type::text as event_type,
        v2.event_action_type::text as event_action_type,
        v2.event_type_name::text as action_type,
        v2.wall_clock_time::text as wall_clock_time,
        v2.game_clock_time::text as game_clock_time,
        
        -- Event descriptions
        v2.home_description::text as home_description,
        v2.visitor_description::text as visitor_description,
        v2.neutral_description::text as neutral_description,
        null::text as description,
        
        -- Score information
        v2.score::text as score,
        case 
            when v2.score_margin = 'TIE' then 0
            else v2.score_margin::integer
        end as score_margin,
        null::integer as home_score,
        null::integer as away_score,
        
        -- Player 1 information
        v2.player1_id::integer as player1_id,
        v2.player1_name::text as player1_name,
        v2.player1_team_id::integer as player1_team_id,
        v2.player1_team_city::text as player1_team_city,
        v2.player1_team_nickname::text as player1_team_nickname,
        v2.player1_team_tricode::text as player1_team_tricode,
        
        -- Player 2 information
        v2.player2_id::integer as player2_id,
        v2.player2_name::text as player2_name,
        v2.player2_team_id::integer as player2_team_id,
        v2.player2_team_city::text as player2_team_city,
        v2.player2_team_nickname::text as player2_team_nickname,
        v2.player2_team_tricode::text as player2_team_tricode,
        
        -- Player 3 information
        v2.player3_id::integer as player3_id,
        v2.player3_name::text as player3_name,
        v2.player3_team_id::integer as player3_team_id,
        v2.player3_team_city::text as player3_team_city,
        v2.player3_team_nickname::text as player3_team_nickname,
        v2.player3_team_tricode::text as player3_team_tricode,
        
        -- Shot details (null for v2)
        null::text as sub_type,
        null::text as shot_result,
        null::integer as points,
        null::integer as shot_value,
        null::text as shot_zone,
        null::integer as shot_distance,
        null::integer as shot_x,
        null::integer as shot_y,
        
        -- Additional context
        null::text as location
    from v2_events v2
    left join game_dates gd on v2.game_id = gd.game_id

    union all

    select 
        -- Event identifiers
        concat(v3.game_id, '_', v3.event_number)::text as game_id_event_key,
        v3.game_id::text as game_id,
        v3.event_number::integer as event_number,
        v3.period::integer as period,
        'v3'::text as source_version,
        gd.game_date,
        
        -- Event details
        null::text as event_type,
        null::text as event_action_type,
        v3.action_type::text as action_type,
        null::text as wall_clock_time,
        null::text as game_clock_time,
        
        -- Event descriptions
        null::text as home_description,
        null::text as visitor_description,
        null::text as neutral_description,
        v3.description::text as description,
        
        -- Score information
        null::text as score,
        null::integer as score_margin,
        v3.home_score::integer as home_score,
        v3.away_score::integer as away_score,
        
        -- Player information (null for v3)
        null::integer as player1_id,
        null::text as player1_name,
        null::integer as player1_team_id,
        null::text as player1_team_city,
        null::text as player1_team_nickname,
        null::text as player1_team_tricode,
        
        null::integer as player2_id,
        null::text as player2_name,
        null::integer as player2_team_id,
        null::text as player2_team_city,
        null::text as player2_team_nickname,
        null::text as player2_team_tricode,
        
        null::integer as player3_id,
        null::text as player3_name,
        null::integer as player3_team_id,
        null::text as player3_team_city,
        null::text as player3_team_nickname,
        null::text as player3_team_tricode,
        
        -- Shot details
        v3.sub_type::text as sub_type,
        v3.shot_result::text as shot_result,
        v3.points::integer as points,
        v3.shot_value::integer as shot_value,
        v3.shot_zone::text as shot_zone,
        v3.shot_distance::integer as shot_distance,
        v3.shot_x::integer as shot_x,
        v3.shot_y::integer as shot_y,
        
        -- Additional context
        v3.location::text as location
    from v3_events v3
    left join game_dates gd on v3.game_id = gd.game_id
)

select * from final 