{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_matchup_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_matchup_key']},
        ]
    )
}}

-- int_player_matchups_bxsc.sql
with box_scores as (
    select * from {{ ref('stg__player_matchups_bxsc') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct gl.game_id
        from {{ ref('stg__game_logs_league') }} gl
        where gl.game_date > (select max(game_date) from {{ this }})
    )
    {% endif %}
),

league_game_logs as (
    select distinct
        game_id::varchar as game_id,
        game_date,
        season_id,
        team_id,
        matchup,
        team_tricode
    from {{ ref('stg__game_logs_league') }}
),

player_game_logs as (
    select distinct on (game_id)
        game_id::varchar as game_id,
        season_year
    from {{ ref('stg__game_logs_player') }}
    order by game_id, season_year
),

parsed_matchups as (
    select distinct on (game_id, team_id)
        game_id::varchar as game_id,
        team_id,
        team_tricode,
        case 
            when matchup like '% vs. %' then 'HOME'
            when matchup like '% @ %' then 'AWAY'
        end as home_away,
        case 
            when matchup like '% vs. %' then split_part(matchup, ' vs. ', 2)
            when matchup like '% @ %' then split_part(matchup, ' @ ', 2)
        end as opponent
    from {{ ref('stg__game_logs_league') }}
    order by game_id, team_id, matchup
)

select distinct on (bs.player_matchup_key)
    -- Identity and Context
    bs.player_matchup_key,
    pgl.season_year,
    bs.off_first_name,
    bs.off_family_name,
    concat(bs.off_first_name, ' ', bs.off_family_name) as off_player_name,
    bs.def_first_name,
    bs.def_family_name,
    concat(bs.def_first_name, ' ', bs.def_family_name) as def_player_name,
    pm.team_tricode,
    gl.game_date,
    pm.home_away,
    pm.opponent,
    
    -- Matchup Time Stats
    bs.matchup_min,
    bs.matchup_min_sort,
    bs.partial_poss,
    bs.def_time_pct,
    bs.off_time_pct,
    bs.both_on_time_pct,
    bs.switches_on,
    
    -- Offensive Production
    bs.off_player_pts,
    bs.off_team_pts,
    bs.off_matchup_ast,
    bs.off_matchup_pot_ast,
    bs.off_matchup_tov,
    
    -- Offensive Shooting
    bs.off_matchup_fgm,
    bs.off_matchup_fga,
    bs.off_matchup_fg_pct,
    bs.off_matchup_fg3m,
    bs.off_matchup_fg3a,
    bs.off_matchup_fg3_pct,
    bs.off_matchup_ftm,
    bs.off_matchup_fta,
    
    -- Defensive Stats
    bs.def_matchup_blk,
    bs.def_help_blk,
    bs.def_help_fgm,
    bs.def_help_fga,
    bs.def_help_fg_pct,
    bs.def_shooting_fouls,
    
    -- IDs and Metadata
    bs.game_id,
    bs.off_player_id::integer as off_player_id,
    bs.def_player_id::integer as def_player_id,
    bs.team_id,
    gl.season_id,
    bs.created_at,
    bs.updated_at
from box_scores bs
left join league_game_logs gl on bs.game_id = gl.game_id
left join parsed_matchups pm on bs.game_id = pm.game_id and bs.team_id = pm.team_id
left join player_game_logs pgl on gl.game_id = pgl.game_id
order by bs.player_matchup_key, gl.game_date desc