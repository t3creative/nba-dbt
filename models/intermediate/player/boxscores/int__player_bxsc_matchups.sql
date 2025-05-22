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
           COALESCE(
            pgl.season_year,
            CASE
                -- Ensure game_id is in the expected format '002YYNNNNN'
                WHEN bs.game_id IS NOT NULL AND length(bs.game_id) = 10 AND substring(bs.game_id, 1, 3) = '002' THEN
                    (
                        CASE
                            -- Heuristic for 2-digit year: YY >= 70 implies 19YY (e.g., 98 -> 1998)
                            -- Otherwise, implies 20YY (e.g., 01 -> 2001, 12 -> 2012)
                            -- This covers seasons like 1970-71 through 2069-70.
                            WHEN substring(bs.game_id, 4, 2)::integer >= 70 AND substring(bs.game_id, 4, 2)::integer <= 99
                            THEN '19' || substring(bs.game_id, 4, 2)
                            ELSE '20' || substring(bs.game_id, 4, 2)
                        END
                    ) || '-' || lpad(((substring(bs.game_id, 4, 2)::integer + 1) % 100)::text, 2, '0')
                ELSE NULL -- Fallback if game_id is not in the expected '002YYNNNNN' format
            END
        ) as season_year,
    bs.off_first_name,
    bs.off_family_name,
    concat(bs.off_first_name, ' ', bs.off_family_name) as off_player_name,
    bs.def_first_name,
    bs.def_family_name,
    bs.team_tricode,
    concat(bs.def_first_name, ' ', bs.def_family_name) as def_player_name,
        gl.game_date, -- This will be NULL for older games if gopp doesn't have them
        CASE -- game_sort_key: YYYY_S_NNNNN (e.g., 1998_2_00412)
            WHEN bs.game_id IS NOT NULL AND length(bs.game_id) = 10 AND substring(bs.game_id, 1, 2) = '00' THEN
                (CASE
                    WHEN substring(bs.game_id, 4, 2)::integer >= 70 AND substring(bs.game_id, 4, 2)::integer <= 99
                    THEN '19' || substring(bs.game_id, 4, 2)
                    ELSE '20' || substring(bs.game_id, 4, 2)
                END) || '_' || substring(bs.game_id, 3, 1) || '_' || substring(bs.game_id, 6, 5)
            ELSE bs.game_id -- Fallback to game_id itself if it doesn't match pattern
        END as game_sort_key,
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