{{
  config(
    materialized='incremental',
    unique_key='game_id',
    tags=['game_context', 'intermediate']
  )
}}

with game_logs as (
    select * from {{ ref('stg__game_logs_team') }}
),

parsed_matchups as (
    select
        game_id,
        season_year,
        game_date,
        team_id,
        team_tricode,
        matchup,
        -- Extract both teams from matchup
        split_part(
            replace(
                replace(matchup, ' vs. ', '|'),
                ' @ ', '|'
            ),
            '|', 1
        ) as first_team,
        split_part(
            replace(
                replace(matchup, ' vs. ', '|'),
                ' @ ', '|'
            ),
            '|', 2
        ) as second_team,
        -- Determine if home or away by checking format
        case
            when position(' vs. ' in matchup) > 0 then split_part(matchup, ' vs. ', 1)
            else split_part(matchup, ' @ ', 2)
        end as home_team_tricode,
        case
            when position(' vs. ' in matchup) > 0 then split_part(matchup, ' vs. ', 2)
            else split_part(matchup, ' @ ', 1)
        end as away_team_tricode
    from game_logs
),

team_mapping as (
    select distinct
        team_id,
        team_tricode
    from game_logs
),

final as (
    select 
        pm.game_id,
        pm.season_year,
        pm.game_date,
        max(case when tm_home.team_tricode = pm.home_team_tricode then tm_home.team_id end) as home_team_id,
        max(pm.home_team_tricode) as home_team_tricode,
        max(case when tm_away.team_tricode = pm.away_team_tricode then tm_away.team_id end) as away_team_id,
        max(pm.away_team_tricode) as away_team_tricode,
        -- Add metadata
        current_timestamp as created_at
    from parsed_matchups pm
    left join team_mapping tm_home
        on tm_home.team_tricode = pm.home_team_tricode
    left join team_mapping tm_away
        on tm_away.team_tricode = pm.away_team_tricode
    group by pm.game_id, pm.season_year, pm.game_date
)

select * from final
