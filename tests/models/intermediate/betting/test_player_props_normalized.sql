-- Test to ensure player_id and team_id are properly assigned
with source_data as (
    select
        player_name_raw,
        player_id,
        player_name,
        team_tricode_raw,
        team_id,
        team_tricode_standardized,
        -- Count number of distinct prop lines per player
        count(*) as prop_count,
        -- Track match rate
        max(case when player_id::text like 'p_%' then 0 else 1 end) as has_real_player_id,
        max(case when team_id = -1 then 0 else 1 end) as has_real_team_id
    from {{ ref('int_betting__player_props_probabilities') }}
    group by
        player_name_raw,
        player_id,
        player_name,
        team_tricode_raw,
        team_id,
        team_tricode_standardized
),

validation as (
    select
        player_name_raw,
        player_id,
        player_name,
        team_tricode_raw,
        team_id,
        team_tricode_standardized,
        prop_count,
        has_real_player_id,
        has_real_team_id,
        -- Check that player_id is not null
        player_id is not null as has_player_id,
        -- Check that team_id is not null and valid (-1 is allowed as fallback)
        team_id is not null as has_team_id,
        -- Check that standardized names are not null
        player_name is not null as has_player_name,
        team_tricode_standardized is not null as has_team_abbr
    from source_data
)

-- Return rows failing validation or the top 10 most frequently appearing players without matches
select * from validation
where not (has_player_id and has_team_id and has_player_name and has_team_abbr)
   or (has_real_player_id = 0 and prop_count > 10)
   or (has_real_team_id = 0 and prop_count > 10)
order by prop_count desc
limit 20 