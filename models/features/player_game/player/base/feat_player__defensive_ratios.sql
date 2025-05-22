{{ config(
    materialized='incremental',
    unique_key='player_game_key',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['features', 'player', 'ratios', 'defensive'],
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

with player_defensive_base_data as (
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
        possessions, -- Player's team possessions while on court

        -- Defensive Stats from int_player__combined_boxscore
        matchup_min,       -- Total minutes spent in defensive matchups
        partial_poss,      -- Number of possessions player defended the ball handler
        def_switches,
        pts_allowed,       -- Points scored by players guarded by this player
        ast_allowed,       -- Assists by players guarded by this player
        tov_forced,        -- Turnovers forced on players guarded by this player
        matchup_fgm,       -- Field goals made by players guarded
        matchup_fga,       -- Field goals attempted by players guarded
        matchup_fg_pct,    -- FG% of players guarded
        matchup_fg3m,      -- 3PT field goals made by players guarded
        matchup_fg3a,      -- 3PT field goals attempted by players guarded
        matchup_fg3_pct,   -- 3PT FG% of players guarded

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

        -- Existing Defensive Ratios
        matchup_fg_pct,
        matchup_fg3_pct,

        -- Def Switches Ratios
        case when min > 0 then round((def_switches / min)::numeric, 3) else 0 end as def_switches_per_min,
        case when possessions > 0 and min > 0 then round(((def_switches / possessions) * 100)::numeric, 3) else 0 end as def_switches_per_100_poss,
        case when partial_poss > 0 then round((def_switches / partial_poss)::numeric, 3) else 0 end as def_switches_per_partial_poss, -- Per possession defended ball handler

        -- Matchup-Based Ratios (per matchup minute)
        case when matchup_min > 0 then round((pts_allowed / matchup_min)::numeric, 3) else 0 end as pts_allowed_per_matchup_min,
        case when matchup_min > 0 then round((ast_allowed / matchup_min)::numeric, 3) else 0 end as ast_allowed_per_matchup_min,
        case when matchup_min > 0 then round((tov_forced / matchup_min)::numeric, 3) else 0 end as tov_forced_per_matchup_min,
        case when matchup_min > 0 then round((matchup_fga / matchup_min)::numeric, 3) else 0 end as matchup_fga_per_matchup_min, -- Rate of shots defended per matchup minute
        case when matchup_min > 0 then round((matchup_fgm / matchup_min)::numeric, 3) else 0 end as matchup_fgm_per_matchup_min,
        case when matchup_min > 0 then round((matchup_fg3a / matchup_min)::numeric, 3) else 0 end as matchup_fg3a_per_matchup_min, -- Rate of 3pt shots defended per matchup minute
        case when matchup_min > 0 then round((matchup_fg3m / matchup_min)::numeric, 3) else 0 end as matchup_fg3m_per_matchup_min,
        
        updated_at
    from player_defensive_base_data
)

select *
from final
