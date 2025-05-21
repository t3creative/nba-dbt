{{
    config(
        schema='features',
        materialized='incremental',
        unique_key='player_matchup_key',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['player_matchup_key'], 'unique': true},
            {'columns': ['game_id']},
            {'columns': ['off_player_id']},
            {'columns': ['def_player_id']},
            {'columns': ['season_year']}
        ]
    )
}}

/*
Player Matchup Features - Calculates player-vs-player effectiveness metrics.

Features:
- matchup_points_allowed_per_100_poss: Points the defender allowed per 100 possessions guarding the offensive player.
- defensive_field_goal_diff: Difference between the offensive player's season avg FG% and their FG% when guarded by this defender.
- offensive_matchup_advantage: Difference between the offensive player's points per 100 possessions in this matchup vs. their season average.
*/

with matchups as (
    select
        player_matchup_key,
        game_id,
        off_player_id,
        def_player_id,
        season_year,
        game_date,
        off_player_name,
        def_player_name,
        team_tricode,
        home_away,
        opponent,
        partial_poss,
        off_player_pts,
        off_matchup_fgm,
        off_matchup_fga
    from {{ ref('int__player_bxsc_matchups') }}
    {% if is_incremental() %}
    -- Process games newer than the most recent game date in the table
    where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

player_game_stats as (
    -- Combine traditional stats (pts, fgm, fga) and advanced stats (possessions) per player per game
    select
        trad.player_id,
        trad.season_year,
        trad.pts,
        trad.fgm,
        trad.fga,
        adv.possessions
    from {{ ref('int__player_bxsc_traditional') }} as trad
    left join {{ ref('stg__player_advanced_bxsc') }} as adv
        on trad.player_game_key = adv.player_game_key -- Assumes player_game_key uniquely identifies player-game instance across boxscore types
    where adv.possessions is not null and trad.fga is not null -- Ensure necessary stats are available
),

player_season_avg_stats as (
    -- Calculate season averages for offensive players
    select
        player_id,
        season_year,
        {{ dbt_utils.safe_divide('sum(pts)', 'sum(fga)') }} as avg_fg_pct,
        {{ dbt_utils.safe_divide('sum(pts) * 100', 'sum(possessions)') }} as avg_pts_per_100_poss
    from player_game_stats
    group by 1, 2
),

final as (
    select
        -- Identifiers
        m.player_matchup_key,
        m.game_id,
        m.off_player_id,
        m.def_player_id,
        m.season_year,
        m.game_date,
        m.off_player_name,
        m.def_player_name,
        m.team_tricode,
        m.home_away,
        m.opponent,

        -- Matchup Raw Stats (for context)
        m.partial_poss,
        m.off_player_pts,
        m.off_matchup_fgm,
        m.off_matchup_fga,

        -- Calculated Matchup Features
        coalesce({{ dbt_utils.safe_divide('m.off_player_pts * 100', 'm.partial_poss') }}, 0) as matchup_points_allowed_per_100_poss,

        coalesce(
            avgs.avg_fg_pct - {{ dbt_utils.safe_divide('m.off_matchup_fgm', 'm.off_matchup_fga') }},
            0 -- Default to 0 if calculation fails (e.g., no avg data or zero attempts in matchup)
        ) as defensive_field_goal_diff,

        coalesce(
            {{ dbt_utils.safe_divide('m.off_player_pts * 100', 'm.partial_poss') }} - avgs.avg_pts_per_100_poss,
            0 -- Default to 0 if calculation fails
        ) as offensive_matchup_advantage

    from matchups m
    left join player_season_avg_stats avgs
        on m.off_player_id = avgs.player_id
        and m.season_year = avgs.season_year
    where m.season_year >= '2017-18'
)

select * from final
