{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='prop_id',
        tags=['betting', 'intermediate', 'player_props'],
        indexes=[
            {'columns': ['player_id']},
            {'columns': ['team_id']},
            {'columns': ['game_date']}
        ]
    )
}}

with props as (
    select * from {{ ref('stg__player_props') }}
    
    {% if is_incremental() %}
        where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

players as (
    select 
        player_id,
        player_first_name,
        player_last_name,
        player_first_name || ' ' || player_last_name as player_name,
        player_slug
    from {{ ref('stg__player_index') }}
),

teams as (
    select 
        team_id,
        team_tricode as team_abbreviation,
        team_full_name
    from {{ ref('stg__teams') }}
),

final as (
    select
        props.prop_id,
        props.offer_id,
        props.event_id,
        props.player as player_name_raw,
        coalesce(players.player_id, -1) as player_id,
        coalesce(players.player_name, props.player) as player_name,
        props.player_slug,
        props.team as team_abbr_raw,
        coalesce(teams.team_id, -1) as team_id,
        coalesce(teams.team_abbreviation, props.team) as team_tricode,
        props.market_id,
        props.market,
        props.line,
        props.sportsbook,
        props.over_odds,
        props.under_odds,
        props.over_implied_probability,
        props.under_implied_probability,
        props.game_date,
        props.source_file,
        {{ dbt_utils.generate_surrogate_key(['player_id', 'market_id', 'game_date', 'line']) }} as prop_key
    from props
    left join players
        on props.player_slug = players.player_slug
    left join teams
        on props.team = teams.team_abbreviation
)

select * from final 