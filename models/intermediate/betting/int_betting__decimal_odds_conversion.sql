{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='prop_id'
    )
}}

-- int__betting_pros__player_props_decimal_odds.sql

with stg_player_props as (
    select * from {{ ref('stg__player_props') }}
),

dim_players as (
    select
        player_id,
        player_slug
    from {{ ref('dim__players') }} -- Assuming 'dim_players' is your player dimension model
    -- Add a distinct if player_slug is not unique in dim_players, or use a more robust source
),

transformed as (
    select
        spp.prop_id,
        spp.offer_id,
        spp.bp_event_id,
        spp.player_name,
        dp.player_id,
        spp.player_slug,
        spp.team_tricode,
        spp.sportsbook,
        spp.market_id,
        spp.market,
        spp.line,
        round(({{ american_to_decimal_odds('spp.over_odds') }})::numeric, 2) as over_odds_decimal,
        round(({{ american_to_decimal_odds('spp.under_odds') }})::numeric, 2) as under_odds_decimal,
        spp.game_date,
        spp.source_file
    from stg_player_props spp
    left join dim_players dp on spp.player_slug = dp.player_slug -- Joining on player_slug
)

select * from transformed