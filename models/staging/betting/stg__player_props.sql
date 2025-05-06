{{
    config(
        schema='staging',
        materialized='view',
    )
}}

with source as (
    select * from {{ source('betting_pros', 'player_props') }}
),

renamed as (
    select
    -- Generate a unique identifier for each prop line
        {{ dbt_utils.generate_surrogate_key(['player_slug', 'game_date', 'market_id', 'line', 'sportsbook']) }} as prop_id,
        -- Betting Pros IDs
        offer_id,
        event_id as bp_event_id,
        -- Player Info
        player as player_name,
        player_slug,
        -- Team Info
        team as team_tricode,
        -- Market Info
        sportsbook,
        market_id,
        market,
        -- Odds
        line,
        over_odds,
        under_odds,
        -- Date and Source
        game_date,
        source_file

    from source
),

final as (
    select
        prop_id,
        offer_id,
        bp_event_id,
        player_name,
        player_slug,
        team_tricode,
        sportsbook,
        market_id,
        market,
        line,
        over_odds,
        under_odds,
        game_date,
        source_file
    from renamed
)

select * from final 