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
        player_slug,
        game_date,
        game_id,
        opponent_id

    from {{ ref('int__player_traditional_bxsc') }}
),

teams as (
    select 
        team_id,
        team_tricode,
        team_full_name
        
    from {{ ref('stg__teams') }}
),

final as (
    select
        players.player_id,
        players.player_slug,
        players.game_date,
        players.game_id,
        players.opponent_id,
        props.prop_id,
        props.offer_id,
        props.bp_event_id,
        props.player_name,
        teams.team_tricode,
        teams.team_full_name,
        teams.team_id,
        props.market_id,
        props.market,
        props.line,
        props.sportsbook,
        props.over_odds,
        props.under_odds,
        props.source_file,
        {{ dbt_utils.generate_surrogate_key(['players.player_id', 'props.market_id', 'props.game_date', 'props.line']) }} as prop_key,
        -- Implied probabilities
        case 
            when over_odds > 0 then 100 / (over_odds + 100)
            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
            when over_odds = -100 then 0.5  -- Special case for -100 odds (even)
            else null
        end as over_implied_prob,
        
        case 
            when under_odds > 0 then 100 / (under_odds + 100)
            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
            when under_odds = -100 then 0.5  -- Special case for -100 odds (even)
            else null
        end as under_implied_prob,
        
        -- Total implied probability (to measure the vig)
        case 
            when over_odds is not null and under_odds is not null then
                (case 
                    when over_odds > 0 then 100 / (over_odds + 100)
                    when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                    when over_odds = -100 then 0.5
                    else 0
                end) +
                (case 
                    when under_odds > 0 then 100 / (under_odds + 100)
                    when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                    when under_odds = -100 then 0.5
                    else 0
                end)
            else null
        end as total_implied_prob,
        
        -- No-vig fair probabilities
        case 
            when over_odds is not null and under_odds is not null then
                case
                    when (case 
                            when over_odds > 0 then 100 / (over_odds + 100)
                            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                            when over_odds = -100 then 0.5
                            else 0
                        end +
                        case 
                            when under_odds > 0 then 100 / (under_odds + 100)
                            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                            when under_odds = -100 then 0.5
                            else 0
                        end) = 0 then null
                    else
                        (case 
                            when over_odds > 0 then 100 / (over_odds + 100)
                            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                            when over_odds = -100 then 0.5
                            else 0
                        end) / 
                        (case 
                            when over_odds > 0 then 100 / (over_odds + 100)
                            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                            when over_odds = -100 then 0.5
                            else 0
                        end +
                        case 
                            when under_odds > 0 then 100 / (under_odds + 100)
                            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                            when under_odds = -100 then 0.5
                            else 0
                        end)
                end
            else null
        end as over_no_vig_prob,
        
        case 
            when over_odds is not null and under_odds is not null then
                case
                    when (case 
                            when over_odds > 0 then 100 / (over_odds + 100)
                            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                            when over_odds = -100 then 0.5
                            else 0
                        end +
                        case 
                            when under_odds > 0 then 100 / (under_odds + 100)
                            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                            when under_odds = -100 then 0.5
                            else 0
                        end) = 0 then null
                    else
                        (case 
                            when under_odds > 0 then 100 / (under_odds + 100)
                            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                            when under_odds = -100 then 0.5
                            else 0
                        end) / 
                        (case 
                            when over_odds > 0 then 100 / (over_odds + 100)
                            when over_odds < 0 and over_odds != -100 then abs(over_odds) / (abs(over_odds) + 100)
                            when over_odds = -100 then 0.5
                            else 0
                        end +
                        case 
                            when under_odds > 0 then 100 / (under_odds + 100)
                            when under_odds < 0 and under_odds != -100 then abs(under_odds) / (abs(under_odds) + 100)
                            when under_odds = -100 then 0.5
                            else 0
                        end)
                end
            else null
        end as under_no_vig_prob,
        
        -- Bookmaker's hold percentage (vig)
        case 
            when over_odds is not null and under_odds is not null then
                ((case 
                    when over_odds > 0 then 100 / (over_odds + 100)
                    when over_odds < 0 then abs(over_odds) / (abs(over_odds) + 100)
                    else 0
                end) +
                (case 
                    when under_odds > 0 then 100 / (under_odds + 100)
                    when under_odds < 0 then abs(under_odds) / (abs(under_odds) + 100)
                    else 0
                end)) - 1.0
            else null
        end as hold_percentage
    from props
    left join players
        on props.player_slug = players.player_slug
        and props.game_date = players.game_date
    left join teams
        on props.team_tricode = teams.team_tricode
)

select * from final 