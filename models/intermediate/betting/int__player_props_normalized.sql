{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='prop_key',
        on_schema_change='sync_all_columns',
        tags=['betting', 'intermediate', 'player_props'],
        indexes=[
            {'columns': ['prop_key'], 'unique': True},
            {'columns': ['player_id']},
            {'columns': ['game_id']},
            {'columns': ['game_date']}
        ]
    )
}}

with props_source as (
    select * from {{ ref('stg__player_props') }}
    
    {% if is_incremental() %}
        where game_date > (select coalesce(max(game_date), '2017-01-01') from {{ this }})
    {% endif %}
),

player_mapping as (
    select distinct
        player_id,
        player_slug,
        game_date,
        game_id,
        opponent_id
    from {{ ref('int__player_traditional_bxsc') }}
    where game_date >= (select coalesce(min(game_date), '2017-01-01') from props_source)
    {% if is_incremental() %}
        and game_date > (select coalesce(max(game_date), '2017-01-01') from {{ this }})
    {% endif %}
),

teams as (
    select 
        team_id,
        team_tricode,
        team_full_name
    from {{ ref('stg__teams') }}
),

prepared_data as (
    select
        pm.player_id,
        ps.player_slug,
        ps.player_name,
        ps.game_date,
        pm.game_id,
        pm.opponent_id,
        t.team_id,
        ps.team_tricode,
        t.team_full_name,
        ps.market_id as source_market_id,
        ps.offer_id,
        ps.market,
        case 
            when ps.market like 'Points%' then 'PTS'
            when ps.market like 'Rebounds%' then 'REB'
            when ps.market like 'Assists%' then 'AST'
            when ps.market like 'Blocks%' then 'BLK'
            when ps.market like 'Steals%' then 'STL'
            when ps.market like 'Threes%' then 'FG3M'
            when ps.market like 'Pts+Ast%' then 'PTS+AST'
            when ps.market like 'Pts+Reb%' then 'PTS+REB'
            when ps.market like 'Reb+Ast%' then 'REB+AST'
            when ps.market like 'Pts+Ast+Reb%' then 'PTS+AST+REB'
            when ps.market like 'Double-Double%' then 'DBL_DBL'
            when ps.market like 'Triple-Double%' then 'TRP_DBL'
            when ps.market like 'First Basket%' then 'FIRST_BASKET'
            else regexp_replace(ps.market, '\s+O/U$', '')
        end as market_cleaned,
        case
            when ps.market like 'Pts+Ast%' then true
            when ps.market like 'Pts+Reb%' then true
            when ps.market like 'Reb+Ast%' then true
            when ps.market like 'Pts+Ast+Reb%' then true
            else false
        end as is_combined_stat,
        ps.line,
        ps.source_file,
        ps.sportsbook,
        ps.over_odds,
        ps.under_odds
    from props_source ps
    left join player_mapping pm
        on ps.player_slug = pm.player_slug and ps.game_date = pm.game_date
    left join teams t
        on ps.team_tricode = t.team_tricode
),

{% set sportsbooks_query %}
select distinct sportsbook from {{ ref('stg__player_props') }}
{% if is_incremental() %}
    where game_date > (select coalesce(max(game_date), '2017-01-01') from {{ this }})
{% endif %}
order by 1
{% endset %}

{% set results = run_query(sportsbooks_query) %}

{% if execute %}
{% set distinct_sportsbooks = results.columns[0].values() %}
{% else %}
{% set distinct_sportsbooks = [] %} 
{% endif %}

pivoted_data as (
    select
        -- Fields that define the unique prop and are part of the prop_key or descriptive & consistent
        player_id,
        game_id,
        market_cleaned,
        line,
        game_date,

        -- Descriptive fields (should be consistent for the group)
        player_slug,
        player_name,
        opponent_id,
        team_id,
        team_tricode,
        team_full_name,
        is_combined_stat,
        
        -- Key generation
        {{ dbt_utils.generate_surrogate_key(['player_id', 'game_id', 'market_cleaned', 'line', 'game_date']) }} as prop_key,

        -- Aggregated fields that might vary per source record but not per unique prop line
        max(source_market_id) as max_source_market_id,
        max(market) as max_market,
        max(offer_id) as max_source_offer_id, 
        max(source_file) as last_source_file

        {%- for s_book in distinct_sportsbooks %}
        , max(case when sportsbook = '{{ s_book | replace("'''", "''''''") }}' then over_odds else null end) as {{ (s_book | lower | replace(' ', '_') | replace('.', '_') | replace('/', '_') | replace('(', '') | replace(')', '') ~ '_over_odds') }}
        , max(case when sportsbook = '{{ s_book | replace("'''", "''''''") }}' then under_odds else null end) as {{ (s_book | lower | replace(' ', '_') | replace('.', '_') | replace('/', '_') | replace('(', '') | replace(')', '') ~ '_under_odds') }}
        {%- endfor %}
        
    from prepared_data
    where player_id is not null and game_id is not null -- Ensure key components are not null
    group by
        -- Group strictly by the components of the prop_key and their consistent descriptive fields
        player_id,
        game_id,
        market_cleaned,
        line,
        game_date,
        player_slug, -- Assuming consistent for player_id
        player_name, -- Assuming consistent for player_id
        opponent_id, -- Assuming consistent for game_id
        team_id,     -- Assuming consistent for player_id in game_id context
        team_tricode,
        team_full_name,
        is_combined_stat -- Should be consistent if market_normalized is
        -- Removed source_market_id, market_original, source_file from group by
),

final_props_with_probabilities as (
    select
        pd.*

        {%- for s_book in distinct_sportsbooks %}
        {%- set s_book_slug = (s_book | lower | replace(' ', '_') | replace('.', '_') | replace('/', '_') | replace('(', '') | replace(')', '')) %}
        {%- set over_odds_col = s_book_slug ~ '_over_odds' %}
        {%- set under_odds_col = s_book_slug ~ '_under_odds' %}

        , case 
            when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
            when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
            when pd.{{ over_odds_col }} = -100 then 0.5
            else null
        end as {{ s_book_slug ~ '_over_implied_prob' }}
        
        , case 
            when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
            when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
            when pd.{{ under_odds_col }} = -100 then 0.5
            else null
        end as {{ s_book_slug ~ '_under_implied_prob' }}
        
        , (
            (case 
                when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                when pd.{{ over_odds_col }} = -100 then 0.5
                else 0.0 
            end) +
            (case 
                when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                when pd.{{ under_odds_col }} = -100 then 0.5
                else 0.0 
            end)
        ) as {{ s_book_slug ~ '_total_implied_prob' }}

        , case 
            when 
                ((case 
                    when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                    when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                    when pd.{{ over_odds_col }} = -100 then 0.5 else 0.0 end) + 
                (case 
                    when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                    when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                    when pd.{{ under_odds_col }} = -100 then 0.5 else 0.0 end)) > 0
            then
                (case 
                    when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                    when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                    when pd.{{ over_odds_col }} = -100 then 0.5 else 0.0 end)
                /
                ((case 
                    when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                    when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                    when pd.{{ over_odds_col }} = -100 then 0.5 else 0.0 end) + 
                (case 
                    when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                    when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                    when pd.{{ under_odds_col }} = -100 then 0.5 else 0.0 end))
            else null 
        end as {{ s_book_slug ~ '_over_no_vig_prob' }}

        , case 
            when 
                ((case 
                    when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                    when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                    when pd.{{ over_odds_col }} = -100 then 0.5 else 0.0 end) + 
                (case 
                    when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                    when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                    when pd.{{ under_odds_col }} = -100 then 0.5 else 0.0 end)) > 0
            then
                (case 
                    when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                    when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                    when pd.{{ under_odds_col }} = -100 then 0.5 else 0.0 end)
                /
                ((case 
                    when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                    when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                    when pd.{{ over_odds_col }} = -100 then 0.5 else 0.0 end) + 
                (case 
                    when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                    when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                    when pd.{{ under_odds_col }} = -100 then 0.5 else 0.0 end))
            else null
        end as {{ s_book_slug ~ '_under_no_vig_prob' }}

        , (
            (case 
                when pd.{{ over_odds_col }} > 0 then 100.0 / (pd.{{ over_odds_col }} + 100.0)
                when pd.{{ over_odds_col }} < 0 and pd.{{ over_odds_col }} != -100 then abs(pd.{{ over_odds_col }}) / (abs(pd.{{ over_odds_col }}) + 100.0)
                when pd.{{ over_odds_col }} = -100 then 0.5
                else 0.0
            end) +
            (case 
                when pd.{{ under_odds_col }} > 0 then 100.0 / (pd.{{ under_odds_col }} + 100.0)
                when pd.{{ under_odds_col }} < 0 and pd.{{ under_odds_col }} != -100 then abs(pd.{{ under_odds_col }}) / (abs(pd.{{ under_odds_col }}) + 100.0)
                when pd.{{ under_odds_col }} = -100 then 0.5
                else 0.0
            end)
        ) - 1.0 as {{ s_book_slug ~ '_hold_percentage' }}
        {%- endfor %}
        
    from pivoted_data pd
)

select * from final_props_with_probabilities 