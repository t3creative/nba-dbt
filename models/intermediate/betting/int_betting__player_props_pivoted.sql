{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='player_prop_key',
        on_schema_change='sync_all_columns',
        tags=['betting', 'intermediate', 'pivot']
    )
}}

{%- set consensus_slug_var = 'consensus' -%} {# IMPORTANT: Ensure this slug exists in your dim_sportsbooks output #}

{%- set distinct_sportsbook_slugs = ['placeholder_slug_for_parsing'] -%} {# Default for parsing #}
{%- if execute -%}
    {%- set sportsbook_slugs_query %}
        select distinct sportsbook_slug
        from {{ ref('dim__sportsbooks') }}
        order by 1
    {%- endset %}
    {%- set results = run_query(sportsbook_slugs_query) -%}
    {%- if results and results.columns[0].values() | length > 0 -%}
        {%- set distinct_sportsbook_slugs = results.columns[0].values() | list -%}
    {%- endif -%} {# If query returns no rows, distinct_sportsbook_slugs remains ['placeholder_slug_for_parsing'] #}
{%- endif -%}

with decimal_odds_data as (
    select * from {{ ref('int_betting__decimal_odds_conversion') }} -- Model with American odds converted to decimal
    
    {% if is_incremental() %}
        where game_date > (select coalesce(max(game_date), '1900-01-01') from {{ this }})
    {% endif %}
),

sportsbook_dim as (
    select
        sportsbook_code, -- Original sportsbook name, matches 'sportsbook' in decimal_odds_data
        sportsbook_slug  -- Cleaned slug for column names
    from {{ ref('dim__sportsbooks') }}
),

data_to_pivot as (
    select
        -- Key columns for joining and grouping
        {{ dbt_utils.generate_surrogate_key(['dod.player_slug', 'dod.game_date', 'dod.market_id', 'dod.line']) }} as player_prop_key,
        dod.player_slug,
        dod.player_id,
        dod.game_date,
        dod.market_id,
        dod.line,
        
        -- Other descriptive columns you want to carry through
        dod.player_name, 
        dod.market,      
        
        -- The column whose values will become new column names
        sb.sportsbook_slug,
        
        -- The values to be aggregated into the new columns
        dod.over_odds_decimal,
        dod.under_odds_decimal

    from decimal_odds_data dod
    inner join sportsbook_dim sb on dod.sportsbook = sb.sportsbook_code
),

pivoted_data as (
    select
        -- Grouping columns (user-defined order)
        game_date,
        player_name,
        market,
        line,
        player_prop_key,
        player_slug,
        player_id,
        market_id,

        -- Pivot for over_odds_decimal
        {% if distinct_sportsbook_slugs | length > 0 %} 
        {{ dbt_utils.pivot(
            column='sportsbook_slug',
            values=distinct_sportsbook_slugs,
            agg='max', 
            then_value='over_odds_decimal',
            prefix='over_odds_',
            suffix='_decimal',
            quote_identifiers=false
        ) }},
        {% endif %}

        -- Pivot for under_odds_decimal
        {% if distinct_sportsbook_slugs | length > 0 %}
        {{ dbt_utils.pivot(
            column='sportsbook_slug',
            values=distinct_sportsbook_slugs,
            agg='max',
            then_value='under_odds_decimal',
            prefix='under_odds_',
            suffix='_decimal',
            quote_identifiers=false
        ) }}
        {% endif %}

    from data_to_pivot
    group by
        player_prop_key,
        player_slug,
        player_id,
        game_date,
        market_id,
        line,
        player_name,
        market
),

final_selection as (
    select
        pd.game_date,
        pd.player_name,
        pd.market,
        pd.line,

        pd.{{ 'over_odds_' ~ consensus_slug_var ~ '_decimal' }} as over_odds_decimal,
        pd.{{ 'under_odds_' ~ consensus_slug_var ~ '_decimal' }} as under_odds_decimal,

        pd.player_prop_key,
        pd.player_id,
        pd.player_slug,
        pd.market_id

        {# Loop to add other sportsbook columns, carefully handling commas #}
        {%- for slug in distinct_sportsbook_slugs %}
            {%- if slug != consensus_slug_var and slug != 'placeholder_slug_for_parsing' %}
        , pd.{{ 'over_odds_' ~ slug ~ '_decimal' }} as {{ 'over_odds_' ~ slug ~ '_decimal' }}
        , pd.{{ 'under_odds_' ~ slug ~ '_decimal' }} as {{ 'under_odds_' ~ slug ~ '_decimal' }}
            {%- endif %}
        {%- endfor %}

        {# Handle placeholder if it's distinct and not consensus, and wasn't picked up above #}
        {# This logic is to ensure its columns appear if it was the only "other" slug #}
        {%- if 'placeholder_slug_for_parsing' in distinct_sportsbook_slugs and
               'placeholder_slug_for_parsing' != consensus_slug_var and
               not (distinct_sportsbook_slugs | reject('equalto', consensus_slug_var) | reject('equalto', 'placeholder_slug_for_parsing') | list | length > 0) %}
                 {# This condition means placeholder is the only non-consensus slug #}
        , pd.over_odds_placeholder_slug_for_parsing_decimal as over_odds_placeholder_slug_for_parsing_decimal
        , pd.under_odds_placeholder_slug_for_parsing_decimal as under_odds_placeholder_slug_for_parsing_decimal
        {%- endif %}

    from pivoted_data pd
)

select * from final_selection
