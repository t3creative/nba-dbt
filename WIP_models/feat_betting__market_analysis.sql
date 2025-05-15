-- depends_on: {{ ref('dim__sportsbooks') }}
{{
    config(
        schema='features',
        materialized='table',
        unique_key='player_prop_key'
    )
}}

{%- set consensus_slug_var = 'consensus' -%} 
{%- set placeholder_slug = 'placeholder_slug_for_parsing' -%}

{# Fetch distinct sportsbook slugs to iterate over relevant columns #}
{%- set distinct_sportsbook_slugs_for_run = [] -%} {# Initialize empty for actual run #}
{%- if execute -%}
    {%- set sportsbook_slugs_query %}
        select distinct sportsbook_slug
        from {{ ref('dim__sportsbooks') }}
        order by 1
    {%- endset %}
    {%- set results = run_query(sportsbook_slugs_query) -%}
    {%- if results and results.columns[0].values() | length > 0 -%}
        {%- set distinct_sportsbook_slugs_for_run = results.columns[0].values() | list -%}
    {%- endif -%} 
{%- endif -%}

{%- set iterate_slugs = distinct_sportsbook_slugs_for_run if execute and distinct_sportsbook_slugs_for_run | length > 0 else [placeholder_slug] -%}
{{ log("Iterating with slugs: " ~ iterate_slugs, info=True) }}

with source_probabilities as (
    select * from {{ ref('int_betting__player_props_probabilities') }}
    -- No incremental logic here as we usually want to analyze the current state of all props
),

market_calculations as (
    select
        -- Select core identifying columns
        player_prop_key,
        game_date,
        player_name,
        market,
        line,
        over_odds_decimal as consensus_over_odds_decimal, -- from int_betting__player_props_probabilities
        under_odds_decimal as consensus_under_odds_decimal, -- from int_betting__player_props_probabilities
        over_implied_prob as consensus_over_implied_prob,
        under_implied_prob as consensus_under_implied_prob,
        total_implied_prob as consensus_total_implied_prob,
        over_no_vig_prob as consensus_over_no_vig_prob,
        under_no_vig_prob as consensus_under_no_vig_prob,
        hold_percentage as consensus_hold_percentage,
        player_slug,
        market_id,

        -- Calculate Best Market Odds
        {%- set over_odds_cols_to_compare = [] -%}
        {%- for slug in iterate_slugs %}
            {%- if slug != placeholder_slug and slug != consensus_slug_var %} 
                {%- do over_odds_cols_to_compare.append('source_probabilities.over_odds_' ~ slug ~ '_decimal') -%}
            {%- endif %}
        {%- endfor %}

        {%- set under_odds_cols_to_compare = [] -%}
        {%- for slug in iterate_slugs %}
            {%- if slug != placeholder_slug and slug != consensus_slug_var %}
                {%- do under_odds_cols_to_compare.append('source_probabilities.under_odds_' ~ slug ~ '_decimal') -%}
            {%- endif %}
        {%- endfor %}

        greatest(
            {%- if over_odds_cols_to_compare | length > 0 -%}
                {{ over_odds_cols_to_compare | join(', ') }}
            {%- else -%}
                null {# Fallback if no valid columns found #}
            {%- endif -%}
        ) as market_best_over_odds,

        greatest(
            {%- if under_odds_cols_to_compare | length > 0 -%}
                {{ under_odds_cols_to_compare | join(', ') }}
            {%- else -%}
                null {# Fallback if no valid columns found #}
            {%- endif -%}
        ) as market_best_under_odds,
        
        -- Calculate No-Vig Probability Stats (Average and StdDev for Over prob across books)
        -- This requires unpivoting or more complex conditional aggregation.
        -- For simplicity in this step, let's focus on a count of books and average vig.
        -- True AVG/STDDEV of no-vig probs across books is harder with current wide format without unpivoting.

        -- Count of sportsbooks offering this prop
        (
            0 -- Start with 0
            {%- for slug in iterate_slugs %}
                {%- if slug != placeholder_slug and slug != consensus_slug_var %}
            + case when source_probabilities.{{ 'over_odds_' ~ slug ~ '_decimal' }} is not null then 1 else 0 end
                {%- endif %}
            {%- endfor %}
        ) as num_books_offering_prop,

        -- Average hold percentage across books (illustrative, requires care for NULLs)
        -- A true average would sum all holds and divide by count of books with holds.
        -- This is also better with unpivoted data.
        -- For now, let's take the consensus hold as a market indicator.
        hold_percentage as market_consensus_hold_percentage


        -- Columns for individual sportsbook odds and their calculated probabilities can be selected too if needed
        -- For example:
        -- {% for slug in distinct_sportsbook_slugs %}
        --    {%- if slug != consensus_slug_var and slug != placeholder_slug and ('over_odds_' ~ slug ~ '_decimal') in adapter.get_columns_in_relation(ref('int_betting__player_props_probabilities')) %}
        -- , {{ 'over_odds_' ~ slug ~ '_decimal' }}
        -- , {{ slug ~ '_over_implied_prob' }} 
        --    {% endif %}
        -- {% endfor %}

    from source_probabilities
)

select * from market_calculations
